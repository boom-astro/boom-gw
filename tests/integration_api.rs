//! In-process integration test for the boom-gw HTTP API.
//!
//! Marked `#[ignore]` so a plain `cargo test` skips it; the GitHub
//! Actions `integration-kafka` job spins up a `mongo:8.0` services
//! container and runs this test with `cargo test -- --ignored`.
//!
//! The test wires the [`boom_gw::api`] router into an
//! `actix_web::test::init_service` app — no real socket is opened —
//! seeds the archive with one event, one superevent (with FITS), and
//! one localize request/result, then issues HTTP requests against the
//! in-process service and asserts on the response envelopes.

use actix_web::{test, web, App};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use boom_gw::storage::skymap::{build_storage, SkymapBackendKind, SkymapBlob, SkymapStorage};
use boom_gw::{
    api, api::MaybeAlertPublisher, Archive, ArchiveConfig, LocalizeRequest, LocalizeResult,
    LocalizeStatus, SkyMapFits, Superevent,
};
use igwn_ligolw::CoincInspiralEvent;
use serde_json::Value;

fn mongo_uri() -> String {
    std::env::var("BOOM_GW_MONGO_URI").unwrap_or_else(|_| "mongodb://localhost:27017".into())
}

fn dummy_event(graceid: &str, snr: f64, pipeline: &str) -> boom_gw::GwEvent {
    let coinc = CoincInspiralEvent {
        coinc_event_id: graceid.into(),
        ifos: "H1,L1".into(),
        combined_far: 1e-9,
        snr,
        mass: None,
        mchirp: None,
        end_time: 1_400_000_000.0,
        sngls: vec![],
    };
    boom_gw::GwEvent {
        pipeline: pipeline.into(),
        graceid: graceid.into(),
        producer_timestamp: 1_700_000_000.0,
        message_type: "new".into(),
        submitter: "ci".into(),
        end_time: 1_400_000_000.0,
        ifos: "H1,L1".into(),
        snr,
        far: 1e-9,
        mchirp: None,
        total_mass: None,
        coinc,
    }
}

async fn build_archive() -> (Archive, String) {
    let uri = mongo_uri();
    let pid = std::process::id();
    let mut cfg = ArchiveConfig::new(&uri);
    cfg.database = format!("boom_gw_api_test_{pid}");
    let client = mongodb::Client::with_uri_str(&uri).await.expect("mongo");
    client
        .database(&cfg.database)
        .drop()
        .await
        .expect("drop test db");
    let archive = Archive::connect(cfg.clone())
        .await
        .expect("archive connect");
    (archive, cfg.database)
}

async fn drop_database(database: &str) {
    let client = mongodb::Client::with_uri_str(&mongo_uri())
        .await
        .expect("mongo");
    client.database(database).drop().await.expect("drop db");
}

#[actix_web::test]
#[ignore]
async fn api_round_trip_against_mongo() {
    let (archive, db_name) = build_archive().await;

    // Seed two events on different pipelines so we can test the
    // ?pipeline= filter.
    let ev1 = dummy_event("G_api_1", 10.0, "gstlal");
    let ev2 = dummy_event("G_api_2", 8.5, "mbta");
    archive.record_event(&ev1).await.unwrap();
    archive.record_event(&ev2).await.unwrap();

    // One superevent with a FITS attached.
    let superevent_id = "S_api_001".to_string();
    let fits_bytes = b"FITS-API-PAYLOAD".to_vec();
    let s = Superevent {
        id: superevent_id.clone(),
        t_0: 1_400_000_000.0,
        t_start: 1_399_999_997.5,
        t_end: 1_400_000_002.5,
        preferred_event: ev1.clone(),
        g_events: vec![ev1.clone(), ev2.clone()],
        skymap: Some(SkyMapFits {
            bytes: fits_bytes.clone(),
            elapsed_ms: 137,
        }),
    };
    archive.upsert_superevent(&s).await.unwrap();

    // Seed the FITS bytes into the SkymapStorage (mongo backend)
    // — bytes no longer live inline on the SupereventDoc.
    let skymap_storage = std::sync::Arc::new(
        build_storage(SkymapBackendKind::Mongo, archive.database(), None)
            .await
            .expect("build mongo skymap storage"),
    );
    skymap_storage
        .upsert(SkymapBlob {
            superevent_id: superevent_id.clone(),
            bytes: fits_bytes.clone(),
            elapsed_ms: 137,
        })
        .await
        .expect("upsert skymap blob");

    // One localize request + result tied to that superevent.
    let req = LocalizeRequest::from_coinc_xml(
        "req-api-1",
        &superevent_id,
        "G_api_1",
        "gstlal",
        b"<?xml version='1.0'?><LIGO_LW></LIGO_LW>",
    );
    archive.record_localize_request(&req).await.unwrap();
    let result = LocalizeResult {
        request_id: "req-api-1".into(),
        superevent_id: superevent_id.clone(),
        graceid: "G_api_1".into(),
        status: LocalizeStatus::Ok,
        skymap_fits: Some(BASE64.encode(&fits_bytes)),
        error_message: None,
        elapsed_ms: 137,
    };
    archive.record_localize_result(&result).await.unwrap();

    // Mount the boom-gw API on an in-process actix instance.
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(archive.clone()))
            .app_data(web::Data::new(MaybeAlertPublisher(None)))
            .app_data(web::Data::from(skymap_storage.clone()))
            .configure(api::configure),
    )
    .await;

    // GET /api/health
    let req = test::TestRequest::get().uri("/api/health").to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["message"], "success");
    assert_eq!(body["data"]["status"], "ok");

    // GET /api/events (no filter) — both events come back.
    let req = test::TestRequest::get().uri("/api/events").to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let events = body["data"].as_array().unwrap();
    assert_eq!(events.len(), 2);

    // GET /api/events?pipeline=mbta — only G_api_2.
    let req = test::TestRequest::get()
        .uri("/api/events?pipeline=mbta")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let events = body["data"].as_array().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["_id"], "G_api_2");

    // GET /api/events?limit=1 — regression for the `?limit=N`
    // numeric query-param bug. `serde_urlencoded` hands the value
    // to serde as a string, so without our `de_opt_from_str`
    // helper this would 400 with `invalid type: string, expected i64`.
    let req = test::TestRequest::get()
        .uri("/api/events?limit=1")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
    let body: Value = test::read_body_json(resp).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 1);

    // GET /api/events/{graceid}
    let req = test::TestRequest::get()
        .uri("/api/events/G_api_1")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["data"]["_id"], "G_api_1");
    assert_eq!(body["data"]["pipeline"], "gstlal");

    // GET /api/events/{unknown} → 404
    let req = test::TestRequest::get()
        .uri("/api/events/G_does_not_exist")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 404);

    // GET /api/superevents
    let req = test::TestRequest::get()
        .uri("/api/superevents")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let superevents = body["data"].as_array().unwrap();
    assert_eq!(superevents.len(), 1);
    assert_eq!(superevents[0]["_id"], "S_api_001");

    // GET /api/superevents?has_skymap=true — should still match.
    let req = test::TestRequest::get()
        .uri("/api/superevents?has_skymap=true")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 1);

    // GET /api/superevents?has_skymap=false — should match nothing.
    let req = test::TestRequest::get()
        .uri("/api/superevents?has_skymap=false")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 0);

    // GET /api/superevents/{id}
    let req = test::TestRequest::get()
        .uri("/api/superevents/S_api_001")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["data"]["_id"], "S_api_001");
    assert_eq!(body["data"]["preferred_graceid"], "G_api_1");

    // GET /api/superevents/{id}/skymap — raw FITS bytes, application/fits.
    let req = test::TestRequest::get()
        .uri("/api/superevents/S_api_001/skymap")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(ct.contains("application/fits"), "got content-type {ct}");
    let body = test::read_body(resp).await;
    assert_eq!(body.as_ref(), fits_bytes.as_slice());

    // GET /api/superevents/{unknown}/skymap → 404
    let req = test::TestRequest::get()
        .uri("/api/superevents/S_unknown/skymap")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 404);

    // GET /api/localize-requests?superevent_id=...
    let req = test::TestRequest::get()
        .uri(&format!(
            "/api/localize-requests?superevent_id={superevent_id}"
        ))
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let items = body["data"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["_id"], "req-api-1");

    // GET /api/localize-results?superevent_id=...
    let req = test::TestRequest::get()
        .uri(&format!(
            "/api/localize-results?superevent_id={superevent_id}"
        ))
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let items = body["data"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["_id"], "req-api-1");
    assert_eq!(items[0]["status"], "ok");

    // Annotations: empty list initially.
    let req = test::TestRequest::get()
        .uri(&format!("/api/superevents/{superevent_id}/annotations"))
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 0);

    // POST a p_astro annotation.
    let req = test::TestRequest::post()
        .uri(&format!("/api/superevents/{superevent_id}/annotations"))
        .set_json(serde_json::json!({
            "kind": "p_astro",
            "payload": {"bns": 0.05, "nsbh": 0.02, "bbh": 0.93, "terrestrial": 0.0},
            "author": "ml-classifier"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 201);
    let body: Value = test::read_body_json(resp).await;
    let annotation_id = body["data"]["_id"].as_str().unwrap().to_string();
    assert!(!annotation_id.is_empty());
    assert_eq!(body["data"]["kind"], "p_astro");
    assert_eq!(body["data"]["author"], "ml-classifier");
    assert!((body["data"]["payload"]["bbh"].as_f64().unwrap() - 0.93).abs() < 1e-9);

    // POST a second annotation (manual note); default author when omitted is "system".
    let req = test::TestRequest::post()
        .uri(&format!("/api/superevents/{superevent_id}/annotations"))
        .set_json(serde_json::json!({
            "kind": "manual_note",
            "payload": "looks like a glitch in L1"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 201);
    let body: Value = test::read_body_json(resp).await;
    assert_eq!(body["data"]["author"], "system");

    // GET — both annotations come back, newest first.
    let req = test::TestRequest::get()
        .uri(&format!("/api/superevents/{superevent_id}/annotations"))
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let items = body["data"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    // Newest first: the manual_note we posted second should be first.
    assert_eq!(items[0]["kind"], "manual_note");
    assert_eq!(items[1]["kind"], "p_astro");

    // POST against an unknown superevent → 404, not silent-create.
    let req = test::TestRequest::post()
        .uri("/api/superevents/S_does_not_exist/annotations")
        .set_json(serde_json::json!({"kind": "x", "payload": null}))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 404);

    // GET on an unknown superevent → 404.
    let req = test::TestRequest::get()
        .uri("/api/superevents/S_does_not_exist/annotations")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 404);

    // Alerts: empty initially.
    let req = test::TestRequest::get()
        .uri(&format!("/api/superevents/{superevent_id}/alerts"))
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 0);

    // POST a PRELIMINARY alert in dry_run mode — no Kafka publisher
    // is configured on this app, so dry_run is required.
    let req = test::TestRequest::post()
        .uri(&format!("/api/superevents/{superevent_id}/alerts"))
        .set_json(serde_json::json!({
            "alert_type": "PRELIMINARY",
            "dry_run": true,
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 201);
    let body: Value = test::read_body_json(resp).await;
    assert_eq!(body["data"]["audit"]["published"], false);
    assert_eq!(body["data"]["alert"]["alert_type"], "PRELIMINARY");
    assert_eq!(body["data"]["alert"]["superevent_id"], superevent_id);
    // The alert should carry both the FITS we attached earlier and
    // the p_astro annotation we posted earlier.
    assert!(body["data"]["alert"]["event"]["skymap"].is_string());
    assert!(body["data"]["alert"]["event"]["classification"].is_object());

    // POST without dry_run → 503 because no publisher is configured.
    let req = test::TestRequest::post()
        .uri(&format!("/api/superevents/{superevent_id}/alerts"))
        .set_json(serde_json::json!({"alert_type": "INITIAL"}))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 503);

    // The audit row from the successful dry-run should now be listed.
    let req = test::TestRequest::get()
        .uri(&format!("/api/superevents/{superevent_id}/alerts"))
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let items = body["data"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["alert_type"], "PRELIMINARY");
    assert_eq!(items[0]["published"], false);

    drop_database(&db_name).await;
}
