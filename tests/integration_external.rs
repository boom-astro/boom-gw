//! Integration coverage for the FRB + neutrino ingest path. Pins
//! down the REST routes (`/api/frb-alerts`, `/api/neutrino-alerts`)
//! and the scan-cross-match path's iteration over the new
//! collections.
//!
//! Marked `#[ignore]` so a plain `cargo test` skips it; the CI
//! integration job runs it via `cargo test -- --ignored`
//! alongside the other Mongo-dependent suites.

use actix_web::{test, web, App};
use boom_gw::archive::{FrbAlertDoc, NeutrinoAlertDoc};
use boom_gw::frb::{parse_frb_alert, CHIME_INSTRUMENT_LABEL, DSA110_INSTRUMENT_LABEL};
use boom_gw::neutrino::{parse_icecube_single_neutrino_alert, parse_km3net_alert};
use boom_gw::storage::skymap::{build_storage, SkymapBackendKind};
use boom_gw::{
    api, api::MaybeAlertPublisher, stub_principal_middleware, Archive, ArchiveConfig,
};
use serde_json::Value;

fn mongo_uri() -> String {
    std::env::var("BOOM_GW_MONGO_URI").unwrap_or_else(|_| "mongodb://localhost:27017".into())
}

async fn build_archive() -> (Archive, String) {
    let uri = mongo_uri();
    let suffix = uuid::Uuid::new_v4().simple();
    let mut cfg = ArchiveConfig::new(&uri);
    cfg.database = format!("boom_gw_external_test_{suffix}");
    let client = mongodb::Client::with_uri_str(&uri).await.expect("mongo");
    client.database(&cfg.database).drop().await.expect("drop");
    let archive = Archive::connect(cfg.clone()).await.expect("connect");
    (archive, cfg.database)
}

async fn drop_database(db: &str) {
    let client = mongodb::Client::with_uri_str(&mongo_uri()).await.unwrap();
    client.database(db).drop().await.unwrap();
}

#[actix_web::test]
#[ignore]
async fn frb_and_neutrino_routes_round_trip() {
    let (archive, db_name) = build_archive().await;

    // Seed one FRB from each parser (CHIME + DSA110) and one
    // neutrino from each (IceCube + KM3NeT). The payloads are the
    // example fixtures pruned to the fields the parsers consume.
    let chime = parse_frb_alert(
        r#"{
            "id": "chime_42",
            "trigger_time": "2024-09-18T07:19:10Z",
            "ra": 10.0,
            "dec": 20.0,
            "ra_dec_error": [0.5, 0.6],
            "snr": 12.5,
            "dm": 279.4,
            "importance": 0.99
        }"#,
        CHIME_INSTRUMENT_LABEL,
    )
    .unwrap();
    archive
        .upsert_frb_alert(&FrbAlertDoc::from_alert(chime))
        .await
        .unwrap();

    let dsa = parse_frb_alert(
        r#"{
            "id": "dsa_99",
            "trigger_time": "2024-10-01T12:00:00Z",
            "ra": 200.0,
            "dec": -45.0,
            "ra_dec_error": [0.02, 0.03],
            "snr": 8.0,
            "dm": 320.0
        }"#,
        DSA110_INSTRUMENT_LABEL,
    )
    .unwrap();
    archive
        .upsert_frb_alert(&FrbAlertDoc::from_alert(dsa))
        .await
        .unwrap();

    let icecube = parse_icecube_single_neutrino_alert(
        r#"{
            "id": ["run_evt_7"],
            "event_name": ["IceCube-240901A"],
            "trigger_time": "2024-09-01T01:16:47Z",
            "ra": 30.0,
            "dec": 40.0,
            "ra_dec_error": 0.4,
            "alert_topology": "Track",
            "pipeline": "Gold Track Alert",
            "nu_energy": 250.0,
            "p_astro": 0.85
        }"#,
    )
    .unwrap();
    archive
        .upsert_neutrino_alert(&NeutrinoAlertDoc::from_alert(icecube))
        .await
        .unwrap();

    let km3 = parse_km3net_alert(
        r#"{
            "id": "km3_1",
            "event_name": "KM3-240901A",
            "trigger_time": "2024-09-01T01:16:47Z",
            "ra": 11.0,
            "dec": 21.0,
            "ra_dec_error": 0.9,
            "pipeline": "orca_HE",
            "p_value": 0.04
        }"#,
    )
    .unwrap();
    archive
        .upsert_neutrino_alert(&NeutrinoAlertDoc::from_alert(km3))
        .await
        .unwrap();

    // Re-upsert the same chime alert — exercises the natural-key
    // (instrument, trigger_id) upsert path and confirms the list
    // stays at 2 items rather than fanning out.
    let chime_again = parse_frb_alert(
        r#"{
            "id": "chime_42",
            "trigger_time": "2024-09-18T07:19:11Z",
            "ra": 10.0,
            "dec": 20.0,
            "snr": 13.0
        }"#,
        CHIME_INSTRUMENT_LABEL,
    )
    .unwrap();
    archive
        .upsert_frb_alert(&FrbAlertDoc::from_alert(chime_again))
        .await
        .unwrap();

    let skymap_storage = std::sync::Arc::new(
        build_storage(SkymapBackendKind::Mongo, archive.database(), None)
            .await
            .expect("build storage"),
    );
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(archive.clone()))
            .app_data(web::Data::new(MaybeAlertPublisher(None)))
            .app_data(web::Data::from(skymap_storage.clone()))
            .wrap(actix_web::middleware::from_fn(stub_principal_middleware))
            .configure(api::configure),
    )
    .await;

    // GET /api/frb-alerts — both FRBs, newest-by-ingest first.
    let req = test::TestRequest::get().uri("/api/frb-alerts").to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let items = body["data"].as_array().unwrap();
    assert_eq!(items.len(), 2, "expected 2 FRB alerts, got {items:?}");

    // Filter by instrument.
    let req = test::TestRequest::get()
        .uri("/api/frb-alerts?instrument=CHIME-FRB")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let items = body["data"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["instrument"], "CHIME-FRB");
    assert_eq!(items[0]["trigger_id"], "chime_42");

    // GET /api/frb-alerts/{instrument}/{trigger_id}
    let req = test::TestRequest::get()
        .uri("/api/frb-alerts/DSA110-FRB/dsa_99")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["data"]["trigger_id"], "dsa_99");
    assert_eq!(body["data"]["dm"], 320.0);

    // Unknown FRB → 404.
    let req = test::TestRequest::get()
        .uri("/api/frb-alerts/CHIME-FRB/does_not_exist")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 404);

    // GET /api/neutrino-alerts — both alerts.
    let req = test::TestRequest::get()
        .uri("/api/neutrino-alerts")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let items = body["data"].as_array().unwrap();
    assert_eq!(items.len(), 2);

    // Filter by instrument.
    let req = test::TestRequest::get()
        .uri("/api/neutrino-alerts?instrument=IceCube")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let items = body["data"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["pipeline"], "Gold Track Alert");
    // significance is sourced from p_astro for IceCube.
    let sig = items[0]["significance"].as_f64().unwrap();
    assert!((sig - 0.85).abs() < 1e-9);

    // GET /api/neutrino-alerts/{instrument}/{trigger_id}
    let req = test::TestRequest::get()
        .uri("/api/neutrino-alerts/KM3NeT/km3_1")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["data"]["pipeline"], "orca_HE");
    let sig = body["data"]["significance"].as_f64().unwrap();
    assert!((sig - 0.04).abs() < 1e-9);

    // Unknown neutrino → 404.
    let req = test::TestRequest::get()
        .uri("/api/neutrino-alerts/IceCube/does_not_exist")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 404);

    // GPS time-window filter shape. The neutrino fixtures are both
    // on 2024-09-01 (GPS ~1.409e9); a window covering all of 2024
    // catches both; an explicitly-empty window catches none.
    let req = test::TestRequest::get()
        .uri("/api/neutrino-alerts?since=1.4e9&until=1.5e9")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let items = body["data"].as_array().unwrap();
    assert_eq!(items.len(), 2, "broad window should include both alerts");

    let req = test::TestRequest::get()
        .uri("/api/neutrino-alerts?since=1.0&until=2.0")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let items = body["data"].as_array().unwrap();
    assert_eq!(items.len(), 0, "absurdly-early window should match nothing");

    drop_database(&db_name).await;
}

#[actix_web::test]
#[ignore]
async fn frb_neutrino_lvk_post_routes_round_trip() {
    let (archive, db_name) = build_archive().await;

    // Seed one superevent up front so the LVK POST has a valid
    // path id to attach to. No skymap — the LVK POST doesn't
    // need one (it's a coincidence-search result, not a scan).
    let event = boom_gw::GwEvent {
        pipeline: "gstlal".into(),
        graceid: "G_lvk_seed".into(),
        producer_timestamp: 1_700_000_000.0,
        message_type: "new".into(),
        submitter: "ci".into(),
        end_time: 1_400_000_000.0,
        ifos: "H1,L1".into(),
        snr: 12.0,
        far: 1e-10,
        mchirp: None,
        total_mass: None,
        coinc: igwn_ligolw::CoincInspiralEvent {
            coinc_event_id: "G_lvk_seed".into(),
            ifos: "H1,L1".into(),
            combined_far: 1e-10,
            snr: 12.0,
            mass: None,
            mchirp: None,
            end_time: 1_400_000_000.0,
            sngls: vec![],
        },
    };
    archive.record_event(&event).await.unwrap();
    let s = boom_gw::Superevent {
        id: "S_lvk_test".into(),
        t_0: 1_400_000_000.0,
        t_start: 1_399_999_997.5,
        t_end: 1_400_000_002.5,
        preferred_event: event.clone(),
        g_events: vec![event.clone()],
        skymap: None,
    };
    archive.upsert_superevent(&s).await.unwrap();

    let skymap_storage = std::sync::Arc::new(
        build_storage(SkymapBackendKind::Mongo, archive.database(), None)
            .await
            .expect("build storage"),
    );
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(archive.clone()))
            .app_data(web::Data::new(MaybeAlertPublisher(None)))
            .app_data(web::Data::from(skymap_storage.clone()))
            .wrap(actix_web::middleware::from_fn(stub_principal_middleware))
            .configure(api::configure),
    )
    .await;

    // ---- POST /api/boom-alerts ----
    let boom_body = serde_json::json!({
        "alert_id": "2026-01-15T00:00:00Z__POST_TEST",
        "alert_time": 1_400_000_100.0,
        "event_name": "POST_TEST",
        "ra": 150.0,
        "dec": 10.0,
        "error_radius_deg": 0.001,
        "classification": "kilonova",
        "classification_score": 0.8,
        "cross_match_summary": null,
        "photometry": [],
        "first_detection_time": 1_400_000_120.0,
        "last_non_detection_time": 1_399_999_900.0,
        "body": {}
    });
    let req = test::TestRequest::post()
        .uri("/api/boom-alerts")
        .set_json(&boom_body)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 201, "boom POST should be 201 Created");
    // Re-POST → 200 OK (upsert).
    let req = test::TestRequest::post()
        .uri("/api/boom-alerts")
        .set_json(&boom_body)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200, "boom re-POST should be 200 Ok");

    // ---- POST /api/frb-alerts ----
    let frb_body = serde_json::json!({
        "trigger_id": "post_frb_1",
        "instrument": "CHIME-FRB",
        "trigger_time": 1_400_000_200.0,
        "position": {"ra": 50.0, "dec": -10.0, "uncertainty_arcsec": 36.0},
        "significance": 10.0,
        "skymap_url": null,
        "error_radius_deg": 0.01,
        "dm": 412.0,
        "dm_error": 0.5,
        "importance": 0.95,
        "snr": 10.0,
        "known_source": null,
        "body": {}
    });
    let req = test::TestRequest::post()
        .uri("/api/frb-alerts")
        .set_json(&frb_body)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 201);
    let body: Value = test::read_body_json(resp).await;
    assert_eq!(body["data"]["instrument"], "CHIME-FRB");
    assert_eq!(body["data"]["dm"], 412.0);

    // ---- POST /api/neutrino-alerts ----
    let nu_body = serde_json::json!({
        "trigger_id": "post_nu_1",
        "instrument": "IceCube",
        "trigger_time": 1_400_000_050.0,
        "position": {"ra": 30.0, "dec": 40.0, "uncertainty_arcsec": 1800.0},
        "significance": 0.85,
        "skymap_url": null,
        "error_radius_deg": 0.5,
        "alert_topology": "Track",
        "pipeline": "Gold Track Alert",
        "nu_energy": 250.0,
        "p_astro": 0.85,
        "p_value": null,
        "far": 8.029e-8,
        "healpix_url": null,
        "event_name": "IceCube-POST-A",
        "body": {}
    });
    let req = test::TestRequest::post()
        .uri("/api/neutrino-alerts")
        .set_json(&nu_body)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 201);
    let body: Value = test::read_body_json(resp).await;
    assert_eq!(body["data"]["pipeline"], "Gold Track Alert");

    // ---- POST /api/superevents/{id}/icecube-lvk-searches ----
    let lvk_body = serde_json::json!({
        "superevent_id": "S_lvk_test",
        "alert_time": 1_400_000_300.0,
        "trigger_time": 1_400_000_000.0,
        "observation_start": 1_399_999_500.0,
        "observation_stop": 1_400_000_500.0,
        "observation_livetime": 1000.0,
        "pval_generic": 0.02,
        "pval_bayesian": 0.05,
        "n_events_coincident": 1,
        "coincident_events": [{
            "id": "138590_post",
            "event_dt": 5.0,
            "localization": {"ra": 17.5, "dec": 16.2, "uncertainty_arcsec": 1800.0},
            "event_pval_generic": 0.02,
            "event_pval_bayesian": null
        }],
        "most_probable_direction": {"ra": 17.5, "dec": 16.2, "uncertainty_arcsec": 1800.0},
        "flux_sensitivity_range": [0.03, 0.6],
        "sensitive_energy_range": [500.0, 23_000_000.0],
        "body": {}
    });
    let req = test::TestRequest::post()
        .uri("/api/superevents/S_lvk_test/icecube-lvk-searches")
        .set_json(&lvk_body)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 201);

    // Mismatched superevent_id between body and URL → 400.
    let req = test::TestRequest::post()
        .uri("/api/superevents/S_wrong/icecube-lvk-searches")
        .set_json(&lvk_body)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 400);

    // GET back to verify the POST landed.
    let req = test::TestRequest::get()
        .uri("/api/superevents/S_lvk_test/icecube-lvk-searches")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let items = body["data"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["n_events_coincident"], 1);

    drop_database(&db_name).await;
}
