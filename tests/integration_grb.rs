//! In-process integration test for the GRB ingest + GW × GRB
//! cross-match HTTP routes.
//!
//! Marked `#[ignore]` so a plain `cargo test` skips it; the GitHub
//! Actions `integration-kafka` job spins up a `mongo:8.0` services
//! container and runs this test alongside the other integration
//! tests with `cargo test -- --ignored`.
//!
//! Coverage focus:
//!
//! * `POST /api/grb-triggers` — both the `Raw { format, payload }`
//!   shape (exercises `gcn::parse_fermi_gbm_json`) and the
//!   `Parsed(GrbTrigger)` shape.
//! * Idempotency — the same `(instrument, trigger_id)` upserts in
//!   place rather than fanning out.
//! * `GET /api/grb-triggers` — listing + `instrument` filter +
//!   `since` / `until` GPS window filters.
//! * `GET /api/grb-triggers/{instrument}/{trigger_id}` — single
//!   lookup + 404 for unknown.
//! * `GET /api/superevents/{id}/cross-matches` — initial empty
//!   list, then non-empty after a manual seed via the archive.
//! * `POST /api/superevents/{id}/cross-matches` against a
//!   superevent with no attached skymap → 404, exercises the
//!   "missing skymap" branch.
//!
//! The actual cross-match math is exercised by the
//! `crossmatch::tests::real_bayestar_spatial` unit test against a
//! real BAYESTAR fixture (gated on `BAYESTAR_FIXTURE`); it can't
//! easily run in CI because no fixture is bundled. The
//! integration test here covers the *wiring* — routes, archive,
//! storage lookup, error paths.

use actix_web::{test, web, App};
use boom_gw::archive::{CrossMatchDoc, GrbTriggerDoc};
use boom_gw::grb::{CrossMatchResult, GrbTrigger, SkyPosition};
use boom_gw::storage::skymap::{build_storage, SkymapBackendKind};
use boom_gw::{api, api::MaybeAlertPublisher, Archive, ArchiveConfig, Superevent};
use igwn_ligolw::CoincInspiralEvent;
use serde_json::{json, Value};

fn mongo_uri() -> String {
    std::env::var("BOOM_GW_MONGO_URI").unwrap_or_else(|_| "mongodb://localhost:27017".into())
}

fn dummy_event(graceid: &str) -> boom_gw::GwEvent {
    let coinc = CoincInspiralEvent {
        coinc_event_id: graceid.into(),
        ifos: "H1,L1".into(),
        combined_far: 1e-9,
        snr: 10.0,
        mass: None,
        mchirp: None,
        end_time: 1_400_000_000.0,
        sngls: vec![],
    };
    boom_gw::GwEvent {
        pipeline: "gstlal".into(),
        graceid: graceid.into(),
        producer_timestamp: 1_700_000_000.0,
        message_type: "new".into(),
        submitter: "ci".into(),
        end_time: 1_400_000_000.0,
        ifos: "H1,L1".into(),
        snr: 10.0,
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
    cfg.database = format!("boom_gw_grb_test_{pid}");
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
async fn grb_routes_round_trip() {
    let (archive, db_name) = build_archive().await;

    // Seed a single superevent. No skymap attached — we'll exercise
    // the "missing skymap → 404" branch on cross-match.
    let ev = dummy_event("G_grb_1");
    archive.record_event(&ev).await.unwrap();
    let s = Superevent {
        id: "S_grb_001".into(),
        t_0: 1_400_000_000.0,
        t_start: 1_399_999_997.5,
        t_end: 1_400_000_002.5,
        preferred_event: ev.clone(),
        g_events: vec![ev.clone()],
        skymap: None,
    };
    archive.upsert_superevent(&s).await.unwrap();

    let skymap_storage = std::sync::Arc::new(
        build_storage(SkymapBackendKind::Mongo, archive.database(), None)
            .await
            .expect("build mongo skymap storage"),
    );

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(archive.clone()))
            .app_data(web::Data::new(MaybeAlertPublisher(None)))
            .app_data(web::Data::from(skymap_storage.clone()))
            .configure(api::configure),
    )
    .await;

    // --- POST /api/grb-triggers (Raw / Fermi GBM JSON) ---
    let raw_body = json!({
        "format": "fermi_gbm_json",
        "instrument": "Fermi-GBM-FIN",
        "payload": r#"{
            "trigger_id": "bn250101000",
            "trigger_time": 757382400.0,
            "ra": 135.2,
            "dec": -15.4,
            "error_radius": 2.5,
            "reliability": 7.5
        }"#
    });
    let req = test::TestRequest::post()
        .uri("/api/grb-triggers")
        .set_json(&raw_body)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 201, "raw POST should be 201 Created");
    let body: Value = test::read_body_json(resp).await;
    assert_eq!(body["data"]["instrument"], "Fermi-GBM-FIN");
    assert_eq!(body["data"]["trigger_id"], "bn250101000");
    // RA / Dec made it through the parser.
    assert_eq!(body["data"]["position"]["ra"], 135.2);
    assert_eq!(body["data"]["position"]["dec"], -15.4);
    // MET → GPS conversion applied.
    let t = body["data"]["trigger_time"].as_f64().unwrap();
    assert!(t > 1.4e9, "trigger_time should be GPS-scale; got {t}");

    // Re-POST the same trigger — should upsert (200 OK, not 201).
    let req = test::TestRequest::post()
        .uri("/api/grb-triggers")
        .set_json(&raw_body)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200, "re-POST should be 200 Ok (upsert)");

    // --- POST /api/grb-triggers (Parsed shape) ---
    let parsed_body = serde_json::to_value(&GrbTrigger {
        trigger_id: "swift_42".into(),
        instrument: "Swift-BAT".into(),
        trigger_time: 1_400_000_010.0,
        position: Some(SkyPosition::new(50.0, 30.0, 120.0)),
        significance: 9.1,
        skymap_url: None,
        error_radius_deg: Some(0.033),
    })
    .unwrap();
    let req = test::TestRequest::post()
        .uri("/api/grb-triggers")
        .set_json(&parsed_body)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 201);

    // --- GET /api/grb-triggers (no filter) ---
    let req = test::TestRequest::get()
        .uri("/api/grb-triggers")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let triggers = body["data"].as_array().unwrap();
    assert_eq!(triggers.len(), 2);

    // --- GET /api/grb-triggers?instrument=Fermi-GBM-FIN ---
    let req = test::TestRequest::get()
        .uri("/api/grb-triggers?instrument=Fermi-GBM-FIN")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let triggers = body["data"].as_array().unwrap();
    assert_eq!(triggers.len(), 1);
    assert_eq!(triggers[0]["instrument"], "Fermi-GBM-FIN");

    // --- GET /api/grb-triggers?since=...&until=... (GPS window) ---
    // The Swift trigger is at 1.4e9; the Fermi one is far in the
    // future (post-MET conversion). The narrow window catches only
    // the Swift one.
    let req = test::TestRequest::get()
        .uri("/api/grb-triggers?since=1399999000&until=1400001000")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let triggers = body["data"].as_array().unwrap();
    assert_eq!(triggers.len(), 1);
    assert_eq!(triggers[0]["trigger_id"], "swift_42");

    // --- GET /api/grb-triggers/{instrument}/{trigger_id} ---
    let req = test::TestRequest::get()
        .uri("/api/grb-triggers/Swift-BAT/swift_42")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["data"]["trigger_id"], "swift_42");
    assert_eq!(body["data"]["instrument"], "Swift-BAT");

    // Unknown trigger → 404.
    let req = test::TestRequest::get()
        .uri("/api/grb-triggers/Swift-BAT/does_not_exist")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 404);

    // --- POST /api/grb-triggers (bad format) → 400 ---
    let bad = json!({"format": "made_up", "payload": "{}"});
    let req = test::TestRequest::post()
        .uri("/api/grb-triggers")
        .set_json(&bad)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 400);

    // --- GET /api/superevents/{id}/cross-matches (initially empty) ---
    let req = test::TestRequest::get()
        .uri("/api/superevents/S_grb_001/cross-matches")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert!(body["data"].as_array().unwrap().is_empty());

    // --- POST /api/superevents/{id}/cross-matches with no skymap → 404 ---
    let body = json!({"instrument": "Swift-BAT", "trigger_id": "swift_42"});
    let req = test::TestRequest::post()
        .uri("/api/superevents/S_grb_001/cross-matches")
        .set_json(&body)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(
        resp.status(),
        404,
        "expected 404 when superevent has no attached skymap"
    );

    // --- POST cross-match for an unknown superevent → 404 ---
    let req = test::TestRequest::post()
        .uri("/api/superevents/S_does_not_exist/cross-matches")
        .set_json(&body)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 404);

    // --- POST cross-match for an unknown trigger → 404 ---
    let body = json!({"instrument": "Swift-BAT", "trigger_id": "does_not_exist"});
    let req = test::TestRequest::post()
        .uri("/api/superevents/S_grb_001/cross-matches")
        .set_json(&body)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 404);

    // --- Seed a CrossMatchDoc directly and check it lists ---
    // (Bypasses the math, exercises the read path + sort order.)
    let trigger = GrbTrigger {
        trigger_id: "swift_42".into(),
        instrument: "Swift-BAT".into(),
        trigger_time: 1_400_000_010.0,
        position: Some(SkyPosition::new(50.0, 30.0, 120.0)),
        significance: 9.1,
        skymap_url: None,
        error_radius_deg: Some(0.033),
    };
    let cm = CrossMatchResult {
        time_offset_sec: 10.0,
        spatial_overlap: 0.42,
        in_50cr: true,
        in_90cr: true,
        joint_far_per_year: Some(1.5e-3),
        p_value: None,
        p_value_trials: None,
        joint_far_remapped_per_year: None,
        associated: false,
    };
    let cm_doc = CrossMatchDoc::new("S_grb_001", &trigger, cm);
    archive
        .upsert_cross_match(&cm_doc)
        .await
        .expect("seed cross-match");

    let req = test::TestRequest::get()
        .uri("/api/superevents/S_grb_001/cross-matches")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let matches = body["data"].as_array().unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0]["superevent_id"], "S_grb_001");
    assert_eq!(matches[0]["instrument"], "Swift-BAT");
    assert_eq!(matches[0]["trigger_id"], "swift_42");
    assert_eq!(matches[0]["spatial_overlap"], 0.42);
    assert_eq!(matches[0]["in_50cr"], true);

    // Idempotency on the cross-match side too — re-upsert with a
    // different overlap should replace, not duplicate.
    let cm2 = CrossMatchResult {
        time_offset_sec: 10.0,
        spatial_overlap: 0.55,
        in_50cr: true,
        in_90cr: true,
        joint_far_per_year: Some(1.0e-3),
        p_value: None,
        p_value_trials: None,
        joint_far_remapped_per_year: None,
        associated: false,
    };
    let cm_doc2 = CrossMatchDoc::new("S_grb_001", &trigger, cm2);
    archive.upsert_cross_match(&cm_doc2).await.unwrap();

    let req = test::TestRequest::get()
        .uri("/api/superevents/S_grb_001/cross-matches")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let matches = body["data"].as_array().unwrap();
    assert_eq!(
        matches.len(),
        1,
        "cross-match upsert should replace, not append"
    );
    assert_eq!(matches[0]["spatial_overlap"], 0.55);

    // Tidy up — we left junk on the test database; drop it.
    let _ = GrbTriggerDoc::from_trigger(trigger); // exercise the type-export at least once

    drop_database(&db_name).await;
}
