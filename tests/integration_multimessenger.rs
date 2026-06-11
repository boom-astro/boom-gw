//! End-to-end multi-messenger workflow over the real scan + filter
//! path: one GW superevent with a synthetic localization, five external
//! triggers (GRB, FRB, neutrino, optical) inside the GW region plus one
//! far outside, then
//!
//!   1. scan-cross-matches → spatial overlap + credible-region
//!      membership + RAVEN joint FAR + empirical p-value for each;
//!   2. a science filter (`require_in_90cr` + confidence tiers) → only
//!      the in-region matches survive, tier-tagged;
//!   3. a stream-restricted filter → cross-matches from streams the
//!      filter doesn't draw from are gated out.
//!
//! The GW side uses the shared synthetic-skymap builder
//! (`boom_gw::skymap_synth`), the same hand-written multi-order FITS the
//! demo loader seeds and the live scan consumes — so this exercises the
//! real geometry (the MOC integral, the contour membership test, the
//! Monte Carlo) without a checked-in BAYESTAR fixture.
//!
//! `#[ignore]` — needs Mongo. Run via `cargo test -- --ignored` with
//! `BOOM_GW_MONGO_URI` set. The stub principal signs in as
//! `integration-test@boom-gw`, who (first user, no site-admins) is
//! bootstrapped to Super admin and so can access every stream.

use actix_web::{test, web, App};
use boom_gw::contour::compute_contour_moc;
use boom_gw::skymap_synth::build_uniform_cone_skymap;
use boom_gw::storage::skymap::{build_storage, SkymapBackendKind, SkymapBlob};
use boom_gw::{api, api::MaybeAlertPublisher, stub_principal_middleware, Archive, ArchiveConfig};
use boom_gw::{AuthConfig, SUPEREVENTS_COLLECTION};
use mongodb::bson::doc;
use serde_json::{json, Value};

fn mongo_uri() -> String {
    std::env::var("BOOM_GW_MONGO_URI").unwrap_or_else(|_| "mongodb://localhost:27017".into())
}

async fn build_archive() -> (Archive, String) {
    let uri = mongo_uri();
    let suffix = uuid::Uuid::new_v4().simple();
    let mut cfg = ArchiveConfig::new(&uri);
    cfg.database = format!("boom_gw_mm_test_{suffix}");
    let client = mongodb::Client::with_uri_str(&uri).await.expect("mongo");
    client.database(&cfg.database).drop().await.expect("drop");
    let archive = Archive::connect(cfg.clone()).await.expect("connect");
    (archive, cfg.database)
}

async fn drop_database(db: &str) {
    let client = mongodb::Client::with_uri_str(&mongo_uri()).await.unwrap();
    client.database(db).drop().await.unwrap();
}

/// Pull the cross-match list out of an `{message, data}` envelope and
/// key it by instrument for assertions.
fn by_instrument(body: &Value) -> std::collections::HashMap<String, Value> {
    body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| (m["instrument"].as_str().unwrap().to_string(), m.clone()))
        .collect()
}

#[actix_web::test]
#[ignore]
async fn multimessenger_scan_filter_and_stream_gating() {
    let (archive, db_name) = build_archive().await;

    // --- GW side: synthetic localization at (150, 15), 6° radius ---------
    let sid = "S_MM_0001";
    let (gw_ra, gw_dec) = (150.0, 15.0);
    let t0 = 1_400_000_000.0_f64; // GPS seconds
    let gw_fits = build_uniform_cone_skymap(gw_ra, gw_dec, 6.0, 8);

    // Superevent doc (raw insert — the scan only needs it to exist and a
    // sky map in storage; it doesn't read the full GwEvent aggregate).
    archive
        .database()
        .collection::<mongodb::bson::Document>(SUPEREVENTS_COLLECTION)
        .insert_one(doc! {
            "_id": sid,
            "t_0": t0,
            "t_start": t0 - 2.5,
            "t_end": t0 + 2.5,
            "preferred_graceid": "G_MM_1",
            "preferred_snr": 14.0,
            "g_event_graceids": ["G_MM_1"],
            "skymap_summary": { "bytes_size": gw_fits.len() as i64, "elapsed_ms": 5000 },
        })
        .await
        .expect("seed superevent");

    // Store the GW sky map + its 50%/90% credible-region contours so the
    // scan can read them (mirrors `ingest::store_superevent`).
    let storage = std::sync::Arc::new(
        build_storage(SkymapBackendKind::Mongo, archive.database(), None)
            .await
            .expect("storage"),
    );
    storage
        .upsert(SkymapBlob {
            superevent_id: sid.into(),
            bytes: gw_fits.clone(),
            elapsed_ms: 5000,
        })
        .await
        .expect("store gw skymap");
    for lvl in [50u8, 90u8] {
        let moc = compute_contour_moc(&gw_fits, lvl as f64 / 100.0).expect("contour");
        storage
            .upsert_contour(sid, lvl, moc)
            .await
            .expect("contour upsert");
    }

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(archive.clone()))
            .app_data(web::Data::new(AuthConfig::defaults()))
            .app_data(web::Data::new(MaybeAlertPublisher(None)))
            .app_data(web::Data::from(storage.clone()))
            .wrap(actix_web::middleware::from_fn(stub_principal_middleware))
            .configure(api::configure),
    )
    .await;

    // --- Ingest the multi-messenger triggers -----------------------------
    // Five inside the GW region (near 150,15) + one GRB far outside.
    let post = |uri: &'static str, b: Value| {
        let app = &app;
        async move {
            let req = test::TestRequest::post().uri(uri).set_json(b).to_request();
            let resp = test::call_service(app, req).await;
            assert!(
                resp.status().is_success(),
                "POST {uri} failed: {}",
                resp.status()
            );
        }
    };

    // GRBs (gcn_grb stream)
    post(
        "/api/grb-triggers",
        json!({"trigger_id":"sw_in","instrument":"Swift-BAT","trigger_time":t0+0.9,
               "position":{"ra":150.1,"dec":15.1,"uncertainty_arcsec":180.0},
               "significance":9.1,"error_radius_deg":0.05}),
    )
    .await;
    post(
        "/api/grb-triggers",
        json!({"trigger_id":"gbm_in","instrument":"Fermi-GBM-FIN","trigger_time":t0+2.4,
               "position":{"ra":149.5,"dec":14.5,"uncertainty_arcsec":7200.0},
               "significance":7.2,"error_radius_deg":2.0}),
    )
    .await;
    post(
        "/api/grb-triggers",
        json!({"trigger_id":"gbm_far","instrument":"Swift-BAT","trigger_time":t0+3.0,
               "position":{"ra":30.0,"dec":-40.0,"uncertainty_arcsec":3600.0},
               "significance":6.0,"error_radius_deg":1.0}),
    )
    .await;
    // FRB (gcn_frb stream)
    post(
        "/api/frb-alerts",
        json!({"trigger_id":"chime_in","instrument":"CHIME-FRB","trigger_time":t0+1.2,
               "position":{"ra":150.2,"dec":15.2,"uncertainty_arcsec":1800.0},
               "significance":12.5,"error_radius_deg":0.5,"dm":279.4,"body":{}}),
    )
    .await;
    // Neutrino (gcn_neutrino stream)
    post(
        "/api/neutrino-alerts",
        json!({"trigger_id":"ic_in","instrument":"IceCube","trigger_time":t0+4.0,
               "position":{"ra":150.0,"dec":15.3,"uncertainty_arcsec":1440.0},
               "significance":4.2,"error_radius_deg":0.4,
               "alert_topology":"Track","pipeline":"Gold","nu_energy":250.0,"p_astro":0.85,"body":{}}),
    )
    .await;
    // Optical (boom_optical stream)
    post(
        "/api/boom-alerts",
        json!({"alert_id":"ztf_in","event_name":"ZTF_mm","alert_time":t0+5.0,
               "ra":150.15,"dec":15.05,"error_radius_deg":0.02,
               "classification":"kilonova candidate","classification_score":0.7,
               // Detection bracket straddling t_0 (last non-detection before,
               // first detection after) — the BOOM scan's turn-on criterion.
               "last_non_detection_time":t0-3600.0,"first_detection_time":t0+3600.0,
               "photometry":[],"body":{}}),
    )
    .await;

    // --- Scan: compute cross-matches across all messengers ---------------
    let req = test::TestRequest::post()
        .uri(&format!("/api/superevents/{sid}/scan-cross-matches"))
        .set_json(json!({"time_window_sec": 60, "p_value_trials": 100}))
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let matches = by_instrument(&body);

    // All four messenger families produced a cross-match (six triggers,
    // but Swift-BAT appears once per trigger_id; we key by instrument so
    // the two Swift rows collapse — assert the families instead).
    let all = body["data"].as_array().unwrap();
    assert_eq!(all.len(), 6, "expected 6 cross-matches, got {}", all.len());
    for inst in ["Swift-BAT", "Fermi-GBM-FIN", "CHIME-FRB", "IceCube", "BOOM"] {
        assert!(
            matches.contains_key(inst),
            "missing cross-match for {inst}; got {:?}",
            matches.keys().collect::<Vec<_>>()
        );
    }

    // In-region triggers: positive spatial overlap and inside the 90% CR.
    for inst in ["Fermi-GBM-FIN", "CHIME-FRB", "IceCube", "BOOM"] {
        let m = &matches[inst];
        assert!(
            m["spatial_overlap"].as_f64().unwrap() > 0.0,
            "{inst} should overlap the GW map: {m}"
        );
        assert_eq!(m["in_90cr"], json!(true), "{inst} should be in the 90% CR");
    }
    // The far-away GRB shares no localization: ~zero overlap, outside CR.
    // (It's a Swift-BAT trigger_id `gbm_far`; find it in the raw list.)
    let far = all
        .iter()
        .find(|m| m["trigger_id"] == json!("gbm_far"))
        .expect("far trigger present");
    assert!(
        far["spatial_overlap"].as_f64().unwrap() < 1e-6,
        "far trigger should have ~0 overlap: {far}"
    );
    assert_eq!(
        far["in_90cr"],
        json!(false),
        "far trigger must be outside CR"
    );

    // The Monte Carlo ran (contours present + trials > 0) → remapped FAR
    // and an empirical p-value exist for an in-region match.
    let gbm = &matches["Fermi-GBM-FIN"];
    assert!(
        gbm["p_value"].as_f64().is_some(),
        "p-value should be computed"
    );
    assert!(
        gbm["joint_far_remapped_per_year"].as_f64().is_some(),
        "remapped joint FAR should be computed"
    );

    // --- Science filter: require_in_90cr, with a generous gold tier ------
    let req = test::TestRequest::post()
        .uri("/api/science-filters")
        .set_json(json!({
            "name": "In 90% CR",
            "cuts": { "require_in_90cr": true },
            "confidence_tiers": [{ "name": "gold", "joint_far_remapped_max_per_year": 1e30 }]
        }))
        .to_request();
    let f: Value = test::call_and_read_body_json(&app, req).await;
    let fid = f["data"]["_id"].as_str().unwrap().to_string();

    let req = test::TestRequest::get()
        .uri(&format!(
            "/api/superevents/{sid}/cross-matches?filter_id={fid}"
        ))
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let passing = by_instrument(&body);
    // The five in-region matches pass; the far GRB is cut. (Swift-BAT's
    // in-region row passes, so Swift-BAT is present.)
    assert!(
        !body["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["trigger_id"] == json!("gbm_far")),
        "the out-of-region GRB must be filtered out"
    );
    for inst in ["Swift-BAT", "Fermi-GBM-FIN", "CHIME-FRB", "IceCube", "BOOM"] {
        assert!(passing.contains_key(inst), "{inst} should pass the filter");
        assert_eq!(passing[inst]["confidence_tier"], json!("gold"));
    }

    // --- Stream gating: a group + filter restricted to GRB + optical -----
    let g: Value = {
        let req = test::TestRequest::post()
            .uri("/api/groups")
            .set_json(json!({"name":"GRB+optical team"}))
            .to_request();
        test::call_and_read_body_json(&app, req).await
    };
    let gid = g["data"]["id"].as_str().unwrap().to_string();
    for s in ["gcn_grb", "boom_optical"] {
        let req = test::TestRequest::post()
            .uri(&format!("/api/groups/{gid}/streams"))
            .set_json(json!({"stream_id": s}))
            .to_request();
        assert!(test::call_service(&app, req).await.status().is_success());
    }
    let req = test::TestRequest::post()
        .uri("/api/science-filters")
        .set_json(json!({
            "name": "GRB + optical only",
            "group_id": gid,
            "stream_ids": ["gcn_grb", "boom_optical"]
        }))
        .to_request();
    let f2: Value = test::call_and_read_body_json(&app, req).await;
    let fid2 = f2["data"]["_id"].as_str().unwrap().to_string();

    let req = test::TestRequest::get()
        .uri(&format!(
            "/api/superevents/{sid}/cross-matches?filter_id={fid2}"
        ))
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let streamed = by_instrument(&body);
    assert!(
        streamed.contains_key("Swift-BAT")
            && streamed.contains_key("Fermi-GBM-FIN")
            && streamed.contains_key("BOOM"),
        "GRB + optical matches should survive stream gating: {:?}",
        streamed.keys().collect::<Vec<_>>()
    );
    assert!(
        !streamed.contains_key("CHIME-FRB") && !streamed.contains_key("IceCube"),
        "FRB + neutrino matches should be gated out (streams not in the filter)"
    );

    drop_database(&db_name).await;
}
