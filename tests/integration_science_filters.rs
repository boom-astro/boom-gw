//! Integration coverage for the science-filter prototype: the
//! `/api/science-filters` CRUD routes and the filtered cross-match
//! view (`GET /api/superevents/{id}/cross-matches?filter_id=...`)
//! with confidence-tier tagging.
//!
//! Marked `#[ignore]` so a plain `cargo test` skips it; the CI
//! integration job runs it via `cargo test -- --ignored` alongside
//! the other Mongo-dependent suites.

use actix_web::{test, web, App};
use boom_gw::archive::{
    ConfidenceTier, CrossMatchDoc, FilterCuts, SUPEREVENTS_COLLECTION,
};
use boom_gw::grb::{CrossMatchResult, GrbTrigger};
use boom_gw::storage::skymap::{build_storage, SkymapBackendKind};
use boom_gw::{api, api::MaybeAlertPublisher, stub_principal_middleware, Archive, ArchiveConfig};
use mongodb::bson::doc;
use serde_json::{json, Value};

fn mongo_uri() -> String {
    std::env::var("BOOM_GW_MONGO_URI").unwrap_or_else(|_| "mongodb://localhost:27017".into())
}

async fn build_archive() -> (Archive, String) {
    let uri = mongo_uri();
    let suffix = uuid::Uuid::new_v4().simple();
    let mut cfg = ArchiveConfig::new(&uri);
    cfg.database = format!("boom_gw_filters_test_{suffix}");
    let client = mongodb::Client::with_uri_str(&uri).await.expect("mongo");
    client.database(&cfg.database).drop().await.expect("drop");
    let archive = Archive::connect(cfg.clone()).await.expect("connect");
    (archive, cfg.database)
}

async fn drop_database(db: &str) {
    let client = mongodb::Client::with_uri_str(&mongo_uri()).await.unwrap();
    client.database(db).drop().await.unwrap();
}

/// A bare GRB trigger — only the identity fields matter for keying
/// the seeded cross-match document.
fn trigger(instrument: &str, id: &str) -> GrbTrigger {
    GrbTrigger {
        trigger_id: id.into(),
        instrument: instrument.into(),
        trigger_time: 0.0,
        position: None,
        significance: 0.0,
        skymap_url: None,
        error_radius_deg: None,
        far_hz: None,
    }
}

/// A cross-match result with just the fields the filter cuts read.
#[allow(clippy::too_many_arguments)]
fn result(
    time_offset_sec: f64,
    spatial_overlap: f64,
    in_90cr: bool,
    p_value: Option<f64>,
    joint_far_remapped_per_year: Option<f64>,
) -> CrossMatchResult {
    CrossMatchResult {
        time_offset_sec,
        spatial_overlap,
        in_50cr: in_90cr,
        in_90cr,
        joint_far_per_year: joint_far_remapped_per_year,
        p_value,
        p_value_trials: p_value.map(|_| 200),
        joint_far_remapped_per_year,
        targeted_joint_far_per_year: None,
        associated: false,
    }
}

#[actix_web::test]
#[ignore]
async fn science_filter_crud_and_filtered_cross_matches() {
    let (archive, db_name) = build_archive().await;

    // Seed a superevent (the filtered list checks it exists first).
    // Insert as a raw document to avoid constructing a full
    // Superevent aggregate.
    archive
        .database()
        .collection::<mongodb::bson::Document>(SUPEREVENTS_COLLECTION)
        .insert_one(doc! {
            "_id": "S250101a",
            "t_0": 0.0,
            "t_start": -1.0,
            "t_end": 1.0,
            "preferred_graceid": "G1",
            "preferred_snr": 12.0,
            "g_event_graceids": ["G1"],
        })
        .await
        .expect("seed superevent");

    // Three cross-matches with deliberately different metrics:
    //   gold   — tight, well inside, very low FAR
    //   silver — clears the cuts but a larger FAR
    //   junk   — fails essentially every cut
    let seeds = [
        CrossMatchDoc::new(
            "S250101a",
            &trigger("Fermi-GBM", "bn1"),
            result(2.0, 0.8, true, Some(0.001), Some(0.5)),
        ),
        CrossMatchDoc::new(
            "S250101a",
            &trigger("Swift-BAT", "sw1"),
            result(5.0, 0.3, true, Some(0.02), Some(5.0)),
        ),
        CrossMatchDoc::new(
            "S250101a",
            &trigger("Fermi-GBM", "bn2"),
            result(50.0, 0.05, false, Some(0.5), Some(200.0)),
        ),
    ];
    archive
        .cross_matches()
        .insert_many(&seeds)
        .await
        .expect("seed cross-matches");

    let skymap_storage = std::sync::Arc::new(
        build_storage(SkymapBackendKind::Mongo, archive.database(), None)
            .await
            .expect("build storage"),
    );
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(archive.clone()))
            .app_data(web::Data::new(boom_gw::AuthConfig::defaults()))
            .app_data(web::Data::new(MaybeAlertPublisher(None)))
            .app_data(web::Data::from(skymap_storage.clone()))
            .wrap(actix_web::middleware::from_fn(stub_principal_middleware))
            .configure(api::configure),
    )
    .await;

    // --- Create a filter -------------------------------------------------
    let cuts = FilterCuts {
        instruments: vec!["Fermi-GBM".into(), "Swift-BAT".into()],
        time_window_sec: Some(10.0),
        spatial_overlap_min: Some(0.1),
        p_value_max: Some(0.05),
        joint_far_remapped_max_per_year: Some(12.0),
        require_in_90cr: Some(true),
    };
    let tiers = vec![
        ConfidenceTier {
            name: "silver".into(),
            joint_far_remapped_max_per_year: 12.0,
        },
        ConfidenceTier {
            name: "gold".into(),
            joint_far_remapped_max_per_year: 1.0,
        },
    ];
    let req = test::TestRequest::post()
        .uri("/api/science-filters")
        .set_json(json!({
            "name": "GRB+GW gold/silver",
            "group": "umn-mma",
            "cuts": cuts,
            "confidence_tiers": tiers,
        }))
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let fid = body["data"]["_id"].as_str().expect("filter id").to_string();
    assert_eq!(body["data"]["owner"], "integration-test@boom-gw");
    // Tiers are stored most-significant first.
    assert_eq!(body["data"]["confidence_tiers"][0]["name"], "gold");

    // --- List + get ------------------------------------------------------
    let req = test::TestRequest::get()
        .uri("/api/science-filters")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 1);

    let req = test::TestRequest::get()
        .uri(&format!("/api/science-filters/{fid}"))
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["data"]["name"], "GRB+GW gold/silver");

    // --- Filtered cross-matches -----------------------------------------
    let req = test::TestRequest::get()
        .uri(&format!(
            "/api/superevents/S250101a/cross-matches?filter_id={fid}"
        ))
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let items = body["data"].as_array().unwrap();
    // Only gold + silver pass; junk is cut. Sorted most-significant
    // (smallest remapped FAR) first.
    assert_eq!(items.len(), 2, "expected 2 passing matches, got {items:?}");
    assert_eq!(items[0]["trigger_id"], "bn1");
    assert_eq!(items[0]["confidence_tier"], "gold");
    assert_eq!(items[1]["trigger_id"], "sw1");
    assert_eq!(items[1]["confidence_tier"], "silver");

    // The unfiltered list still returns everything.
    let req = test::TestRequest::get()
        .uri("/api/superevents/S250101a/cross-matches")
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 3);

    // --- Patch (loosen tiers, narrow instruments) -----------------------
    let req = test::TestRequest::patch()
        .uri(&format!("/api/science-filters/{fid}"))
        .set_json(json!({ "name": "renamed", "active": true }))
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["data"]["name"], "renamed");
    assert_eq!(body["data"]["active"], true);

    // --- Unknown filter id → 404 ----------------------------------------
    let req = test::TestRequest::get()
        .uri("/api/superevents/S250101a/cross-matches?filter_id=nope")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 404);

    // --- Delete ----------------------------------------------------------
    let req = test::TestRequest::delete()
        .uri(&format!("/api/science-filters/{fid}"))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
    let req = test::TestRequest::get()
        .uri(&format!("/api/science-filters/{fid}"))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 404);

    drop_database(&db_name).await;
}
