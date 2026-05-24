//! Integration coverage for the BOOM scan-cross-match time
//! filter. Locks in the rule that the GW merger time `t_0` must
//! lie inside the optical transient's
//! `(last_non_detection_time, first_detection_time)` bracket —
//! the kilonova-style turn-on window, **not** the symmetric
//! `±time_window_sec` window we use for GRB triggers.
//!
//! Marked `#[ignore]` so a plain `cargo test` skips it; the CI
//! integration job runs it via `cargo test -- --ignored`
//! alongside the other Mongo-dependent suites.
//!
//! We deliberately test at the Mongo-filter layer rather than the
//! `/scan-cross-matches` HTTP layer because the HTTP path also
//! needs a real BAYESTAR skymap attached to the superevent —
//! supplying one would obscure what this test is actually
//! pinning down. The filter expression here is byte-identical to
//! the one in `scan_cross_matches`, so any drift between the two
//! breaks the test.

use boom_gw::archive::BoomAlertDoc;
use boom_gw::boom::{BoomPhotometry, BoomTransient};
use boom_gw::{Archive, ArchiveConfig};
use futures::stream::StreamExt;
use mongodb::bson::doc;
use serde_json::json;

fn mongo_uri() -> String {
    std::env::var("BOOM_GW_MONGO_URI").unwrap_or_else(|_| "mongodb://localhost:27017".into())
}

async fn build_archive() -> (Archive, String) {
    let uri = mongo_uri();
    // Per-test DB name. Cargo runs the two #[ignore] tests in this
    // file in parallel inside the same process, so `pid` alone
    // collides; a uuid is the cheapest way to guarantee isolation.
    let suffix = uuid::Uuid::new_v4().simple();
    let mut cfg = ArchiveConfig::new(&uri);
    cfg.database = format!("boom_gw_boom_test_{suffix}");
    let client = mongodb::Client::with_uri_str(&uri).await.expect("mongo");
    client.database(&cfg.database).drop().await.expect("drop");
    let archive = Archive::connect(cfg.clone()).await.expect("connect");
    (archive, cfg.database)
}

async fn drop_database(db: &str) {
    let client = mongodb::Client::with_uri_str(&mongo_uri()).await.unwrap();
    client.database(db).drop().await.unwrap();
}

/// Build a synthetic [`BoomAlertDoc`] with explicit bracket times.
/// `first_det` / `last_non_det` are optional so the test can
/// exercise the "only one bookend" branches.
fn synth_alert(id: &str, first_det: Option<f64>, last_non_det: Option<f64>) -> BoomAlertDoc {
    let transient = BoomTransient {
        alert_id: id.to_string(),
        alert_time: 1_400_000_500.0,
        event_name: "ZTF25test".to_string(),
        ra: Some(150.0),
        dec: Some(10.0),
        error_radius_deg: Some(0.001),
        classification: Some("kilonova".to_string()),
        classification_score: Some(0.9),
        cross_match_summary: None,
        photometry: vec![BoomPhotometry {
            observation_start: None,
            telescope: None,
            instrument: None,
            filter: None,
            mag: None,
            mag_error: None,
            mag_system: None,
            limiting_mag: None,
        }],
        first_detection_time: first_det,
        last_non_detection_time: last_non_det,
        body: json!({}),
    };
    let mut doc = BoomAlertDoc::from_transient(transient);
    // `from_transient` already copies the bracket fields, but be
    // explicit in case future refactors decouple them.
    doc.first_detection_time = first_det;
    doc.last_non_detection_time = last_non_det;
    doc
}

#[actix_web::test]
#[ignore]
async fn boom_bracket_filter_keeps_only_alerts_whose_window_spans_t0() {
    let (archive, db_name) = build_archive().await;
    let t_0 = 1_400_000_000.0_f64;

    // Inside the bracket: last_non_det < t_0 < first_det → matches.
    archive
        .upsert_boom_alert(&synth_alert(
            "inside",
            Some(t_0 + 60.0),
            Some(t_0 - 3_600.0),
        ))
        .await
        .unwrap();
    // Bracket entirely before t_0 → excluded.
    archive
        .upsert_boom_alert(&synth_alert(
            "before",
            Some(t_0 - 60.0),
            Some(t_0 - 3_600.0),
        ))
        .await
        .unwrap();
    // Bracket entirely after t_0 → excluded.
    archive
        .upsert_boom_alert(&synth_alert("after", Some(t_0 + 3_600.0), Some(t_0 + 60.0)))
        .await
        .unwrap();
    // Only first_detection_time → excluded (criterion undefined).
    archive
        .upsert_boom_alert(&synth_alert("only-first-det", Some(t_0 + 60.0), None))
        .await
        .unwrap();
    // Only last_non_detection_time → excluded (criterion undefined).
    archive
        .upsert_boom_alert(&synth_alert("only-non-det", None, Some(t_0 - 60.0)))
        .await
        .unwrap();
    // No bracket at all → excluded.
    archive
        .upsert_boom_alert(&synth_alert("no-bracket", None, None))
        .await
        .unwrap();

    // Exactly the filter `scan_cross_matches` uses for BOOM. If the
    // two ever drift, this assertion catches it.
    let filter = doc! {
        "first_detection_time": {"$gte": t_0},
        "last_non_detection_time": {"$lte": t_0},
    };
    let mut cursor = archive.boom_alerts().find(filter).await.unwrap();
    let mut matched_ids = Vec::new();
    while let Some(d) = cursor.next().await {
        matched_ids.push(d.unwrap().alert_id);
    }
    matched_ids.sort();
    assert_eq!(
        matched_ids,
        vec!["inside".to_string()],
        "only the alert whose bracket spans t_0 should match"
    );

    drop_database(&db_name).await;
}

#[actix_web::test]
#[ignore]
async fn boom_bracket_filter_treats_t0_on_boundary_as_match() {
    // The criterion uses `$gte` / `$lte`, so a transient whose
    // first_detection_time equals t_0 (or whose last_non_det
    // equals t_0) should still be returned. This pins down that
    // intentional choice — the alternative (strict inequality)
    // would miss alerts where the merger lines up exactly with a
    // photometry timestamp, which happens in synthetic injections
    // and in alerts with second-precision timestamps.
    let (archive, db_name) = build_archive().await;
    let t_0 = 1_400_000_000.0_f64;

    archive
        .upsert_boom_alert(&synth_alert(
            "first-det-equals-t0",
            Some(t_0),
            Some(t_0 - 60.0),
        ))
        .await
        .unwrap();
    archive
        .upsert_boom_alert(&synth_alert(
            "last-non-det-equals-t0",
            Some(t_0 + 60.0),
            Some(t_0),
        ))
        .await
        .unwrap();

    let filter = doc! {
        "first_detection_time": {"$gte": t_0},
        "last_non_detection_time": {"$lte": t_0},
    };
    let mut cursor = archive.boom_alerts().find(filter).await.unwrap();
    let mut matched_ids = Vec::new();
    while let Some(d) = cursor.next().await {
        matched_ids.push(d.unwrap().alert_id);
    }
    matched_ids.sort();
    assert_eq!(
        matched_ids,
        vec![
            "first-det-equals-t0".to_string(),
            "last-non-det-equals-t0".to_string()
        ],
        "boundary-equal bracket timestamps should still match"
    );

    drop_database(&db_name).await;
}
