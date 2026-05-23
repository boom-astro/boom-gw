//! End-to-end MongoDB archive test.
//!
//! Marked `#[ignore]` so a plain `cargo test` skips it; the GitHub
//! Actions `integration-kafka` job spins up a `mongo:8.0` services
//! container and runs this test with `cargo test -- --ignored`.
//!
//! Required environment:
//!
//! * `BOOM_GW_MONGO_URI` — connection string (default `mongodb://localhost:27017`)
//! * a MongoDB instance reachable on that URI
//!
//! The test connects, ensures indices, writes one of each
//! (event / superevent / localize-request / localize-result), reads
//! them back, and asserts the round-trip preserves the canonical
//! `_id` fields and the FITS payload on the superevent.

use boom_gw::{
    Archive, ArchiveConfig, LocalizeRequest, LocalizeResult, LocalizeStatus, SkyMapFits,
    Superevent, SupereventDoc,
};
use igwn_ligolw::CoincInspiralEvent;
use mongodb::bson::doc;

fn mongo_uri() -> String {
    std::env::var("BOOM_GW_MONGO_URI").unwrap_or_else(|_| "mongodb://localhost:27017".into())
}

fn dummy_event(graceid: &str) -> boom_gw::GwEvent {
    let coinc = CoincInspiralEvent {
        coinc_event_id: graceid.into(),
        ifos: "H1,L1".into(),
        combined_far: 1e-9,
        snr: 12.3,
        mass: None,
        mchirp: None,
        end_time: 1_400_000_000.0,
        sngls: vec![],
    };
    boom_gw::GwEvent {
        pipeline: "gstlal".into(),
        graceid: graceid.into(),
        producer_timestamp: 0.0,
        message_type: "new".into(),
        submitter: "ci".into(),
        end_time: 1_400_000_000.0,
        ifos: "H1,L1".into(),
        snr: 12.3,
        far: 1e-9,
        mchirp: None,
        total_mass: None,
        coinc,
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn archive_round_trip_through_mongo() {
    let uri = mongo_uri();
    // Per-process database to avoid cross-test interference if anyone
    // ever runs --test-threads=N>1 against the same mongo.
    let pid = std::process::id();
    let mut cfg = ArchiveConfig::new(&uri);
    cfg.database = format!("boom_gw_test_{pid}");

    // Start from a clean slate so the test is repeatable.
    drop_database(&uri, &cfg.database).await;
    let archive = Archive::connect(cfg.clone())
        .await
        .expect("connect to MongoDB archive");

    // Event
    let event = dummy_event("G_archive_test");
    archive.record_event(&event).await.expect("record_event");
    let read_back: Option<boom_gw::EventDoc> = archive
        .events()
        .find_one(doc! {"_id": &event.graceid})
        .await
        .unwrap();
    let ev_doc = read_back.expect("event was not written");
    assert_eq!(ev_doc.graceid, "G_archive_test");
    assert_eq!(ev_doc.pipeline, "gstlal");
    assert!((ev_doc.snr - 12.3).abs() < 1e-9);

    // Superevent (with skymap attached)
    let superevent = Superevent {
        id: format!("S_archive_{pid}"),
        t_0: 1_400_000_000.0,
        t_start: 1_399_999_997.5,
        t_end: 1_400_000_002.5,
        preferred_event: event.clone(),
        g_events: vec![event.clone()],
        skymap: Some(SkyMapFits {
            bytes: b"FITS-PAYLOAD".to_vec(),
            elapsed_ms: 137,
        }),
    };
    archive
        .upsert_superevent(&superevent)
        .await
        .expect("upsert_superevent");
    let se_doc: SupereventDoc = archive
        .superevents()
        .find_one(doc! {"_id": &superevent.id})
        .await
        .unwrap()
        .expect("superevent was not written");
    assert_eq!(se_doc.id, superevent.id);
    assert_eq!(se_doc.preferred_graceid, "G_archive_test");
    let sky = se_doc.skymap.expect("skymap missing on archive doc");
    assert_eq!(sky.bytes, b"FITS-PAYLOAD");
    assert_eq!(sky.elapsed_ms, 137);

    // Upsert is idempotent: a second call (with a different skymap
    // representing a re-localization) overwrites the prior document.
    let mut updated = superevent.clone();
    updated.skymap = Some(SkyMapFits {
        bytes: b"FITS-PAYLOAD-V2".to_vec(),
        elapsed_ms: 200,
    });
    archive
        .upsert_superevent(&updated)
        .await
        .expect("upsert_superevent #2");
    let count = archive
        .superevents()
        .count_documents(doc! {"_id": &superevent.id})
        .await
        .unwrap();
    assert_eq!(count, 1, "upsert must not create duplicates");
    let refreshed: SupereventDoc = archive
        .superevents()
        .find_one(doc! {"_id": &superevent.id})
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        refreshed.skymap.unwrap().bytes,
        b"FITS-PAYLOAD-V2",
        "second upsert should replace the skymap"
    );

    // Localize request + result audit trail
    let req = LocalizeRequest::from_coinc_xml(
        format!("req-archive-{pid}"),
        &superevent.id,
        "G_archive_test",
        "gstlal",
        b"<?xml version='1.0'?><LIGO_LW></LIGO_LW>",
    );
    archive
        .record_localize_request(&req)
        .await
        .expect("record_localize_request");
    let result = LocalizeResult {
        request_id: req.request_id.clone(),
        superevent_id: req.superevent_id.clone(),
        graceid: req.graceid.clone(),
        status: LocalizeStatus::Ok,
        skymap_fits: Some({
            use base64::engine::general_purpose::STANDARD as BASE64;
            use base64::Engine as _;
            BASE64.encode(b"FITS-PAYLOAD-V2")
        }),
        error_message: None,
        elapsed_ms: 200,
    };
    archive
        .record_localize_result(&result)
        .await
        .expect("record_localize_result");

    let req_doc: boom_gw::LocalizeRequestDoc = archive
        .localize_requests()
        .find_one(doc! {"_id": &req.request_id})
        .await
        .unwrap()
        .expect("localize_request was not written");
    assert_eq!(req_doc.superevent_id, superevent.id);
    let res_doc: boom_gw::LocalizeResultDoc = archive
        .localize_results()
        .find_one(doc! {"_id": &req.request_id})
        .await
        .unwrap()
        .expect("localize_result was not written");
    assert!(matches!(res_doc.status, LocalizeStatus::Ok));
    assert_eq!(
        res_doc.skymap_fits_bytes,
        Some(b"FITS-PAYLOAD-V2".len() as i64)
    );

    drop_database(&uri, &cfg.database).await;
}

async fn drop_database(uri: &str, database: &str) {
    // The archive struct only exposes its own database handle, so the
    // teardown uses the raw client.
    let client = mongodb::Client::with_uri_str(uri)
        .await
        .expect("mongo client for drop");
    client
        .database(database)
        .drop()
        .await
        .expect("drop test database");
}
