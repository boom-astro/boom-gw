//! End-to-end public-alert publish test.
//!
//! Marked `#[ignore]` so a plain `cargo test` skips it; the GitHub
//! Actions `integration-kafka` job spins up Kafka + Mongo + the stub
//! bayestar-service and then runs this with `cargo test -- --ignored`.
//!
//! What it exercises:
//!
//! 1. Seed the archive with one event, one superevent (with FITS
//!    attached), and one `p_astro` annotation.
//! 2. Stand up the in-process API with a real [`AlertPublisher`]
//!    pointed at the local Kafka.
//! 3. Subscribe a `BaseConsumer` to the alert topic.
//! 4. `POST /api/superevents/{id}/alerts` with `alert_type=PRELIMINARY`.
//! 5. Assert that (a) the audit row landed with `published=true`,
//!    (b) the message landed on the Kafka topic, and (c) the JSON
//!    body matches the IGWN public-alert shape (superevent_id,
//!    alert_type, event.{instruments, classification, skymap}).

use std::time::{Duration, Instant};

use actix_web::{test, web, App};
use boom_gw::{
    api, api::MaybeAlertPublisher, stub_principal_middleware, AlertPublisher, AlertPublisherConfig,
    AnnotationDoc, Archive, ArchiveConfig, SkyMapFits, Superevent,
};
use igwn_ligolw::CoincInspiralEvent;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use serde_json::Value;

const ALERT_TOPIC: &str = "boom-gw.alerts.integration";

fn mongo_uri() -> String {
    std::env::var("BOOM_GW_MONGO_URI").unwrap_or_else(|_| "mongodb://localhost:27017".into())
}

fn kafka_bootstrap() -> String {
    std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".into())
}

fn dummy_event(graceid: &str) -> boom_gw::GwEvent {
    let coinc = CoincInspiralEvent {
        coinc_event_id: graceid.into(),
        ifos: "H1,L1".into(),
        combined_far: 1e-12,
        snr: 15.0,
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
        snr: 15.0,
        far: 1e-12,
        mchirp: None,
        total_mass: None,
        coinc,
    }
}

#[actix_web::test]
#[ignore]
async fn alert_publish_lands_on_kafka_and_persists_audit() {
    let uri = mongo_uri();
    let bootstrap = kafka_bootstrap();
    let pid = std::process::id();

    // Clean per-process db.
    let mut cfg = ArchiveConfig::new(&uri);
    cfg.database = format!("boom_gw_alert_test_{pid}");
    let raw = mongodb::Client::with_uri_str(&uri).await.unwrap();
    raw.database(&cfg.database).drop().await.unwrap();
    let archive = Archive::connect(cfg.clone()).await.unwrap();

    // Seed event + superevent (with FITS) + p_astro annotation.
    let graceid = format!("G_alert_{pid}");
    let event = dummy_event(&graceid);
    archive.record_event(&event).await.unwrap();

    let superevent_id = format!("S_alert_{pid}");
    let fits_bytes = b"FITS-ALERT-TEST".to_vec();
    let s = Superevent {
        id: superevent_id.clone(),
        t_0: 1_400_000_000.0,
        t_start: 1_399_999_997.5,
        t_end: 1_400_000_002.5,
        preferred_event: event.clone(),
        g_events: vec![event.clone()],
        skymap: Some(SkyMapFits {
            bytes: fits_bytes.clone(),
            elapsed_ms: 137,
        }),
    };
    archive.upsert_superevent(&s).await.unwrap();

    // Seed the FITS bytes into the SkymapStorage too — the alert
    // builder reads bytes from there now (post-PR #313-style
    // refactor), not from SupereventDoc.
    use boom_gw::storage::skymap::{build_storage, SkymapBackendKind, SkymapBlob};
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

    let p_astro_payload = serde_json::json!({
        "BNS": 0.0,
        "NSBH": 0.0,
        "BBH": 0.99,
        "Terrestrial": 0.01,
    });
    let bson_payload = mongodb::bson::to_bson(&p_astro_payload).unwrap();
    let annotation = AnnotationDoc::new(&superevent_id, "p_astro", "ml-classifier", bson_payload);
    archive.insert_annotation(&annotation).await.unwrap();

    // Subscribe a Kafka consumer *before* we POST so the assignment
    // is in place by the time the publisher acknowledges.
    let group = format!("integration-alert-{pid}");
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", &group)
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "10000")
        .create()
        .unwrap();
    consumer.subscribe(&[ALERT_TOPIC]).unwrap();
    for _ in 0..30 {
        consumer.poll(Duration::from_millis(200));
        if !consumer
            .assignment()
            .map(|a| a.count() == 0)
            .unwrap_or(true)
        {
            break;
        }
    }

    // Stand up the API with a real AlertPublisher on a test-only
    // topic.
    let mut pub_cfg = AlertPublisherConfig::new(&bootstrap);
    pub_cfg.topic = ALERT_TOPIC.into();
    let publisher = AlertPublisher::new(pub_cfg).expect("build AlertPublisher");

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(archive.clone()))
            .app_data(web::Data::new(MaybeAlertPublisher(Some(publisher))))
            .app_data(actix_web::web::Data::from(skymap_storage.clone()))
            .wrap(actix_web::middleware::from_fn(stub_principal_middleware))
            .configure(api::configure),
    )
    .await;

    // POST a real (non-dry-run) PRELIMINARY alert.
    let req = test::TestRequest::post()
        .uri(&format!("/api/superevents/{superevent_id}/alerts"))
        .set_json(serde_json::json!({"alert_type": "PRELIMINARY"}))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 201);
    let body: Value = test::read_body_json(resp).await;
    assert_eq!(body["data"]["audit"]["published"], true);

    // Poll the topic until we see our alert or 30 s elapses.
    let deadline = Instant::now() + Duration::from_secs(30);
    let alert = loop {
        if Instant::now() > deadline {
            panic!("timed out waiting for alert on topic {ALERT_TOPIC}");
        }
        match consumer.poll(Duration::from_secs(2)) {
            Some(Ok(msg)) => {
                let Some(payload) = msg.payload() else {
                    continue;
                };
                let parsed: Value = match serde_json::from_slice(payload) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("decode error on alert payload: {e}");
                        continue;
                    }
                };
                if parsed["superevent_id"].as_str() == Some(superevent_id.as_str()) {
                    break parsed;
                }
                eprintln!(
                    "skipping unrelated alert superevent_id={} (waiting for {superevent_id})",
                    parsed["superevent_id"]
                );
            }
            Some(Err(e)) => eprintln!("kafka error: {e}"),
            None => {}
        }
    };

    // IGWN schema checks.
    assert_eq!(alert["alert_type"], "PRELIMINARY");
    assert_eq!(alert["event"]["pipeline"], "gstlal");
    assert_eq!(alert["event"]["group"], "CBC");
    assert_eq!(alert["event"]["search"], "AllSky");
    let ifos: Vec<&str> = alert["event"]["instruments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(ifos.contains(&"H1") && ifos.contains(&"L1"));
    assert_eq!(alert["event"]["significant"], true);
    assert!(alert["event"]["classification"]["BBH"].as_f64().unwrap() > 0.9);
    assert!(alert["event"]["skymap"].is_string());

    // The audit row should be queryable through the API too.
    let req = test::TestRequest::get()
        .uri(&format!("/api/superevents/{superevent_id}/alerts"))
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let items = body["data"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["published"], true);
    assert_eq!(items[0]["alert_type"], "PRELIMINARY");

    raw.database(&cfg.database).drop().await.unwrap();
}
