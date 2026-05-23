//! End-to-end Kafka self-loop test.
//!
//! Marked `#[ignore]` so a plain `cargo test` skips it; the GitHub
//! Actions `integration-kafka` job spins up a single-broker Kafka,
//! runs `bayestar-service --stub` against it, and invokes this test
//! with `cargo test -- --ignored`.
//!
//! Required environment:
//!
//! * `KAFKA_BOOTSTRAP_SERVERS` — bootstrap host:port (default localhost:9092)
//! * `bayestar-service` running with `--stub` against the same broker
//!
//! The test publishes a synthetic [`LocalizeRequest`] on the request
//! topic, waits for a [`LocalizeResult`] on the result topic, and
//! asserts the stub FITS payload, status, and IDs come back intact.

use std::time::{Duration, Instant};

use boom_gw::{
    LocalizeRequest, LocalizeResult, LocalizeStatus, LocalizerClient, LocalizerClientConfig,
    LocalizerResultConsumer, LocalizerResultConsumerConfig, SupereventCreator, SupereventUpdate,
    DEFAULT_RESULT_TOPIC,
};
use igwn_ligolw::CoincInspiralEvent;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;

/// The canned FITS bytes the Python `--stub` localizer emits. Mirrored
/// here so the assertion fails loudly if either side drifts.
const STUB_FITS_BYTES: &[u8] = b"STUB_FITS_PAYLOAD_for_integration_tests";

fn bootstrap_servers() -> String {
    std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".into())
}

#[test]
#[ignore]
fn kafka_self_loop_publishes_request_and_receives_stub_result() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let bootstrap = bootstrap_servers();
    let request_id = format!("integration-{}", std::process::id());
    let superevent_id = format!("S{}", std::process::id());

    // Result consumer first so it is subscribed before we publish.
    let group = format!("integration-test-result-{}", std::process::id());
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", &group)
        // earliest so we do not race a slow rebalance after subscribe.
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "10000")
        .create()
        .expect("create result consumer");
    consumer
        .subscribe(&[DEFAULT_RESULT_TOPIC])
        .expect("subscribe to result topic");

    // Burn the first poll to drive subscription assignment before the
    // request is published; otherwise the broker has not finished
    // joining this consumer to the group when our message lands and
    // the stub-service response is published before we are listening.
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

    // Publish the request.
    let client = LocalizerClient::new(LocalizerClientConfig::new(&bootstrap))
        .expect("build LocalizerClient");
    let req = LocalizeRequest::from_coinc_xml(
        &request_id,
        &superevent_id,
        "G42",
        "gstlal",
        b"<?xml version='1.0'?><LIGO_LW></LIGO_LW>",
    );
    rt.block_on(client.submit(&req)).expect("publish request");

    // Poll the result topic until our request_id round-trips back, or
    // 60 s elapses. We deliberately filter by `request_id` rather than
    // accepting the first message we see, so concurrent CI jobs do not
    // shadow each other.
    let deadline = Instant::now() + Duration::from_secs(60);
    let result = loop {
        if Instant::now() > deadline {
            panic!("timed out waiting for LocalizeResult with request_id={request_id}");
        }
        match consumer.poll(Duration::from_secs(2)) {
            Some(Ok(msg)) => {
                let Some(payload) = msg.payload() else {
                    continue;
                };
                let result: LocalizeResult = match serde_json::from_slice(payload) {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("decode error on result payload: {e}");
                        continue;
                    }
                };
                if result.request_id == request_id {
                    break result;
                }
                eprintln!(
                    "skipping unrelated result request_id={} (waiting for {request_id})",
                    result.request_id
                );
            }
            Some(Err(e)) => eprintln!("kafka error: {e}"),
            None => {}
        }
    };

    assert_eq!(result.superevent_id, superevent_id);
    assert_eq!(result.graceid, "G42");
    assert_eq!(result.status, LocalizeStatus::Ok);
    let fits = result
        .skymap_fits_bytes()
        .expect("decode skymap_fits")
        .expect("status=ok carries skymap_fits");
    assert_eq!(
        fits, STUB_FITS_BYTES,
        "stub FITS bytes drifted between Python and Rust sides"
    );
}

/// Drive the full clusterer-side wiring: build a `SupereventCreator`,
/// process a synthetic event into a Superevent, fire a
/// `LocalizeRequest` via the high-level `LocalizerClient`, then receive
/// the response on the background `LocalizerResultConsumer` stream and
/// confirm `attach_skymap` lands the FITS on the open superevent.
///
/// This is the end-to-end version of the localize round-trip: rather
/// than asserting on a raw `LocalizeResult`, we assert that the BOOM
/// in-memory superevent state has the sky map attached.
#[test]
#[ignore]
fn clusterer_round_trip_attaches_skymap_to_superevent() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let bootstrap = bootstrap_servers();
    let pid = std::process::id();
    let group = format!("integration-test-attach-{pid}");

    // Spawn the background result-consumer first so its assignment is
    // settled before we publish.
    let mut consumer_cfg = LocalizerResultConsumerConfig::new(&bootstrap, &group);
    consumer_cfg.poll_timeout = Duration::from_millis(200);
    let stream =
        LocalizerResultConsumer::spawn(consumer_cfg).expect("spawn LocalizerResultConsumer");
    // Burn a few hundred ms so the background consumer has assigned a
    // partition before we publish.
    std::thread::sleep(Duration::from_millis(500));

    // Process the event through a SupereventCreator to create a real
    // open superevent. We use a coinc.xml stub keyed to the same id we
    // pass to attach_skymap below.
    let mut creator = SupereventCreator::with_default_window();
    let graceid = format!("G_attach_{pid}");
    let event = synthetic_event(&graceid, 1_400_000_000.0, 10.0);
    let update = creator.process(event.clone());
    let superevent_id = match update {
        SupereventUpdate::Created { superevent } => superevent.id,
        other => panic!("expected Created, got {other:?}"),
    };

    // Publish a localize request keyed to that superevent_id; the
    // stub bayestar-service will reply with STUB_FITS_BYTES.
    let client = LocalizerClient::new(LocalizerClientConfig::new(&bootstrap))
        .expect("build LocalizerClient");
    let request_id = format!("req-attach-{pid}");
    let req = LocalizeRequest::from_coinc_xml(
        &request_id,
        &superevent_id,
        &graceid,
        "gstlal",
        b"<?xml version='1.0'?><LIGO_LW></LIGO_LW>",
    );
    rt.block_on(client.submit(&req)).expect("publish request");

    // Drain the background channel until our request_id surfaces, or
    // 60 s elapses.
    let deadline = Instant::now() + Duration::from_secs(60);
    let result = loop {
        if Instant::now() > deadline {
            panic!("timed out waiting for LocalizeResult request_id={request_id}");
        }
        if let Some(r) = stream.recv_timeout(Duration::from_secs(2)) {
            if r.request_id == request_id {
                break r;
            }
            eprintln!(
                "skipping unrelated result request_id={} (waiting for {request_id})",
                r.request_id
            );
        }
    };

    // Attach via the clustering-level API and assert the FITS reaches
    // the superevent.
    assert_eq!(result.status, LocalizeStatus::Ok);
    let fits = result
        .skymap_fits_bytes()
        .expect("decode skymap_fits")
        .expect("status=ok carries skymap_fits");
    assert_eq!(fits, STUB_FITS_BYTES);

    let update = creator
        .attach_skymap(&superevent_id, fits.clone(), result.elapsed_ms)
        .expect("attach_skymap finds the open superevent");
    match update {
        SupereventUpdate::SkymapAttached { superevent } => {
            assert_eq!(superevent.id, superevent_id);
            let sky = superevent.skymap.expect("skymap attached");
            assert_eq!(sky.bytes, STUB_FITS_BYTES);
        }
        other => panic!("expected SkymapAttached, got {other:?}"),
    }

    // And the creator's own view of the open superevent must carry
    // the FITS now too.
    let stored = creator
        .superevents()
        .find(|s| s.id == superevent_id)
        .unwrap();
    assert!(stored.skymap.is_some());

    stream.shutdown();
}

/// Build a `GwEvent` with the bare-minimum coinc_inspiral fields the
/// clustering layer needs. Tests only — we never serialize this
/// upstream.
fn synthetic_event(graceid: &str, end_time: f64, snr: f64) -> boom_gw::GwEvent {
    let coinc = CoincInspiralEvent {
        coinc_event_id: graceid.into(),
        ifos: "H1,L1".into(),
        combined_far: 1e-9,
        snr,
        mass: None,
        mchirp: None,
        end_time,
        sngls: vec![],
    };
    boom_gw::GwEvent {
        pipeline: "gstlal".into(),
        graceid: graceid.into(),
        producer_timestamp: 0.0,
        message_type: "new".into(),
        submitter: "ci".into(),
        end_time,
        ifos: "H1,L1".into(),
        snr,
        far: 1e-9,
        mchirp: None,
        total_mass: None,
        coinc,
    }
}
