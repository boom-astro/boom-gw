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
    DEFAULT_RESULT_TOPIC,
};
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
