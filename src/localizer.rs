//! Localization microservice client.
//!
//! Boom-gw publishes a [`LocalizeRequest`] to a Kafka topic whenever it
//! opens a superevent (or promotes a higher-SNR preferred event). A
//! Python `bayestar-service` companion process — see the
//! `bayestar-service/` directory in this repository — consumes those
//! requests, runs `ligo.skymap.bayestar.localize` to produce a HEALPix
//! MOC FITS sky map, and publishes a [`LocalizeResult`] back on a
//! second topic. Boom-gw subscribes to that result topic, correlates
//! the response with the open superevent by `superevent_id` /
//! `request_id`, and attaches the sky map.
//!
//! The architecture deliberately keeps the LIGO LALSuite / Python
//! runtime out of the Rust process. The two sides communicate over
//! JSON-encoded Kafka messages and never share a process image.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, warn};

/// Topic boom-gw publishes localize requests on. The bayestar-service
/// must subscribe to it.
pub const DEFAULT_REQUEST_TOPIC: &str = "boom-gw.localize-request";

/// Topic the bayestar-service publishes localize results on. Boom-gw
/// must subscribe to it.
pub const DEFAULT_RESULT_TOPIC: &str = "boom-gw.localize-result";

/// One localization request. The `coinc_xml` field carries the full
/// LIGO_LW coinc.xml document, base64-encoded, exactly as it arrived
/// on the GraceDB Kafka topic — the bayestar-service hands the
/// decoded bytes straight to `ligo.skymap.io.events.ligolw.open`
/// without re-parsing on our side.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalizeRequest {
    /// Caller-chosen ID echoed back in the result for correlation.
    pub request_id: String,
    /// Open superevent the result should attach to.
    pub superevent_id: String,
    /// The graceid of the preferred event the localization was
    /// triggered from. Useful for logs and for fall-back correlation if
    /// the request_id round-trip is broken.
    pub graceid: String,
    /// Source pipeline (`gstlal`, `mbta`, …) — diagnostic only.
    pub pipeline: String,
    /// Base64-encoded LIGO_LW coinc.xml.
    pub coinc_xml: String,
}

impl LocalizeRequest {
    /// Construct a request from the raw coinc.xml bytes. The bytes are
    /// base64-encoded for the JSON envelope; the bayestar-service
    /// decodes them and feeds them straight to BAYESTAR.
    pub fn from_coinc_xml(
        request_id: impl Into<String>,
        superevent_id: impl Into<String>,
        graceid: impl Into<String>,
        pipeline: impl Into<String>,
        coinc_xml_bytes: &[u8],
    ) -> Self {
        Self {
            request_id: request_id.into(),
            superevent_id: superevent_id.into(),
            graceid: graceid.into(),
            pipeline: pipeline.into(),
            coinc_xml: BASE64.encode(coinc_xml_bytes),
        }
    }

    /// Decode the embedded coinc.xml bytes (mostly for tests and the
    /// bayestar-service's reference Rust replays).
    pub fn coinc_xml_bytes(&self) -> Result<Vec<u8>, LocalizerError> {
        Ok(BASE64.decode(self.coinc_xml.as_bytes())?)
    }
}

/// One localization result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalizeResult {
    pub request_id: String,
    pub superevent_id: String,
    pub graceid: String,
    pub status: LocalizeStatus,
    /// Base64-encoded HEALPix MOC FITS sky map. Present on
    /// [`LocalizeStatus::Ok`], `None` otherwise.
    #[serde(default)]
    pub skymap_fits: Option<String>,
    /// Human-readable error message. Present on
    /// [`LocalizeStatus::Error`], `None` otherwise.
    #[serde(default)]
    pub error_message: Option<String>,
    /// Wall-clock time spent in the BAYESTAR call on the bayestar
    /// service side, in milliseconds.
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalizeStatus {
    Ok,
    Error,
}

impl LocalizeResult {
    /// Decode the embedded FITS bytes if the result was successful.
    pub fn skymap_fits_bytes(&self) -> Result<Option<Vec<u8>>, LocalizerError> {
        match self.skymap_fits.as_deref() {
            Some(s) => Ok(Some(BASE64.decode(s.as_bytes())?)),
            None => Ok(None),
        }
    }
}

#[derive(Debug, Error)]
pub enum LocalizerError {
    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("base64 decode error: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("delivery failed: {0}")]
    Delivery(String),
}

/// Configuration for the [`LocalizerClient`].
#[derive(Debug, Clone)]
pub struct LocalizerClientConfig {
    pub bootstrap_servers: String,
    pub request_topic: String,
    pub timeout: Duration,
}

impl LocalizerClientConfig {
    pub fn new(bootstrap_servers: impl Into<String>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            request_topic: DEFAULT_REQUEST_TOPIC.into(),
            timeout: Duration::from_secs(5),
        }
    }
}

/// Publisher of [`LocalizeRequest`]s. Boom-gw constructs one of these
/// at startup and calls [`Self::submit`] from the clusterer whenever a
/// superevent's preferred event changes.
pub struct LocalizerClient {
    producer: FutureProducer,
    topic: String,
    timeout: Duration,
}

impl LocalizerClient {
    pub fn new(config: LocalizerClientConfig) -> Result<Self, LocalizerError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .set("batch.size", "16384")
            .set("linger.ms", "5")
            .set("acks", "1")
            .set("retries", "3")
            // Per-superevent ordering is important: the bayestar-service
            // should process create / prefer events in the order
            // boom-gw emitted them. Keying by superevent_id at the
            // producer level lands them on the same partition.
            .set("max.in.flight.requests.per.connection", "1")
            .create()?;
        Ok(Self {
            producer,
            topic: config.request_topic,
            timeout: config.timeout,
        })
    }

    /// Serialize `req` to JSON and publish it. Returns once the broker
    /// acknowledges or the timeout fires. Keyed by `superevent_id` so
    /// the bayestar-service can guarantee per-superevent ordering.
    pub async fn submit(&self, req: &LocalizeRequest) -> Result<(), LocalizerError> {
        let key = req.superevent_id.clone();
        let payload = serde_json::to_vec(req)?;
        let record = FutureRecord::to(&self.topic).key(&key).payload(&payload);
        match self.producer.send(record, self.timeout).await {
            Ok(delivery) => {
                debug!(
                    topic = %self.topic,
                    partition = delivery.partition,
                    offset = delivery.offset,
                    superevent = %req.superevent_id,
                    request_id = %req.request_id,
                    "published localize request"
                );
                Ok(())
            }
            Err((err, _)) => Err(LocalizerError::from(err)),
        }
    }
}

/// Configuration for the [`LocalizerResultConsumer`].
#[derive(Debug, Clone)]
pub struct LocalizerResultConsumerConfig {
    pub bootstrap_servers: String,
    pub result_topic: String,
    pub group_id: String,
    /// Where to start when no committed offset exists. Defaults to
    /// `"earliest"` so a clusterer that briefly fell behind still
    /// picks up the results queued while it was down.
    pub auto_offset_reset: String,
    /// Poll timeout per iteration on the background thread.
    pub poll_timeout: Duration,
}

impl LocalizerResultConsumerConfig {
    pub fn new(bootstrap_servers: impl Into<String>, group_id: impl Into<String>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            result_topic: DEFAULT_RESULT_TOPIC.into(),
            group_id: group_id.into(),
            auto_offset_reset: "earliest".into(),
            poll_timeout: Duration::from_millis(500),
        }
    }
}

/// Handle to a running background result consumer. The
/// [`Self::try_recv`] method drains decoded [`LocalizeResult`]s from
/// the channel without blocking; the channel buffers up to 1024
/// pending results before the background thread starts blocking.
pub struct LocalizerResultStream {
    rx: std::sync::mpsc::Receiver<LocalizeResult>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl LocalizerResultStream {
    /// Receive the next decoded result, blocking up to `timeout`.
    /// Returns `None` if no result arrives in that window. Used by the
    /// integration tests; production code should prefer
    /// [`Self::try_recv`] inside the main event loop.
    pub fn recv_timeout(&self, timeout: Duration) -> Option<LocalizeResult> {
        self.rx.recv_timeout(timeout).ok()
    }

    /// Non-blocking drain. Returns the next pending result, or `None`
    /// if the channel is currently empty.
    pub fn try_recv(&self) -> Option<LocalizeResult> {
        self.rx.try_recv().ok()
    }

    /// Stop the background thread and join it. Drops any pending
    /// undelivered results.
    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for LocalizerResultStream {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Background subscriber to the localize-result topic. Decoded
/// [`LocalizeResult`]s are pushed into an mpsc channel; the main
/// clusterer loop calls [`LocalizerResultStream::try_recv`] between
/// inbound-event polls to attach FITS bytes to open superevents.
pub struct LocalizerResultConsumer;

impl LocalizerResultConsumer {
    /// Spawn a background thread that subscribes to the configured
    /// topic and forwards decoded results into the returned stream.
    /// The thread shuts down when [`LocalizerResultStream::shutdown`]
    /// is called or the stream is dropped.
    pub fn spawn(
        config: LocalizerResultConsumerConfig,
    ) -> Result<LocalizerResultStream, LocalizerError> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("group.id", &config.group_id)
            .set("auto.offset.reset", &config.auto_offset_reset)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "10000")
            .create()?;
        consumer.subscribe(&[&config.result_topic])?;

        let (tx, rx) = std::sync::mpsc::sync_channel::<LocalizeResult>(1024);
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = stop.clone();
        let topic = config.result_topic.clone();
        let timeout = config.poll_timeout;
        let handle = thread::spawn(move || {
            while !stop_thread.load(Ordering::Relaxed) {
                match consumer.poll(timeout) {
                    Some(Ok(msg)) => {
                        let Some(payload) = msg.payload() else {
                            continue;
                        };
                        match serde_json::from_slice::<LocalizeResult>(payload) {
                            Ok(result) => {
                                debug!(
                                    topic = %topic,
                                    request_id = %result.request_id,
                                    superevent = %result.superevent_id,
                                    "received localize result"
                                );
                                if tx.send(result).is_err() {
                                    // Receiver dropped — stop.
                                    break;
                                }
                            }
                            Err(e) => {
                                warn!("failed to decode localize result on {topic}: {e}");
                            }
                        }
                    }
                    Some(Err(e)) => warn!("kafka error on {topic}: {e}"),
                    None => {}
                }
            }
        });

        Ok(LocalizerResultStream {
            rx,
            stop,
            handle: Some(handle),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_xml() -> Vec<u8> {
        b"<?xml version='1.0'?><LIGO_LW></LIGO_LW>".to_vec()
    }

    #[test]
    fn request_round_trips_through_json() {
        let req =
            LocalizeRequest::from_coinc_xml("req-abc", "S000001", "G42", "gstlal", &dummy_xml());
        let bytes = serde_json::to_vec(&req).unwrap();
        let restored: LocalizeRequest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(restored.request_id, "req-abc");
        assert_eq!(restored.superevent_id, "S000001");
        assert_eq!(restored.graceid, "G42");
        assert_eq!(restored.pipeline, "gstlal");
        assert_eq!(restored.coinc_xml_bytes().unwrap(), dummy_xml());
    }

    #[test]
    fn request_json_shape_matches_python_expectations() {
        // The bayestar-service is keyed to these exact field names.
        // Lock them in here so a Rust-side rename of any field surfaces
        // immediately as a Rust test failure rather than as a quiet
        // production deserialization error on the Python side.
        let req = LocalizeRequest::from_coinc_xml("req-1", "S0", "G7", "mbta", b"<x/>");
        let v: serde_json::Value = serde_json::to_value(&req).unwrap();
        let obj = v.as_object().unwrap();
        let keys: std::collections::BTreeSet<&str> = obj.keys().map(String::as_str).collect();
        let expected: std::collections::BTreeSet<&str> = [
            "request_id",
            "superevent_id",
            "graceid",
            "pipeline",
            "coinc_xml",
        ]
        .into_iter()
        .collect();
        assert_eq!(keys, expected, "request schema field set drifted");
        assert_eq!(v["coinc_xml"], "PHgvPg==");
    }

    #[test]
    fn ok_result_round_trips_with_skymap() {
        let fits_bytes = b"SIMPLE  =                    T".to_vec();
        let result = LocalizeResult {
            request_id: "req-1".into(),
            superevent_id: "S0".into(),
            graceid: "G7".into(),
            status: LocalizeStatus::Ok,
            skymap_fits: Some(BASE64.encode(&fits_bytes)),
            error_message: None,
            elapsed_ms: 137,
        };
        let bytes = serde_json::to_vec(&result).unwrap();
        let restored: LocalizeResult = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(restored.status, LocalizeStatus::Ok);
        assert_eq!(restored.skymap_fits_bytes().unwrap().unwrap(), fits_bytes);
    }

    #[test]
    fn error_result_round_trips_without_skymap() {
        let bytes = br#"{
            "request_id": "req-1",
            "superevent_id": "S0",
            "graceid": "G7",
            "status": "error",
            "error_message": "PSDs missing",
            "elapsed_ms": 12
        }"#;
        let restored: LocalizeResult = serde_json::from_slice(bytes).unwrap();
        assert_eq!(restored.status, LocalizeStatus::Error);
        assert_eq!(restored.error_message.as_deref(), Some("PSDs missing"));
        assert!(restored.skymap_fits_bytes().unwrap().is_none());
    }

    #[test]
    fn result_status_serializes_lowercase() {
        let ok = serde_json::to_value(LocalizeStatus::Ok).unwrap();
        let err = serde_json::to_value(LocalizeStatus::Error).unwrap();
        assert_eq!(ok, serde_json::Value::String("ok".into()));
        assert_eq!(err, serde_json::Value::String("error".into()));
    }
}
