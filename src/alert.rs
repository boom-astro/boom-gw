//! Public GW alert assembly and publishing.
//!
//! Produces JSON messages matching the IGWN public-alert schema as
//! documented for the GraceDB `igwn.gwalert` topic (preliminary /
//! initial / update / retraction). Boom-gw's role here is the
//! upstream assembler: turn a [`Superevent`] plus the
//! [`AnnotationDoc`]s attached to it (`p_astro` classification, etc.)
//! and any sky-map FITS into a single JSON document keyed by
//! `superevent_id`, and publish it on a Kafka topic.
//!
//! Production authentication to `kafka.gcn.nasa.gov` (SCRAM/SSL) is
//! **not** wired up here — the publisher takes a bootstrap-server
//! configuration so we test against the self-loop Kafka and the
//! production credentials slot in via additional `ClientConfig`
//! settings later.
//!
//! The schema implemented matches the JSON form used on the GraceDB
//! `igwn.gwalert` topic. Field names and semantics:
//!
//! * `alert_type` — `"PRELIMINARY"`, `"INITIAL"`, `"UPDATE"`, or `"RETRACTION"`.
//! * `time_created` — RFC 3339 UTC timestamp the alert was assembled.
//! * `superevent_id` — boom-gw's superevent id (`S000000`-style for now).
//! * `urls.gracedb` — gracedb superevent page; empty until we publish there.
//! * `event.time` — preferred event's GPS time, converted to UTC.
//! * `event.far` — preferred event's combined FAR (Hz).
//! * `event.significant` — boolean, `true` when `far < 1 / 30 days`.
//! * `event.instruments` — IFOs parsed from the preferred event.
//! * `event.group` — `"CBC"` (the only pipeline class we ingest today).
//! * `event.pipeline` — preferred event's source pipeline.
//! * `event.search` — `"AllSky"` placeholder.
//! * `event.classification` — populated from a `kind="p_astro"`
//!   annotation if one exists on the superevent.
//! * `event.skymap` — base64-encoded MOC FITS from `Superevent.skymap`,
//!   if attached.

use std::time::Duration;

use mongodb::bson;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::debug;

use crate::archive::AnnotationDoc;
use crate::clustering::Superevent;

/// Topic boom-gw publishes its assembled public alerts on. Operators
/// override per-environment; the default mirrors the GraceDB topic
/// name for symmetry but **does not** authenticate against any real
/// production broker.
pub const DEFAULT_ALERT_TOPIC: &str = "igwn.gwalert";

/// Unix-epoch offset of GPS 0 (1980-01-06 00:00:00 UTC).
const GPS_EPOCH_UNIX: i64 = 315_964_800;
/// Leap seconds added between the GPS and UTC timescales as of 2017.
/// No leap second has been added since 2017-01-01; if a future one is
/// announced this constant must be bumped before the boundary day.
const LEAP_SECONDS: i64 = 18;

/// Significance threshold used to populate `event.significant`.
/// Matches the LVK preliminary-significant threshold (1 per 30 days).
const SIGNIFICANT_FAR_HZ: f64 = 1.0 / (30.0 * 86_400.0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum AlertType {
    Preliminary,
    Initial,
    Update,
    Retraction,
}

impl AlertType {
    pub fn as_str(self) -> &'static str {
        match self {
            AlertType::Preliminary => "PRELIMINARY",
            AlertType::Initial => "INITIAL",
            AlertType::Update => "UPDATE",
            AlertType::Retraction => "RETRACTION",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicAlert {
    pub alert_type: AlertType,
    pub time_created: String,
    pub superevent_id: String,
    pub urls: AlertUrls,
    pub event: AlertEvent,
    /// Optional joint-detection block. Always `None` for boom-gw today;
    /// reserved so the wire shape matches the IGWN schema.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_coinc: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertUrls {
    /// GraceDB superevent page — empty until boom-gw publishes there.
    pub gracedb: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEvent {
    pub time: String,
    pub far: f64,
    pub significant: bool,
    pub instruments: Vec<String>,
    pub group: String,
    pub pipeline: String,
    pub search: String,
    /// Populated from a `kind="p_astro"` annotation when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub classification: Option<serde_json::Value>,
    /// Base64-encoded MOC FITS payload (when the superevent has one).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skymap: Option<String>,
}

#[derive(Debug, Error)]
pub enum AlertError {
    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("bson error: {0}")]
    Bson(#[from] bson::ser::Error),
    #[error("time conversion error: {0}")]
    Time(String),
}

/// Assemble a [`PublicAlert`] from the current state of a superevent
/// and the annotations attached to it.
pub fn build_alert(
    superevent: &Superevent,
    annotations: &[AnnotationDoc],
    alert_type: AlertType,
) -> Result<PublicAlert, AlertError> {
    let preferred = &superevent.preferred_event;
    let event_time = gps_to_utc_iso(preferred.end_time)?;
    let time_created = bson::DateTime::now()
        .try_to_rfc3339_string()
        .map_err(|e| AlertError::Time(format!("time_created: {e}")))?;

    let instruments: Vec<String> = preferred
        .ifos
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let classification = latest_payload_for(annotations, "p_astro");

    let skymap = superevent.skymap.as_ref().map(|sky| {
        use base64::engine::general_purpose::STANDARD as BASE64;
        use base64::Engine as _;
        BASE64.encode(&sky.bytes)
    });

    Ok(PublicAlert {
        alert_type,
        time_created,
        superevent_id: superevent.id.clone(),
        urls: AlertUrls {
            gracedb: String::new(),
        },
        event: AlertEvent {
            time: event_time,
            far: preferred.far,
            significant: preferred.far < SIGNIFICANT_FAR_HZ,
            instruments,
            group: "CBC".into(),
            pipeline: preferred.pipeline.clone(),
            search: "AllSky".into(),
            classification,
            skymap,
        },
        external_coinc: None,
    })
}

/// Convert a GPS time (seconds since 1980-01-06 UTC) to an RFC 3339
/// UTC string, applying the static leap-second offset.
fn gps_to_utc_iso(gps_seconds: f64) -> Result<String, AlertError> {
    let unix_ms = ((gps_seconds + (GPS_EPOCH_UNIX - LEAP_SECONDS) as f64) * 1000.0) as i64;
    bson::DateTime::from_millis(unix_ms)
        .try_to_rfc3339_string()
        .map_err(|e| AlertError::Time(format!("event.time: {e}")))
}

/// Return the `payload` field of the most-recent annotation whose
/// `kind` matches, serialized as `serde_json::Value`. Returns `None`
/// when no annotation of that kind exists or its payload cannot be
/// represented in JSON.
fn latest_payload_for(annotations: &[AnnotationDoc], kind: &str) -> Option<serde_json::Value> {
    annotations
        .iter()
        .filter(|a| a.kind == kind)
        .max_by_key(|a| a.created_at)
        .map(|a| a.payload.clone().into_relaxed_extjson())
}

/// Configuration for the [`AlertPublisher`].
#[derive(Debug, Clone)]
pub struct AlertPublisherConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    pub timeout: Duration,
}

impl AlertPublisherConfig {
    pub fn new(bootstrap_servers: impl Into<String>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            topic: DEFAULT_ALERT_TOPIC.into(),
            timeout: Duration::from_secs(5),
        }
    }
}

/// Publishes [`PublicAlert`]s to a Kafka topic. Keyed by
/// `superevent_id` so consumers can do per-superevent ordering on a
/// single partition.
pub struct AlertPublisher {
    producer: FutureProducer,
    topic: String,
    timeout: Duration,
}

impl AlertPublisher {
    pub fn new(config: AlertPublisherConfig) -> Result<Self, AlertError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .set("acks", "1")
            .set("retries", "3")
            .create()?;
        Ok(Self {
            producer,
            topic: config.topic,
            timeout: config.timeout,
        })
    }

    pub async fn publish(&self, alert: &PublicAlert) -> Result<(), AlertError> {
        let key = alert.superevent_id.clone();
        let payload = serde_json::to_vec(alert)?;
        let record = FutureRecord::to(&self.topic).key(&key).payload(&payload);
        match self.producer.send(record, self.timeout).await {
            Ok(delivery) => {
                debug!(
                    topic = %self.topic,
                    partition = delivery.partition,
                    offset = delivery.offset,
                    superevent = %key,
                    alert_type = alert.alert_type.as_str(),
                    "published public alert"
                );
                Ok(())
            }
            Err((err, _)) => Err(AlertError::from(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clustering::SkyMapFits;
    use igwn_ligolw::CoincInspiralEvent;

    fn dummy_superevent(snr: f64, far: f64, has_skymap: bool) -> Superevent {
        let coinc = CoincInspiralEvent {
            coinc_event_id: "G42".into(),
            ifos: "H1,L1".into(),
            combined_far: far,
            snr,
            mass: None,
            mchirp: None,
            end_time: 1_400_000_000.0,
            sngls: vec![],
        };
        let event = crate::event::GwEvent {
            pipeline: "gstlal".into(),
            graceid: "G42".into(),
            producer_timestamp: 0.0,
            message_type: "new".into(),
            submitter: "ci".into(),
            end_time: 1_400_000_000.0,
            ifos: "H1,L1".into(),
            snr,
            far,
            mchirp: None,
            total_mass: None,
            coinc,
        };
        Superevent {
            id: "S000001".into(),
            t_0: 1_400_000_000.0,
            t_start: 1_399_999_997.5,
            t_end: 1_400_000_002.5,
            preferred_event: event.clone(),
            g_events: vec![event],
            skymap: if has_skymap {
                Some(SkyMapFits {
                    bytes: b"FITS-PAYLOAD".to_vec(),
                    elapsed_ms: 137,
                })
            } else {
                None
            },
        }
    }

    #[test]
    fn alert_type_serializes_uppercase() {
        assert_eq!(
            serde_json::to_value(AlertType::Preliminary).unwrap(),
            serde_json::Value::String("PRELIMINARY".into())
        );
        assert_eq!(
            serde_json::to_value(AlertType::Retraction).unwrap(),
            serde_json::Value::String("RETRACTION".into())
        );
    }

    #[test]
    fn build_alert_fills_required_fields() {
        let s = dummy_superevent(12.0, 1e-12, true);
        let alert = build_alert(&s, &[], AlertType::Preliminary).unwrap();
        assert_eq!(alert.alert_type, AlertType::Preliminary);
        assert_eq!(alert.superevent_id, "S000001");
        assert_eq!(alert.event.instruments, vec!["H1", "L1"]);
        assert_eq!(alert.event.pipeline, "gstlal");
        assert_eq!(alert.event.group, "CBC");
        assert_eq!(alert.event.search, "AllSky");
        assert!(alert.event.significant, "1e-12 Hz is well below 1/30 days");
        assert!(alert.event.skymap.is_some());
    }

    #[test]
    fn build_alert_marks_high_far_as_not_significant() {
        // 1 Hz is enormously above the threshold.
        let s = dummy_superevent(12.0, 1.0, false);
        let alert = build_alert(&s, &[], AlertType::Preliminary).unwrap();
        assert!(!alert.event.significant);
        assert!(alert.event.skymap.is_none());
    }

    #[test]
    fn build_alert_picks_up_p_astro_annotation() {
        let s = dummy_superevent(12.0, 1e-12, false);
        let p_astro = serde_json::json!({
            "BNS": 0.05,
            "NSBH": 0.02,
            "BBH": 0.92,
            "Terrestrial": 0.01
        });
        let bson_payload = mongodb::bson::to_bson(&p_astro).unwrap();
        let annotation = AnnotationDoc::new(&s.id, "p_astro", "ml", bson_payload);
        let alert = build_alert(&s, &[annotation], AlertType::Initial).unwrap();
        let classification = alert.event.classification.expect("classification missing");
        assert!((classification["BBH"].as_f64().unwrap() - 0.92).abs() < 1e-9);
    }

    #[test]
    fn build_alert_event_time_is_rfc3339() {
        let s = dummy_superevent(12.0, 1e-12, false);
        let alert = build_alert(&s, &[], AlertType::Preliminary).unwrap();
        // RFC 3339 strings end with 'Z' or +HH:MM. bson emits 'Z'.
        assert!(
            alert.event.time.ends_with('Z') || alert.event.time.contains('+'),
            "got non-RFC-3339 time: {}",
            alert.event.time
        );
        assert!(alert.time_created.ends_with('Z') || alert.time_created.contains('+'));
    }
}
