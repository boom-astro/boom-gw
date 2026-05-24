//! Live Kafka consumer for GCN (Gamma-ray Coordinates Network)
//! alerts.
//!
//! Connects to `kafka.gcn.nasa.gov` (or any Kafka broker — see
//! `--bootstrap-servers` in `bin/gw_gcn_consumer.rs`), subscribes
//! to the requested Fermi-GBM notice topics, parses each payload
//! via [`crate::gcn`], and pushes the resulting [`GrbTrigger`] to
//! the caller's handler.
//!
//! Auth uses the OIDC OAUTHBEARER mechanism that librdkafka has
//! built in (the same path the official `gcn-kafka` Python client
//! uses under the hood). The caller supplies the GCN-issued
//! `client_id` / `client_secret`; librdkafka fetches and refreshes
//! the token transparently.
//!
//! Mirrors [`crate::kafka::GwAlertConsumer`] in shape — same
//! `run(handler)` API and stop-flag pattern — so wiring this into
//! a binary alongside the existing GW consumer is uniform.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::gcn::{fermi_instrument_for_voevent_topic, parse_fermi_gbm_json, parse_fermi_voevent};
use crate::grb::GrbTrigger;

/// Default OIDC token endpoint for the GCN broker. Override only
/// if you're targeting a non-NASA mirror.
pub const DEFAULT_GCN_TOKEN_URL: &str = "https://auth.gcn.nasa.gov/oauth2/token";

/// Default bootstrap servers for the production GCN Kafka broker.
pub const DEFAULT_GCN_BOOTSTRAP_SERVERS: &str = "kafka.gcn.nasa.gov:9092";

/// Modern Fermi-GBM JSON-notice topics. We default to subscribing
/// to all four trigger stages (flight, ground, final, subthreshold)
/// — operators downsample by ignoring lower-confidence prefixes if
/// they care about latency vs. accuracy trade-offs.
pub const DEFAULT_FERMI_GBM_TOPICS: &[&str] = &[
    "gcn.notices.fermi.gbm.flight_position",
    "gcn.notices.fermi.gbm.ground_position",
    "gcn.notices.fermi.gbm.final_position",
    "gcn.notices.fermi.gbm.subthreshold",
];

#[derive(Debug, Clone)]
pub struct GcnKafkaConfig {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub auto_offset_reset: String,
    pub poll_timeout: Duration,
    /// Subscribed topics. Each element should be a real Kafka topic
    /// name — for Fermi-GBM that means `gcn.notices.fermi.gbm.*`
    /// (JSON) or `gcn.classic.voevent.FERMI_GBM_*` (VOEvent XML).
    pub topics: Vec<String>,
    pub auth: GcnAuth,
}

/// How to authenticate to the broker.
#[derive(Debug, Clone)]
pub enum GcnAuth {
    /// Plaintext connection (`security.protocol=PLAINTEXT`,
    /// no SASL). Used for local Kafka in docker-compose and for
    /// the CI integration test. The real GCN broker only accepts
    /// OIDC.
    Plaintext,
    /// OIDC OAUTHBEARER over TLS — the production GCN
    /// authentication path. `client_id` / `client_secret` are
    /// issued at <https://gcn.nasa.gov/quickstart>.
    OidcOauthBearer {
        client_id: String,
        client_secret: String,
        token_url: String,
        /// Optional path to a CA bundle for the TLS handshake.
        ca_cert_path: Option<PathBuf>,
    },
}

#[derive(Debug, Error)]
pub enum GcnConsumerError {
    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
}

/// What the handler is called with for each successfully-decoded
/// alert. The topic is carried so handlers can disambiguate
/// JSON vs. VOEvent routing decisions and trace upstream sources.
#[derive(Debug, Clone)]
pub struct GcnAlert {
    pub topic: String,
    pub trigger: GrbTrigger,
}

pub struct GcnAlertConsumer {
    pub config: GcnKafkaConfig,
    stop: Arc<AtomicBool>,
}

impl GcnAlertConsumer {
    pub fn new(config: GcnKafkaConfig) -> Self {
        Self {
            config,
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn stop_flag(&self) -> Arc<AtomicBool> {
        self.stop.clone()
    }

    fn client_config(&self) -> ClientConfig {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &self.config.bootstrap_servers)
            .set("group.id", &self.config.group_id)
            .set("auto.offset.reset", &self.config.auto_offset_reset)
            // We commit explicitly inside the handler loop — auto-
            // commit while a handler is mid-processing risks
            // losing an alert if the process dies between commit
            // and persist.
            .set("enable.auto.offset.store", "false");
        match &self.config.auth {
            GcnAuth::Plaintext => {
                cfg.set("security.protocol", "PLAINTEXT");
            }
            GcnAuth::OidcOauthBearer {
                client_id,
                client_secret,
                token_url,
                ca_cert_path,
            } => {
                cfg.set("security.protocol", "SASL_SSL")
                    .set("sasl.mechanisms", "OAUTHBEARER")
                    .set("sasl.oauthbearer.method", "oidc")
                    .set("sasl.oauthbearer.client.id", client_id)
                    .set("sasl.oauthbearer.client.secret", client_secret)
                    .set("sasl.oauthbearer.token.endpoint.url", token_url);
                if let Some(ca) = ca_cert_path {
                    cfg.set("ssl.ca.location", ca.display().to_string());
                }
            }
        }
        cfg
    }

    /// Drive the consume loop until [`Self::stop_flag`] is set or
    /// the handler returns [`HandlerControl::Stop`]. Each decoded
    /// alert is passed to the handler; parse failures are logged
    /// and skipped (so a single malformed message doesn't break
    /// the stream).
    pub fn run<F>(&self, mut handler: F) -> Result<(), GcnConsumerError>
    where
        F: FnMut(GcnAlert) -> HandlerControl,
    {
        let consumer: BaseConsumer = self.client_config().create()?;
        let topic_refs: Vec<&str> = self.config.topics.iter().map(String::as_str).collect();
        consumer.subscribe(&topic_refs)?;
        info!(
            topics = ?self.config.topics,
            servers = %self.config.bootstrap_servers,
            group = %self.config.group_id,
            "subscribed to GCN Kafka topics"
        );

        while !self.stop.load(Ordering::Relaxed) {
            match consumer.poll(self.config.poll_timeout) {
                Some(Ok(msg)) => {
                    let topic = msg.topic().to_string();
                    let payload = match msg.payload() {
                        Some(p) => p,
                        None => {
                            debug!("empty payload on {topic}, skipping");
                            continue;
                        }
                    };
                    let parsed = decode_alert(&topic, payload);
                    match parsed {
                        Ok(trigger) => {
                            let control = handler(GcnAlert { topic, trigger });
                            // Commit *after* the handler finishes —
                            // at-least-once semantics: if the
                            // process dies between handler return
                            // and commit, we'll re-process on
                            // restart, which the
                            // (instrument, trigger_id) upsert
                            // handles idempotently.
                            if let Err(e) = consumer.store_offset_from_message(&msg) {
                                warn!("offset store failed: {e}");
                            }
                            match control {
                                HandlerControl::Continue => {}
                                HandlerControl::Stop => break,
                            }
                        }
                        Err(e) => {
                            warn!(topic = %msg.topic(), "decode failed: {e}");
                        }
                    }
                }
                Some(Err(e)) => {
                    warn!("kafka error: {e}");
                }
                None => continue,
            }
        }
        Ok(())
    }
}

/// Return value of an alert handler — same shape as
/// [`crate::kafka::HandlerControl`], replicated here so callers
/// don't have to cross-import.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandlerControl {
    Continue,
    Stop,
}

/// Pick the right parser based on the Kafka topic name.
///
/// * `gcn.notices.fermi.gbm.*` → JSON parser.
/// * `gcn.classic.voevent.FERMI_GBM_*` → VOEvent parser.
/// * Anything else → JSON attempted first, then VOEvent as a
///   fallback. Returns the parse error from the first attempt
///   when both fail (more often the failure mode that matters).
fn decode_alert(topic: &str, payload: &[u8]) -> Result<GrbTrigger, crate::gcn::GcnParseError> {
    let payload_str = match std::str::from_utf8(payload) {
        Ok(s) => s,
        Err(e) => {
            return Err(crate::gcn::GcnParseError::Json(serde_json::Error::io(
                std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()),
            )));
        }
    };
    if topic.contains("classic.voevent") {
        let instrument = fermi_instrument_for_voevent_topic(topic);
        return parse_fermi_voevent(payload_str, instrument);
    }
    // Modern JSON notices: derive the instrument suffix from the
    // topic tail so we can distinguish FLT vs. GND vs. FIN even
    // when payloads themselves don't tag the stage.
    let instrument = match topic {
        t if t.ends_with("flight_position") => "Fermi-GBM-FLT",
        t if t.ends_with("ground_position") => "Fermi-GBM-GND",
        t if t.ends_with("final_position") => "Fermi-GBM-FIN",
        t if t.ends_with("subthreshold") => "Fermi-GBM-SUBTHRESH",
        _ => "Fermi-GBM",
    };
    parse_fermi_gbm_json(payload_str, instrument)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_routes_voevent_topic_to_voevent_parser() {
        let xml = r#"<voe:VOEvent>
<What><Param name="TrigID" value="1" /></What>
</voe:VOEvent>"#;
        let t = decode_alert("gcn.classic.voevent.FERMI_GBM_FLT_POS", xml.as_bytes()).unwrap();
        assert_eq!(t.instrument, "Fermi-GBM-FLT");
        assert_eq!(t.trigger_id, "1");
    }

    #[test]
    fn decode_routes_json_topic_to_json_parser() {
        let json = br#"{"trigger_id":"bn1","trigger_time":1.0,"ra":1.0,"dec":2.0}"#;
        let t = decode_alert("gcn.notices.fermi.gbm.flight_position", json).unwrap();
        assert_eq!(t.instrument, "Fermi-GBM-FLT");
        assert_eq!(t.trigger_id, "bn1");
    }

    #[test]
    fn decode_unknown_topic_uses_generic_label() {
        let json = br#"{"trigger_id":"x","trigger_time":1.0,"ra":1.0,"dec":2.0}"#;
        let t = decode_alert("gcn.notices.something.weird", json).unwrap();
        assert_eq!(t.instrument, "Fermi-GBM");
    }

    #[test]
    fn client_config_picks_sasl_for_oidc_auth() {
        let cfg = GcnKafkaConfig {
            bootstrap_servers: "kafka.gcn.nasa.gov:9092".into(),
            group_id: "g".into(),
            auto_offset_reset: "earliest".into(),
            poll_timeout: Duration::from_millis(500),
            topics: vec!["gcn.notices.fermi.gbm.flight_position".into()],
            auth: GcnAuth::OidcOauthBearer {
                client_id: "id".into(),
                client_secret: "sec".into(),
                token_url: DEFAULT_GCN_TOKEN_URL.into(),
                ca_cert_path: None,
            },
        };
        let consumer = GcnAlertConsumer::new(cfg);
        let cc = consumer.client_config();
        assert_eq!(cc.get("security.protocol"), Some("SASL_SSL"));
        assert_eq!(cc.get("sasl.mechanisms"), Some("OAUTHBEARER"));
        assert_eq!(cc.get("sasl.oauthbearer.method"), Some("oidc"));
        assert_eq!(cc.get("sasl.oauthbearer.client.id"), Some("id"));
    }

    #[test]
    fn client_config_plaintext_skips_sasl() {
        let cfg = GcnKafkaConfig {
            bootstrap_servers: "localhost:9092".into(),
            group_id: "g".into(),
            auto_offset_reset: "earliest".into(),
            poll_timeout: Duration::from_millis(500),
            topics: vec![],
            auth: GcnAuth::Plaintext,
        };
        let consumer = GcnAlertConsumer::new(cfg);
        let cc = consumer.client_config();
        assert_eq!(cc.get("security.protocol"), Some("PLAINTEXT"));
        assert!(cc.get("sasl.mechanisms").is_none());
    }
}
