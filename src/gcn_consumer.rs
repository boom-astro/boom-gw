//! Live Kafka consumer for GCN (Gamma-ray Coordinates Network)
//! alerts.
//!
//! Connects to `kafka.gcn.nasa.gov` (or any Kafka broker — see
//! `--bootstrap-servers` in `bin/gw_gcn_consumer.rs`), subscribes
//! to the requested Fermi-GBM notice topics, parses each payload
//! via [`crate::gcn`], and pushes the resulting [`GrbTrigger`] to
//! the caller's handler.
//!
//! Auth uses librdkafka's built-in OIDC OAUTHBEARER path
//! (`sasl.oauthbearer.method=oidc`). On macOS, that requires
//! rdkafka to be built with the `curl-static` feature so the
//! statically-linked OpenSSL we vendor is also used by libcurl;
//! otherwise the broker connection silently hangs in TRY_CONNECT
//! (see nasa-gcn/gcn-kafka-rust PR #12). The Cargo.toml feature
//! list already enables this.
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

use crate::boom::{parse_boom_alert, BoomTransient};
use crate::gcn::{fermi_instrument_for_voevent_topic, parse_fermi_gbm_json, parse_fermi_voevent};
use crate::grb::GrbTrigger;

/// Default OIDC token endpoint for the GCN broker. Override only
/// if you're targeting a non-NASA mirror.
pub const DEFAULT_GCN_TOKEN_URL: &str = "https://auth.gcn.nasa.gov/oauth2/token";

/// Default bootstrap servers for the production GCN Kafka broker.
pub const DEFAULT_GCN_BOOTSTRAP_SERVERS: &str = "kafka.gcn.nasa.gov:9092";

/// Fermi-GBM topics to subscribe to by default. Topic names verified
/// against the live GCN broker via `origen`'s router (which is the
/// known-working reference for these names): the modern JSON
/// notices use `flt_pos` / `gnd_pos` / `fin_pos` (abbreviated, NOT
/// `flight_position` / etc.), plus a separate `alert` topic that
/// fires on the initial trigger before positions are computed. We
/// also subscribe to the classic VOEvent stream — `gcn.notices.*`
/// for Fermi GBM is still rolling out and the classic stream is
/// the only one with full historical coverage.
pub const DEFAULT_FERMI_GBM_TOPICS: &[&str] = &[
    "gcn.notices.fermi.gbm.alert",
    "gcn.notices.fermi.gbm.flt_pos",
    "gcn.notices.fermi.gbm.gnd_pos",
    "gcn.notices.fermi.gbm.fin_pos",
    "gcn.classic.voevent.FERMI_GBM_FLT_POS",
    "gcn.classic.voevent.FERMI_GBM_GND_POS",
    "gcn.classic.voevent.FERMI_GBM_FIN_POS",
    "gcn.classic.voevent.FERMI_GBM_SUBTHRESH",
];

/// BOOM cross-matched optical-transient stream — same Kafka broker
/// as the Fermi topics; published by the BOOM/Babamul team.
/// Schema reference: `gcn-schema/gcn/notices/boom/alert.schema.json`.
pub const DEFAULT_BOOM_TOPICS: &[&str] = &["gcn.notices.boom.alert"];

/// All topics the consumer subscribes to when the caller doesn't
/// specify any. Combines Fermi GBM + BOOM so a fresh deployment
/// gets both streams without extra config.
pub fn default_topics() -> Vec<String> {
    DEFAULT_FERMI_GBM_TOPICS
        .iter()
        .chain(DEFAULT_BOOM_TOPICS.iter())
        .map(|s| s.to_string())
        .collect()
}

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
    /// Optional librdkafka `debug` subsystem list (e.g.
    /// `"security,broker"`). Forwarded as the `debug` client
    /// config option — librdkafka emits noisy internal traces to
    /// the rdkafka tracing target when set. Use sparingly.
    pub debug: Option<String>,
}

/// How to authenticate to the broker.
#[derive(Debug, Clone)]
pub enum GcnAuth {
    /// Plaintext connection (`security.protocol=PLAINTEXT`,
    /// no SASL). Used for local Kafka in docker-compose and for
    /// the CI integration test. The real GCN broker only accepts
    /// OAUTHBEARER.
    Plaintext,
    /// OAUTHBEARER over TLS with custom OIDC token fetch — the
    /// production GCN authentication path. `client_id` /
    /// `client_secret` are issued at
    /// <https://gcn.nasa.gov/quickstart>. We fetch the token via
    /// the [`GcnContext`] callback rather than letting librdkafka
    /// do it natively (broken on macOS).
    OidcOauthBearer {
        client_id: String,
        client_secret: String,
        token_url: String,
        /// Optional path to a CA bundle for the broker TLS
        /// handshake (also forwarded to curl via `--cacert`).
        ca_cert_path: Option<PathBuf>,
    },
}

#[derive(Debug, Error)]
pub enum GcnConsumerError {
    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
}

/// Default OAuth scope requested at the GCN token endpoint. Matches
/// the value gcn-kafka's Python client uses.
pub const GCN_OAUTH_SCOPE: &str = "gcn.nasa.gov/kafka-public-consumer";

/// What the handler is called with for each successfully-decoded
/// alert. The topic is carried so handlers can disambiguate
/// JSON vs. VOEvent routing decisions and trace upstream sources.
#[derive(Debug, Clone)]
pub struct GcnAlert {
    pub topic: String,
    pub payload: GcnPayload,
}

/// Decoded alert payload — discriminated on the upstream topic by
/// [`decode_alert`]. Each variant is normalized into the in-memory
/// type the rest of the codebase already uses for that source.
#[derive(Debug, Clone)]
pub enum GcnPayload {
    /// A GRB trigger from Fermi GBM (JSON `gcn.notices.fermi.gbm.*`
    /// or classic `gcn.classic.voevent.FERMI_GBM_*`).
    Grb(GrbTrigger),
    /// BOOM cross-matched optical alert (`gcn.notices.boom.alert`).
    /// One upstream envelope explodes into 1..N transients (one per
    /// `data.targets[]` entry).
    Boom(Vec<BoomTransient>),
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
        if let Some(d) = &self.config.debug {
            cfg.set("debug", d);
        }
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
                    .set("sasl.oauthbearer.token.endpoint.url", token_url)
                    .set("sasl.oauthbearer.scope", GCN_OAUTH_SCOPE);
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
                        Ok(payload) => {
                            let control = handler(GcnAlert { topic, payload });
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
/// Decode-failure error type covering both upstream parsers.
#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("payload was not valid utf-8: {0}")]
    Utf8(String),
    #[error("gcn fermi parse failed: {0}")]
    Gcn(#[from] crate::gcn::GcnParseError),
    #[error("boom alert parse failed: {0}")]
    Boom(#[from] crate::boom::BoomParseError),
}

fn decode_alert(topic: &str, payload: &[u8]) -> Result<GcnPayload, DecodeError> {
    let payload_str = std::str::from_utf8(payload).map_err(|e| DecodeError::Utf8(e.to_string()))?;

    if topic == "gcn.notices.boom.alert" || topic.contains("boom") {
        let transients = parse_boom_alert(payload_str)?;
        return Ok(GcnPayload::Boom(transients));
    }

    if topic.contains("classic.voevent") {
        let instrument = fermi_instrument_for_voevent_topic(topic);
        return Ok(GcnPayload::Grb(parse_fermi_voevent(
            payload_str,
            instrument,
        )?));
    }
    // Modern Fermi GBM JSON notices: derive the instrument suffix
    // from the topic tail so we can distinguish FLT vs. GND vs.
    // FIN even when payloads themselves don't tag the stage. Topic
    // suffixes follow `gcn.notices.fermi.gbm.*` — `flt_pos`,
    // `gnd_pos`, `fin_pos`, plus the position-less `alert` topic.
    let instrument = match topic {
        t if t.ends_with(".flt_pos") => "Fermi-GBM-FLT",
        t if t.ends_with(".gnd_pos") => "Fermi-GBM-GND",
        t if t.ends_with(".fin_pos") => "Fermi-GBM-FIN",
        t if t.ends_with(".alert") => "Fermi-GBM",
        _ => "Fermi-GBM",
    };
    Ok(GcnPayload::Grb(parse_fermi_gbm_json(
        payload_str,
        instrument,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_routes_voevent_topic_to_voevent_parser() {
        let xml = r#"<voe:VOEvent>
<What><Param name="TrigID" value="1" /></What>
</voe:VOEvent>"#;
        let p = decode_alert("gcn.classic.voevent.FERMI_GBM_FLT_POS", xml.as_bytes()).unwrap();
        let GcnPayload::Grb(t) = p else {
            panic!("expected Grb payload, got {p:?}")
        };
        assert_eq!(t.instrument, "Fermi-GBM-FLT");
        assert_eq!(t.trigger_id, "1");
    }

    fn unwrap_grb(p: GcnPayload) -> GrbTrigger {
        match p {
            GcnPayload::Grb(t) => t,
            other => panic!("expected Grb, got {other:?}"),
        }
    }

    #[test]
    fn decode_routes_json_topic_to_json_parser() {
        let json = br#"{"trigger_id":"bn1","trigger_time":1.0,"ra":1.0,"dec":2.0}"#;
        let t = unwrap_grb(decode_alert("gcn.notices.fermi.gbm.flt_pos", json).unwrap());
        assert_eq!(t.instrument, "Fermi-GBM-FLT");
        assert_eq!(t.trigger_id, "bn1");
    }

    #[test]
    fn decode_routes_all_fermi_gbm_json_suffixes() {
        let json = br#"{"trigger_id":"bn1","trigger_time":1.0,"ra":1.0,"dec":2.0}"#;
        let cases = [
            ("gcn.notices.fermi.gbm.flt_pos", "Fermi-GBM-FLT"),
            ("gcn.notices.fermi.gbm.gnd_pos", "Fermi-GBM-GND"),
            ("gcn.notices.fermi.gbm.fin_pos", "Fermi-GBM-FIN"),
            ("gcn.notices.fermi.gbm.alert", "Fermi-GBM"),
        ];
        for (topic, expected) in cases {
            let t = unwrap_grb(decode_alert(topic, json).expect(topic));
            assert_eq!(t.instrument, expected, "topic={topic}");
        }
    }

    #[test]
    fn decode_unknown_topic_uses_generic_label() {
        let json = br#"{"trigger_id":"x","trigger_time":1.0,"ra":1.0,"dec":2.0}"#;
        let t = unwrap_grb(decode_alert("gcn.notices.something.weird", json).unwrap());
        assert_eq!(t.instrument, "Fermi-GBM");
    }

    #[test]
    fn decode_routes_boom_topic_to_boom_parser() {
        let payload = br#"{
            "alert_datetime": "2026-01-15T00:00:00Z",
            "data": {"targets":[{"event_name":"ZTF1","ra":1.0,"dec":2.0}],"photometry":[]}
        }"#;
        let parsed = decode_alert("gcn.notices.boom.alert", payload).unwrap();
        match parsed {
            GcnPayload::Boom(ts) => {
                assert_eq!(ts.len(), 1);
                assert_eq!(ts[0].event_name, "ZTF1");
            }
            other => panic!("expected Boom, got {other:?}"),
        }
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
            debug: None,
        };
        let consumer = GcnAlertConsumer::new(cfg);
        let cc = consumer.client_config();
        assert_eq!(cc.get("security.protocol"), Some("SASL_SSL"));
        assert_eq!(cc.get("sasl.mechanisms"), Some("OAUTHBEARER"));
        assert_eq!(cc.get("sasl.oauthbearer.method"), Some("oidc"));
        assert_eq!(cc.get("sasl.oauthbearer.client.id"), Some("id"));
        assert_eq!(cc.get("sasl.oauthbearer.scope"), Some(GCN_OAUTH_SCOPE));
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
            debug: None,
        };
        let consumer = GcnAlertConsumer::new(cfg);
        let cc = consumer.client_config();
        assert_eq!(cc.get("security.protocol"), Some("PLAINTEXT"));
        assert!(cc.get("sasl.mechanisms").is_none());
    }
}
