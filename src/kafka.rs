//! Kafka consumer for the GraceDB pipeline topics, authenticated via
//! SCITokens over SASL/OAUTHBEARER.
//!
//! This is the equivalent of the Python snippet:
//!
//! ```python
//! GraceDbKafkaConsumer(
//!     bootstrap_servers="kafka-dev.ligo.org:9092",
//!     topics=["gstlal","mbta","pycbc","spiir","aframe","cwb","mly"],
//!     service_url="https://gracedb-test.ligo.org/api/",
//!     ca_cert_path=certifi.where(),
//!     group_id="my-meg-consumer",
//! )
//! ```
//!
//! The Python client acquires SCITokens itself by talking to the GraceDB
//! service URL. Our Rust client expects the token to have been acquired
//! out-of-band (typically by `htgettoken` against the LIGO vault) and to be
//! supplied through a [`TokenSource`]; rdkafka invokes the SCITokens-backed
//! OAUTHBEARER callback on every refresh.

use std::error::Error as StdError;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rdkafka::client::OAuthToken;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::message::Message;
use rdkafka::ClientContext;
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::envelope::{EnvelopeError, EventEnvelope};
use crate::event::{extract_gw_event_with_xml, GwEvent, GwEventError};
use crate::scitokens::{decode_claims, TokenError, TokenSource};

/// The seven GraceDB low-latency pipelines. **These are pipeline
/// short-names, not bare Kafka topic names.** On `kafka-dev.ligo.org`
/// the actual topic each pipeline writes to is namespaced by the
/// GraceDB instance, so the real wire name is e.g.
/// `gracedb-test.gstlal` (matching what
/// `ligo.gracedb.kafka.GraceDbKafkaConsumer` does internally —
/// see `make_topic_name` there).
///
/// Callers should compose the wire topic with
/// [`pipeline_topics_for_instance`] or pass the fully-qualified
/// names directly via `GwKafkaConfig::topics`.
pub const DEFAULT_PIPELINES: &[&str] =
    &["gstlal", "mbta", "pycbc", "spiir", "aframe", "cwb", "mly"];

/// Default GraceDB instance for low-latency event flow on
/// `kafka-dev.ligo.org` — the "test" instance. Override per
/// deployment when consuming from production gracedb.
pub const DEFAULT_GRACEDB_INSTANCE: &str = "gracedb-test";

/// Compose the wire topic names for the seven default pipelines under
/// a given GraceDB instance. With `instance = "gracedb-test"`,
/// returns `["gracedb-test.gstlal", "gracedb-test.mbta", ...]`.
pub fn pipeline_topics_for_instance(instance: &str) -> Vec<String> {
    DEFAULT_PIPELINES
        .iter()
        .map(|p| format!("{instance}.{p}"))
        .collect()
}

/// Backward-compatible alias preserved so external callers that
/// referenced the old constant keep compiling. Prefer
/// [`pipeline_topics_for_instance`] for new code.
#[deprecated(note = "These are pipeline short-names, not topic names. Use \
            `pipeline_topics_for_instance(\"gracedb-test\")` or pass \
            fully-qualified topics in `GwKafkaConfig::topics`.")]
pub const DEFAULT_PIPELINE_TOPICS: &[&str] = DEFAULT_PIPELINES;

/// Configuration for a GraceDB Kafka consumer.
#[derive(Debug, Clone)]
pub struct GwKafkaConfig {
    /// Comma-separated bootstrap broker list (e.g. "kafka-dev.ligo.org:9092").
    pub bootstrap_servers: String,
    /// Topic names to subscribe to.
    pub topics: Vec<String>,
    /// Kafka consumer group ID.
    pub group_id: String,
    /// Whether the broker requires TLS (`SASL_SSL`) or plaintext (`SASL_PLAINTEXT`).
    pub use_tls: bool,
    /// Optional CA bundle. When `None`, rdkafka falls back to the system CA
    /// store; setting this to `certifi.where()`-equivalent matches the Python
    /// client.
    pub ca_cert_path: Option<PathBuf>,
    /// Where to start when no committed offset exists ("earliest" or "latest").
    pub auto_offset_reset: String,
    /// Poll timeout per iteration.
    pub poll_timeout: Duration,
}

impl Default for GwKafkaConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "kafka-dev.ligo.org:9092".to_string(),
            topics: pipeline_topics_for_instance(DEFAULT_GRACEDB_INSTANCE),
            group_id: "boom-gw-consumer".to_string(),
            use_tls: true,
            ca_cert_path: None,
            auto_offset_reset: "latest".to_string(),
            poll_timeout: Duration::from_secs(1),
        }
    }
}

/// rdkafka ClientContext that supplies SCITokens bearer tokens via the
/// OAUTHBEARER mechanism on every refresh.
pub struct ScitokensContext {
    token_source: Arc<dyn TokenSource>,
}

impl ScitokensContext {
    pub fn new(token_source: Arc<dyn TokenSource>) -> Self {
        Self { token_source }
    }
}

impl ClientContext for ScitokensContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(&self, _config: Option<&str>) -> Result<OAuthToken, Box<dyn StdError>> {
        let token = self.token_source.current_token()?;
        let claims = decode_claims(&token)?;
        let principal = claims
            .sub
            .clone()
            .unwrap_or_else(|| "boom-gw-consumer".to_string());
        // `exp` is seconds since the Unix epoch; rdkafka wants ms.
        let lifetime_ms = claims.exp.saturating_mul(1000);
        debug!(
            principal = %principal,
            iss = ?claims.iss,
            scope = ?claims.scope,
            exp = claims.exp,
            "issued OAUTHBEARER token to rdkafka"
        );
        Ok(OAuthToken {
            token,
            principal_name: principal,
            lifetime_ms,
        })
    }
}

impl ConsumerContext for ScitokensContext {}

#[derive(Debug, Error)]
pub enum GwConsumerError {
    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("token error: {0}")]
    Token(#[from] TokenError),
}

#[derive(Debug, Error)]
pub enum GwProcessError {
    #[error("envelope error: {0}")]
    Envelope(#[from] EnvelopeError),
    #[error("event extraction error: {0}")]
    Event(#[from] GwEventError),
}

/// Outcome of handling a single Kafka message.
pub enum HandlerControl {
    Continue,
    Stop,
}

/// Consumer driver that connects, subscribes, and pushes each decoded
/// [`GwEvent`] to the provided handler.
pub struct GwAlertConsumer {
    pub config: GwKafkaConfig,
    pub token_source: Arc<dyn TokenSource>,
    stop: Arc<AtomicBool>,
}

impl GwAlertConsumer {
    pub fn new(config: GwKafkaConfig, token_source: Arc<dyn TokenSource>) -> Self {
        Self {
            config,
            token_source,
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Returns a clone of the shutdown flag. Set it to `true` to stop the
    /// `run` loop after the current poll iteration.
    pub fn stop_flag(&self) -> Arc<AtomicBool> {
        self.stop.clone()
    }

    /// Build the rdkafka ClientConfig for this consumer. Exposed so tests
    /// (and the CLI binary) can inspect the configuration without subscribing.
    pub fn client_config(&self) -> ClientConfig {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &self.config.bootstrap_servers)
            .set("group.id", &self.config.group_id)
            .set("auto.offset.reset", &self.config.auto_offset_reset)
            // The application controls offset commits explicitly.
            .set("enable.auto.offset.store", "false")
            // Force IPv4 — `kafka-dev.ligo.org` (and other LIGO
            // brokers) are dual-stack, and many NRP nodes have no
            // IPv6 egress route. Without this, librdkafka picks
            // the AAAA and hits `Network is unreachable`. v4-only
            // is safe across all our deployment targets.
            .set("broker.address.family", "v4");

        if self.config.use_tls {
            cfg.set("security.protocol", "SASL_SSL");
            if let Some(ca) = &self.config.ca_cert_path {
                cfg.set("ssl.ca.location", ca.display().to_string());
            }
        } else {
            cfg.set("security.protocol", "SASL_PLAINTEXT");
        }
        cfg.set("sasl.mechanisms", "OAUTHBEARER");
        cfg
    }

    /// Subscribe to topics and drive the consume loop until [`Self::stop_flag`]
    /// is set or the handler returns [`HandlerControl::Stop`]. Errors decoding
    /// individual messages are logged and skipped rather than aborting the loop.
    pub fn run<F>(&self, mut handler: F) -> Result<(), GwConsumerError>
    where
        F: FnMut(Result<GwEvent, GwProcessError>) -> HandlerControl,
    {
        self.run_with_xml(|result| handler(result.map(|(ev, _xml)| ev)))
    }

    /// Like [`Self::run`], but the handler also receives the raw
    /// coinc.xml bytes that produced the [`GwEvent`]. Used by the
    /// clusterer when the localization microservice is enabled: the
    /// bytes are forwarded verbatim in the [`LocalizeRequest`] so
    /// BAYESTAR consumes the same payload the pipeline published.
    pub fn run_with_xml<F>(&self, mut handler: F) -> Result<(), GwConsumerError>
    where
        F: FnMut(Result<(GwEvent, Vec<u8>), GwProcessError>) -> HandlerControl,
    {
        let context = ScitokensContext::new(self.token_source.clone());
        let consumer: BaseConsumer<ScitokensContext> =
            self.client_config().create_with_context(context)?;
        let topic_refs: Vec<&str> = self.config.topics.iter().map(String::as_str).collect();
        consumer.subscribe(&topic_refs)?;
        info!(
            topics = ?self.config.topics,
            servers = %self.config.bootstrap_servers,
            group = %self.config.group_id,
            "subscribed to GraceDB Kafka topics"
        );

        while !self.stop.load(Ordering::Relaxed) {
            match consumer.poll(self.config.poll_timeout) {
                Some(Ok(msg)) => {
                    let payload = match msg.payload() {
                        Some(p) => p,
                        None => {
                            debug!("received empty payload, skipping");
                            continue;
                        }
                    };
                    let result = decode_message(payload);
                    match handler(result) {
                        HandlerControl::Continue => {}
                        HandlerControl::Stop => break,
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

fn decode_message(payload: &[u8]) -> Result<(GwEvent, Vec<u8>), GwProcessError> {
    let envelope = EventEnvelope::from_json(payload)?;
    Ok(extract_gw_event_with_xml(&envelope)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scitokens::EnvTokenSource;

    #[test]
    fn pipeline_topics_for_instance_namespaces_each_pipeline() {
        let topics = pipeline_topics_for_instance("gracedb-test");
        assert_eq!(topics.len(), 7);
        assert_eq!(topics[0], "gracedb-test.gstlal");
        assert!(topics.contains(&"gracedb-test.mly".to_string()));
        // Different instance, different prefix.
        let prod = pipeline_topics_for_instance("gracedb");
        assert_eq!(prod[0], "gracedb.gstlal");
    }

    #[test]
    fn default_config_has_seven_pipeline_topics() {
        let cfg = GwKafkaConfig::default();
        assert_eq!(cfg.topics.len(), 7);
        // Topics are namespaced by the GraceDB instance — bare
        // pipeline names don't exist on the wire.
        assert!(cfg.topics.contains(&"gracedb-test.gstlal".to_string()));
        assert!(cfg.topics.contains(&"gracedb-test.mly".to_string()));
        assert!(cfg.use_tls);
    }

    #[test]
    fn client_config_sets_sasl_ssl_and_oauthbearer() {
        let token_source = Arc::new(EnvTokenSource::new("UNUSED")) as Arc<dyn TokenSource>;
        let consumer = GwAlertConsumer::new(GwKafkaConfig::default(), token_source);
        let cfg = consumer.client_config();
        assert_eq!(cfg.get("security.protocol"), Some("SASL_SSL"));
        assert_eq!(cfg.get("sasl.mechanisms"), Some("OAUTHBEARER"));
        assert_eq!(
            cfg.get("bootstrap.servers"),
            Some("kafka-dev.ligo.org:9092")
        );
    }

    #[test]
    fn plaintext_path_drops_tls() {
        let token_source = Arc::new(EnvTokenSource::new("UNUSED")) as Arc<dyn TokenSource>;
        let consumer = GwAlertConsumer::new(
            GwKafkaConfig {
                use_tls: false,
                ..GwKafkaConfig::default()
            },
            token_source,
        );
        let cfg = consumer.client_config();
        assert_eq!(cfg.get("security.protocol"), Some("SASL_PLAINTEXT"));
        assert_eq!(cfg.get("sasl.mechanisms"), Some("OAUTHBEARER"));
    }

    #[test]
    fn scitokens_context_returns_token_and_principal() {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
        use base64::Engine as _;
        let header = B64URL.encode(b"{\"alg\":\"none\"}");
        let payload = B64URL.encode(br#"{"sub":"alice","exp":1800000000}"#);
        let sig = B64URL.encode(b"x");
        let jwt = format!("{header}.{payload}.{sig}");
        std::env::set_var("BOOM_TEST_SCITOKEN_CTX", &jwt);
        let src = Arc::new(EnvTokenSource::new("BOOM_TEST_SCITOKEN_CTX")) as Arc<dyn TokenSource>;
        let ctx = ScitokensContext::new(src);
        let tok = ctx.generate_oauth_token(None).unwrap();
        assert_eq!(tok.principal_name, "alice");
        assert_eq!(tok.lifetime_ms, 1_800_000_000_000);
        assert_eq!(tok.token, jwt);
        std::env::remove_var("BOOM_TEST_SCITOKEN_CTX");
    }
}
