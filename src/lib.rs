// Bumped for the heavy AWS SDK macro expansions used by the S3
// skymap storage backend (mirrors BOOM proper's PR #313).
#![recursion_limit = "512"]

//! Gravitational-wave alert ingestion and superevent clustering on top of
//! the LIGO/Virgo/KAGRA GraceDB Kafka topics.
//!
//! This crate consumes pipeline events (`gstlal`, `mbta`, `pycbc`, `spiir`,
//! `aframe`, `cwb`, `mly`) over SASL/OAUTHBEARER with SCITokens, decodes the
//! JSON envelope, parses the embedded coinc.xml payload with the
//! `igwn-ligolw` crate, and clusters the resulting events into superevents
//! using the same time-window / SNR-preferred policy as `sgn-llai`. The
//! resulting superevent stream can be published to a downstream Kafka topic
//! and the open-superevent state can be persisted in Redis for restart
//! safety.
//!
//! The crate is intentionally framework-independent: it only depends on
//! `rdkafka`, `redis`, `serde`, `tokio`, and `igwn-ligolw`. It can be
//! embedded in another application or driven directly via its `bin/`
//! binaries.

pub mod alert;
pub mod api;
pub mod archive;
pub mod auth;
pub mod boom;
pub mod clustering;
pub mod contour;
pub mod crossmatch;
pub mod envelope;
pub mod event;
pub mod frb;
pub mod gcn;
pub mod gcn_consumer;
pub mod grb;
pub mod icecube_lvk;
pub mod ingest;
pub mod joint_skymap;
pub mod kafka;
pub mod localizer;
pub mod metrics;
pub mod neutrino;
pub mod publisher;
pub mod pvalue;
pub mod scitokens;
pub mod state;
pub mod storage;

/// Serialize / deserialize a `Vec<u8>` as a base64 string. Used for FITS
/// payloads that travel inside JSON envelopes (Kafka, Redis).
pub(crate) mod base64_bytes {
    use base64::engine::general_purpose::STANDARD as BASE64;
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&BASE64.encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        BASE64
            .decode(s.as_bytes())
            .map_err(serde::de::Error::custom)
    }
}

pub use alert::{
    build_alert, AlertError, AlertEvent, AlertPublisher, AlertPublisherConfig, AlertType,
    AlertUrls, PublicAlert, DEFAULT_ALERT_TOPIC,
};
pub use archive::{
    AlertDoc, AnnotationDoc, Archive, ArchiveConfig, ArchiveError, EventDoc, FrbAlertDoc,
    IceCubeLvkSearchDoc, LocalizeRequestDoc, LocalizeResultDoc, NeutrinoAlertDoc, SupereventDoc,
    ALERTS_COLLECTION, ANNOTATIONS_COLLECTION, DEFAULT_DB_NAME, EVENTS_COLLECTION,
    FRB_ALERTS_COLLECTION, ICECUBE_LVK_SEARCHES_COLLECTION, LOCALIZE_REQUESTS_COLLECTION,
    LOCALIZE_RESULTS_COLLECTION, NEUTRINO_ALERTS_COLLECTION, SUPEREVENTS_COLLECTION,
};
pub use auth::{
    auth_middleware, forbidden, require_alert_publisher, validate_token, AuthConfig, AuthError,
    JwksCache, Principal, DEFAULT_AUDIENCES, DEFAULT_ISSUERS, DEFAULT_REQUIRED_SCOPE,
};
pub use clustering::{
    summarize, EventAssignment, SkipReason, SkyMapFits, Superevent, SupereventCreator,
    SupereventUpdate, DEFAULT_WINDOW_SECS,
};
pub use envelope::{decode_event_file, EventEnvelope, EventFile};
pub use event::{extract_gw_event, extract_gw_event_with_xml, GwEvent, GwEventError};
#[allow(deprecated)]
pub use kafka::DEFAULT_PIPELINE_TOPICS;
pub use kafka::{
    pipeline_topics_for_instance, GwAlertConsumer, GwConsumerError, GwKafkaConfig, GwProcessError,
    HandlerControl, ScitokensContext, DEFAULT_GRACEDB_INSTANCE, DEFAULT_PIPELINES,
};
pub use localizer::{
    LocalizeRequest, LocalizeResult, LocalizeStatus, LocalizerClient, LocalizerClientConfig,
    LocalizerError, LocalizerResultConsumer, LocalizerResultConsumerConfig, LocalizerResultStream,
    DEFAULT_REQUEST_TOPIC, DEFAULT_RESULT_TOPIC,
};
pub use publisher::{PublisherConfig, PublisherError, SupereventPublisher};
pub use scitokens::{
    decode_claims, Claims, EnvTokenSource, FileTokenSource, TokenError, TokenSource,
};
pub use state::{load_from_redis, save_to_redis, StateError};
