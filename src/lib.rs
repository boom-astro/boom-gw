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

pub mod clustering;
pub mod envelope;
pub mod event;
pub mod kafka;
pub mod publisher;
pub mod scitokens;
pub mod state;

pub use clustering::{
    summarize, EventAssignment, SkipReason, Superevent, SupereventCreator, SupereventUpdate,
    DEFAULT_WINDOW_SECS,
};
pub use envelope::{decode_event_file, EventEnvelope, EventFile};
pub use event::{extract_gw_event, GwEvent, GwEventError};
pub use kafka::{
    GwAlertConsumer, GwConsumerError, GwKafkaConfig, GwProcessError, HandlerControl,
    ScitokensContext, DEFAULT_PIPELINE_TOPICS,
};
pub use publisher::{PublisherConfig, PublisherError, SupereventPublisher};
pub use scitokens::{
    decode_claims, Claims, EnvTokenSource, FileTokenSource, TokenError, TokenSource,
};
pub use state::{load_from_redis, save_to_redis, StateError};
