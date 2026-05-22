//! Publish `SupereventUpdate` records to a Kafka topic so downstream
//! services (an alert assembler, a multi-messenger correlator, a GraceDB
//! archival sink, ...) can consume them.
//!
//! Each update is encoded as a single JSON document. The action variant
//! is carried in an `action` discriminator field; `superevent_id` is set
//! on the producer record key so a downstream Kafka consumer can use
//! key-based partitioning to keep all updates for a given superevent on
//! the same partition.

use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use thiserror::Error;
use tracing::debug;

use crate::clustering::SupereventUpdate;

#[derive(Debug, Clone)]
pub struct PublisherConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    /// Per-message produce timeout. Defaults to 5 s.
    pub timeout: Duration,
}

impl PublisherConfig {
    pub fn new(bootstrap_servers: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            topic: topic.into(),
            timeout: Duration::from_secs(5),
        }
    }
}

pub struct SupereventPublisher {
    producer: FutureProducer,
    topic: String,
    timeout: Duration,
}

#[derive(Debug, Error)]
pub enum PublisherError {
    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("delivery failed: {0}")]
    Delivery(String),
}

impl SupereventPublisher {
    pub fn new(config: PublisherConfig) -> Result<Self, PublisherError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .set("batch.size", "16384")
            .set("linger.ms", "5")
            .set("acks", "1")
            .set("retries", "3")
            .create()?;
        Ok(Self {
            producer,
            topic: config.topic,
            timeout: config.timeout,
        })
    }

    /// Serialize `update` to JSON and publish it. Returns once the broker
    /// acknowledges or the timeout fires. Keyed by `superevent_id` so
    /// downstream consumers can keep ordering per superevent.
    pub async fn publish(&self, update: &SupereventUpdate) -> Result<(), PublisherError> {
        let key = superevent_id(update).to_string();
        let payload = serde_json::to_vec(update)?;
        let record = FutureRecord::to(&self.topic).key(&key).payload(&payload);
        match self.producer.send(record, self.timeout).await {
            Ok(delivery) => {
                debug!(
                    topic = %self.topic,
                    partition = delivery.partition,
                    offset = delivery.offset,
                    key = %key,
                    "published"
                );
                Ok(())
            }
            Err((err, _)) => Err(PublisherError::from(err)),
        }
    }
}

fn superevent_id(update: &SupereventUpdate) -> &str {
    match update {
        SupereventUpdate::Created { superevent } => &superevent.id,
        SupereventUpdate::PreferredUpdated { superevent, .. } => &superevent.id,
        SupereventUpdate::Skipped { superevent_id, .. } => superevent_id.as_str(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clustering::{Superevent, SupereventUpdate};
    use crate::event::GwEvent;
    use ligo_lw::CoincInspiralEvent;

    fn dummy_event(graceid: &str, end_time: f64, snr: f64) -> GwEvent {
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
        GwEvent {
            pipeline: "test".into(),
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

    #[test]
    fn created_serializes_with_action_discriminator() {
        let ev = dummy_event("G42", 1000.0, 10.0);
        let s = Superevent {
            id: "S000000".into(),
            t_0: 1000.0,
            t_start: 997.5,
            t_end: 1002.5,
            preferred_event: ev.clone(),
            g_events: vec![ev],
        };
        let update = SupereventUpdate::Created { superevent: s };
        let json = serde_json::to_value(&update).unwrap();
        assert_eq!(json["action"], "created");
        assert_eq!(json["superevent"]["id"], "S000000");
        assert_eq!(json["superevent"]["preferred_event"]["graceid"], "G42");
    }

    #[test]
    fn roundtrip_via_json() {
        let ev = dummy_event("G42", 1000.0, 10.0);
        let s = Superevent {
            id: "S000001".into(),
            t_0: 1000.0,
            t_start: 997.5,
            t_end: 1002.5,
            preferred_event: ev.clone(),
            g_events: vec![ev],
        };
        let original = SupereventUpdate::Created { superevent: s };
        let bytes = serde_json::to_vec(&original).unwrap();
        let restored: SupereventUpdate = serde_json::from_slice(&bytes).unwrap();
        match restored {
            SupereventUpdate::Created { superevent } => {
                assert_eq!(superevent.id, "S000001");
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }
}
