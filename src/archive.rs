//! Durable MongoDB archive for ingested events, superevents, and
//! localization audit records.
//!
//! Redis (see [`crate::state`]) holds the *open* superevent windows so
//! a restart does not lose in-flight state. MongoDB is the *history*:
//! every event ever ingested, every superevent ever opened (including
//! ones whose Redis-side window has long been pruned), every
//! localization request submitted, and every result received. The
//! distinction matters because Redis-side state turns over on the
//! order of minutes (one window per superevent) while the archive
//! grows forever.
//!
//! Conventions mirror BOOM proper: the `mongodb` crate (v3.x) on
//! tokio; cloned `mongodb::Database` handles shared by value; typed
//! structs serialized with serde+bson; caller-supplied string `_id`s
//! (graceid, superevent_id, request_id) so every collection has a
//! natural primary key. Indices are created at startup.
//!
//! Boom-gw runs against its own MongoDB database — there is no
//! requirement that it share the same MongoDB instance as BOOM proper.

use mongodb::bson::doc;
use mongodb::options::ClientOptions;
use mongodb::{Client, Collection, Database, IndexModel};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::info;

use crate::clustering::{SkyMapFits, Superevent};
use crate::event::GwEvent;
use crate::localizer::{LocalizeRequest, LocalizeResult, LocalizeStatus};

pub const DEFAULT_DB_NAME: &str = "boom_gw";
pub const EVENTS_COLLECTION: &str = "events";
pub const SUPEREVENTS_COLLECTION: &str = "superevents";
pub const LOCALIZE_REQUESTS_COLLECTION: &str = "localize_requests";
pub const LOCALIZE_RESULTS_COLLECTION: &str = "localize_results";
pub const ANNOTATIONS_COLLECTION: &str = "annotations";

#[derive(Debug, Error)]
pub enum ArchiveError {
    #[error("mongo error: {0}")]
    Mongo(#[from] mongodb::error::Error),
    #[error("bson serialization error: {0}")]
    Bson(#[from] mongodb::bson::ser::Error),
}

#[derive(Debug, Clone)]
pub struct ArchiveConfig {
    pub uri: String,
    pub database: String,
    /// Optional `app_name` reported to the Mongo server, useful for
    /// distinguishing boom-gw connections in `db.currentOp()`.
    pub app_name: Option<String>,
}

impl ArchiveConfig {
    pub fn new(uri: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            database: DEFAULT_DB_NAME.into(),
            app_name: Some("boom-gw".into()),
        }
    }
}

/// One ingested GW event, keyed by graceid. Stores both the envelope
/// metadata and the parsed `coinc_inspiral` row so the archive doubles
/// as the system of record for what the pipelines emitted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventDoc {
    #[serde(rename = "_id")]
    pub graceid: String,
    pub pipeline: String,
    pub producer_timestamp: f64,
    pub message_type: String,
    pub submitter: String,
    pub end_time: f64,
    pub ifos: String,
    pub snr: f64,
    pub far: f64,
    pub mchirp: Option<f64>,
    pub total_mass: Option<f64>,
    /// The full `CoincInspiralEvent` payload, captured verbatim from
    /// the LIGO_LW parser so the constituent SNGL triggers are
    /// available downstream.
    pub coinc: mongodb::bson::Bson,
}

impl EventDoc {
    pub fn from_event(event: &GwEvent) -> Result<Self, ArchiveError> {
        let coinc = mongodb::bson::to_bson(&event.coinc)?;
        Ok(Self {
            graceid: event.graceid.clone(),
            pipeline: event.pipeline.clone(),
            producer_timestamp: event.producer_timestamp,
            message_type: event.message_type.clone(),
            submitter: event.submitter.clone(),
            end_time: event.end_time,
            ifos: event.ifos.clone(),
            snr: event.snr,
            far: event.far,
            mchirp: event.mchirp,
            total_mass: event.total_mass,
            coinc,
        })
    }
}

/// One superevent, keyed by superevent_id. Updated in place as the
/// preferred event changes and as the sky map arrives.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupereventDoc {
    #[serde(rename = "_id")]
    pub id: String,
    pub t_0: f64,
    pub t_start: f64,
    pub t_end: f64,
    pub preferred_graceid: String,
    pub preferred_snr: f64,
    pub g_event_graceids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skymap: Option<SkyMapFits>,
}

impl SupereventDoc {
    pub fn from_superevent(s: &Superevent) -> Self {
        Self {
            id: s.id.clone(),
            t_0: s.t_0,
            t_start: s.t_start,
            t_end: s.t_end,
            preferred_graceid: s.preferred_event.graceid.clone(),
            preferred_snr: s.preferred_event.snr,
            g_event_graceids: s.g_events.iter().map(|e| e.graceid.clone()).collect(),
            skymap: s.skymap.clone(),
        }
    }
}

/// Audit record for one `LocalizeRequest` boom-gw published.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalizeRequestDoc {
    #[serde(rename = "_id")]
    pub request_id: String,
    pub superevent_id: String,
    pub graceid: String,
    pub pipeline: String,
}

impl LocalizeRequestDoc {
    pub fn from_request(req: &LocalizeRequest) -> Self {
        Self {
            request_id: req.request_id.clone(),
            superevent_id: req.superevent_id.clone(),
            graceid: req.graceid.clone(),
            pipeline: req.pipeline.clone(),
        }
    }
}

/// Audit record for one `LocalizeResult` boom-gw received.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalizeResultDoc {
    #[serde(rename = "_id")]
    pub request_id: String,
    pub superevent_id: String,
    pub graceid: String,
    pub status: LocalizeStatus,
    pub elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skymap_fits_bytes: Option<i64>,
}

impl LocalizeResultDoc {
    pub fn from_result(result: &LocalizeResult) -> Result<Self, ArchiveError> {
        let skymap_fits_bytes = result
            .skymap_fits_bytes()
            .ok()
            .flatten()
            .map(|b| b.len() as i64);
        Ok(Self {
            request_id: result.request_id.clone(),
            superevent_id: result.superevent_id.clone(),
            graceid: result.graceid.clone(),
            status: result.status,
            elapsed_ms: result.elapsed_ms,
            error_message: result.error_message.clone(),
            skymap_fits_bytes,
        })
    }
}

/// Free-form annotation attached to a superevent (e.g.
/// `p_astro` from a downstream classifier, an ML score, a manual
/// operator note). Annotations are append-only: corrections take the
/// form of a new annotation with a later `created_at`. The `payload`
/// field is a free-form BSON document so the archive does not need to
/// know about every annotation kind ahead of time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnnotationDoc {
    #[serde(rename = "_id")]
    pub id: String,
    pub superevent_id: String,
    /// Short label identifying the annotation kind
    /// (e.g. `"p_astro"`, `"ml_classification"`, `"manual_note"`).
    pub kind: String,
    /// Who created the annotation. Strings rather than refs into a
    /// users collection until we add auth. `"system"` is reserved for
    /// boom-gw-internal annotations.
    pub author: String,
    /// Free-form payload. Anything BSON-encodable goes here.
    pub payload: mongodb::bson::Bson,
    /// Server-assigned creation time.
    pub created_at: mongodb::bson::DateTime,
}

impl AnnotationDoc {
    /// Build a new annotation with a freshly-allocated UUID `_id` and
    /// `created_at = now`.
    pub fn new(
        superevent_id: impl Into<String>,
        kind: impl Into<String>,
        author: impl Into<String>,
        payload: mongodb::bson::Bson,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            superevent_id: superevent_id.into(),
            kind: kind.into(),
            author: author.into(),
            payload,
            created_at: mongodb::bson::DateTime::now(),
        }
    }
}

/// Live MongoDB archive handle. Cheap to clone — wraps a
/// `mongodb::Database` which is itself a thin handle around a shared
/// connection pool.
#[derive(Clone)]
pub struct Archive {
    db: Database,
}

impl Archive {
    /// Connect to MongoDB, ping the server to confirm reachability,
    /// and create the indices boom-gw relies on. Safe to call on a
    /// cold database — indices are idempotent.
    pub async fn connect(config: ArchiveConfig) -> Result<Self, ArchiveError> {
        let mut opts = ClientOptions::parse(&config.uri).await?;
        opts.app_name = config.app_name.clone();
        let client = Client::with_options(opts)?;
        let db = client.database(&config.database);
        // Ping so we fail fast if the server is unreachable / auth is
        // wrong, rather than discovering it on the first write.
        db.run_command(doc! {"ping": 1}).await?;
        let archive = Self { db };
        archive.ensure_indices().await?;
        info!(database = %config.database, "connected to MongoDB archive");
        Ok(archive)
    }

    async fn ensure_indices(&self) -> Result<(), ArchiveError> {
        self.events()
            .create_index(IndexModel::builder().keys(doc! {"_id": 1}).build())
            .await?;
        self.events()
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"producer_timestamp": 1})
                    .build(),
            )
            .await?;
        self.events()
            .create_index(IndexModel::builder().keys(doc! {"pipeline": 1}).build())
            .await?;

        self.superevents()
            .create_index(IndexModel::builder().keys(doc! {"_id": 1}).build())
            .await?;
        self.superevents()
            .create_index(IndexModel::builder().keys(doc! {"t_0": 1}).build())
            .await?;

        self.localize_requests()
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"superevent_id": 1})
                    .build(),
            )
            .await?;
        self.localize_results()
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"superevent_id": 1})
                    .build(),
            )
            .await?;

        self.annotations()
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"superevent_id": 1, "created_at": -1})
                    .build(),
            )
            .await?;

        // The `_id` indices above are explicit duplicates of the
        // implicit-unique one mongo creates on every collection
        // automatically; they exist for parity with BOOM's pattern of
        // declaring each collection's primary index alongside its
        // secondaries.
        Ok(())
    }

    /// Borrow the underlying database handle. Useful for opening
    /// dynamically-typed collections (e.g. for read handlers that
    /// pull a different deserialization target than the writer used).
    pub fn database(&self) -> &Database {
        &self.db
    }

    pub fn events(&self) -> Collection<EventDoc> {
        self.db.collection(EVENTS_COLLECTION)
    }

    pub fn superevents(&self) -> Collection<SupereventDoc> {
        self.db.collection(SUPEREVENTS_COLLECTION)
    }

    pub fn localize_requests(&self) -> Collection<LocalizeRequestDoc> {
        self.db.collection(LOCALIZE_REQUESTS_COLLECTION)
    }

    pub fn localize_results(&self) -> Collection<LocalizeResultDoc> {
        self.db.collection(LOCALIZE_RESULTS_COLLECTION)
    }

    pub fn annotations(&self) -> Collection<AnnotationDoc> {
        self.db.collection(ANNOTATIONS_COLLECTION)
    }

    /// Upsert one event by graceid. Idempotent: replays of the same
    /// graceid simply overwrite the prior document.
    pub async fn record_event(&self, event: &GwEvent) -> Result<(), ArchiveError> {
        let doc = EventDoc::from_event(event)?;
        let filter = doc! {"_id": &doc.graceid};
        self.events().replace_one(filter, &doc).upsert(true).await?;
        Ok(())
    }

    /// Upsert one superevent by id. Called after every clustering
    /// update and every sky-map attachment, so the archive always
    /// reflects the current state of the open window.
    pub async fn upsert_superevent(&self, superevent: &Superevent) -> Result<(), ArchiveError> {
        let doc = SupereventDoc::from_superevent(superevent);
        let filter = doc! {"_id": &doc.id};
        self.superevents()
            .replace_one(filter, &doc)
            .upsert(true)
            .await?;
        Ok(())
    }

    pub async fn record_localize_request(&self, req: &LocalizeRequest) -> Result<(), ArchiveError> {
        let doc = LocalizeRequestDoc::from_request(req);
        let filter = doc! {"_id": &doc.request_id};
        self.localize_requests()
            .replace_one(filter, &doc)
            .upsert(true)
            .await?;
        Ok(())
    }

    pub async fn record_localize_result(
        &self,
        result: &LocalizeResult,
    ) -> Result<(), ArchiveError> {
        let doc = LocalizeResultDoc::from_result(result)?;
        let filter = doc! {"_id": &doc.request_id};
        self.localize_results()
            .replace_one(filter, &doc)
            .upsert(true)
            .await?;
        Ok(())
    }

    /// Insert an annotation. Annotations are append-only — the
    /// `_id` is a freshly-allocated UUID, so consecutive calls always
    /// produce distinct documents.
    pub async fn insert_annotation(&self, annotation: &AnnotationDoc) -> Result<(), ArchiveError> {
        self.annotations().insert_one(annotation).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use base64::Engine as _;
    use igwn_ligolw::CoincInspiralEvent;

    fn dummy_event(graceid: &str, snr: f64) -> GwEvent {
        let coinc = CoincInspiralEvent {
            coinc_event_id: graceid.into(),
            ifos: "H1,L1".into(),
            combined_far: 1e-9,
            snr,
            mass: None,
            mchirp: None,
            end_time: 1_400_000_000.0,
            sngls: vec![],
        };
        GwEvent {
            pipeline: "gstlal".into(),
            graceid: graceid.into(),
            producer_timestamp: 0.0,
            message_type: "new".into(),
            submitter: "ci".into(),
            end_time: 1_400_000_000.0,
            ifos: "H1,L1".into(),
            snr,
            far: 1e-9,
            mchirp: None,
            total_mass: None,
            coinc,
        }
    }

    #[test]
    fn event_doc_uses_graceid_as_id() {
        let doc = EventDoc::from_event(&dummy_event("G42", 10.0)).unwrap();
        let bson = mongodb::bson::to_bson(&doc).unwrap();
        let document = bson.as_document().unwrap();
        assert_eq!(document.get_str("_id").unwrap(), "G42");
        assert_eq!(document.get_str("pipeline").unwrap(), "gstlal");
    }

    #[test]
    fn superevent_doc_summarizes_g_event_ids() {
        let ev = dummy_event("G42", 10.0);
        let s = Superevent {
            id: "S000001".into(),
            t_0: 1_400_000_000.0,
            t_start: 1_399_999_997.5,
            t_end: 1_400_000_002.5,
            preferred_event: ev.clone(),
            g_events: vec![ev],
            skymap: None,
        };
        let doc = SupereventDoc::from_superevent(&s);
        assert_eq!(doc.id, "S000001");
        assert_eq!(doc.preferred_graceid, "G42");
        assert_eq!(doc.g_event_graceids, vec!["G42".to_string()]);
        assert!(doc.skymap.is_none());
    }

    #[test]
    fn superevent_doc_carries_skymap_when_attached() {
        let ev = dummy_event("G42", 10.0);
        let s = Superevent {
            id: "S000001".into(),
            t_0: 1_400_000_000.0,
            t_start: 1_399_999_997.5,
            t_end: 1_400_000_002.5,
            preferred_event: ev.clone(),
            g_events: vec![ev],
            skymap: Some(SkyMapFits {
                bytes: b"FITS-BYTES".to_vec(),
                elapsed_ms: 137,
            }),
        };
        let doc = SupereventDoc::from_superevent(&s);
        let sky = doc.skymap.unwrap();
        assert_eq!(sky.bytes, b"FITS-BYTES");
        assert_eq!(sky.elapsed_ms, 137);
    }

    #[test]
    fn localize_request_doc_uses_request_id_as_id() {
        let req = LocalizeRequest::from_coinc_xml("req-1", "S0", "G42", "gstlal", b"<x/>");
        let doc = LocalizeRequestDoc::from_request(&req);
        let bson = mongodb::bson::to_bson(&doc).unwrap();
        let document = bson.as_document().unwrap();
        assert_eq!(document.get_str("_id").unwrap(), "req-1");
        assert_eq!(document.get_str("superevent_id").unwrap(), "S0");
    }

    #[test]
    fn annotation_doc_generates_unique_ids() {
        let a = AnnotationDoc::new("S0", "p_astro", "ci", mongodb::bson::Bson::Double(0.9));
        let b = AnnotationDoc::new("S0", "p_astro", "ci", mongodb::bson::Bson::Double(0.9));
        assert_ne!(a.id, b.id, "uuid v4 should not collide");
        assert_eq!(a.superevent_id, "S0");
        assert_eq!(a.kind, "p_astro");
        assert_eq!(a.author, "ci");
    }

    #[test]
    fn annotation_doc_serializes_id_as_underscore_id() {
        let ann = AnnotationDoc::new("S0", "manual_note", "ci", mongodb::bson::Bson::Null);
        let bson = mongodb::bson::to_bson(&ann).unwrap();
        let document = bson.as_document().unwrap();
        assert_eq!(document.get_str("_id").unwrap(), ann.id);
        assert!(document.contains_key("created_at"));
    }

    #[test]
    fn localize_result_doc_records_fits_size_not_bytes() {
        let fits = b"FITS-PAYLOAD".to_vec();
        let result = LocalizeResult {
            request_id: "req-1".into(),
            superevent_id: "S0".into(),
            graceid: "G42".into(),
            status: LocalizeStatus::Ok,
            skymap_fits: Some(BASE64.encode(&fits)),
            error_message: None,
            elapsed_ms: 137,
        };
        let doc = LocalizeResultDoc::from_result(&result).unwrap();
        // The archive stores the size, not the bytes themselves — the
        // FITS lives in the superevent document where it is queried
        // alongside the rest of the state.
        assert_eq!(doc.skymap_fits_bytes, Some(fits.len() as i64));
        assert_eq!(doc.elapsed_ms, 137);
        assert!(matches!(doc.status, LocalizeStatus::Ok));
    }
}
