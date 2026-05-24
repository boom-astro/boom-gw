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

use crate::clustering::Superevent;
use crate::event::GwEvent;
use crate::localizer::{LocalizeRequest, LocalizeResult, LocalizeStatus};

pub const DEFAULT_DB_NAME: &str = "boom_gw";
pub const EVENTS_COLLECTION: &str = "events";
pub const SUPEREVENTS_COLLECTION: &str = "superevents";
pub const LOCALIZE_REQUESTS_COLLECTION: &str = "localize_requests";
pub const LOCALIZE_RESULTS_COLLECTION: &str = "localize_results";
pub const ANNOTATIONS_COLLECTION: &str = "annotations";
pub const ALERTS_COLLECTION: &str = "alerts";
pub const GRB_TRIGGERS_COLLECTION: &str = "grb_triggers";
pub const CROSS_MATCHES_COLLECTION: &str = "superevent_grb_matches";
pub const BOOM_ALERTS_COLLECTION: &str = "boom_alerts";
pub const FRB_ALERTS_COLLECTION: &str = "frb_alerts";
pub const NEUTRINO_ALERTS_COLLECTION: &str = "neutrino_alerts";
pub const ICECUBE_LVK_SEARCHES_COLLECTION: &str = "icecube_lvk_searches";

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

/// Lightweight summary of an attached sky map. Inlined in
/// [`SupereventDoc`] so list queries can answer "does this
/// superevent have a sky map yet, and how big is it?" without
/// pulling the FITS bytes. The actual FITS lives in the
/// [`crate::storage::skymap::SkymapStorage`] (mongo `skymaps`
/// collection or S3), keyed by `superevent_id`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkymapSummary {
    pub bytes_size: i64,
    pub elapsed_ms: u64,
    /// Representative position for the localization, in degrees —
    /// the sphere-average of the 50% credible region's cell
    /// centers. Set by `POST /api/superevents` when the FITS is
    /// parseable; `None` for pre-localization superevents or when
    /// centroid computation failed. The frontend uses this to
    /// initial-center the Aladin viewer so the operator doesn't
    /// have to pan to find the contour.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub center_ra: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub center_dec: Option<f64>,
}

/// One superevent, keyed by superevent_id. Updated in place as the
/// preferred event changes and as the sky map arrives.
///
/// **Storage note**: the FITS bytes are NOT stored on this
/// document; they live in a separate `skymaps` collection (mongo)
/// or object-store bucket (S3). See
/// [`crate::storage::skymap::SkymapStorage`] for the dispatch.
/// The `skymap_summary` field below is enough for list endpoints
/// to filter on "has-a-skymap" without paying the cost of fetching
/// every FITS.
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
    pub skymap_summary: Option<SkymapSummary>,
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
            skymap_summary: s.skymap.as_ref().map(|sky| SkymapSummary {
                bytes_size: sky.bytes.len() as i64,
                elapsed_ms: sky.elapsed_ms,
                center_ra: None,
                center_dec: None,
            }),
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

/// Audit record for one public [`PublicAlert`](crate::alert::PublicAlert)
/// boom-gw assembled and (optionally) published. Stores the alert's
/// full JSON body so future replays and post-hoc inspection do not
/// need to re-derive the wire shape from the live state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertDoc {
    #[serde(rename = "_id")]
    pub id: String,
    pub superevent_id: String,
    pub alert_type: String,
    /// The wire-shape alert body, persisted as the same JSON we sent.
    pub body: mongodb::bson::Bson,
    /// Server-assigned creation time.
    pub created_at: mongodb::bson::DateTime,
    /// `true` once a Kafka publish acknowledged successfully. `false`
    /// when the operator built the alert but a downstream publish
    /// failed (so the audit row still tells us we attempted).
    pub published: bool,
}

impl AlertDoc {
    pub fn new(
        superevent_id: impl Into<String>,
        alert_type: impl Into<String>,
        body: mongodb::bson::Bson,
        published: bool,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            superevent_id: superevent_id.into(),
            alert_type: alert_type.into(),
            body,
            created_at: mongodb::bson::DateTime::now(),
            published,
        }
    }
}

/// One ingested GRB trigger (Fermi GBM, Swift BAT, etc.). `_id` is
/// the natural `(instrument, trigger_id)` composite — same trigger
/// arriving twice (e.g. GBM flight → ground → final updates) upserts
/// in place rather than fanning out.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrbTriggerDoc {
    #[serde(rename = "_id")]
    pub id: GrbTriggerId,
    #[serde(flatten)]
    pub trigger: crate::grb::GrbTrigger,
    /// Server-side ingest time. Distinct from
    /// `trigger.trigger_time` (the GPS time the instrument flagged
    /// the burst) — `ingested_at` is when boom-gw received it.
    pub ingested_at: mongodb::bson::DateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GrbTriggerId {
    pub instrument: String,
    pub trigger_id: String,
}

impl GrbTriggerDoc {
    pub fn from_trigger(trigger: crate::grb::GrbTrigger) -> Self {
        let id = GrbTriggerId {
            instrument: trigger.instrument.clone(),
            trigger_id: trigger.trigger_id.clone(),
        };
        Self {
            id,
            trigger,
            ingested_at: mongodb::bson::DateTime::now(),
        }
    }
}

/// One GW superevent × GRB trigger cross-match result. `_id` is
/// composite `(superevent_id, instrument, trigger_id)` so repeated
/// cross-match runs against the same pair overwrite cleanly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossMatchDoc {
    #[serde(rename = "_id")]
    pub id: CrossMatchId,
    /// Denormalized FK fields so the natural query (`find all
    /// matches for superevent X`) doesn't have to project on `_id`.
    pub superevent_id: String,
    pub instrument: String,
    pub trigger_id: String,
    #[serde(flatten)]
    pub result: crate::grb::CrossMatchResult,
    /// When this match was computed. Important because GRB
    /// positions update (FLT → GND → FIN) and the matched GW
    /// skymap can be re-issued; the most recent `computed_at` is
    /// the operational truth.
    pub computed_at: mongodb::bson::DateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossMatchId {
    pub superevent_id: String,
    pub instrument: String,
    pub trigger_id: String,
}

impl CrossMatchDoc {
    pub fn new(
        superevent_id: impl Into<String>,
        trigger: &crate::grb::GrbTrigger,
        result: crate::grb::CrossMatchResult,
    ) -> Self {
        let superevent_id = superevent_id.into();
        let id = CrossMatchId {
            superevent_id: superevent_id.clone(),
            instrument: trigger.instrument.clone(),
            trigger_id: trigger.trigger_id.clone(),
        };
        Self {
            id,
            superevent_id,
            instrument: trigger.instrument.clone(),
            trigger_id: trigger.trigger_id.clone(),
            result,
            computed_at: mongodb::bson::DateTime::now(),
        }
    }
}

/// Persisted form of one BOOM optical transient. Carries the
/// typed summary fields the list view renders + the upstream
/// `BoomTransient` (which itself retains the full alert envelope)
/// so callers can drill in without re-querying GCN.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoomAlertDoc {
    #[serde(rename = "_id")]
    pub id: String,
    pub alert_id: String,
    pub alert_time: f64,
    pub event_name: String,
    #[serde(default)]
    pub ra: Option<f64>,
    #[serde(default)]
    pub dec: Option<f64>,
    #[serde(default)]
    pub error_radius_deg: Option<f64>,
    #[serde(default)]
    pub classification: Option<String>,
    #[serde(default)]
    pub classification_score: Option<f64>,
    #[serde(default)]
    pub cross_match_summary: Option<String>,
    /// Denormalized at ingest from `transient.first_detection_time`
    /// / `transient.last_non_detection_time` so the scan query
    /// `last_non_det <= t_0 <= first_det` is a simple two-field
    /// index lookup. Optical transients that have no detection
    /// row leave both as `None` and are excluded from the scan.
    #[serde(default)]
    pub first_detection_time: Option<f64>,
    #[serde(default)]
    pub last_non_detection_time: Option<f64>,
    /// Full `BoomTransient` payload, including photometry + raw
    /// envelope body.
    pub transient: crate::boom::BoomTransient,
    pub ingested_at: mongodb::bson::DateTime,
}

impl BoomAlertDoc {
    pub fn from_transient(t: crate::boom::BoomTransient) -> Self {
        Self {
            id: t.alert_id.clone(),
            alert_id: t.alert_id.clone(),
            alert_time: t.alert_time,
            event_name: t.event_name.clone(),
            ra: t.ra,
            dec: t.dec,
            error_radius_deg: t.error_radius_deg,
            classification: t.classification.clone(),
            classification_score: t.classification_score,
            cross_match_summary: t.cross_match_summary.clone(),
            first_detection_time: t.first_detection_time,
            last_non_detection_time: t.last_non_detection_time,
            transient: t,
            ingested_at: mongodb::bson::DateTime::now(),
        }
    }
}

/// Persisted form of one FRB alert (CHIME or DSA110). Carries the
/// GRB-shaped trigger view that scan-cross-matches consumes, plus
/// the source-specific fields the External Streams table renders.
/// `_id` is the natural `(instrument, trigger_id)` composite so a
/// re-published alert with the same id upserts in place.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrbAlertDoc {
    #[serde(rename = "_id")]
    pub id: GrbTriggerId,
    #[serde(flatten)]
    pub alert: crate::frb::FrbAlert,
    pub ingested_at: mongodb::bson::DateTime,
}

impl FrbAlertDoc {
    pub fn from_alert(alert: crate::frb::FrbAlert) -> Self {
        let id = GrbTriggerId {
            instrument: alert.trigger.instrument.clone(),
            trigger_id: alert.trigger.trigger_id.clone(),
        };
        Self {
            id,
            alert,
            ingested_at: mongodb::bson::DateTime::now(),
        }
    }
}

/// Persisted form of one high-energy neutrino alert (IceCube
/// single-neutrino + KM3NeT). Same `(instrument, trigger_id)`
/// composite key strategy as [`FrbAlertDoc`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NeutrinoAlertDoc {
    #[serde(rename = "_id")]
    pub id: GrbTriggerId,
    #[serde(flatten)]
    pub alert: crate::neutrino::NeutrinoAlert,
    pub ingested_at: mongodb::bson::DateTime,
}

impl NeutrinoAlertDoc {
    pub fn from_alert(alert: crate::neutrino::NeutrinoAlert) -> Self {
        let id = GrbTriggerId {
            instrument: alert.trigger.instrument.clone(),
            trigger_id: alert.trigger.trigger_id.clone(),
        };
        Self {
            id,
            alert,
            ingested_at: mongodb::bson::DateTime::now(),
        }
    }
}

/// Persisted form of one IceCube LVK Nu Track Search result. Each
/// search runs against exactly one superevent; the natural key is
/// `(superevent_id, alert_time)` so a re-issued search (refined
/// numbers after more livetime accumulates) upserts in place.
///
/// `superevent_id` is reachable at the document root via the
/// flattened `search` field, so the list-by-superevent filter can
/// query `{"superevent_id": "..."}` without a separate
/// denormalized column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IceCubeLvkSearchDoc {
    #[serde(rename = "_id")]
    pub id: IceCubeLvkSearchId,
    #[serde(flatten)]
    pub search: crate::icecube_lvk::IceCubeLvkSearch,
    pub ingested_at: mongodb::bson::DateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IceCubeLvkSearchId {
    pub superevent_id: String,
    /// `alert_time` in GPS seconds, stringified so the BSON
    /// composite key sorts cleanly. Mongo composite keys with raw
    /// floats are technically allowed but bite on
    /// equality/hashing; the string form sidesteps that.
    pub alert_time_gps: String,
}

impl IceCubeLvkSearchDoc {
    pub fn from_search(search: crate::icecube_lvk::IceCubeLvkSearch) -> Self {
        let id = IceCubeLvkSearchId {
            superevent_id: search.superevent_id.clone(),
            alert_time_gps: format!("{:.6}", search.alert_time),
        };
        Self {
            id,
            search,
            ingested_at: mongodb::bson::DateTime::now(),
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

        self.alerts()
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"superevent_id": 1, "created_at": -1})
                    .build(),
            )
            .await?;

        // GRB triggers — supports both lookups by ingest order
        // (operator dashboards) and by trigger time (cross-match
        // window queries).
        self.grb_triggers()
            .create_index(IndexModel::builder().keys(doc! {"ingested_at": -1}).build())
            .await?;
        self.grb_triggers()
            .create_index(IndexModel::builder().keys(doc! {"trigger_time": 1}).build())
            .await?;

        // FRBs + high-energy neutrinos use the same access pattern
        // as GRBs: list by ingest order, filter the scan by GPS
        // trigger time. Same two indices on each.
        for coll_name in [FRB_ALERTS_COLLECTION, NEUTRINO_ALERTS_COLLECTION] {
            let coll = self.db.collection::<mongodb::bson::Document>(coll_name);
            coll.create_index(IndexModel::builder().keys(doc! {"ingested_at": -1}).build())
                .await?;
            coll.create_index(IndexModel::builder().keys(doc! {"trigger_time": 1}).build())
                .await?;
        }

        // IceCube LVK searches are looked up "all searches for this
        // superevent, newest first".
        self.icecube_lvk_searches()
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"superevent_id": 1, "alert_time": -1})
                    .build(),
            )
            .await?;

        // Cross-matches — primary access is "all matches for this
        // superevent, most-recently-computed first".
        self.cross_matches()
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"superevent_id": 1, "computed_at": -1})
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

    pub fn alerts(&self) -> Collection<AlertDoc> {
        self.db.collection(ALERTS_COLLECTION)
    }

    pub fn grb_triggers(&self) -> Collection<GrbTriggerDoc> {
        self.db.collection(GRB_TRIGGERS_COLLECTION)
    }

    pub fn cross_matches(&self) -> Collection<CrossMatchDoc> {
        self.db.collection(CROSS_MATCHES_COLLECTION)
    }

    pub fn boom_alerts(&self) -> Collection<BoomAlertDoc> {
        self.db.collection(BOOM_ALERTS_COLLECTION)
    }

    pub fn frb_alerts(&self) -> Collection<FrbAlertDoc> {
        self.db.collection(FRB_ALERTS_COLLECTION)
    }

    pub fn neutrino_alerts(&self) -> Collection<NeutrinoAlertDoc> {
        self.db.collection(NEUTRINO_ALERTS_COLLECTION)
    }

    pub fn icecube_lvk_searches(&self) -> Collection<IceCubeLvkSearchDoc> {
        self.db.collection(ICECUBE_LVK_SEARCHES_COLLECTION)
    }

    /// Upsert one BOOM optical transient. `_id` is the natural
    /// `(alert_datetime, event_name)` key from the upstream
    /// envelope, so a re-published alert overwrites in place.
    pub async fn upsert_boom_alert(&self, alert: &BoomAlertDoc) -> Result<bool, ArchiveError> {
        let filter = doc! {"_id": &alert.id};
        let res = self
            .boom_alerts()
            .replace_one(filter, alert)
            .upsert(true)
            .await?;
        Ok(res.upserted_id.is_some())
    }

    /// Upsert one FRB alert. Returns `true` when the document was
    /// freshly created, `false` when an existing one was replaced —
    /// the HTTP handler uses the flag to pick 201 vs 200.
    pub async fn upsert_frb_alert(&self, doc: &FrbAlertDoc) -> Result<bool, ArchiveError> {
        let filter = mongodb::bson::to_document(&doc! {"_id": mongodb::bson::to_bson(&doc.id)?})?;
        let res = self
            .frb_alerts()
            .replace_one(filter, doc)
            .upsert(true)
            .await?;
        Ok(res.upserted_id.is_some())
    }

    /// Upsert one high-energy neutrino alert. Same created-vs-
    /// replaced semantics as [`Self::upsert_frb_alert`].
    pub async fn upsert_neutrino_alert(
        &self,
        doc: &NeutrinoAlertDoc,
    ) -> Result<bool, ArchiveError> {
        let filter = mongodb::bson::to_document(&doc! {"_id": mongodb::bson::to_bson(&doc.id)?})?;
        let res = self
            .neutrino_alerts()
            .replace_one(filter, doc)
            .upsert(true)
            .await?;
        Ok(res.upserted_id.is_some())
    }

    /// Upsert one IceCube LVK Nu Track Search result. The key is
    /// `(superevent_id, alert_time)` — a re-issued search for the
    /// same superevent overwrites the prior result.
    pub async fn upsert_icecube_lvk_search(
        &self,
        doc: &IceCubeLvkSearchDoc,
    ) -> Result<bool, ArchiveError> {
        let filter = mongodb::bson::to_document(&doc! {"_id": mongodb::bson::to_bson(&doc.id)?})?;
        let res = self
            .icecube_lvk_searches()
            .replace_one(filter, doc)
            .upsert(true)
            .await?;
        Ok(res.upserted_id.is_some())
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

    /// Insert an alert audit row. Like annotations, alerts are
    /// append-only.
    pub async fn insert_alert(&self, alert: &AlertDoc) -> Result<(), ArchiveError> {
        self.alerts().insert_one(alert).await?;
        Ok(())
    }

    /// Upsert a GRB trigger. Same `(instrument, trigger_id)` from a
    /// later notice (FLT → GND → FIN) overwrites the earlier doc.
    /// Returns `true` if a new doc was created, `false` if an
    /// existing one was replaced — useful for the API handler to
    /// pick the right HTTP status.
    pub async fn upsert_grb_trigger(&self, doc: &GrbTriggerDoc) -> Result<bool, ArchiveError> {
        let filter = mongodb::bson::to_document(&doc! {"_id": mongodb::bson::to_bson(&doc.id)?})?;
        let res = self
            .grb_triggers()
            .replace_one(filter, doc)
            .upsert(true)
            .await?;
        Ok(res.upserted_id.is_some())
    }

    /// Upsert a cross-match result. Always overwrites prior matches
    /// for the same `(superevent_id, instrument, trigger_id)` — the
    /// freshest computation is the operational truth.
    pub async fn upsert_cross_match(&self, doc: &CrossMatchDoc) -> Result<(), ArchiveError> {
        let filter = mongodb::bson::to_document(&doc! {"_id": mongodb::bson::to_bson(&doc.id)?})?;
        self.cross_matches()
            .replace_one(filter, doc)
            .upsert(true)
            .await?;
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
        assert!(doc.skymap_summary.is_none());
    }

    #[test]
    fn superevent_doc_carries_summary_when_skymap_attached() {
        // The doc itself never holds the FITS bytes anymore — they
        // live in the SkymapStorage. The summary lets list queries
        // tell "has-skymap" + size without pulling the bytes.
        let ev = dummy_event("G42", 10.0);
        let s = Superevent {
            id: "S000001".into(),
            t_0: 1_400_000_000.0,
            t_start: 1_399_999_997.5,
            t_end: 1_400_000_002.5,
            preferred_event: ev.clone(),
            g_events: vec![ev],
            skymap: Some(crate::clustering::SkyMapFits {
                bytes: b"FITS-BYTES".to_vec(),
                elapsed_ms: 137,
            }),
        };
        let doc = SupereventDoc::from_superevent(&s);
        let summary = doc.skymap_summary.expect("summary present");
        assert_eq!(summary.bytes_size, 10);
        assert_eq!(summary.elapsed_ms, 137);
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
