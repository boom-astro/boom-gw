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

use futures::TryStreamExt;
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
pub const LOCALIZE_SKIPS_COLLECTION: &str = "localize_skips";
pub const ANNOTATIONS_COLLECTION: &str = "annotations";
pub const ALERTS_COLLECTION: &str = "alerts";
pub const GRB_TRIGGERS_COLLECTION: &str = "grb_triggers";
pub const CROSS_MATCHES_COLLECTION: &str = "superevent_grb_matches";
pub const BOOM_ALERTS_COLLECTION: &str = "boom_alerts";
pub const FRB_ALERTS_COLLECTION: &str = "frb_alerts";
pub const NEUTRINO_ALERTS_COLLECTION: &str = "neutrino_alerts";
pub const ICECUBE_LVK_SEARCHES_COLLECTION: &str = "icecube_lvk_searches";
pub const HEALTH_CONFIG_COLLECTION: &str = "health_config";
pub const SCIENCE_FILTERS_COLLECTION: &str = "science_filters";
// SkyPortal-style access control: persisted users, roles, groups,
// membership, and streams. See `crate::access`.
pub const USERS_COLLECTION: &str = "users";
pub const ROLES_COLLECTION: &str = "roles";
pub const GROUPS_COLLECTION: &str = "groups";
pub const GROUP_USERS_COLLECTION: &str = "group_users";
pub const STREAMS_COLLECTION: &str = "streams";
pub const GROUP_STREAMS_COLLECTION: &str = "group_streams";
pub const STREAM_USERS_COLLECTION: &str = "stream_users";

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
    /// Wall-clock receipt time. `default` so docs written before
    /// this field existed deserialize as epoch and don't break
    /// `find_one` queries; the dashboard's freshness panel treats
    /// epoch as "no signal" and falls back to the row count.
    #[serde(default = "default_event_doc_ingested_at")]
    pub ingested_at: mongodb::bson::DateTime,
}

fn default_event_doc_ingested_at() -> mongodb::bson::DateTime {
    mongodb::bson::DateTime::from_millis(0)
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
            ingested_at: mongodb::bson::DateTime::now(),
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

/// Audit record for one `LocalizeRequest` boom-gw did *not* publish
/// because the SNR/FAR gate fired. `_id` is the request-id we would
/// have published (`{superevent_id}-{graceid}`) so a re-run of the
/// same g-event idempotently overwrites rather than fanning out.
/// The dashboard's gate panel reads counts from this collection to
/// compute submitted/skipped ratios.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalizeSkipDoc {
    #[serde(rename = "_id")]
    pub request_id: String,
    pub superevent_id: String,
    pub graceid: String,
    pub pipeline: String,
    pub snr: f64,
    pub far: f64,
    pub min_snr: f64,
    pub max_far_hz: f64,
    pub skipped_at: mongodb::bson::DateTime,
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

/// A named confidence tier inside a [`ScienceFilterDoc`]. A
/// cross-match is tagged with the most-significant tier whose
/// threshold it clears. Tiers cut on the bias-corrected *remapped*
/// joint FAR (events/year); smaller is more significant.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConfidenceTier {
    /// Display label, e.g. `"gold"` / `"silver"`.
    pub name: String,
    /// Upper bound on `joint_far_remapped_per_year` (events/year) to
    /// qualify for this tier.
    pub joint_far_remapped_max_per_year: f64,
}

/// Threshold cuts a [`ScienceFilterDoc`] applies to the objective
/// cross-match metrics. Every field is optional — an unset cut is not
/// applied. Each cut maps to a field already stored on
/// [`CrossMatchDoc`], so a filtered query is a Mongo query plus tier
/// tagging: none of the (expensive) geometry is recomputed.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct FilterCuts {
    /// Restrict to these instruments (matched against
    /// `CrossMatchDoc.instrument`). Empty → any instrument.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub instruments: Vec<String>,
    /// Half-width of the coincidence window in seconds: keep matches
    /// with `|time_offset_sec| <= time_window_sec`. Note this narrows
    /// the *stored* metrics by their already-computed time offset; it
    /// does not re-derive the joint FAR at a new window (that needs
    /// the per-filter recompute deferred to a later phase).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_window_sec: Option<f64>,
    /// Minimum spatial overlap (GW probability inside the external
    /// error region), in [0, 1].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spatial_overlap_min: Option<f64>,
    /// Maximum empirical p-value. Matches without a computed p-value
    /// are excluded when this cut is set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p_value_max: Option<f64>,
    /// Maximum bias-corrected remapped joint FAR (events/year).
    /// Matches without a remapped FAR are excluded when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub joint_far_remapped_max_per_year: Option<f64>,
    /// Require the external position to fall in the GW 90% credible
    /// region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_in_90cr: Option<bool>,
}

/// A saved, per-user science filter: the cuts that decide which GW ×
/// external cross-matches count as associations, plus the named
/// confidence tiers they fall into. The objective metrics live on
/// [`CrossMatchDoc`]; a filter is a cheap, reusable predicate over
/// them, so two users can surface different associations at different
/// confidence from the same stored metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScienceFilterDoc {
    #[serde(rename = "_id")]
    pub id: String,
    /// Principal `sub` that owns (and may edit) this filter.
    pub owner: String,
    /// Group this filter is shared with (FK to [`GroupDoc::id`]).
    /// `None` → private to the owner. Visible to the owner plus members
    /// of the group; editable by owner, group admins, or holders of the
    /// `Manage science filters` ACL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    /// Messenger streams this filter draws from (FK to [`StreamDoc::id`]).
    /// Must be a subset of the group's streams. Empty → unconstrained.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stream_ids: Vec<String>,
    pub name: String,
    /// Whether this filter participates in stream-time evaluation.
    /// Unused by the query-time prototype but stored so the schema is
    /// stable when the alerting path lands.
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub cuts: FilterCuts,
    /// Confidence tiers, kept sorted most-significant first (smallest
    /// `joint_far_remapped_max_per_year` first) on write. A match is
    /// tagged with the first tier it clears.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub confidence_tiers: Vec<ConfidenceTier>,
    pub created_at: mongodb::bson::DateTime,
    pub updated_at: mongodb::bson::DateTime,
}

impl ScienceFilterDoc {
    /// Build a new filter with a freshly-allocated UUID `_id`, default
    /// (empty) cuts, and `created_at = updated_at = now`.
    pub fn new(owner: impl Into<String>, name: impl Into<String>) -> Self {
        let now = mongodb::bson::DateTime::now();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            owner: owner.into(),
            group_id: None,
            stream_ids: Vec::new(),
            name: name.into(),
            active: false,
            cuts: FilterCuts::default(),
            confidence_tiers: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Sort the confidence tiers most-significant first (ascending
    /// `joint_far_remapped_max_per_year`) so [`Self::tier_for`] can
    /// stop at the first match. Call after setting `confidence_tiers`
    /// from client input, which may arrive in any order.
    pub fn sort_tiers(&mut self) {
        self.confidence_tiers.sort_by(|a, b| {
            a.joint_far_remapped_max_per_year
                .partial_cmp(&b.joint_far_remapped_max_per_year)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    /// The Mongo filter selecting the cross-matches that pass this
    /// filter's cuts for `superevent_id`. Because every cut maps to a
    /// field stored on `CrossMatchDoc`, the database does the work and
    /// pagination still applies.
    pub fn match_query(&self, superevent_id: &str) -> mongodb::bson::Document {
        use mongodb::bson::Bson;
        let mut q = doc! { "superevent_id": superevent_id };
        let c = &self.cuts;
        if !c.instruments.is_empty() {
            q.insert("instrument", doc! { "$in": &c.instruments });
        }
        if let Some(w) = c.time_window_sec {
            q.insert("time_offset_sec", doc! { "$gte": -w, "$lte": w });
        }
        if let Some(min) = c.spatial_overlap_min {
            q.insert("spatial_overlap", doc! { "$gte": min });
        }
        if let Some(max) = c.p_value_max {
            q.insert("p_value", doc! { "$ne": Bson::Null, "$lte": max });
        }
        if let Some(max) = c.joint_far_remapped_max_per_year {
            q.insert(
                "joint_far_remapped_per_year",
                doc! { "$ne": Bson::Null, "$lte": max },
            );
        }
        if c.require_in_90cr == Some(true) {
            q.insert("in_90cr", true);
        }
        q
    }

    /// The most-significant confidence tier a remapped joint FAR
    /// qualifies for, or `None` if it clears no tier (or the FAR is
    /// absent). Relies on [`Self::sort_tiers`] having run.
    pub fn tier_for(&self, joint_far_remapped_per_year: Option<f64>) -> Option<&ConfidenceTier> {
        let far = joint_far_remapped_per_year?;
        self.confidence_tiers
            .iter()
            .find(|t| far <= t.joint_far_remapped_max_per_year)
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

/// Operator-tunable knobs for the System Health dashboard. Lives
/// in `health_config:default` as a singleton — overwrite the doc
/// via `mongosh` (or a future PATCH endpoint) to change behavior
/// without a redeploy.
///
/// All fields default via [`HealthConfigDoc::default_doc`] so a
/// fresh DB or partial doc still yields a sensible dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthConfigDoc {
    #[serde(rename = "_id")]
    pub id: String,
    #[serde(default)]
    pub stream_stale_sec: StreamStaleConfig,
}

pub const HEALTH_CONFIG_DEFAULT_ID: &str = "default";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamStaleConfig {
    #[serde(default = "default_gracedb_gw_stale_sec")]
    pub gracedb_gw: u64,
    #[serde(default = "default_gcn_grb_stale_sec")]
    pub gcn_grb: u64,
    #[serde(default = "default_gcn_frb_stale_sec")]
    pub gcn_frb: u64,
    #[serde(default = "default_gcn_neutrino_stale_sec")]
    pub gcn_neutrino: u64,
    #[serde(default = "default_gcn_boom_stale_sec")]
    pub gcn_boom: u64,
}

fn default_gracedb_gw_stale_sec() -> u64 {
    900
}
fn default_gcn_grb_stale_sec() -> u64 {
    24 * 3600
}
fn default_gcn_frb_stale_sec() -> u64 {
    24 * 3600
}
fn default_gcn_neutrino_stale_sec() -> u64 {
    24 * 3600
}
fn default_gcn_boom_stale_sec() -> u64 {
    3600
}

impl Default for StreamStaleConfig {
    fn default() -> Self {
        Self {
            gracedb_gw: default_gracedb_gw_stale_sec(),
            gcn_grb: default_gcn_grb_stale_sec(),
            gcn_frb: default_gcn_frb_stale_sec(),
            gcn_neutrino: default_gcn_neutrino_stale_sec(),
            gcn_boom: default_gcn_boom_stale_sec(),
        }
    }
}

impl HealthConfigDoc {
    pub fn default_doc() -> Self {
        Self {
            id: HEALTH_CONFIG_DEFAULT_ID.into(),
            stream_stale_sec: StreamStaleConfig::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// SkyPortal-style access-control documents.
//
// Identity is otherwise token-derived per request (see `crate::auth`).
// These collections persist who exists, what they can do (roles → ACLs),
// which groups they belong to, and which messenger streams they can see.
// The read/enforce side lives in `crate::access`.
// ---------------------------------------------------------------------------

/// A persisted user, keyed by the OIDC `sub`. JIT-provisioned at login
/// (and lazily on the bearer path) — see `Archive::upsert_user`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserDoc {
    #[serde(rename = "_id")]
    pub sub: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Role slugs (FK to [`RoleDoc::id`]). Effective ACLs are the union
    /// over these roles; see [`crate::access`].
    #[serde(default)]
    pub role_ids: Vec<String>,
    #[serde(default = "default_true")]
    pub active: bool,
    pub created_at: mongodb::bson::DateTime,
    pub last_seen_at: mongodb::bson::DateTime,
}

fn default_true() -> bool {
    true
}

/// A named bundle of ACL strings. The default roles are seeded
/// (`system = true`) by [`Archive::seed_access_defaults`]; admins may
/// add or edit non-system roles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleDoc {
    #[serde(rename = "_id")]
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub acls: Vec<String>,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub system: bool,
}

/// A collaboration group. Data (science filters, future sources) is
/// shared with a group; membership is the unit of visibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupDoc {
    #[serde(rename = "_id")]
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// A user's personal single-member group (SkyPortal parity), so
    /// there's always somewhere to "share" to. Hidden from group lists.
    #[serde(default)]
    pub single_user_group: bool,
    pub created_at: mongodb::bson::DateTime,
}

impl GroupDoc {
    /// Build a new group with a freshly-allocated UUID `_id`.
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.into(),
            description: description.into(),
            single_user_group: false,
            created_at: mongodb::bson::DateTime::now(),
        }
    }
}

/// Group membership join row. Composite `_id = (group_id, user_sub)`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GroupUserId {
    pub group_id: String,
    pub user_sub: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupUserDoc {
    #[serde(rename = "_id")]
    pub id: GroupUserId,
    /// Denormalized so the natural queries ("members of group X",
    /// "groups of user Y") don't have to project on `_id`.
    pub group_id: String,
    pub user_sub: String,
    /// Group admins manage the group's membership and streams.
    #[serde(default)]
    pub admin: bool,
    pub created_at: mongodb::bson::DateTime,
}

impl GroupUserDoc {
    pub fn new(group_id: impl Into<String>, user_sub: impl Into<String>, admin: bool) -> Self {
        let group_id = group_id.into();
        let user_sub = user_sub.into();
        Self {
            id: GroupUserId {
                group_id: group_id.clone(),
                user_sub: user_sub.clone(),
            },
            group_id,
            user_sub,
            admin,
            created_at: mongodb::bson::DateTime::now(),
        }
    }
}

/// A messenger ingest channel. Seeded with the five boom-gw channels
/// (`gracedb_gw`, `gcn_grb`, `gcn_frb`, `gcn_neutrino`, `boom_optical`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamDoc {
    #[serde(rename = "_id")]
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub system: bool,
}

/// Which streams a group can access. Composite `_id = (group_id, stream_id)`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GroupStreamId {
    pub group_id: String,
    pub stream_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupStreamDoc {
    #[serde(rename = "_id")]
    pub id: GroupStreamId,
    pub group_id: String,
    pub stream_id: String,
    pub created_at: mongodb::bson::DateTime,
}

impl GroupStreamDoc {
    pub fn new(group_id: impl Into<String>, stream_id: impl Into<String>) -> Self {
        let group_id = group_id.into();
        let stream_id = stream_id.into();
        Self {
            id: GroupStreamId {
                group_id: group_id.clone(),
                stream_id: stream_id.clone(),
            },
            group_id,
            stream_id,
            created_at: mongodb::bson::DateTime::now(),
        }
    }
}

/// Which streams a user can access directly. Composite
/// `_id = (stream_id, user_sub)`. Adding a member to a group also
/// inserts these for the group's streams (see `crate::access_api`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamUserId {
    pub stream_id: String,
    pub user_sub: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamUserDoc {
    #[serde(rename = "_id")]
    pub id: StreamUserId,
    pub stream_id: String,
    pub user_sub: String,
    pub created_at: mongodb::bson::DateTime,
}

impl StreamUserDoc {
    pub fn new(stream_id: impl Into<String>, user_sub: impl Into<String>) -> Self {
        let stream_id = stream_id.into();
        let user_sub = user_sub.into();
        Self {
            id: StreamUserId {
                stream_id: stream_id.clone(),
                user_sub: user_sub.clone(),
            },
            stream_id,
            user_sub,
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
        archive.seed_access_defaults().await?;
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

        // Science filters are listed "all filters visible to this
        // caller" — owner-scoped plus group-shared — so index by owner
        // and by group for the membership-scoped `$in` query.
        self.science_filters()
            .create_index(IndexModel::builder().keys(doc! {"owner": 1}).build())
            .await?;
        self.science_filters()
            .create_index(IndexModel::builder().keys(doc! {"group_id": 1}).build())
            .await?;

        // Access control. Users keyed by sub (implicit _id index);
        // email looked up rarely. Groups have a unique name. The join
        // collections are queried from both sides, so index both FKs.
        self.users()
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"email": 1})
                    .options(
                        mongodb::options::IndexOptions::builder()
                            .sparse(true)
                            .build(),
                    )
                    .build(),
            )
            .await?;
        self.groups()
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"name": 1})
                    .options(
                        mongodb::options::IndexOptions::builder()
                            .unique(true)
                            .build(),
                    )
                    .build(),
            )
            .await?;
        self.group_users()
            .create_index(IndexModel::builder().keys(doc! {"group_id": 1}).build())
            .await?;
        self.group_users()
            .create_index(IndexModel::builder().keys(doc! {"user_sub": 1}).build())
            .await?;
        self.group_streams()
            .create_index(IndexModel::builder().keys(doc! {"group_id": 1}).build())
            .await?;
        self.group_streams()
            .create_index(IndexModel::builder().keys(doc! {"stream_id": 1}).build())
            .await?;
        self.stream_users()
            .create_index(IndexModel::builder().keys(doc! {"stream_id": 1}).build())
            .await?;
        self.stream_users()
            .create_index(IndexModel::builder().keys(doc! {"user_sub": 1}).build())
            .await?;

        // The `_id` indices above are explicit duplicates of the
        // implicit-unique one mongo creates on every collection
        // automatically; they exist for parity with BOOM's pattern of
        // declaring each collection's primary index alongside its
        // secondaries.
        Ok(())
    }

    /// Find-or-insert the default access-control roles and the five
    /// messenger streams. Idempotent: existing rows are left untouched
    /// (so admin edits and restarts never clobber), missing ones are
    /// inserted. Runs from [`Self::connect`] in every binary.
    async fn seed_access_defaults(&self) -> Result<(), ArchiveError> {
        for role in crate::access::default_roles() {
            self.roles()
                .update_one(
                    doc! {"_id": &role.id},
                    doc! {"$setOnInsert": mongodb::bson::to_document(&role)?},
                )
                .upsert(true)
                .await?;
        }
        for stream in crate::access::default_streams() {
            self.streams()
                .update_one(
                    doc! {"_id": &stream.id},
                    doc! {"$setOnInsert": mongodb::bson::to_document(&stream)?},
                )
                .upsert(true)
                .await?;
        }
        Ok(())
    }

    /// JIT-provision a user. Find-or-insert keyed by `sub`: on insert,
    /// stamp `created_at` and seed `role_ids` (Super admin when
    /// `bootstrap_super_admin`, else empty / Full user). On every call
    /// bump `last_seen_at` and refresh `email`/`display_name` when the
    /// caller has them (login path). Never resets `role_ids` on
    /// re-login. Returns the resulting user.
    pub async fn upsert_user(
        &self,
        sub: &str,
        email: Option<&str>,
        display_name: Option<&str>,
        bootstrap_super_admin: bool,
    ) -> Result<UserDoc, ArchiveError> {
        let now = mongodb::bson::DateTime::now();
        let initial_roles: Vec<String> = if bootstrap_super_admin {
            vec![crate::access::ROLE_SUPER_ADMIN.to_string()]
        } else {
            vec![crate::access::ROLE_FULL_USER.to_string()]
        };
        let mut set_on_insert = doc! {
            "created_at": now,
            "role_ids": &initial_roles,
            "active": true,
        };
        let mut set = doc! { "last_seen_at": now };
        // Only overwrite contact fields when we actually have them, so
        // a bearer-path call (sub only) doesn't wipe an email captured
        // at OIDC login.
        if let Some(email) = email {
            set.insert("email", email);
        } else {
            set_on_insert.insert("email", mongodb::bson::Bson::Null);
        }
        if let Some(dn) = display_name {
            set.insert("display_name", dn);
        } else {
            set_on_insert.insert("display_name", mongodb::bson::Bson::Null);
        }
        self.users()
            .update_one(
                doc! {"_id": sub},
                doc! {"$set": set, "$setOnInsert": set_on_insert},
            )
            .upsert(true)
            .await?;
        // The upsert guarantees the row exists; the fallback is purely
        // defensive (e.g. a concurrent delete between write and read).
        let found = self.users().find_one(doc! {"_id": sub}).await?;
        Ok(found.unwrap_or(UserDoc {
            sub: sub.to_string(),
            email: email.map(str::to_string),
            display_name: display_name.map(str::to_string),
            role_ids: initial_roles,
            active: true,
            created_at: now,
            last_seen_at: now,
        }))
    }

    /// Count provisioned users. Used by the JIT bootstrap to decide
    /// whether the very first user should become Super admin.
    pub async fn user_count(&self) -> Result<u64, ArchiveError> {
        Ok(self.users().count_documents(doc! {}).await?)
    }

    /// Idempotently grant a role to a user (`$addToSet`). No-op if the
    /// user already has it. Returns whether the user doc matched.
    pub async fn add_user_role(&self, sub: &str, role_id: &str) -> Result<bool, ArchiveError> {
        let res = self
            .users()
            .update_one(doc! {"_id": sub}, doc! {"$addToSet": {"role_ids": role_id}})
            .await?;
        Ok(res.matched_count == 1)
    }

    /// Replace a user's role set. Used by the `Manage users` endpoint.
    pub async fn set_user_roles(
        &self,
        sub: &str,
        role_ids: &[String],
    ) -> Result<bool, ArchiveError> {
        let res = self
            .users()
            .update_one(doc! {"_id": sub}, doc! {"$set": {"role_ids": role_ids}})
            .await?;
        Ok(res.matched_count == 1)
    }

    /// Delete a group and its membership + stream-access join rows.
    pub async fn delete_group_cascade(&self, group_id: &str) -> Result<bool, ArchiveError> {
        let res = self.groups().delete_one(doc! {"_id": group_id}).await?;
        self.group_users()
            .delete_many(doc! {"group_id": group_id})
            .await?;
        self.group_streams()
            .delete_many(doc! {"group_id": group_id})
            .await?;
        Ok(res.deleted_count == 1)
    }

    /// Stream ids a group can access.
    pub async fn group_stream_ids(&self, group_id: &str) -> Result<Vec<String>, ArchiveError> {
        let mut cursor = self
            .group_streams()
            .find(doc! {"group_id": group_id})
            .await?;
        let mut ids = Vec::new();
        while let Some(gs) = cursor.try_next().await? {
            ids.push(gs.stream_id);
        }
        Ok(ids)
    }

    /// Count the admins of a group — used by the last-admin lockout guard.
    pub async fn group_admin_count(&self, group_id: &str) -> Result<u64, ArchiveError> {
        Ok(self
            .group_users()
            .count_documents(doc! {"group_id": group_id, "admin": true})
            .await?)
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

    pub fn localize_skips(&self) -> Collection<LocalizeSkipDoc> {
        self.db.collection(LOCALIZE_SKIPS_COLLECTION)
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

    pub fn health_config(&self) -> Collection<HealthConfigDoc> {
        self.db.collection(HEALTH_CONFIG_COLLECTION)
    }

    pub fn science_filters(&self) -> Collection<ScienceFilterDoc> {
        self.db.collection(SCIENCE_FILTERS_COLLECTION)
    }

    pub fn users(&self) -> Collection<UserDoc> {
        self.db.collection(USERS_COLLECTION)
    }

    pub fn roles(&self) -> Collection<RoleDoc> {
        self.db.collection(ROLES_COLLECTION)
    }

    pub fn groups(&self) -> Collection<GroupDoc> {
        self.db.collection(GROUPS_COLLECTION)
    }

    pub fn group_users(&self) -> Collection<GroupUserDoc> {
        self.db.collection(GROUP_USERS_COLLECTION)
    }

    pub fn streams(&self) -> Collection<StreamDoc> {
        self.db.collection(STREAMS_COLLECTION)
    }

    pub fn group_streams(&self) -> Collection<GroupStreamDoc> {
        self.db.collection(GROUP_STREAMS_COLLECTION)
    }

    pub fn stream_users(&self) -> Collection<StreamUserDoc> {
        self.db.collection(STREAM_USERS_COLLECTION)
    }

    /// Read the singleton `health_config:default` doc, falling back
    /// to the built-in defaults if the doc hasn't been written yet.
    /// Used by the System Health dashboard endpoint to drive
    /// per-stream staleness thresholds without a redeploy.
    pub async fn load_health_config(&self) -> Result<HealthConfigDoc, ArchiveError> {
        let found = self
            .health_config()
            .find_one(doc! {"_id": HEALTH_CONFIG_DEFAULT_ID})
            .await?;
        Ok(found.unwrap_or_else(HealthConfigDoc::default_doc))
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

    pub async fn record_localize_skip(&self, doc: &LocalizeSkipDoc) -> Result<(), ArchiveError> {
        let filter = doc! {"_id": &doc.request_id};
        self.localize_skips()
            .replace_one(filter, doc)
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

    fn tier(name: &str, max: f64) -> ConfidenceTier {
        ConfidenceTier {
            name: name.into(),
            joint_far_remapped_max_per_year: max,
        }
    }

    #[test]
    fn sort_tiers_orders_most_significant_first() {
        let mut f = ScienceFilterDoc::new("me", "f");
        f.confidence_tiers = vec![
            tier("silver", 12.0),
            tier("gold", 1.0),
            tier("bronze", 100.0),
        ];
        f.sort_tiers();
        let names: Vec<&str> = f.confidence_tiers.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["gold", "silver", "bronze"]);
    }

    #[test]
    fn tier_for_picks_most_significant_clearing_tier() {
        let mut f = ScienceFilterDoc::new("me", "f");
        f.confidence_tiers = vec![tier("silver", 12.0), tier("gold", 1.0)];
        f.sort_tiers();
        // Well inside gold.
        assert_eq!(f.tier_for(Some(0.5)).map(|t| t.name.as_str()), Some("gold"));
        // Clears silver but not gold.
        assert_eq!(
            f.tier_for(Some(5.0)).map(|t| t.name.as_str()),
            Some("silver")
        );
        // Clears nothing.
        assert!(f.tier_for(Some(50.0)).is_none());
        // No FAR → no tier.
        assert!(f.tier_for(None).is_none());
    }

    #[test]
    fn match_query_only_includes_set_cuts() {
        let mut f = ScienceFilterDoc::new("me", "f");
        // Empty cuts → just the superevent scope.
        let q = f.match_query("S1");
        assert_eq!(q.get_str("superevent_id").unwrap(), "S1");
        assert_eq!(q.len(), 1, "no cuts set should add no constraints");

        f.cuts = FilterCuts {
            instruments: vec!["Fermi-GBM".into()],
            time_window_sec: Some(10.0),
            spatial_overlap_min: Some(0.1),
            p_value_max: Some(0.05),
            joint_far_remapped_max_per_year: Some(12.0),
            require_in_90cr: Some(true),
        };
        let q = f.match_query("S1");
        assert_eq!(
            q.get_document("instrument")
                .unwrap()
                .get_array("$in")
                .unwrap()
                .len(),
            1
        );
        // Symmetric ±window on the stored time offset.
        let tw = q.get_document("time_offset_sec").unwrap();
        assert_eq!(tw.get_f64("$gte").unwrap(), -10.0);
        assert_eq!(tw.get_f64("$lte").unwrap(), 10.0);
        // p-value and remapped-FAR cuts exclude missing values.
        assert!(q.get_document("p_value").unwrap().contains_key("$ne"));
        assert!(q
            .get_document("joint_far_remapped_per_year")
            .unwrap()
            .contains_key("$ne"));
        assert_eq!(q.get_bool("in_90cr").unwrap(), true);
    }

    #[test]
    fn require_in_90cr_false_adds_no_constraint() {
        let mut f = ScienceFilterDoc::new("me", "f");
        f.cuts.require_in_90cr = Some(false);
        let q = f.match_query("S1");
        assert!(!q.contains_key("in_90cr"), "false should not constrain");
    }
}
