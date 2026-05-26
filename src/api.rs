//! Read-only HTTP API over the MongoDB archive.
//!
//! Mirrors BOOM proper's conventions: actix-web 4, scope-based routing,
//! `web::Data<mongodb::Database>` as shared state, response shape
//! `{"message": "...", "data": ...}`. Auth is deliberately omitted —
//! boom-gw runs behind an internal load balancer for now. We will
//! layer in an `auth_middleware` (same shape as BOOM's) once the
//! exposure model demands it.
//!
//! Endpoints (all under `/api`):
//!
//! | method | path                                       | purpose                                     |
//! |--------|--------------------------------------------|---------------------------------------------|
//! | GET    | /api/health                                | liveness probe                              |
//! | GET    | /api/events                                | paginated list of ingested events           |
//! | GET    | /api/events/{graceid}                      | one event                                   |
//! | GET    | /api/superevents                           | paginated list of superevents               |
//! | GET    | /api/superevents/{id}                      | one superevent                              |
//! | GET    | /api/superevents/{id}/skymap               | raw FITS bytes (application/fits)           |
//! | GET    | /api/superevents/{id}/contour              | credible-region MOC FITS (?level=50/90)     |
//! | GET    | /api/superevents/{id}/annotations          | list annotations on this superevent         |
//! | POST   | /api/superevents/{id}/annotations          | create an annotation on this superevent     |
//! | GET    | /api/superevents/{id}/alerts               | list public alerts assembled for this id    |
//! | POST   | /api/superevents/{id}/alerts               | assemble + publish a public alert           |
//! | GET    | /api/superevents/{id}/cross-matches        | list GW × external cross-matches            |
//! | POST   | /api/superevents/{id}/cross-matches        | compute (and persist) one cross-match       |
//! | POST   | /api/superevents/{id}/scan-cross-matches   | scan all ext. events in ±window, persist    |
//! | PATCH  | /api/superevents/{id}/cross-matches/{instrument}/{trigger_id} | flip the associated flag |
//! | GET    | /api/localize-requests                     | audit log of localize requests              |
//! | GET    | /api/localize-results                      | audit log of localize results               |
//! | GET    | /api/grb-triggers                          | list ingested GRB triggers                  |
//! | POST   | /api/grb-triggers                          | ingest a GRB trigger (raw or parsed)        |
//! | GET    | /api/grb-triggers/{instrument}/{trigger_id}| one GRB trigger                             |
//! | GET    | /api/grb-triggers/{instrument}/{trigger_id}/skymap | canonical GRB MOC FITS bytes        |
//! | GET    | /api/superevents/{id}/joint-skymap/{instrument}/{trigger_id} | combined GW × external posterior FITS |
//! | GET    | /api/boom-alerts                           | list BOOM optical-transient alerts          |
//! | POST   | /api/boom-alerts                           | ingest a BOOM alert (typed shape)           |
//! | GET    | /api/boom-alerts/{alert_id}                | one BOOM alert by composite alert_id        |
//! | GET    | /api/frb-alerts                            | list CHIME / DSA110 FRB alerts              |
//! | POST   | /api/frb-alerts                            | ingest an FRB alert (typed shape)           |
//! | GET    | /api/frb-alerts/{instrument}/{trigger_id}  | one FRB alert                               |
//! | GET    | /api/neutrino-alerts                       | list IceCube / KM3NeT neutrino alerts       |
//! | POST   | /api/neutrino-alerts                       | ingest a neutrino alert (typed shape)       |
//! | GET    | /api/neutrino-alerts/{instrument}/{trigger_id} | one neutrino alert                      |
//! | GET    | /api/superevents/{id}/icecube-lvk-searches | IceCube LVK Nu Track Search results         |
//! | POST   | /api/superevents/{id}/icecube-lvk-searches | ingest an LVK Nu Track Search result        |
//! | POST   | /api/superevents                           | upsert a fully-formed superevent + skymap   |

use std::sync::LazyLock;

use actix_web::body::MessageBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::{from_fn, Next};
use actix_web::{web, App, Error, HttpRequest, HttpResponse, HttpServer, Responder};
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::options::FindOptions;
use opentelemetry::metrics::Counter;
use opentelemetry::KeyValue;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::info;

use crate::metrics::API_METER;

use crate::alert::{build_alert, AlertPublisher, AlertType};
use crate::archive::{
    AlertDoc, AnnotationDoc, Archive, CrossMatchDoc, EventDoc, GrbTriggerDoc, LocalizeRequestDoc,
    LocalizeResultDoc, SupereventDoc, ALERTS_COLLECTION, ANNOTATIONS_COLLECTION,
    CROSS_MATCHES_COLLECTION, EVENTS_COLLECTION, GRB_TRIGGERS_COLLECTION,
    LOCALIZE_REQUESTS_COLLECTION, LOCALIZE_RESULTS_COLLECTION, SUPEREVENTS_COLLECTION,
};
use crate::auth::{
    auth_middleware, require_alert_publisher, require_principal, AuthConfig, JwksCache,
};
use crate::grb::GrbTrigger;
use crate::login::{config as auth_config, dev_login, logout as auth_logout, me as auth_me};
use crate::oidc::{callback as oidc_callback, login as oidc_login, DiscoveryCache, OidcConfig};
use crate::session::SessionConfig;

/// Counter incremented once per inbound HTTP request, labelled by
/// `method` and `status_code`. Mirrors BOOM proper's
/// `api.request` counter for cross-service Grafana dashboards.
static REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    API_METER
        .u64_counter("boom_gw.api.request")
        .with_unit("{request}")
        .with_description("HTTP requests handled by the boom-gw API service.")
        .build()
});

/// Actix middleware that increments [`REQUESTS`] on every served
/// request. Modeled on BOOM's `request_metrics_middleware`.
pub async fn request_metrics_middleware(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, Error> {
    let method = req.method().as_str().to_string();
    let response = next.call(req).await;
    let status_code = response
        .as_ref()
        .map(|sr| sr.status().as_u16())
        .unwrap_or(500);
    REQUESTS.add(
        1,
        &[
            KeyValue::new("method", method),
            KeyValue::new("status_code", status_code.to_string()),
        ],
    );
    response
}

/// Default page size used when a request omits `limit`. Matches the
/// "show me the most recent few minutes of pipeline activity" use case.
pub const DEFAULT_LIMIT: i64 = 50;
/// Hard upper bound on a single page, to keep the API cheap.
pub const MAX_LIMIT: i64 = 500;

/// Deserialize an optional value from a query string, accepting
/// either the typed JSON representation or a stringified form.
/// `serde_urlencoded` (what actix's `web::Query` uses under the
/// hood) gives every value to serde as a string, so a query like
/// `?limit=5` would otherwise fail with `"invalid type: string,
/// expected i64"`. This helper does the `str.parse::<T>()` for us.
fn de_opt_from_str<'de, D, T>(d: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let s: Option<String> = Option::deserialize(d)?;
    match s.as_deref() {
        None | Some("") => Ok(None),
        Some(s) => s.parse::<T>().map(Some).map_err(serde::de::Error::custom),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Pagination {
    #[serde(default, deserialize_with = "de_opt_from_str")]
    pub limit: Option<i64>,
    #[serde(default, deserialize_with = "de_opt_from_str")]
    pub skip: Option<u64>,
}

impl Pagination {
    pub fn limit_clamped(&self) -> i64 {
        self.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
    }

    pub fn skip_value(&self) -> u64 {
        self.skip.unwrap_or(0)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct EventsQuery {
    #[serde(flatten)]
    pub page: Pagination,
    pub pipeline: Option<String>,
    /// Filter to events whose `producer_timestamp` is at or after this
    /// value (unix seconds).
    #[serde(default, deserialize_with = "de_opt_from_str")]
    pub since: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SupereventsQuery {
    #[serde(flatten)]
    pub page: Pagination,
    /// Filter to superevents whose `t_0` is at or after this value.
    #[serde(default, deserialize_with = "de_opt_from_str")]
    pub t0_min: Option<f64>,
    /// Filter to superevents whose `t_0` is at or before this value.
    #[serde(default, deserialize_with = "de_opt_from_str")]
    pub t0_max: Option<f64>,
    /// `true` → only return superevents with a sky map attached;
    /// `false` → only those without; omit to disable the filter.
    #[serde(default, deserialize_with = "de_opt_from_str")]
    pub has_skymap: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuditQuery {
    #[serde(flatten)]
    pub page: Pagination,
    pub superevent_id: Option<String>,
}

/// Body of `POST /api/superevents/{id}/annotations`. `author` defaults
/// to `"system"` so cron jobs / classifiers do not have to spell it
/// out; once we add auth, this field will be populated from the
/// caller's identity instead of being client-supplied.
#[derive(Debug, Clone, Deserialize)]
pub struct CreateAnnotationBody {
    pub kind: String,
    pub payload: serde_json::Value,
    #[serde(default)]
    pub author: Option<String>,
}

/// Body of `POST /api/superevents/{id}/alerts`.
#[derive(Debug, Clone, Deserialize)]
pub struct CreateAlertBody {
    pub alert_type: AlertType,
    /// When `true`, build the alert and persist the audit row but
    /// skip the Kafka publish. Defaults to `false`. Used in tests
    /// and for replays where the production broker is unreachable.
    #[serde(default)]
    pub dry_run: bool,
}

/// Optional [`AlertPublisher`] handle bundled into the API's shared
/// state. Wrapped in its own type so handlers can take
/// `web::Data<MaybeAlertPublisher>` even when alerting is disabled.
pub struct MaybeAlertPublisher(pub Option<AlertPublisher>);

/// Response envelope. Mirrors BOOM proper's `{"message": ..., "data":
/// ...}` shape so cross-product tooling does not need to special-case
/// boom-gw responses.
#[derive(Debug, Serialize)]
struct ApiEnvelope<T: Serialize> {
    message: &'static str,
    data: T,
}

fn ok<T: Serialize>(data: T) -> HttpResponse {
    HttpResponse::Ok().json(ApiEnvelope {
        message: "success",
        data,
    })
}

fn not_found(what: &str) -> HttpResponse {
    HttpResponse::NotFound().json(json!({"message": format!("{what} not found"), "data": null}))
}

fn internal_error(err: impl std::fmt::Display) -> HttpResponse {
    HttpResponse::InternalServerError().json(json!({"message": format!("{err}"), "data": null}))
}

/// Build the standard 201-Created-or-200-Ok response envelope for
/// an idempotent POST. `created` is the boolean the archive
/// upsert returns (true → freshly inserted, false → replaced).
/// Centralizing this keeps the per-resource create_*_alert
/// handlers from each open-coding the same `if created { Created
/// } else { Ok }` block.
fn upsert_response<T: Serialize>(created: bool, doc: T) -> HttpResponse {
    let mut builder = if created {
        HttpResponse::Created()
    } else {
        HttpResponse::Ok()
    };
    builder.json(ApiEnvelope {
        message: "success",
        data: doc,
    })
}

/// Build the boom-gw API service. Accepts an [`Archive`] (or anything
/// that can produce one) so the same router can be mounted in tests
/// against an in-memory tokio runtime via
/// [`actix_web::test::init_service`].
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api")
            .route("/health", web::get().to(get_health))
            .route("/auth/me", web::get().to(auth_me))
            .route("/auth/config", web::get().to(auth_config))
            .route("/auth/logout", web::post().to(auth_logout))
            .route("/auth/dev-login", web::post().to(dev_login))
            .route("/auth/login", web::get().to(oidc_login))
            .route("/auth/callback", web::get().to(oidc_callback))
            .route("/events", web::get().to(list_events))
            .route("/events/{graceid}", web::get().to(get_event))
            .route("/superevents", web::get().to(list_superevents))
            .route("/superevents", web::post().to(create_superevent))
            .route("/superevents/{id}", web::get().to(get_superevent))
            .route(
                "/superevents/{id}/skymap",
                web::get().to(get_superevent_skymap),
            )
            .route(
                "/superevents/{id}/contour",
                web::get().to(get_superevent_contour),
            )
            .route(
                "/superevents/{id}/annotations",
                web::get().to(list_annotations),
            )
            .route(
                "/superevents/{id}/annotations",
                web::post().to(create_annotation),
            )
            .route("/superevents/{id}/alerts", web::get().to(list_alerts))
            .route("/superevents/{id}/alerts", web::post().to(create_alert))
            .route(
                "/superevents/{id}/cross-matches",
                web::get().to(list_cross_matches),
            )
            .route(
                "/superevents/{id}/cross-matches",
                web::post().to(create_cross_match),
            )
            .route(
                "/superevents/{id}/scan-cross-matches",
                web::post().to(scan_cross_matches),
            )
            .route(
                "/superevents/{id}/cross-matches/{instrument}/{trigger_id}",
                web::patch().to(patch_cross_match),
            )
            .route("/localize-requests", web::get().to(list_localize_requests))
            .route("/localize-results", web::get().to(list_localize_results))
            .route("/grb-triggers", web::get().to(list_grb_triggers))
            .route("/grb-triggers", web::post().to(create_grb_trigger))
            .route(
                "/grb-triggers/{instrument}/{trigger_id}",
                web::get().to(get_grb_trigger),
            )
            .route(
                "/grb-triggers/{instrument}/{trigger_id}/skymap",
                web::get().to(get_grb_trigger_skymap),
            )
            .route(
                "/superevents/{id}/joint-skymap/{instrument}/{trigger_id}",
                web::get().to(get_joint_skymap),
            )
            .route("/boom-alerts", web::get().to(list_boom_alerts))
            .route("/boom-alerts", web::post().to(create_boom_alert))
            .route("/boom-alerts/{alert_id}", web::get().to(get_boom_alert))
            .route("/frb-alerts", web::get().to(list_frb_alerts))
            .route("/frb-alerts", web::post().to(create_frb_alert))
            .route(
                "/frb-alerts/{instrument}/{trigger_id}",
                web::get().to(get_frb_alert),
            )
            .route("/neutrino-alerts", web::get().to(list_neutrino_alerts))
            .route("/neutrino-alerts", web::post().to(create_neutrino_alert))
            .route(
                "/neutrino-alerts/{instrument}/{trigger_id}",
                web::get().to(get_neutrino_alert),
            )
            .route(
                "/superevents/{id}/icecube-lvk-searches",
                web::get().to(list_icecube_lvk_searches),
            )
            .route(
                "/superevents/{id}/icecube-lvk-searches",
                web::post().to(create_icecube_lvk_search),
            ),
    );
}

#[derive(Debug, Deserialize)]
struct BoomListQuery {
    #[serde(flatten)]
    page: Pagination,
    /// Filter by upstream survey transient name prefix (e.g.
    /// `"ZTF"`) — supports the operator's "show me the ZTF stream
    /// only" use case.
    #[serde(default)]
    event_name_prefix: Option<String>,
}

async fn list_boom_alerts(
    archive: web::Data<Archive>,
    query: web::Query<BoomListQuery>,
) -> impl Responder {
    let mut filter = doc! {};
    if let Some(prefix) = &query.event_name_prefix {
        // Mongo regex with caret anchor — keeps the index usable
        // and matches a real ZTF/LSST id prefix exactly.
        filter.insert(
            "event_name",
            doc! {"$regex": format!("^{}", regex::escape(prefix))},
        );
    }
    let opts = FindOptions::builder()
        .sort(doc! {"alert_time": -1})
        .limit(query.page.limit_clamped())
        .skip(query.page.skip_value())
        .build();
    match collect::<crate::archive::BoomAlertDoc>(
        &archive,
        crate::archive::BOOM_ALERTS_COLLECTION,
        filter,
        opts,
    )
    .await
    {
        Ok(items) => ok(items),
        Err(e) => internal_error(e),
    }
}

async fn get_boom_alert(archive: web::Data<Archive>, path: web::Path<String>) -> impl Responder {
    let id = path.into_inner();
    match archive.boom_alerts().find_one(doc! {"_id": &id}).await {
        Ok(Some(d)) => ok(d),
        Ok(None) => not_found("boom_alert"),
        Err(e) => internal_error(e),
    }
}

/// Ingest one BOOM optical-transient alert. Accepts the typed
/// [`crate::boom::BoomTransient`] shape — operator-driven or
/// loader-driven inserts skip the GCN envelope (which would
/// otherwise explode into 1..N transients). The live Kafka path
/// parses the envelope and POSTs one body per transient to
/// equivalent storage, so the two paths land identical docs.
async fn create_boom_alert(
    req: HttpRequest,
    archive: web::Data<Archive>,
    storage: Option<web::Data<crate::storage::skymap::SkymapStorage>>,
    body: web::Json<crate::boom::BoomTransient>,
) -> HttpResponse {
    if let Some(resp) = require_principal(&req) {
        return resp;
    }
    match crate::ingest::ingest_boom_alert(
        &archive,
        storage.as_ref().map(|d| d.get_ref()),
        body.into_inner(),
    )
    .await
    {
        Ok((created, doc)) => upsert_response(created, doc),
        Err(e) => internal_error(e),
    }
}

/// Query shared by `/api/frb-alerts` and `/api/neutrino-alerts` —
/// both single-time + single-position external triggers, so the
/// list filters work the same: by instrument, optionally within a
/// GPS time window.
#[derive(Debug, Deserialize)]
struct ExternalTriggerListQuery {
    #[serde(flatten)]
    page: Pagination,
    #[serde(default)]
    instrument: Option<String>,
    #[serde(default, deserialize_with = "de_opt_from_str")]
    since: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_from_str")]
    until: Option<f64>,
}

impl ExternalTriggerListQuery {
    fn build_filter(&self) -> mongodb::bson::Document {
        let mut filter = doc! {};
        if let Some(inst) = &self.instrument {
            filter.insert("instrument", inst);
        }
        if self.since.is_some() || self.until.is_some() {
            let mut range = doc! {};
            if let Some(s) = self.since {
                range.insert("$gte", s);
            }
            if let Some(u) = self.until {
                range.insert("$lte", u);
            }
            filter.insert("trigger_time", range);
        }
        filter
    }
}

async fn list_frb_alerts(
    archive: web::Data<Archive>,
    query: web::Query<ExternalTriggerListQuery>,
) -> impl Responder {
    let opts = FindOptions::builder()
        .sort(doc! {"ingested_at": -1})
        .limit(query.page.limit_clamped())
        .skip(query.page.skip_value())
        .build();
    match collect::<crate::archive::FrbAlertDoc>(
        &archive,
        crate::archive::FRB_ALERTS_COLLECTION,
        query.build_filter(),
        opts,
    )
    .await
    {
        Ok(items) => ok(items),
        Err(e) => internal_error(e),
    }
}

async fn get_frb_alert(
    archive: web::Data<Archive>,
    path: web::Path<(String, String)>,
) -> impl Responder {
    let (instrument, trigger_id) = path.into_inner();
    let filter = doc! {
        "_id.instrument": &instrument,
        "_id.trigger_id": &trigger_id,
    };
    match archive.frb_alerts().find_one(filter).await {
        Ok(Some(d)) => ok(d),
        Ok(None) => not_found("frb_alert"),
        Err(e) => internal_error(e),
    }
}

/// Ingest one FRB alert (CHIME or DSA110). Body is the typed
/// `FrbAlert` shape; thin shim over `crate::ingest::ingest_frb_alert`.
async fn create_frb_alert(
    req: HttpRequest,
    archive: web::Data<Archive>,
    storage: Option<web::Data<crate::storage::skymap::SkymapStorage>>,
    body: web::Json<crate::frb::FrbAlert>,
) -> HttpResponse {
    if let Some(resp) = require_principal(&req) {
        return resp;
    }
    match crate::ingest::ingest_frb_alert(
        &archive,
        storage.as_ref().map(|d| d.get_ref()),
        body.into_inner(),
    )
    .await
    {
        Ok((created, doc)) => upsert_response(created, doc),
        Err(e) => internal_error(e),
    }
}

async fn list_neutrino_alerts(
    archive: web::Data<Archive>,
    query: web::Query<ExternalTriggerListQuery>,
) -> impl Responder {
    let opts = FindOptions::builder()
        .sort(doc! {"ingested_at": -1})
        .limit(query.page.limit_clamped())
        .skip(query.page.skip_value())
        .build();
    match collect::<crate::archive::NeutrinoAlertDoc>(
        &archive,
        crate::archive::NEUTRINO_ALERTS_COLLECTION,
        query.build_filter(),
        opts,
    )
    .await
    {
        Ok(items) => ok(items),
        Err(e) => internal_error(e),
    }
}

async fn get_neutrino_alert(
    archive: web::Data<Archive>,
    path: web::Path<(String, String)>,
) -> impl Responder {
    let (instrument, trigger_id) = path.into_inner();
    let filter = doc! {
        "_id.instrument": &instrument,
        "_id.trigger_id": &trigger_id,
    };
    match archive.neutrino_alerts().find_one(filter).await {
        Ok(Some(d)) => ok(d),
        Ok(None) => not_found("neutrino_alert"),
        Err(e) => internal_error(e),
    }
}

/// Ingest one high-energy neutrino alert (IceCube single-neutrino
/// or KM3NeT). Thin shim over `crate::ingest::ingest_neutrino_alert`.
async fn create_neutrino_alert(
    req: HttpRequest,
    archive: web::Data<Archive>,
    storage: Option<web::Data<crate::storage::skymap::SkymapStorage>>,
    body: web::Json<crate::neutrino::NeutrinoAlert>,
) -> HttpResponse {
    if let Some(resp) = require_principal(&req) {
        return resp;
    }
    match crate::ingest::ingest_neutrino_alert(
        &archive,
        storage.as_ref().map(|d| d.get_ref()),
        body.into_inner(),
    )
    .await
    {
        Ok((created, doc)) => upsert_response(created, doc),
        Err(e) => internal_error(e),
    }
}

/// List every IceCube LVK Nu Track Search result attached to a
/// given superevent — newest by `alert_time` first so the most
/// recent search appears at the top of the per-superevent panel.
async fn list_icecube_lvk_searches(
    archive: web::Data<Archive>,
    path: web::Path<String>,
) -> impl Responder {
    let superevent_id = path.into_inner();
    let filter = doc! {"superevent_id": &superevent_id};
    let opts = FindOptions::builder()
        .sort(doc! {"alert_time": -1})
        .limit(50)
        .build();
    match collect::<crate::archive::IceCubeLvkSearchDoc>(
        &archive,
        crate::archive::ICECUBE_LVK_SEARCHES_COLLECTION,
        filter,
        opts,
    )
    .await
    {
        Ok(items) => ok(items),
        Err(e) => internal_error(e),
    }
}

/// Ingest one IceCube LVK Nu Track Search result, attached to the
/// superevent in the URL path. The body's own `superevent_id`
/// must match the path id — mismatch returns 400 since silently
/// re-keying would mask a bug in the caller.
async fn create_icecube_lvk_search(
    req: HttpRequest,
    archive: web::Data<Archive>,
    path: web::Path<String>,
    body: web::Json<crate::icecube_lvk::IceCubeLvkSearch>,
) -> HttpResponse {
    if let Some(resp) = require_principal(&req) {
        return resp;
    }
    let path_id = path.into_inner();
    match crate::ingest::ingest_icecube_lvk_search(&archive, Some(&path_id), body.into_inner())
        .await
    {
        Ok((created, doc)) => upsert_response(created, doc),
        Err(crate::ingest::IngestError::SuperEventIdMismatch { body, url }) => {
            HttpResponse::BadRequest().json(json!({
                "message": format!("body superevent_id {body:?} does not match URL path {url:?}"),
                "data": null,
            }))
        }
        Err(e) => internal_error(e),
    }
}

/// Serve the canonical MOC FITS bytes for a GRB trigger as
/// `application/fits`. Mirrors the GW `/skymap` and `/contour`
/// endpoints — same content type, same caching story — so the
/// Aladin Lite frontend can hand the URL to `A.MOCFromURL` and
/// overlay it identically to the GW credible-region MOCs.
async fn get_grb_trigger_skymap(
    storage: Option<web::Data<crate::storage::skymap::SkymapStorage>>,
    path: web::Path<(String, String)>,
) -> impl Responder {
    let (instrument, trigger_id) = path.into_inner();
    let Some(storage) = storage else {
        return HttpResponse::ServiceUnavailable().json(json!({
            "message": "skymap storage not configured for this server",
            "data": null,
        }));
    };
    match storage.get_grb_skymap(&instrument, &trigger_id).await {
        Ok(bytes) => HttpResponse::Ok()
            .content_type("application/fits")
            .insert_header((
                "Content-Disposition",
                format!("attachment; filename=\"{instrument}_{trigger_id}.fits\""),
            ))
            .body(bytes),
        Err(crate::storage::skymap::SkymapStorageError::NotFound(_)) => not_found("grb_skymap"),
        Err(e) => internal_error(e),
    }
}

/// Compute and stream the joint (GW × external) posterior sky map
/// as a multi-order PROBDENSITY FITS. The math is
/// [`crate::joint_skymap::combine_gw_with_external_moc`] — a port
/// of `gwcelery.tasks.external_skymaps.combine_skymaps_moc_moc`'s
/// spatial-only path. Computed on-demand from the two FITS we
/// already have in storage (GW skymap + canonical external MOC);
/// not cached because each call is sub-50ms for typical skymap
/// sizes and the inputs change often enough during O4 ops that
/// a cache would just complicate invalidation.
///
/// Returns the joint FITS bytes with `Content-Type:
/// application/fits` so it round-trips into `ligo.skymap` /
/// astropy. 404 when either input is missing.
async fn get_joint_skymap(
    storage: Option<web::Data<crate::storage::skymap::SkymapStorage>>,
    path: web::Path<(String, String, String)>,
) -> impl Responder {
    let (superevent_id, instrument, trigger_id) = path.into_inner();
    let Some(storage) = storage else {
        return HttpResponse::ServiceUnavailable().json(json!({
            "message": "skymap storage not configured for this server",
            "data": null,
        }));
    };
    let gw_fits = match storage.get(&superevent_id).await {
        Ok(blob) => blob.bytes,
        Err(crate::storage::skymap::SkymapStorageError::NotFound(_)) => {
            return not_found("gw_skymap");
        }
        Err(e) => return internal_error(e),
    };
    let ext_moc = match storage.get_grb_skymap(&instrument, &trigger_id).await {
        Ok(bytes) => bytes,
        Err(crate::storage::skymap::SkymapStorageError::NotFound(_)) => {
            return not_found("grb_skymap");
        }
        Err(e) => return internal_error(e),
    };
    // Spawn-blocking because the FITS-parse + per-cell loop is
    // pure CPU work; we don't want it on the actix-rt worker.
    let joint = match tokio::task::spawn_blocking(move || -> Result<Vec<u8>, anyhow::Error> {
        Ok(crate::joint_skymap::combine_gw_with_external_moc(
            &gw_fits, &ext_moc,
        )?)
    })
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(e)) => {
            return HttpResponse::UnprocessableEntity().json(json!({
                "message": format!("joint-skymap computation failed: {e}"),
                "data": null,
            }));
        }
        Err(e) => return internal_error(e),
    };
    HttpResponse::Ok()
        .content_type("application/fits")
        .insert_header((
            "Content-Disposition",
            format!(
                "attachment; filename=\"{superevent_id}_{instrument}_{trigger_id}_joint.fits\""
            ),
        ))
        .body(joint)
}

/// Start a real HTTP server. The clusterer's main process does not run
/// the API in-process today; this entrypoint is used by the
/// `gw-api` binary. `alert_publisher` controls whether the `POST
/// /api/superevents/{id}/alerts` route publishes to Kafka — when
/// `None`, the handler still builds and persists the alert but
/// returns 503 when callers ask for a non-`dry_run` publish.
///
/// `auth` and `jwks` together drive the bearer-token validation. The
/// JWKS cache should already be warmed by the binary before calling
/// `run_server` so the first inbound request does not pay the
/// discovery + key-fetch cost.
pub async fn run_server(
    archive: Archive,
    alert_publisher: Option<AlertPublisher>,
    skymap_storage: Option<std::sync::Arc<crate::storage::skymap::SkymapStorage>>,
    auth: AuthConfig,
    jwks: JwksCache,
    session: SessionConfig,
    oidc: Option<OidcConfig>,
    bind: impl Into<String>,
    static_dir: Option<std::path::PathBuf>,
) -> std::io::Result<()> {
    let bind = bind.into();
    let data = web::Data::new(archive);
    let publisher = web::Data::new(MaybeAlertPublisher(alert_publisher));
    let auth_data = web::Data::new(auth);
    let jwks_data = web::Data::new(jwks);
    let session_data = web::Data::new(session);
    let oidc_data = oidc.map(web::Data::new);
    let discovery_data = web::Data::new(DiscoveryCache::new());
    // SkymapStorage is wrapped in an `Arc` outside `web::Data` so
    // the same storage handle survives reconfiguration without
    // rebuilding the backend (which, for S3, would re-issue
    // `head_bucket`/`create_bucket`).
    let storage_data = skymap_storage.map(web::Data::from);
    let listen = bind.clone();
    if let Some(d) = &static_dir {
        info!(static_dir = %d.display(), "serving SPA bundle from disk");
    }
    info!(listen = %listen, "starting boom-gw API");
    HttpServer::new(move || {
        let mut app = App::new()
            .app_data(data.clone())
            .app_data(publisher.clone())
            .app_data(auth_data.clone())
            .app_data(jwks_data.clone())
            .app_data(session_data.clone())
            .app_data(discovery_data.clone())
            // Outer wraps execute first on the way in and last on
            // the way out. Auth runs before request-metrics so that
            // 401s still count in the metrics; metrics still record
            // status_code correctly.
            .wrap(from_fn(request_metrics_middleware))
            .wrap(from_fn(auth_middleware))
            .wrap(actix_web::middleware::Logger::default())
            .configure(configure);
        if let Some(o) = &oidc_data {
            app = app.app_data(o.clone());
        }
        if let Some(s) = &storage_data {
            app = app.app_data(s.clone());
        }
        // Static SPA goes LAST so /api/* still wins; index_file makes
        // SPA deep-links (/superevents/S250101a) hit index.html, which
        // React Router then resolves client-side.
        if let Some(d) = &static_dir {
            app = app.service(
                actix_files::Files::new("/", d)
                    .index_file("index.html")
                    .default_handler(
                        actix_files::NamedFile::open(d.join("index.html"))
                            .expect("index.html must exist in static dir"),
                    ),
            );
        }
        app
    })
    .bind(&bind)?
    .run()
    .await
}

async fn get_health(_archive: web::Data<Archive>) -> impl Responder {
    // Reaching this handler at all proves the actix worker is alive
    // and the Archive resource was successfully constructed at startup.
    // Server-side reachability of mongo is left to /api/events &c.
    ok(json!({"status": "ok"}))
}

async fn list_events(
    req: HttpRequest,
    archive: web::Data<Archive>,
    query: web::Query<EventsQuery>,
) -> impl Responder {
    if let Some(resp) = require_principal(&req) {
        return resp;
    }
    let mut filter = doc! {};
    if let Some(pipeline) = &query.pipeline {
        filter.insert("pipeline", pipeline);
    }
    if let Some(since) = query.since {
        filter.insert("producer_timestamp", doc! {"$gte": since});
    }
    let opts = FindOptions::builder()
        .sort(doc! {"producer_timestamp": -1})
        .limit(query.page.limit_clamped())
        .skip(query.page.skip_value())
        .build();
    match collect::<EventDoc>(&archive, EVENTS_COLLECTION, filter, opts).await {
        Ok(items) => ok(items),
        Err(e) => internal_error(e),
    }
}

async fn get_event(
    req: HttpRequest,
    archive: web::Data<Archive>,
    path: web::Path<String>,
) -> impl Responder {
    if let Some(resp) = require_principal(&req) {
        return resp;
    }
    let graceid = path.into_inner();
    match archive.events().find_one(doc! {"_id": &graceid}).await {
        Ok(Some(doc)) => ok(doc),
        Ok(None) => not_found("event"),
        Err(e) => internal_error(e),
    }
}

async fn list_superevents(
    archive: web::Data<Archive>,
    query: web::Query<SupereventsQuery>,
) -> impl Responder {
    let mut filter = doc! {};
    let mut t0_range = doc! {};
    if let Some(min) = query.t0_min {
        t0_range.insert("$gte", min);
    }
    if let Some(max) = query.t0_max {
        t0_range.insert("$lte", max);
    }
    if !t0_range.is_empty() {
        filter.insert("t_0", t0_range);
    }
    if let Some(true) = query.has_skymap {
        filter.insert("skymap_summary", doc! {"$exists": true});
    } else if let Some(false) = query.has_skymap {
        filter.insert("skymap_summary", doc! {"$exists": false});
    }
    let opts = FindOptions::builder()
        .sort(doc! {"t_0": -1})
        .limit(query.page.limit_clamped())
        .skip(query.page.skip_value())
        .build();
    match collect::<SupereventDoc>(&archive, SUPEREVENTS_COLLECTION, filter, opts).await {
        Ok(items) => ok(items),
        Err(e) => internal_error(e),
    }
}

async fn get_superevent(archive: web::Data<Archive>, path: web::Path<String>) -> impl Responder {
    let id = path.into_inner();
    match archive.superevents().find_one(doc! {"_id": &id}).await {
        Ok(Some(doc)) => ok(doc),
        Ok(None) => not_found("superevent"),
        Err(e) => internal_error(e),
    }
}

/// Operator / loader path for inserting a fully-formed superevent.
/// The body is the in-memory `clustering::Superevent` shape —
/// includes the constituent g-events and an optional inline
/// skymap (FITS bytes base64-encoded). The handler:
///
///   1. Records each g-event via `archive.record_event` so the
///      `events` collection is consistent with the new superevent.
///   2. Upserts the superevent doc itself.
///   3. If a skymap is present, persists the FITS bytes via
///      [`SkymapStorage`] and derives the 50% / 90% contour MOCs
///      via [`crate::contour::compute_contour_moc`] — same code
///      path the live `gw_clusterer` runs on
///      `SupereventUpdate::SkymapAttached`.
///
/// Production clustering still flows through `gw_clusterer` (it
/// holds the sliding window of g-events and decides when to seal
/// a superevent). This route is for explicit operator ingest
/// (e.g. backfilling a historical superevent) and the demo
/// loader. After this lands, `load_demo_data` stops touching
/// `archive.record_event` / `archive.upsert_superevent` /
/// `storage.upsert` directly — it just POSTs Superevents.
async fn create_superevent(
    req: HttpRequest,
    archive: web::Data<Archive>,
    storage: Option<web::Data<crate::storage::skymap::SkymapStorage>>,
    body: web::Json<crate::clustering::Superevent>,
) -> HttpResponse {
    if let Some(resp) = require_principal(&req) {
        return resp;
    }
    let superevent = body.into_inner();
    // 1. Each g-event lands in `events` so the join between
    //    `events` and `superevents` stays consistent — all of
    //    that lives in `crate::ingest::ingest_superevent`.
    match crate::ingest::ingest_superevent(
        &archive,
        storage.as_ref().map(|d| d.get_ref()),
        superevent,
    )
    .await
    {
        Ok(doc) => ok(doc),
        Err(e) => internal_error(e),
    }
}

/// Return the raw FITS bytes for a superevent's sky map. Content-Type
/// is `application/fits` per the HEALPix MOC FITS convention used by
/// `ligo.skymap`. Returns 404 if no sky map has been stored for
/// this superevent.
///
/// The FITS bytes are loaded via the configured [`SkymapStorage`]
/// (mongo `skymaps` collection or S3), which is registered into
/// the app's `web::Data` at startup. If the route was mounted
/// without a `SkymapStorage` (some tests do this), we fall back
/// to checking whether `SupereventDoc.skymap_summary` exists at
/// all — sufficient for "does this superevent have one" but
/// can't actually serve the bytes.
async fn get_superevent_skymap(
    storage: Option<web::Data<crate::storage::skymap::SkymapStorage>>,
    path: web::Path<String>,
) -> HttpResponse {
    let id = path.into_inner();
    let Some(storage) = storage else {
        return HttpResponse::ServiceUnavailable().json(json!({
            "message": "skymap storage not configured for this server",
            "data": null,
        }));
    };
    match storage.get(&id).await {
        Ok(blob) => HttpResponse::Ok()
            .content_type("application/fits")
            .insert_header((
                "Content-Disposition",
                format!("attachment; filename=\"{id}.fits\""),
            ))
            .body(blob.bytes),
        Err(crate::storage::skymap::SkymapStorageError::NotFound(_)) => not_found("skymap"),
        Err(e) => {
            // Be careful with 500 vs 404 — only NotFound is 404;
            // any other backend error should bubble as 500 so the
            // operator notices.
            internal_error(e)
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct ContourQuery {
    /// Credible-region level as an integer percent (e.g. 50 or 90).
    /// Defaults to 90 to match common GW localization presentation.
    #[serde(default = "default_contour_level")]
    level: u8,
}

fn default_contour_level() -> u8 {
    90
}

/// Return the contour MOC FITS for a superevent's credible region at
/// the requested level (defaults to 90%). These are precomputed when
/// the skymap is attached (see `gw-clusterer::archive_superevent_from`)
/// so this handler only does a storage read — no on-demand contour
/// math, no FITS parsing on the hot path.
async fn get_superevent_contour(
    storage: Option<web::Data<crate::storage::skymap::SkymapStorage>>,
    path: web::Path<String>,
    query: web::Query<ContourQuery>,
) -> HttpResponse {
    let id = path.into_inner();
    let level = query.level;
    if level == 0 || level > 100 {
        return HttpResponse::BadRequest().json(json!({
            "message": format!("level must be 1..=100; got {level}"),
            "data": null,
        }));
    }
    let Some(storage) = storage else {
        return HttpResponse::ServiceUnavailable().json(json!({
            "message": "skymap storage not configured for this server",
            "data": null,
        }));
    };
    match storage.get_contour(&id, level).await {
        Ok(bytes) => HttpResponse::Ok()
            .content_type("application/fits")
            .insert_header((
                "Content-Disposition",
                format!("attachment; filename=\"{id}.contour{level}.fits\""),
            ))
            .body(bytes),
        Err(crate::storage::skymap::SkymapStorageError::NotFound(_)) => not_found("contour"),
        Err(e) => internal_error(e),
    }
}

/// List annotations attached to a given superevent. Returns 404 if
/// the superevent itself does not exist (we look it up first rather
/// than silently returning an empty list, so clients can distinguish
/// "no annotations yet" from "wrong ID").
async fn list_annotations(
    archive: web::Data<Archive>,
    path: web::Path<String>,
    page: web::Query<Pagination>,
) -> HttpResponse {
    let id = path.into_inner();
    match archive.superevents().find_one(doc! {"_id": &id}).await {
        Ok(Some(_)) => {}
        Ok(None) => return not_found("superevent"),
        Err(e) => return internal_error(e),
    }
    let filter = doc! {"superevent_id": &id};
    let opts = FindOptions::builder()
        .sort(doc! {"created_at": -1})
        .limit(page.limit_clamped())
        .skip(page.skip_value())
        .build();
    match collect::<AnnotationDoc>(&archive, ANNOTATIONS_COLLECTION, filter, opts).await {
        Ok(items) => ok(items),
        Err(e) => internal_error(e),
    }
}

async fn create_annotation(
    req: HttpRequest,
    archive: web::Data<Archive>,
    path: web::Path<String>,
    body: web::Json<CreateAnnotationBody>,
) -> HttpResponse {
    if let Some(resp) = require_principal(&req) {
        return resp;
    }
    let superevent_id = path.into_inner();
    // Require the superevent to exist. This is the same trade-off
    // mongo's referential integrity story forces on us — there are no
    // foreign-key constraints, so the API enforces them.
    match archive
        .superevents()
        .find_one(doc! {"_id": &superevent_id})
        .await
    {
        Ok(Some(_)) => {}
        Ok(None) => return not_found("superevent"),
        Err(e) => return internal_error(e),
    }
    let payload_bson = match mongodb::bson::to_bson(&body.payload) {
        Ok(b) => b,
        Err(e) => {
            return HttpResponse::BadRequest()
                .json(json!({"message": format!("invalid payload: {e}"), "data": null}));
        }
    };
    let author = body.author.clone().unwrap_or_else(|| "system".to_string());
    let annotation = AnnotationDoc::new(&superevent_id, &body.kind, author, payload_bson);
    if let Err(e) = archive.insert_annotation(&annotation).await {
        return internal_error(e);
    }
    HttpResponse::Created().json(ApiEnvelope {
        message: "success",
        data: annotation,
    })
}

async fn list_alerts(
    archive: web::Data<Archive>,
    path: web::Path<String>,
    page: web::Query<Pagination>,
) -> HttpResponse {
    let id = path.into_inner();
    match archive.superevents().find_one(doc! {"_id": &id}).await {
        Ok(Some(_)) => {}
        Ok(None) => return not_found("superevent"),
        Err(e) => return internal_error(e),
    }
    let filter = doc! {"superevent_id": &id};
    let opts = FindOptions::builder()
        .sort(doc! {"created_at": -1})
        .limit(page.limit_clamped())
        .skip(page.skip_value())
        .build();
    match collect::<AlertDoc>(&archive, ALERTS_COLLECTION, filter, opts).await {
        Ok(items) => ok(items),
        Err(e) => internal_error(e),
    }
}

/// Assemble a public alert from the current superevent state +
/// annotations and (optionally) publish it on the configured Kafka
/// topic. The audit row is written either way so a publish failure
/// still leaves a record that the operator pressed the button.
async fn create_alert(
    archive: web::Data<Archive>,
    publisher: web::Data<MaybeAlertPublisher>,
    storage: Option<web::Data<crate::storage::skymap::SkymapStorage>>,
    req: actix_web::HttpRequest,
    path: web::Path<String>,
    body: web::Json<CreateAlertBody>,
) -> HttpResponse {
    // Sign-in required: the alert-publisher allowlist short-circuits
    // to "permitted" when the list is empty (dev convenience), so
    // without this gate an anonymous browser visitor in dev mode
    // could press Publish.
    if let Some(resp) = require_principal(&req) {
        return resp;
    }
    // The allowlist gate is enforced only when the app has been wired
    // with an `AuthConfig`. The production `run_server` always
    // installs one; test apps that mount the router directly via
    // `configure` may omit it, in which case the gate is a no-op
    // (the route is still reachable behind any auth middleware the
    // test chose to mount, or none at all).
    if let Some(auth) = req.app_data::<web::Data<AuthConfig>>() {
        if let Some(resp) = require_alert_publisher(&req, auth.get_ref()) {
            return resp;
        }
    }
    let superevent_id = path.into_inner();

    // Load the superevent in its current shape (with skymap, if any).
    let superevent_doc = match archive
        .superevents()
        .find_one(doc! {"_id": &superevent_id})
        .await
    {
        Ok(Some(d)) => d,
        Ok(None) => return not_found("superevent"),
        Err(e) => return internal_error(e),
    };

    // Pull annotations so we can fill `event.classification`.
    let annotations: Vec<AnnotationDoc> = match archive
        .annotations()
        .find(doc! {"superevent_id": &superevent_id})
        .await
    {
        Ok(c) => match c.try_collect().await {
            Ok(v) => v,
            Err(e) => return internal_error(e),
        },
        Err(e) => return internal_error(e),
    };

    // We stored a flattened SupereventDoc; reconstruct enough of a
    // live Superevent for the builder. (The builder only reads
    // `id`, `preferred_event`, `skymap`.) We use the persisted
    // preferred-event row from the `events` collection so the
    // assembler sees the same coinc data downstream consumers do.
    let preferred_event_doc = match archive
        .events()
        .find_one(doc! {"_id": &superevent_doc.preferred_graceid})
        .await
    {
        Ok(Some(e)) => e,
        Ok(None) => {
            return HttpResponse::Conflict().json(json!({
                "message": format!(
                    "superevent {superevent_id} references missing preferred event {}",
                    superevent_doc.preferred_graceid
                ),
                "data": null,
            }))
        }
        Err(e) => return internal_error(e),
    };

    // Fetch the FITS bytes from the storage backend so the alert
    // builder can embed them in the public-alert envelope. If
    // either no storage is configured or the superevent doesn't
    // have a skymap yet, the alert is built without one — which
    // matches the IGWN convention for early alerts that fire
    // before localization completes.
    let skymap_bytes = if superevent_doc.skymap_summary.is_some() {
        match &storage {
            Some(s) => match s.get(&superevent_id).await {
                Ok(blob) => Some(blob),
                Err(crate::storage::skymap::SkymapStorageError::NotFound(_)) => None,
                Err(e) => return internal_error(e),
            },
            None => None,
        }
    } else {
        None
    };

    let superevent = superevent_from_docs(&superevent_doc, preferred_event_doc, skymap_bytes);
    let alert = match build_alert(&superevent, &annotations, body.alert_type) {
        Ok(a) => a,
        Err(e) => return internal_error(e),
    };

    // Persist the audit row first so an operator can always tell that
    // an alert build was attempted, even when the publish below
    // fails.
    let alert_body_bson = match mongodb::bson::to_bson(&alert) {
        Ok(b) => b,
        Err(e) => return internal_error(e),
    };
    let mut audit = AlertDoc::new(
        &superevent_id,
        body.alert_type.as_str(),
        alert_body_bson,
        false,
    );

    let published = if body.dry_run {
        false
    } else {
        match &publisher.0 {
            Some(p) => match p.publish(&alert).await {
                Ok(()) => true,
                Err(e) => {
                    // Persist the audit row with `published=false`
                    // before bailing so the failed attempt is
                    // recorded.
                    if let Err(arch_err) = archive.insert_alert(&audit).await {
                        return internal_error(arch_err);
                    }
                    return internal_error(e);
                }
            },
            None => {
                return HttpResponse::ServiceUnavailable().json(json!({
                    "message": "alert publisher is not configured; resend with dry_run=true to build only",
                    "data": null,
                }));
            }
        }
    };
    audit.published = published;
    if let Err(e) = archive.insert_alert(&audit).await {
        return internal_error(e);
    }

    HttpResponse::Created().json(ApiEnvelope {
        message: "success",
        data: serde_json::json!({"audit": audit, "alert": alert}),
    })
}

/// Rebuild a [`Superevent`] from the persisted `SupereventDoc` plus
/// the persisted preferred [`EventDoc`]. `g_events` is left as just
/// the preferred event because the alert builder does not consult
/// `g_events`; if a future alert variant needs the full set we can
/// hydrate the rest of the list from `g_event_graceids`.
fn superevent_from_docs(
    se: &SupereventDoc,
    preferred: EventDoc,
    skymap_bytes: Option<crate::storage::skymap::SkymapBlob>,
) -> crate::clustering::Superevent {
    use igwn_ligolw::CoincInspiralEvent;
    let coinc: CoincInspiralEvent =
        mongodb::bson::from_bson(preferred.coinc.clone()).unwrap_or(CoincInspiralEvent {
            coinc_event_id: preferred.graceid.clone(),
            ifos: preferred.ifos.clone(),
            combined_far: preferred.far,
            snr: preferred.snr,
            mass: preferred.total_mass,
            mchirp: preferred.mchirp,
            end_time: preferred.end_time,
            sngls: vec![],
        });
    let preferred_event = crate::event::GwEvent {
        pipeline: preferred.pipeline.clone(),
        graceid: preferred.graceid.clone(),
        producer_timestamp: preferred.producer_timestamp,
        message_type: preferred.message_type.clone(),
        submitter: preferred.submitter.clone(),
        end_time: preferred.end_time,
        ifos: preferred.ifos.clone(),
        snr: preferred.snr,
        far: preferred.far,
        mchirp: preferred.mchirp,
        total_mass: preferred.total_mass,
        coinc,
    };
    crate::clustering::Superevent {
        id: se.id.clone(),
        t_0: se.t_0,
        t_start: se.t_start,
        t_end: se.t_end,
        preferred_event: preferred_event.clone(),
        g_events: vec![preferred_event],
        skymap: skymap_bytes.map(|b| crate::clustering::SkyMapFits {
            bytes: b.bytes,
            elapsed_ms: b.elapsed_ms,
        }),
    }
}

async fn list_localize_requests(
    req: HttpRequest,
    archive: web::Data<Archive>,
    query: web::Query<AuditQuery>,
) -> impl Responder {
    if let Some(resp) = require_principal(&req) {
        return resp;
    }
    let mut filter = doc! {};
    if let Some(sid) = &query.superevent_id {
        filter.insert("superevent_id", sid);
    }
    let opts = FindOptions::builder()
        .limit(query.page.limit_clamped())
        .skip(query.page.skip_value())
        .build();
    match collect::<LocalizeRequestDoc>(&archive, LOCALIZE_REQUESTS_COLLECTION, filter, opts).await
    {
        Ok(items) => ok(items),
        Err(e) => internal_error(e),
    }
}

async fn list_localize_results(
    req: HttpRequest,
    archive: web::Data<Archive>,
    query: web::Query<AuditQuery>,
) -> impl Responder {
    if let Some(resp) = require_principal(&req) {
        return resp;
    }
    let mut filter = doc! {};
    if let Some(sid) = &query.superevent_id {
        filter.insert("superevent_id", sid);
    }
    let opts = FindOptions::builder()
        .limit(query.page.limit_clamped())
        .skip(query.page.skip_value())
        .build();
    match collect::<LocalizeResultDoc>(&archive, LOCALIZE_RESULTS_COLLECTION, filter, opts).await {
        Ok(items) => ok(items),
        Err(e) => internal_error(e),
    }
}

// ===================== GRB triggers + cross-matches =====================

/// Body for `POST /api/grb-triggers`. The operator (or an upstream
/// GCN-bridge service) hands us either a raw payload to parse, or a
/// pre-parsed `GrbTrigger`. The raw-payload path is the primary
/// one — bridging services should normalize as little as possible.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum CreateGrbTriggerBody {
    /// Raw alert payload + format hint. We parse it into a
    /// `GrbTrigger` server-side and the bridge service stays dumb.
    Raw {
        /// `"fermi_gbm_json"` or `"fermi_gbm_voevent"`.
        format: String,
        /// Instrument override (e.g. `"Fermi-GBM-FLT"`). For VOEvent
        /// we can also derive this from `topic` if provided.
        #[serde(default)]
        instrument: Option<String>,
        /// Original Kafka topic — used to derive the instrument
        /// suffix for VOEvent payloads. Ignored for JSON.
        #[serde(default)]
        topic: Option<String>,
        payload: String,
    },
    /// Pre-parsed trigger — exercises the same archive path but
    /// skips the parser. Convenient for tests and for operators
    /// inserting a trigger by hand from the UI.
    Parsed(GrbTrigger),
}

async fn create_grb_trigger(
    req: HttpRequest,
    archive: web::Data<Archive>,
    storage: Option<web::Data<crate::storage::skymap::SkymapStorage>>,
    body: web::Json<CreateGrbTriggerBody>,
) -> HttpResponse {
    if let Some(resp) = require_principal(&req) {
        return resp;
    }
    let trigger = match body.into_inner() {
        CreateGrbTriggerBody::Parsed(t) => t,
        CreateGrbTriggerBody::Raw {
            format,
            instrument,
            topic,
            payload,
        } => {
            let result = match format.as_str() {
                "fermi_gbm_json" => {
                    let inst = instrument.as_deref().unwrap_or("Fermi-GBM");
                    crate::gcn::parse_fermi_gbm_json(&payload, inst)
                }
                "fermi_gbm_voevent" => {
                    let inst = instrument.as_deref().map(String::from).unwrap_or_else(|| {
                        topic
                            .as_deref()
                            .map(crate::gcn::fermi_instrument_for_voevent_topic)
                            .unwrap_or("Fermi-GBM-VOEvent")
                            .to_string()
                    });
                    crate::gcn::parse_fermi_voevent(&payload, &inst)
                }
                other => {
                    return HttpResponse::BadRequest().json(json!({
                        "message": format!("unknown format: {other:?}; expected fermi_gbm_json or fermi_gbm_voevent"),
                        "data": null,
                    }));
                }
            };
            match result {
                Ok(t) => t,
                Err(e) => {
                    return HttpResponse::BadRequest().json(json!({
                        "message": format!("parse failed: {e}"),
                        "data": null,
                    }));
                }
            }
        }
    };
    match crate::ingest::ingest_grb_trigger(
        &archive,
        storage.as_ref().map(|d| d.get_ref()),
        trigger,
    )
    .await
    {
        Ok((created, doc)) => upsert_response(created, doc),
        Err(e) => internal_error(e),
    }
}

#[derive(Debug, Deserialize)]
struct GrbListQuery {
    #[serde(flatten)]
    page: Pagination,
    /// Filter by instrument label prefix (e.g. `"Fermi-GBM"` matches
    /// all GBM variants). Trailing `-FLT`/`-GND`/`-FIN` segments
    /// can also be filtered exactly by passing the full label.
    #[serde(default)]
    instrument: Option<String>,
    /// GPS time window — return triggers with
    /// `trigger_time` in `[since, until]`.
    #[serde(default, deserialize_with = "de_opt_from_str")]
    since: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_from_str")]
    until: Option<f64>,
}

async fn list_grb_triggers(
    archive: web::Data<Archive>,
    query: web::Query<GrbListQuery>,
) -> impl Responder {
    let mut filter = doc! {};
    if let Some(inst) = &query.instrument {
        filter.insert("instrument", inst);
    }
    if query.since.is_some() || query.until.is_some() {
        let mut range = doc! {};
        if let Some(s) = query.since {
            range.insert("$gte", s);
        }
        if let Some(u) = query.until {
            range.insert("$lte", u);
        }
        filter.insert("trigger_time", range);
    }
    let opts = FindOptions::builder()
        .sort(doc! {"ingested_at": -1})
        .limit(query.page.limit_clamped())
        .skip(query.page.skip_value())
        .build();
    match collect::<GrbTriggerDoc>(&archive, GRB_TRIGGERS_COLLECTION, filter, opts).await {
        Ok(items) => ok(items),
        Err(e) => internal_error(e),
    }
}

async fn get_grb_trigger(
    archive: web::Data<Archive>,
    path: web::Path<(String, String)>,
) -> impl Responder {
    let (instrument, trigger_id) = path.into_inner();
    let filter = doc! {
        "_id.instrument": &instrument,
        "_id.trigger_id": &trigger_id,
    };
    match archive.grb_triggers().find_one(filter).await {
        Ok(Some(d)) => ok(d),
        Ok(None) => not_found("grb_trigger"),
        Err(e) => internal_error(e),
    }
}

#[derive(Debug, Deserialize)]
struct CreateCrossMatchBody {
    instrument: String,
    trigger_id: String,
    /// Optional override for the coincidence window. Default 10 s
    /// matches the RAVEN GRB search.
    #[serde(default)]
    time_window_sec: Option<f64>,
    /// Optional override for the assumed background GRB rate (Hz).
    /// Default: combined Fermi/Swift/SVOM rate.
    #[serde(default)]
    grb_rate_hz: Option<f64>,
    /// Number of random skymap rotations for the empirical p-value
    /// Monte Carlo. `0` or omitted skips the p-value path
    /// (cheaper). 200–500 is a reasonable default for an
    /// operator-triggered ad-hoc query; live auto-matches use a
    /// smaller value to control per-trigger latency.
    #[serde(default)]
    p_value_trials: Option<usize>,
    /// Maximum GW FAR threshold of the pipeline in Hz, used by the
    /// bias-corrected joint-FAR formula. Defaults to 2/day.
    #[serde(default)]
    far_gw_max_hz: Option<f64>,
}

/// Compute (and persist) a cross-match between a superevent and a
/// GRB trigger on demand. Pulls the superevent doc, GRB trigger
/// doc, and skymap + contour bytes; runs the RAVEN integral;
/// upserts the result; returns it. 404 if either side is missing or
/// the superevent has no attached skymap.
async fn create_cross_match(
    req: HttpRequest,
    archive: web::Data<Archive>,
    storage: Option<web::Data<crate::storage::skymap::SkymapStorage>>,
    path: web::Path<String>,
    body: web::Json<CreateCrossMatchBody>,
) -> HttpResponse {
    if let Some(resp) = require_principal(&req) {
        return resp;
    }
    let superevent_id = path.into_inner();

    let Some(storage) = storage else {
        return HttpResponse::ServiceUnavailable().json(json!({
            "message": "skymap storage not configured for this server",
            "data": null,
        }));
    };

    let superevent = match archive
        .superevents()
        .find_one(doc! {"_id": &superevent_id})
        .await
    {
        Ok(Some(s)) => s,
        Ok(None) => return not_found("superevent"),
        Err(e) => return internal_error(e),
    };
    let trigger_doc = match archive
        .grb_triggers()
        .find_one(doc! {
            "_id.instrument": &body.instrument,
            "_id.trigger_id": &body.trigger_id,
        })
        .await
    {
        Ok(Some(t)) => t,
        Ok(None) => return not_found("grb_trigger"),
        Err(e) => return internal_error(e),
    };

    let skymap_blob = match storage.get(&superevent_id).await {
        Ok(b) => b,
        Err(crate::storage::skymap::SkymapStorageError::NotFound(_)) => {
            return not_found("skymap");
        }
        Err(e) => return internal_error(e),
    };
    // Contours are optional — when missing, in_50cr / in_90cr come
    // back false but the spatial integral and joint FAR still
    // compute fine.
    let contour_50 = storage.get_contour(&superevent_id, 50).await.ok();
    let contour_90 = storage.get_contour(&superevent_id, 90).await.ok();

    let time_window = body.time_window_sec.unwrap_or(10.0);
    let grb_rate = body
        .grb_rate_hz
        .unwrap_or(crate::crossmatch::rates::GRB_RATE_HZ);

    // The RAVEN formula needs the GW FAR. SupereventDoc only
    // carries SNR + preferred_graceid; the FAR lives on the
    // preferred event itself. We look it up; if that fails (the
    // event was pruned, the doc is malformed), fall back to a
    // conservative default of 1e-7 Hz so we still produce a
    // result.
    let gw_far_hz = match archive
        .events()
        .find_one(doc! {"_id": &superevent.preferred_graceid})
        .await
    {
        Ok(Some(ev)) => ev.far,
        _ => 1e-7,
    };

    // Pull the canonical GRB MOC. If it's missing — usually because
    // the trigger was ingested before the canonicalization step was
    // wired — we synthesize on the fly and persist it back for next
    // time. That way old triggers heal themselves on first use.
    let grb_moc_bytes = match storage
        .get_grb_skymap(&body.instrument, &body.trigger_id)
        .await
    {
        Ok(b) => b,
        Err(crate::storage::skymap::SkymapStorageError::NotFound(_)) => {
            match crate::grb::build_canonical_moc_fits(&trigger_doc.trigger) {
                Ok(b) => {
                    let _ = storage
                        .upsert_grb_skymap(&body.instrument, &body.trigger_id, b.clone())
                        .await;
                    b
                }
                Err(e) => {
                    return HttpResponse::UnprocessableEntity().json(json!({
                        "message": format!("grb has no usable localization: {e}"),
                        "data": null,
                    }));
                }
            }
        }
        Err(e) => return internal_error(e),
    };

    let pvalue_opts =
        body.p_value_trials
            .filter(|&n| n > 0)
            .map(|n_trials| crate::crossmatch::PvalueOpts {
                n_trials,
                far_gw_max_hz: body.far_gw_max_hz.unwrap_or(2.0 / 86400.0),
                seed: None,
            });
    let result = match crate::crossmatch::cross_match(
        &trigger_doc.trigger,
        superevent.t_0,
        gw_far_hz,
        &skymap_blob.bytes,
        &grb_moc_bytes,
        contour_50.as_deref(),
        contour_90.as_deref(),
        time_window,
        grb_rate,
        pvalue_opts,
    ) {
        Ok(r) => r,
        Err(e) => {
            return HttpResponse::UnprocessableEntity().json(json!({
                "message": format!("cross-match failed: {e}"),
                "data": null,
            }));
        }
    };
    let doc = CrossMatchDoc::new(&superevent_id, &trigger_doc.trigger, result);
    if let Err(e) = archive.upsert_cross_match(&doc).await {
        return internal_error(e);
    }
    HttpResponse::Created().json(ApiEnvelope {
        message: "success",
        data: doc,
    })
}

#[derive(Debug, Deserialize)]
struct ScanCrossMatchBody {
    /// Coincidence window in seconds, applied symmetrically
    /// around `superevent.t_0`. RAVEN's GRB convention is ±10 s,
    /// but operators may widen it (e.g. ±1 day) to sweep up
    /// late-time optical companions.
    #[serde(default = "default_scan_window_sec")]
    time_window_sec: f64,
    /// Same knob as on the manual cross-match endpoint — controls
    /// how many sky-rotation trials the p-value Monte Carlo runs.
    #[serde(default = "default_scan_p_value_trials")]
    p_value_trials: usize,
    #[serde(default)]
    far_gw_max_hz: Option<f64>,
}

fn default_scan_window_sec() -> f64 {
    10.0
}
fn default_scan_p_value_trials() -> usize {
    200
}

/// Scan every ingested external event (GRB triggers + BOOM
/// optical alerts) with `t ∈ [t_0 ± window]`, compute a full
/// cross-match against the superevent's skymap, persist each, and
/// return the resulting list sorted by remapped joint FAR
/// (most-significant first). Idempotent — re-scanning replaces
/// prior matches in place. The persisted documents start
/// `associated=false`; the operator promotes the ones they
/// believe via the PATCH endpoint.
async fn scan_cross_matches(
    req: HttpRequest,
    archive: web::Data<Archive>,
    storage: Option<web::Data<crate::storage::skymap::SkymapStorage>>,
    path: web::Path<String>,
    body: web::Json<ScanCrossMatchBody>,
) -> HttpResponse {
    if let Some(resp) = require_principal(&req) {
        return resp;
    }
    let superevent_id = path.into_inner();
    let Some(storage) = storage else {
        return HttpResponse::ServiceUnavailable().json(json!({
            "message": "skymap storage not configured for this server",
            "data": null,
        }));
    };
    let opts = crate::ingest::RescanOptions {
        time_window_sec: body.time_window_sec,
        pvalue_opts: (body.p_value_trials > 0).then(|| crate::crossmatch::PvalueOpts {
            n_trials: body.p_value_trials,
            far_gw_max_hz: body.far_gw_max_hz.unwrap_or(2.0 / 86400.0),
            seed: None,
        }),
    };
    match crate::ingest::rescan_superevent_cross_matches(
        archive.get_ref(),
        storage.get_ref(),
        &superevent_id,
        opts,
    )
    .await
    {
        Ok(results) => ok(results),
        Err(crate::ingest::IngestError::SupereventNotFound(_)) => not_found("superevent"),
        Err(crate::ingest::IngestError::SkymapMissing(_)) => not_found("skymap"),
        Err(e) => internal_error(e),
    }
}

#[derive(Debug, Deserialize)]
struct PatchCrossMatchBody {
    /// Flip the operator-association flag. The scan endpoint
    /// always emits `associated=false`; the SPA flips this to
    /// `true` when an analyst stars a row.
    associated: bool,
}

async fn patch_cross_match(
    req: HttpRequest,
    archive: web::Data<Archive>,
    path: web::Path<(String, String, String)>,
    body: web::Json<PatchCrossMatchBody>,
) -> HttpResponse {
    if let Some(resp) = require_principal(&req) {
        return resp;
    }
    let (superevent_id, instrument, trigger_id) = path.into_inner();
    let filter = doc! {
        "_id.superevent_id": &superevent_id,
        "_id.instrument": &instrument,
        "_id.trigger_id": &trigger_id,
    };
    let update = doc! { "$set": { "associated": body.associated } };
    match archive
        .cross_matches()
        .update_one(filter.clone(), update)
        .await
    {
        Ok(res) if res.matched_count == 1 => match archive.cross_matches().find_one(filter).await {
            Ok(Some(d)) => ok(d),
            Ok(None) => not_found("cross_match"),
            Err(e) => internal_error(e),
        },
        Ok(_) => not_found("cross_match"),
        Err(e) => internal_error(e),
    }
}

async fn list_cross_matches(
    archive: web::Data<Archive>,
    path: web::Path<String>,
    page: web::Query<Pagination>,
) -> HttpResponse {
    let id = path.into_inner();
    match archive.superevents().find_one(doc! {"_id": &id}).await {
        Ok(Some(_)) => {}
        Ok(None) => return not_found("superevent"),
        Err(e) => return internal_error(e),
    }
    let filter = doc! {"superevent_id": &id};
    let opts = FindOptions::builder()
        .sort(doc! {"computed_at": -1})
        .limit(page.limit_clamped())
        .skip(page.skip_value())
        .build();
    match collect::<CrossMatchDoc>(&archive, CROSS_MATCHES_COLLECTION, filter, opts).await {
        Ok(items) => ok(items),
        Err(e) => internal_error(e),
    }
}

/// Run a `find()` and collect the typed documents into a Vec. Used by
/// every list handler.
async fn collect<T>(
    archive: &Archive,
    collection: &str,
    filter: mongodb::bson::Document,
    opts: FindOptions,
) -> Result<Vec<T>, mongodb::error::Error>
where
    T: for<'de> serde::Deserialize<'de> + Send + Sync + Unpin,
{
    let col: mongodb::Collection<T> = archive.database().collection(collection);
    let cursor = col.find(filter).with_options(opts).await?;
    cursor.try_collect().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pagination_clamps_limit() {
        let p = Pagination {
            limit: Some(10_000),
            skip: None,
        };
        assert_eq!(p.limit_clamped(), MAX_LIMIT);
    }

    #[test]
    fn pagination_defaults_to_default_limit() {
        let p = Pagination {
            limit: None,
            skip: None,
        };
        assert_eq!(p.limit_clamped(), DEFAULT_LIMIT);
        assert_eq!(p.skip_value(), 0);
    }

    #[test]
    fn pagination_rejects_zero_or_negative() {
        let p = Pagination {
            limit: Some(0),
            skip: None,
        };
        assert_eq!(p.limit_clamped(), 1);
        let p = Pagination {
            limit: Some(-5),
            skip: None,
        };
        assert_eq!(p.limit_clamped(), 1);
    }
}
