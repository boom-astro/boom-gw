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
//! | GET    | /api/superevents/{id}/annotations          | list annotations on this superevent         |
//! | POST   | /api/superevents/{id}/annotations          | create an annotation on this superevent     |
//! | GET    | /api/superevents/{id}/alerts               | list public alerts assembled for this id    |
//! | POST   | /api/superevents/{id}/alerts               | assemble + publish a public alert           |
//! | GET    | /api/localize-requests                     | audit log of localize requests              |
//! | GET    | /api/localize-results                      | audit log of localize results               |

use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::options::FindOptions;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::info;

use crate::alert::{build_alert, AlertPublisher, AlertType};
use crate::archive::{
    AlertDoc, AnnotationDoc, Archive, EventDoc, LocalizeRequestDoc, LocalizeResultDoc,
    SupereventDoc, ALERTS_COLLECTION, ANNOTATIONS_COLLECTION, EVENTS_COLLECTION,
    LOCALIZE_REQUESTS_COLLECTION, LOCALIZE_RESULTS_COLLECTION, SUPEREVENTS_COLLECTION,
};

/// Default page size used when a request omits `limit`. Matches the
/// "show me the most recent few minutes of pipeline activity" use case.
pub const DEFAULT_LIMIT: i64 = 50;
/// Hard upper bound on a single page, to keep the API cheap.
pub const MAX_LIMIT: i64 = 500;

#[derive(Debug, Clone, Deserialize)]
pub struct Pagination {
    pub limit: Option<i64>,
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
    pub since: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SupereventsQuery {
    #[serde(flatten)]
    pub page: Pagination,
    /// Filter to superevents whose `t_0` is at or after this value.
    pub t0_min: Option<f64>,
    /// Filter to superevents whose `t_0` is at or before this value.
    pub t0_max: Option<f64>,
    /// `true` → only return superevents with a sky map attached;
    /// `false` → only those without; omit to disable the filter.
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

/// Build the boom-gw API service. Accepts an [`Archive`] (or anything
/// that can produce one) so the same router can be mounted in tests
/// against an in-memory tokio runtime via
/// [`actix_web::test::init_service`].
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api")
            .route("/health", web::get().to(get_health))
            .route("/events", web::get().to(list_events))
            .route("/events/{graceid}", web::get().to(get_event))
            .route("/superevents", web::get().to(list_superevents))
            .route("/superevents/{id}", web::get().to(get_superevent))
            .route(
                "/superevents/{id}/skymap",
                web::get().to(get_superevent_skymap),
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
            .route("/localize-requests", web::get().to(list_localize_requests))
            .route("/localize-results", web::get().to(list_localize_results)),
    );
}

/// Start a real HTTP server. The clusterer's main process does not run
/// the API in-process today; this entrypoint is used by the
/// `gw-api` binary. `alert_publisher` controls whether the `POST
/// /api/superevents/{id}/alerts` route publishes to Kafka — when
/// `None`, the handler still builds and persists the alert but
/// returns 503 when callers ask for a non-`dry_run` publish.
pub async fn run_server(
    archive: Archive,
    alert_publisher: Option<AlertPublisher>,
    bind: impl Into<String>,
) -> std::io::Result<()> {
    let bind = bind.into();
    let data = web::Data::new(archive);
    let publisher = web::Data::new(MaybeAlertPublisher(alert_publisher));
    let listen = bind.clone();
    info!(listen = %listen, "starting boom-gw API");
    HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .app_data(publisher.clone())
            .wrap(actix_web::middleware::Logger::default())
            .configure(configure)
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
    archive: web::Data<Archive>,
    query: web::Query<EventsQuery>,
) -> impl Responder {
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

async fn get_event(archive: web::Data<Archive>, path: web::Path<String>) -> impl Responder {
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
        filter.insert("skymap", doc! {"$exists": true});
    } else if let Some(false) = query.has_skymap {
        filter.insert("skymap", doc! {"$exists": false});
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

/// Return the raw FITS bytes for a superevent's sky map. Content-Type
/// is `application/fits` per the HEALPix MOC FITS convention used by
/// `ligo.skymap`. Returns 404 if either the superevent or its
/// `skymap` field is missing.
async fn get_superevent_skymap(
    archive: web::Data<Archive>,
    path: web::Path<String>,
) -> HttpResponse {
    let id = path.into_inner();
    match archive.superevents().find_one(doc! {"_id": &id}).await {
        Ok(Some(doc)) => match doc.skymap {
            Some(sky) => HttpResponse::Ok()
                .content_type("application/fits")
                .insert_header((
                    "Content-Disposition",
                    format!("attachment; filename=\"{id}.fits\""),
                ))
                .body(sky.bytes),
            None => not_found("skymap"),
        },
        Ok(None) => not_found("superevent"),
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
    archive: web::Data<Archive>,
    path: web::Path<String>,
    body: web::Json<CreateAnnotationBody>,
) -> HttpResponse {
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
    path: web::Path<String>,
    body: web::Json<CreateAlertBody>,
) -> HttpResponse {
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

    let superevent = superevent_from_docs(&superevent_doc, preferred_event_doc);
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
fn superevent_from_docs(se: &SupereventDoc, preferred: EventDoc) -> crate::clustering::Superevent {
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
        skymap: se.skymap.clone(),
    }
}

async fn list_localize_requests(
    archive: web::Data<Archive>,
    query: web::Query<AuditQuery>,
) -> impl Responder {
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
    archive: web::Data<Archive>,
    query: web::Query<AuditQuery>,
) -> impl Responder {
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
