//! `gw-clusterer` — read GW pipeline events (live from Kafka or replayed
//! from captured JSON envelopes), feed them through the BOOM clustering
//! layer, and print the resulting superevent stream.
//!
//! In replay mode (`--replay-dir`) the binary reads `*.json` envelope files
//! written by `gw_dump`, sorts them by `_producer_timestamp`, and processes
//! them in order. This is the path used for offline comparison against
//! sgn-llai or against gracedb-test's recorded superevent assignments.

use std::collections::HashMap;
use std::fs;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use redis::aio::MultiplexedConnection;
use tokio::runtime::Runtime;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use boom_gw::kafka::{pipeline_topics_for_instance, DEFAULT_GRACEDB_INSTANCE};
use boom_gw::{
    extract_gw_event_with_xml, load_from_redis, metrics, save_to_redis, Archive, ArchiveConfig,
    EventEnvelope, FileTokenSource, GwAlertConsumer, GwEvent, GwKafkaConfig, HandlerControl,
    LocalizeRequest, LocalizeResult, LocalizeSkipDoc, LocalizeStatus, LocalizerClient,
    LocalizerClientConfig, LocalizerResultConsumer, LocalizerResultConsumerConfig,
    LocalizerResultStream, PublisherConfig, SkipReason, SupereventCreator, SupereventPublisher,
    SupereventUpdate, TokenSource, DEFAULT_DB_NAME, DEFAULT_REQUEST_TOPIC, DEFAULT_RESULT_TOPIC,
    DEFAULT_WINDOW_SECS,
};
use opentelemetry::KeyValue;

#[derive(Parser, Debug)]
#[command(
    name = "gw-clusterer",
    about = "Cluster GW pipeline events into superevents"
)]
struct Cli {
    /// Replay envelope JSON files from this directory instead of consuming
    /// from Kafka. Files are sorted by `_producer_timestamp`.
    #[arg(long)]
    replay_dir: Option<PathBuf>,

    /// Kafka bootstrap servers (only used when --replay-dir is omitted).
    #[arg(long, default_value = "kafka-dev.ligo.org:9092")]
    bootstrap_servers: String,

    /// Topics to subscribe to (only used in live mode). When empty,
    /// the seven default pipelines (gstlal, mbta, pycbc, spiir,
    /// aframe, cwb, mly) are subscribed under the
    /// `--gracedb-instance` namespace.
    #[arg(long, value_delimiter = ',')]
    topics: Vec<String>,

    /// GraceDB instance whose Kafka topic namespace we consume from.
    /// On `kafka-dev.ligo.org` the real wire topic for each pipeline
    /// is `{instance}.{pipeline}` — for example
    /// `gracedb-test.gstlal`. This matches what
    /// `ligo.gracedb.kafka.GraceDbKafkaConsumer` does internally
    /// when given `service_url=https://gracedb-test.ligo.org/api/`.
    /// Ignored when `--topics` is supplied.
    #[arg(long, default_value = DEFAULT_GRACEDB_INSTANCE)]
    gracedb_instance: String,

    #[arg(long, default_value = "boom-gw-clusterer")]
    group_id: String,

    #[arg(long)]
    token_file: Option<PathBuf>,

    #[arg(long)]
    ca_cert_path: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    no_tls: bool,

    #[arg(long, default_value = "earliest")]
    auto_offset_reset: String,

    #[arg(long, default_value_t = 1000)]
    poll_timeout_ms: u64,

    /// Time window in seconds for clustering. Default 5.0 to match sgn-llai.
    #[arg(long, default_value_t = DEFAULT_WINDOW_SECS)]
    window_secs: f64,

    /// Stop after this many events (0 = unlimited; live mode only).
    #[arg(long, default_value_t = 0)]
    max_events: u64,

    /// Write one JSON line per processed event to this path; the line
    /// includes the source event metadata and the resulting superevent
    /// assignment. This is the file consumed by the comparison tooling.
    #[arg(long)]
    out_jsonl: Option<PathBuf>,

    /// Publish each [`SupereventUpdate`] to this Kafka topic as a JSON
    /// document keyed by superevent_id. When set, --publish-servers must
    /// also be set.
    #[arg(long)]
    publish_topic: Option<String>,

    /// Bootstrap servers for the output Kafka cluster. Required when
    /// --publish-topic is set.
    #[arg(long)]
    publish_servers: Option<String>,

    /// Redis URL for state persistence (e.g. redis://localhost:6379/).
    /// When set, the clusterer recovers any persisted state at startup
    /// and writes the current state back after every processed event.
    #[arg(long)]
    redis_url: Option<String>,

    /// Key prefix under which state is stored in Redis.
    #[arg(long, default_value = "gw:clusterer:default")]
    redis_prefix: String,

    /// Bootstrap servers for the localization microservice (bayestar-service)
    /// Kafka cluster. When set, the clusterer publishes a LocalizeRequest
    /// for every superevent it opens or whose preferred event it
    /// promotes, and subscribes to the result topic to attach the
    /// returned FITS sky map back onto the superevent.
    #[arg(long)]
    localize_servers: Option<String>,

    /// Topic to publish localize requests on.
    #[arg(long, default_value = DEFAULT_REQUEST_TOPIC)]
    localize_request_topic: String,

    /// Topic to consume localize results from.
    #[arg(long, default_value = DEFAULT_RESULT_TOPIC)]
    localize_result_topic: String,

    /// Consumer group id for the localize-result consumer. When unset,
    /// a per-process group id is generated so each clusterer instance
    /// sees every result independently.
    #[arg(long)]
    localize_result_group_id: Option<String>,

    /// After replay, how long to wait for outstanding localization
    /// results to come back before exiting. Only meaningful when the
    /// localizer is enabled in replay mode.
    #[arg(long, default_value_t = 30)]
    localize_drain_secs: u64,

    /// SNR floor for publishing a `LocalizeRequest`. Events with
    /// preferred-event SNR below this skip the localizer entirely —
    /// matches the live-LIGO posture that BAYESTAR is reserved for
    /// alerts worth a public release. Default 0.0 (always submit;
    /// preserves prior behavior). 8.5 is a sensible production floor;
    /// 11 cuts ~90% of replay traffic.
    #[arg(long, env = "BOOM_GW_LOCALIZE_MIN_SNR", default_value_t = 0.0)]
    localize_min_snr: f64,

    /// FAR ceiling (Hz) for publishing a `LocalizeRequest`. Events
    /// with FAR above this skip localizer. Default `inf` (always
    /// submit). 1e-6 is a sensible production-ish ceiling.
    #[arg(long, env = "BOOM_GW_LOCALIZE_MAX_FAR_HZ", default_value_t = f64::INFINITY)]
    localize_max_far_hz: f64,

    /// Enable the OpenTelemetry OTLP metrics exporter. When set, the
    /// process pushes metrics every 60 s to the collector at
    /// `$OTEL_EXPORTER_OTLP_ENDPOINT` (default `http://localhost:4317`).
    #[arg(long, env = "BOOM_GW_METRICS_ENABLED", default_value_t = false)]
    metrics_enabled: bool,

    /// Deployment environment name reported as the
    /// `deployment.environment.name` resource attribute on emitted
    /// metrics. Only used when `--metrics-enabled` is set.
    #[arg(long, env = "BOOM_GW_DEPLOYMENT_ENV", default_value = "dev")]
    deployment_env: String,

    /// MongoDB connection string for the durable archive (events,
    /// superevents, localize-request / -result audit trail). When
    /// omitted, no archiving is performed. The env var
    /// `BOOM_GW_MONGO_URI` is used as the fallback default.
    #[arg(long, env = "BOOM_GW_MONGO_URI")]
    mongo_uri: Option<String>,

    /// Database name inside the MongoDB instance. Defaults to
    /// `boom_gw`; override per-deployment if multiple instances share
    /// a server.
    #[arg(long, env = "BOOM_GW_MONGO_DB", default_value = DEFAULT_DB_NAME)]
    mongo_db: String,

    /// Backend for storing FITS sky-map blobs. `mongo` writes to a
    /// `skymaps` collection on the same DB; `s3` writes to an
    /// S3-compatible bucket. Defaults to `mongo`.
    #[arg(long, env = "BOOM_GW_SKYMAP_STORAGE", default_value = "mongo")]
    skymap_storage: boom_gw::storage::skymap::SkymapBackendKind,

    #[arg(long, env = "BOOM_GW_S3_BUCKET")]
    s3_bucket: Option<String>,
    #[arg(long, env = "BOOM_GW_S3_KEY_PREFIX", default_value = "boom-gw")]
    s3_key_prefix: String,
    #[arg(long, env = "BOOM_GW_S3_REGION", default_value = "us-east-1")]
    s3_region: String,
    #[arg(long, env = "BOOM_GW_S3_ACCESS_KEY")]
    s3_access_key: Option<String>,
    #[arg(long, env = "BOOM_GW_S3_SECRET_KEY")]
    s3_secret_key: Option<String>,
    /// Override for S3-compatible endpoints (MinIO, rustfs, Wasabi).
    #[arg(long, env = "BOOM_GW_S3_ENDPOINT_URL")]
    s3_endpoint_url: Option<String>,
    #[arg(long, env = "BOOM_GW_S3_COMPRESS", default_value_t = true)]
    s3_compress: bool,
    /// Valkey URL for the optional S3 read cache (used by gw-api,
    /// not the clusterer writer path).
    #[arg(long, env = "BOOM_GW_S3_CACHE_REDIS_URL")]
    s3_cache_redis_url: Option<String>,
    #[arg(long, env = "BOOM_GW_S3_CACHE_TTL_SECONDS", default_value_t = 30)]
    s3_cache_ttl_seconds: u64,
}

/// Owns every long-lived piece of state and the tokio runtime so the
/// per-event hot path can borrow it with a single `&mut self`.
struct Pipeline {
    rt: Runtime,
    creator: SupereventCreator,
    publisher: Option<SupereventPublisher>,
    writer: Option<BufWriter<fs::File>>,
    redis_conn: Option<MultiplexedConnection>,
    redis_prefix: String,
    localizer_client: Option<LocalizerClient>,
    localizer_results: Option<LocalizerResultStream>,
    archive: Option<Archive>,
    skymap_storage: Option<std::sync::Arc<boom_gw::storage::skymap::SkymapStorage>>,
    /// Lower bound on preferred-event SNR for publishing a
    /// `LocalizeRequest`. 0.0 → always submit. See the matching
    /// CLI flag.
    localize_min_snr: f64,
    /// Upper bound on preferred-event FAR (Hz) for publishing.
    /// `f64::INFINITY` → no FAR gate.
    localize_max_far_hz: f64,
}

impl Pipeline {
    /// Process one inbound G event. Submits a localize request if the
    /// update created or promoted a superevent. Then drains any
    /// localize results that have arrived in the meantime.
    fn process_event(&mut self, event: GwEvent, coinc_xml: &[u8]) -> anyhow::Result<()> {
        metrics::clusterer::EVENTS_INGESTED.add(
            1,
            &[
                KeyValue::new("pipeline", event.pipeline.clone()),
                KeyValue::new("result", "ok"),
            ],
        );
        self.archive_event(&event);
        let update = self.creator.process(event.clone());
        record_update_metric(&update);
        self.emit(&update, Some(&event))?;
        self.archive_superevent_from(&update);
        if let Some(req) = localize_request_for(&event, coinc_xml, &update) {
            // Production-style threshold gate. BAYESTAR is expensive
            // (~50 s real-mode); skip it for events the pipeline
            // wouldn't promote to a public alert anyway. SNR floor
            // and FAR ceiling are independent — either trips the
            // skip. Both default to "always submit" so the gate is
            // off unless explicitly configured.
            if event.snr < self.localize_min_snr || event.far > self.localize_max_far_hz {
                info!(
                    superevent = %req.superevent_id,
                    graceid = %event.graceid,
                    snr = event.snr,
                    far = event.far,
                    min_snr = self.localize_min_snr,
                    max_far_hz = self.localize_max_far_hz,
                    "skip localize: below threshold"
                );
                self.archive_localize_skip(&req, event.snr, event.far);
            } else {
                self.submit_localize_request(&req);
                self.archive_localize_request(&req);
            }
        }
        self.persist_state()?;
        self.drain_localize_results()?;
        Ok(())
    }

    /// Print, log to jsonl (only inbound updates carry a source event),
    /// and publish to the output topic.
    fn emit(&mut self, update: &SupereventUpdate, event: Option<&GwEvent>) -> anyhow::Result<()> {
        match event {
            Some(ev) => print_update(ev, update),
            None => print_skymap_attached(update),
        }
        if let (Some(w), Some(ev)) = (self.writer.as_mut(), event) {
            write_update_jsonl(w, ev, update)?;
        }
        if let Some(pub_) = self.publisher.as_ref() {
            // Best-effort: a downstream publish failure (broker
            // unreachable, message too large, etc.) is logged but
            // does not propagate up and kill the binary. The
            // clusterer's authoritative state still lives in mongo
            // + redis; one missed publish is recoverable later, a
            // crash mid-flight is not.
            if let Err(e) = self.rt.block_on(pub_.publish(update)) {
                error!(
                    publish_topic = "superevents",
                    "failed to publish SupereventUpdate: {e}"
                );
            }
        }
        Ok(())
    }

    fn submit_localize_request(&self, req: &LocalizeRequest) {
        let Some(client) = self.localizer_client.as_ref() else {
            return;
        };
        if let Err(e) = self.rt.block_on(client.submit(req)) {
            metrics::clusterer::LOCALIZE_REQUESTS.add(1, &[KeyValue::new("result", "error")]);
            error!(
                superevent = %req.superevent_id,
                request_id = %req.request_id,
                "failed to publish localize request: {e}"
            );
        } else {
            metrics::clusterer::LOCALIZE_REQUESTS.add(1, &[KeyValue::new("result", "ok")]);
            info!(
                superevent = %req.superevent_id,
                request_id = %req.request_id,
                graceid = %req.graceid,
                "submitted localize request"
            );
        }
    }

    /// Drain every pending localize result currently buffered on the
    /// background channel; for each result, attach the FITS to the
    /// open superevent and emit a [`SupereventUpdate::SkymapAttached`].
    fn drain_localize_results(&mut self) -> anyhow::Result<()> {
        if self.localizer_results.is_none() {
            return Ok(());
        }
        let mut pending: Vec<LocalizeResult> = Vec::new();
        {
            let stream = self.localizer_results.as_ref().unwrap();
            while let Some(result) = stream.try_recv() {
                pending.push(result);
            }
        }
        if pending.is_empty() {
            return Ok(());
        }
        for result in pending {
            self.archive_localize_result(&result);
            if let Some(update) = self.apply_localize_result(result) {
                self.emit(&update, None)?;
                self.archive_superevent_from(&update);
            }
        }
        self.persist_state()?;
        Ok(())
    }

    /// Like [`Self::drain_localize_results`], but blocks until either
    /// `deadline` elapses or no superevents are missing a sky map.
    /// Used after replay to give outstanding results a chance to land.
    fn drain_until_quiet(&mut self, deadline: Instant) -> anyhow::Result<()> {
        if self.localizer_results.is_none() {
            return Ok(());
        }
        while Instant::now() < deadline {
            let missing = self.outstanding_skymaps();
            if missing == 0 {
                return Ok(());
            }
            let result = self
                .localizer_results
                .as_ref()
                .unwrap()
                .recv_timeout(Duration::from_millis(500));
            if let Some(result) = result {
                self.archive_localize_result(&result);
                if let Some(update) = self.apply_localize_result(result) {
                    self.emit(&update, None)?;
                    self.archive_superevent_from(&update);
                }
                self.persist_state()?;
            }
        }
        let missing = self.outstanding_skymaps();
        if missing > 0 {
            warn!(
                outstanding = missing,
                "localize-result drain deadline elapsed before every superevent had a sky map"
            );
        }
        Ok(())
    }

    fn outstanding_skymaps(&self) -> usize {
        self.creator
            .superevents()
            .filter(|s| s.skymap.is_none())
            .count()
    }

    fn apply_localize_result(&mut self, result: LocalizeResult) -> Option<SupereventUpdate> {
        match result.status {
            LocalizeStatus::Ok => {
                metrics::clusterer::LOCALIZE_RESULTS.add(1, &[KeyValue::new("status", "ok")]);
            }
            LocalizeStatus::Error => {
                metrics::clusterer::LOCALIZE_RESULTS.add(1, &[KeyValue::new("status", "error")]);
                warn!(
                    superevent = %result.superevent_id,
                    request_id = %result.request_id,
                    "bayestar returned error: {:?}",
                    result.error_message
                );
                return None;
            }
        }
        let fits = match result.skymap_fits_bytes() {
            Ok(Some(b)) => b,
            Ok(None) => {
                warn!(
                    superevent = %result.superevent_id,
                    "ok result has no skymap_fits payload"
                );
                return None;
            }
            Err(e) => {
                warn!(
                    superevent = %result.superevent_id,
                    "failed to base64-decode skymap_fits: {e}"
                );
                return None;
            }
        };
        let attached = self
            .creator
            .attach_skymap(&result.superevent_id, fits, result.elapsed_ms);
        if attached.is_none() {
            metrics::clusterer::LOCALIZE_RESULTS.add(1, &[KeyValue::new("status", "orphan")]);
            warn!(
                superevent = %result.superevent_id,
                "received localize result for an unknown / already-pruned superevent"
            );
        }
        attached
    }

    fn persist_state(&mut self) -> anyhow::Result<()> {
        if let Some(conn) = self.redis_conn.as_mut() {
            if let Err(e) = self
                .rt
                .block_on(save_to_redis(conn, &self.redis_prefix, &self.creator))
            {
                metrics::clusterer::ARCHIVE_ERRORS.add(1, &[KeyValue::new("sink", "redis")]);
                return Err(e.into());
            }
        }
        Ok(())
    }

    fn archive_event(&self, event: &GwEvent) {
        let Some(archive) = self.archive.as_ref() else {
            return;
        };
        if let Err(e) = self.rt.block_on(archive.record_event(event)) {
            metrics::clusterer::ARCHIVE_ERRORS.add(1, &[KeyValue::new("sink", "mongo_event")]);
            warn!(graceid = %event.graceid, "archive: record_event failed: {e}");
        }
    }

    /// Upsert the superevent embedded in `update` into the archive, if
    /// the update carries one. `Skipped` does not have a superevent
    /// payload, so we look it up from the creator.
    fn archive_superevent_from(&self, update: &SupereventUpdate) {
        let Some(archive) = self.archive.as_ref() else {
            return;
        };
        let superevent = match update {
            SupereventUpdate::Created { superevent }
            | SupereventUpdate::PreferredUpdated { superevent, .. }
            | SupereventUpdate::SkymapAttached { superevent } => superevent.clone(),
            SupereventUpdate::Skipped { superevent_id, .. } => {
                match self.creator.superevents().find(|s| &s.id == superevent_id) {
                    Some(s) => s.clone(),
                    None => return,
                }
            }
        };
        if let Err(e) = self.rt.block_on(archive.upsert_superevent(&superevent)) {
            metrics::clusterer::ARCHIVE_ERRORS.add(1, &[KeyValue::new("sink", "mongo_superevent")]);
            warn!(id = %superevent.id, "archive: upsert_superevent failed: {e}");
        }
        // When a sky map was just attached, also write the FITS
        // bytes to the SkymapStorage (separate mongo collection
        // or S3, depending on backend). SupereventDoc only carries
        // the summary; the bytes live here. We also derive the
        // 50% / 90% credible-region contour MOCs from the same
        // FITS — these are tiny (~12 KiB each), Aladin-renderable,
        // and used by the SPA's Localization tab.
        if let SupereventUpdate::SkymapAttached { .. } = update {
            if let (Some(storage), Some(sky)) =
                (self.skymap_storage.as_ref(), superevent.skymap.as_ref())
            {
                // The HTTP `ingest_superevent` path computes the
                // 50%-credible-region centroid and writes it into
                // `skymap_summary.center_{ra,dec}` so the SPA's
                // Aladin viewer points at the localization on
                // first render. The Kafka SkymapAttached path
                // missed that — the upsert above stored the doc
                // with center_{ra,dec}=None, leaving Aladin to
                // default to (0,0) and the operator to a blank
                // patch of sky. Recompute and patch in-place.
                if let Some((ra, dec)) = boom_gw::contour::compute_skymap_centroid(&sky.bytes, 0.5)
                {
                    use mongodb::bson::doc;
                    use std::future::IntoFuture;
                    // Chain into IntoFuture inline — the Collection
                    // returned by `archive.superevents()` is a
                    // temporary; binding the Update action to a
                    // local would drop the Collection while the
                    // borrow is still live.
                    let res = self.rt.block_on(
                        archive
                            .superevents()
                            .update_one(
                                doc! {"_id": &superevent.id},
                                doc! {"$set": {
                                    "skymap_summary.center_ra": ra,
                                    "skymap_summary.center_dec": dec,
                                }},
                            )
                            .into_future(),
                    );
                    if let Err(e) = res {
                        warn!(
                            id = %superevent.id,
                            "skymap_summary.center_{{ra,dec}} patch failed: {e}"
                        );
                    }
                }
                let blob = boom_gw::storage::skymap::SkymapBlob {
                    superevent_id: superevent.id.clone(),
                    bytes: sky.bytes.clone(),
                    elapsed_ms: sky.elapsed_ms,
                };
                if let Err(e) = self.rt.block_on(storage.upsert(blob)) {
                    metrics::clusterer::ARCHIVE_ERRORS
                        .add(1, &[KeyValue::new("sink", "skymap_storage")]);
                    warn!(id = %superevent.id, "skymap storage upsert failed: {e}");
                }
                for level_pct in [50u8, 90u8] {
                    let level = level_pct as f64 / 100.0;
                    match boom_gw::contour::compute_contour_moc(&sky.bytes, level) {
                        Ok(moc_bytes) => {
                            if let Err(e) = self.rt.block_on(storage.upsert_contour(
                                &superevent.id,
                                level_pct,
                                moc_bytes,
                            )) {
                                metrics::clusterer::ARCHIVE_ERRORS
                                    .add(1, &[KeyValue::new("sink", "skymap_contour_storage")]);
                                warn!(
                                    id = %superevent.id, level_pct,
                                    "contour storage upsert failed: {e}"
                                );
                            }
                        }
                        Err(e) => {
                            // Failure here is non-fatal — the raw
                            // FITS is already persisted; the user
                            // just loses the Aladin overlay for
                            // this superevent at this level.
                            metrics::clusterer::ARCHIVE_ERRORS
                                .add(1, &[KeyValue::new("sink", "skymap_contour_compute")]);
                            warn!(
                                id = %superevent.id, level_pct,
                                "contour computation failed: {e}"
                            );
                        }
                    }
                }
                // Now that the new skymap + contours are
                // persisted, refresh every cross-match against
                // external alerts already in window. Without
                // this the cross-match table stays stale until
                // a fresh external alert arrives or the
                // operator clicks "Scan ±window" — exactly the
                // "GW-side updates don't fan out to existing
                // matches" gap the analyst-loop care-abouts
                // depend on. Default options (±10 s window, no
                // p-value MC) match the API's defaults so the
                // re-scan produces the same shape as a manual
                // operator-triggered scan.
                let opts = boom_gw::ingest::RescanOptions::default();
                match self
                    .rt
                    .block_on(boom_gw::ingest::rescan_superevent_cross_matches(
                        archive,
                        storage.as_ref(),
                        &superevent.id,
                        opts,
                    )) {
                    Ok(matches) => {
                        info!(
                            id = %superevent.id,
                            n_matches = matches.len(),
                            "auto-rescanned cross-matches after skymap attached"
                        );
                    }
                    Err(e) => {
                        metrics::clusterer::ARCHIVE_ERRORS
                            .add(1, &[KeyValue::new("sink", "auto_rescan")]);
                        warn!(
                            id = %superevent.id,
                            "auto-rescan after skymap attach failed: {e}"
                        );
                    }
                }
            }
        }
    }

    fn archive_localize_request(&self, req: &LocalizeRequest) {
        let Some(archive) = self.archive.as_ref() else {
            return;
        };
        if let Err(e) = self.rt.block_on(archive.record_localize_request(req)) {
            metrics::clusterer::ARCHIVE_ERRORS
                .add(1, &[KeyValue::new("sink", "mongo_localize_request")]);
            warn!(
                superevent = %req.superevent_id,
                request_id = %req.request_id,
                "archive: record_localize_request failed: {e}"
            );
        }
    }

    fn archive_localize_skip(&self, req: &LocalizeRequest, snr: f64, far: f64) {
        let Some(archive) = self.archive.as_ref() else {
            return;
        };
        let doc = LocalizeSkipDoc {
            request_id: req.request_id.clone(),
            superevent_id: req.superevent_id.clone(),
            graceid: req.graceid.clone(),
            pipeline: req.pipeline.clone(),
            snr,
            far,
            min_snr: self.localize_min_snr,
            max_far_hz: self.localize_max_far_hz,
            skipped_at: mongodb::bson::DateTime::now(),
        };
        if let Err(e) = self.rt.block_on(archive.record_localize_skip(&doc)) {
            metrics::clusterer::ARCHIVE_ERRORS
                .add(1, &[KeyValue::new("sink", "mongo_localize_skip")]);
            warn!(
                superevent = %doc.superevent_id,
                request_id = %doc.request_id,
                "archive: record_localize_skip failed: {e}"
            );
        }
    }

    fn archive_localize_result(&self, result: &LocalizeResult) {
        let Some(archive) = self.archive.as_ref() else {
            return;
        };
        if let Err(e) = self.rt.block_on(archive.record_localize_result(result)) {
            metrics::clusterer::ARCHIVE_ERRORS
                .add(1, &[KeyValue::new("sink", "mongo_localize_result")]);
            warn!(
                superevent = %result.superevent_id,
                request_id = %result.request_id,
                "archive: record_localize_result failed: {e}"
            );
        }
    }
}

fn record_update_metric(update: &SupereventUpdate) {
    let kind = match update {
        SupereventUpdate::Created { .. } => "created",
        SupereventUpdate::PreferredUpdated { .. } => "preferred_updated",
        SupereventUpdate::Skipped { .. } => "skipped",
        SupereventUpdate::SkymapAttached { .. } => "skymap_attached",
    };
    metrics::clusterer::SUPEREVENT_UPDATES.add(1, &[KeyValue::new("kind", kind)]);
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_logging();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    // OTel metrics are off unless explicitly enabled. The keep-alive
    // binding holds the provider so it lives the lifetime of the
    // process; dropping it forces a final flush during shutdown.
    //
    // `rt.enter()` is required around `init_metrics`: the OTLP gRPC
    // exporter builds a Tonic/hyper-util client during construction,
    // and hyper-util panics with `there is no reactor running, must
    // be called from the context of a Tokio 1.x runtime` if no
    // runtime is current. The EnterGuard scopes the runtime
    // task-local so the exporter's `Handle::current()` calls succeed.
    // Once constructed, the exporter holds its own handle reference,
    // so the guard can be dropped at the end of this block.
    let _meter_provider = if cli.metrics_enabled {
        let _enter = rt.enter();
        Some(metrics::init_metrics(
            "gw-clusterer".into(),
            uuid::Uuid::new_v4(),
            cli.deployment_env.clone(),
        )?)
    } else {
        None
    };

    let publisher = match (cli.publish_topic.as_deref(), cli.publish_servers.as_deref()) {
        (Some(topic), Some(servers)) => Some(SupereventPublisher::new(PublisherConfig::new(
            servers, topic,
        ))?),
        (Some(_), None) => anyhow::bail!("--publish-topic requires --publish-servers"),
        _ => None,
    };

    let mut redis_conn = match cli.redis_url.as_deref() {
        Some(url) => {
            let client = redis::Client::open(url)?;
            let conn = rt.block_on(client.get_multiplexed_async_connection())?;
            Some(conn)
        }
        None => None,
    };

    let (localizer_client, localizer_results) = match cli.localize_servers.as_deref() {
        Some(servers) => {
            let mut client_cfg = LocalizerClientConfig::new(servers);
            client_cfg.request_topic = cli.localize_request_topic.clone();
            let client = LocalizerClient::new(client_cfg)?;

            let group_id = cli
                .localize_result_group_id
                .clone()
                .unwrap_or_else(|| format!("boom-gw-clusterer-localize-{}", std::process::id()));
            let mut consumer_cfg = LocalizerResultConsumerConfig::new(servers, group_id);
            consumer_cfg.result_topic = cli.localize_result_topic.clone();
            let stream = LocalizerResultConsumer::spawn(consumer_cfg)?;
            info!(
                request_topic = %cli.localize_request_topic,
                result_topic = %cli.localize_result_topic,
                servers,
                "localization microservice client + result consumer started"
            );
            (Some(client), Some(stream))
        }
        None => (None, None),
    };

    let archive = match cli.mongo_uri.as_deref() {
        Some(uri) => {
            let mut cfg = ArchiveConfig::new(uri);
            cfg.database = cli.mongo_db.clone();
            let archive = rt.block_on(Archive::connect(cfg))?;
            info!(database = %cli.mongo_db, "archive: MongoDB connected");
            Some(archive)
        }
        None => None,
    };

    // Construct the skymap storage. Mongo-backed reuses the
    // existing Archive's DB handle. S3 requires --s3-* flags.
    let skymap_storage = match (&archive, cli.skymap_storage) {
        (Some(archive), backend) => {
            use boom_gw::storage::skymap::{
                build_storage, S3Config, SkymapBackendKind, SkymapCacheConfig,
            };
            use std::time::Duration as StdDuration;
            let s3 = if matches!(backend, SkymapBackendKind::S3) {
                let bucket = cli.s3_bucket.clone().ok_or_else(|| {
                    anyhow::anyhow!("--s3-bucket is required when --skymap-storage=s3")
                })?;
                let access = cli.s3_access_key.clone().ok_or_else(|| {
                    anyhow::anyhow!("--s3-access-key is required when --skymap-storage=s3")
                })?;
                let secret = cli.s3_secret_key.clone().ok_or_else(|| {
                    anyhow::anyhow!("--s3-secret-key is required when --skymap-storage=s3")
                })?;
                let cache = cli
                    .s3_cache_redis_url
                    .as_ref()
                    .map(|url| SkymapCacheConfig {
                        redis_url: url.clone(),
                        ttl: StdDuration::from_secs(cli.s3_cache_ttl_seconds),
                        key_prefix: "boom-gw".into(),
                    });
                Some(S3Config {
                    bucket,
                    key_prefix: cli.s3_key_prefix.clone(),
                    region: cli.s3_region.clone(),
                    access_key: access,
                    secret_key: secret,
                    endpoint_url: cli.s3_endpoint_url.clone(),
                    compress: cli.s3_compress,
                    cache,
                })
            } else {
                None
            };
            let storage = rt.block_on(build_storage(backend, archive.database(), s3))?;
            info!(backend = ?backend, "skymap storage initialized");
            Some(std::sync::Arc::new(storage))
        }
        (None, _) => None,
    };

    let creator = match &mut redis_conn {
        Some(conn) => rt.block_on(load_from_redis(
            conn,
            &cli.redis_prefix,
            Some(cli.window_secs),
        ))?,
        None => SupereventCreator::new(cli.window_secs),
    };
    if creator.len() > 0 {
        info!(
            restored = creator.len(),
            prefix = %cli.redis_prefix,
            "restored open superevent state from redis"
        );
    }

    let writer = match &cli.out_jsonl {
        Some(p) => Some(BufWriter::new(fs::File::create(p)?)),
        None => None,
    };

    let mut pipeline = Pipeline {
        rt,
        creator,
        publisher,
        writer,
        redis_conn,
        redis_prefix: cli.redis_prefix.clone(),
        localizer_client,
        localizer_results,
        archive,
        skymap_storage,
        localize_min_snr: cli.localize_min_snr,
        localize_max_far_hz: cli.localize_max_far_hz,
    };

    if let Some(dir) = &cli.replay_dir {
        let events = load_replay(dir)?;
        info!(count = events.len(), dir = %dir.display(), "replaying events");
        for (event, xml) in events {
            pipeline.process_event(event, &xml)?;
        }
        if pipeline.localizer_results.is_some() {
            let deadline = Instant::now() + Duration::from_secs(cli.localize_drain_secs);
            pipeline.drain_until_quiet(deadline)?;
        }
    } else {
        let token_path = cli.token_file.ok_or_else(|| {
            anyhow::anyhow!(
                "live mode requires --token-file (or pass --replay-dir for offline mode)"
            )
        })?;
        let token_source: Arc<dyn TokenSource> = Arc::new(FileTokenSource::new(token_path));
        let _ = token_source.current_token()?;

        let topics: Vec<String> = if cli.topics.is_empty() {
            pipeline_topics_for_instance(&cli.gracedb_instance)
        } else {
            cli.topics.clone()
        };
        let config = GwKafkaConfig {
            bootstrap_servers: cli.bootstrap_servers,
            topics,
            group_id: cli.group_id,
            use_tls: !cli.no_tls,
            ca_cert_path: cli.ca_cert_path,
            auto_offset_reset: cli.auto_offset_reset,
            poll_timeout: Duration::from_millis(cli.poll_timeout_ms),
        };
        let consumer = GwAlertConsumer::new(config, token_source);
        let stop_flag = consumer.stop_flag();
        ctrlc::set_handler(move || {
            info!("received Ctrl-C, stopping after the current poll");
            stop_flag.store(true, Ordering::Relaxed);
        })
        .ok();

        let max = cli.max_events;
        let mut count: u64 = 0;
        consumer.run_with_xml(|result| match result {
            Ok((event, xml)) => {
                count += 1;
                if let Err(e) = pipeline.process_event(event, &xml) {
                    error!("handler error: {e}");
                }
                if max > 0 && count >= max {
                    HandlerControl::Stop
                } else {
                    HandlerControl::Continue
                }
            }
            Err(e) => {
                metrics::clusterer::EVENTS_INGESTED.add(
                    1,
                    &[
                        KeyValue::new("pipeline", "unknown"),
                        KeyValue::new("result", "decode_error"),
                    ],
                );
                error!("decode error, skipping: {e}");
                HandlerControl::Continue
            }
        })?;
        // Live mode just exited the consumer loop (either Ctrl-C or
        // --max-events hit). Give any in-flight localize requests a
        // chance to come back and attach before we print the final
        // summary — same drain semantics as replay mode.
        if pipeline.localizer_results.is_some() {
            let deadline = Instant::now() + Duration::from_secs(cli.localize_drain_secs);
            pipeline.drain_until_quiet(deadline)?;
        }
    }

    print_final_summary(&pipeline.creator);
    Ok(())
}

/// Build a [`LocalizeRequest`] for the update we just emitted, or
/// `None` if the update is not one that should trigger a (re-)localize.
/// We trigger on `Created` and `PreferredUpdated`: a `Created` is a new
/// superevent that has no sky map yet; a `PreferredUpdated` swapped in
/// a higher-SNR template, so the previous localization is stale.
/// `Skipped` and `SkymapAttached` produce nothing.
fn localize_request_for(
    event: &GwEvent,
    coinc_xml: &[u8],
    update: &SupereventUpdate,
) -> Option<LocalizeRequest> {
    let superevent_id = match update {
        SupereventUpdate::Created { superevent } => &superevent.id,
        SupereventUpdate::PreferredUpdated { superevent, .. } => &superevent.id,
        SupereventUpdate::Skipped { .. } | SupereventUpdate::SkymapAttached { .. } => return None,
    };
    let request_id = format!("{}-{}", superevent_id, event.graceid);
    Some(LocalizeRequest::from_coinc_xml(
        request_id,
        superevent_id,
        &event.graceid,
        &event.pipeline,
        coinc_xml,
    ))
}

fn print_update(event: &GwEvent, update: &SupereventUpdate) {
    match update {
        SupereventUpdate::Created { superevent } => {
            println!(
                "CREATE   {se:<8} t0={t0:>16.6}  graceid={gid:<10} pipeline={pipe:<8} snr={snr:6.2} far={far:.3e}",
                se = superevent.id,
                t0 = superevent.t_0,
                gid = event.graceid,
                pipe = event.pipeline,
                snr = event.snr,
                far = event.far,
            );
        }
        SupereventUpdate::PreferredUpdated {
            superevent,
            previous_preferred_graceid,
        } => {
            println!(
                "PREFER   {se:<8} t0={t0:>16.6}  graceid={gid:<10} pipeline={pipe:<8} snr={snr:6.2} far={far:.3e}  (was {prev})",
                se = superevent.id,
                t0 = superevent.t_0,
                gid = event.graceid,
                pipe = event.pipeline,
                snr = event.snr,
                far = event.far,
                prev = previous_preferred_graceid,
            );
        }
        SupereventUpdate::Skipped {
            superevent_id,
            reason,
            ..
        } => {
            println!(
                "SKIP     {se:<8}                              graceid={gid:<10} pipeline={pipe:<8} snr={snr:6.2} reason={reason:?}",
                se = superevent_id,
                gid = event.graceid,
                pipe = event.pipeline,
                snr = event.snr,
                reason = reason,
            );
        }
        SupereventUpdate::SkymapAttached { superevent } => {
            print_skymap_attached(&SupereventUpdate::SkymapAttached {
                superevent: superevent.clone(),
            });
        }
    }
}

fn print_skymap_attached(update: &SupereventUpdate) {
    if let SupereventUpdate::SkymapAttached { superevent } = update {
        let bytes = superevent
            .skymap
            .as_ref()
            .map(|s| s.bytes.len())
            .unwrap_or(0);
        let elapsed = superevent
            .skymap
            .as_ref()
            .map(|s| s.elapsed_ms)
            .unwrap_or(0);
        println!(
            "SKYMAP   {se:<8} t0={t0:>16.6}  preferred={pref:<10}  fits={bytes} bytes  bayestar={elapsed} ms",
            se = superevent.id,
            t0 = superevent.t_0,
            pref = superevent.preferred_event.graceid,
        );
    }
}

fn write_update_jsonl(
    w: &mut impl std::io::Write,
    event: &GwEvent,
    update: &SupereventUpdate,
) -> anyhow::Result<()> {
    let (action, superevent_id, t_0): (&str, &str, f64) = match update {
        SupereventUpdate::Created { superevent } => {
            ("create", superevent.id.as_str(), superevent.t_0)
        }
        SupereventUpdate::PreferredUpdated { superevent, .. } => {
            ("prefer", superevent.id.as_str(), superevent.t_0)
        }
        SupereventUpdate::Skipped {
            superevent_id,
            reason: SkipReason::LowerSnr,
            ..
        } => ("skip_lower_snr", superevent_id.as_str(), 0.0),
        SupereventUpdate::SkymapAttached { .. } => return Ok(()),
    };
    let line = serde_json::json!({
        "action": action,
        "graceid": event.graceid,
        "pipeline": event.pipeline,
        "end_time": event.end_time,
        "snr": event.snr,
        "far": event.far,
        "superevent_id": superevent_id,
        "t_0": t_0,
    });
    writeln!(w, "{line}")?;
    Ok(())
}

fn print_final_summary(creator: &SupereventCreator) {
    println!();
    println!("=== Superevent summary ===");
    let mut superevents: Vec<_> = creator.superevents().collect();
    superevents.sort_by(|a, b| {
        a.t_0
            .partial_cmp(&b.t_0)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    for s in superevents {
        let g_ids: Vec<&str> = s.g_events.iter().map(|e| e.graceid.as_str()).collect();
        let skymap_note = match &s.skymap {
            Some(sky) => format!("  skymap={} bytes ({} ms)", sky.bytes.len(), sky.elapsed_ms),
            None => String::new(),
        };
        println!(
            "{se:<8} t0={t0:>16.6}  window=[{ts:>16.6}, {te:>16.6}]  preferred={pref:<10} (snr={snr:.2})  events={n}: {ids:?}{sky}",
            se = s.id,
            t0 = s.t_0,
            ts = s.t_start,
            te = s.t_end,
            pref = s.preferred_event.graceid,
            snr = s.preferred_event.snr,
            n = s.g_events.len(),
            ids = g_ids,
            sky = skymap_note,
        );
    }
}

/// Load events from a directory of `gw_dump` envelope JSON files. Sorted by
/// `_producer_timestamp` to mimic the order events would have arrived on the
/// wire.
fn load_replay(dir: &std::path::Path) -> anyhow::Result<Vec<(GwEvent, Vec<u8>)>> {
    let mut events: Vec<(f64, GwEvent, Vec<u8>)> = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let bytes = fs::read(&path)?;
        let envelope = match EventEnvelope::from_json(&bytes) {
            Ok(e) => e,
            Err(e) => {
                error!("skip {}: envelope parse failed: {e}", path.display());
                continue;
            }
        };
        match extract_gw_event_with_xml(&envelope) {
            Ok((event, xml)) => events.push((event.producer_timestamp, event, xml)),
            Err(e) => error!("skip {}: extract failed: {e}", path.display()),
        }
    }
    events.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let mut seen: HashMap<String, ()> = HashMap::new();
    let deduped: Vec<(GwEvent, Vec<u8>)> = events
        .into_iter()
        .filter(|(_, e, _)| seen.insert(e.graceid.clone(), ()).is_none())
        .map(|(_, e, xml)| (e, xml))
        .collect();
    Ok(deduped)
}

fn init_logging() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,boom=info,rdkafka=warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
