//! `gw-gcn-consumer` — subscribe to GCN Fermi-GBM topics, persist
//! every trigger into the boom-gw archive, and (optionally)
//! auto-compute a GW × GRB cross-match against any superevent
//! whose `t_0` falls within a configurable coincidence window of
//! the trigger.
//!
//! Auth: production GCN requires OIDC OAUTHBEARER with a
//! `client_id` / `client_secret` minted at
//! <https://gcn.nasa.gov/quickstart>. For local development and
//! CI integration tests against a plain Kafka broker, pass
//! `--auth plaintext`.
//!
//! Run:
//!
//! ```sh
//! cargo run --bin gw_gcn_consumer -- \
//!   --bootstrap-servers kafka.gcn.nasa.gov:9092 \
//!   --auth oidc \
//!   --gcn-client-id $GCN_CLIENT_ID \
//!   --gcn-client-secret $GCN_CLIENT_SECRET \
//!   --mongo-uri $BOOM_GW_MONGO_URI \
//!   --skymap-storage s3 --s3-bucket boom-gw-skymaps ...
//! ```

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use mongodb::bson::doc;
use tokio::runtime::Runtime;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use boom_gw::archive::CrossMatchDoc;
use boom_gw::crossmatch::{self, cross_match, rates};
use boom_gw::gcn_consumer::{
    default_topics, GcnAlert, GcnAlertConsumer, GcnAuth, GcnKafkaConfig, HandlerControl,
    DEFAULT_GCN_BOOTSTRAP_SERVERS, DEFAULT_GCN_TOKEN_URL,
};
use boom_gw::storage::skymap::{
    build_storage, S3Config, SkymapBackendKind, SkymapCacheConfig, SkymapStorage,
};
use boom_gw::{Archive, ArchiveConfig, DEFAULT_DB_NAME};

#[derive(Parser, Debug)]
#[command(
    name = "gw-gcn-consumer",
    about = "Subscribe to GCN Fermi-GBM topics; persist + cross-match."
)]
struct Cli {
    /// Kafka bootstrap servers. Defaults to the production GCN
    /// broker — point at `localhost:9092` for local testing.
    #[arg(
        long,
        env = "BOOM_GW_GCN_BOOTSTRAP_SERVERS",
        default_value = DEFAULT_GCN_BOOTSTRAP_SERVERS,
    )]
    bootstrap_servers: String,

    #[arg(long, env = "BOOM_GW_GCN_GROUP_ID", default_value = "boom-gw-gcn")]
    group_id: String,

    #[arg(long, env = "BOOM_GW_GCN_AUTO_OFFSET_RESET", default_value = "latest")]
    auto_offset_reset: String,

    /// Comma-separated list of topics. Defaults to the four Fermi
    /// GBM JSON-notice stages.
    #[arg(long, value_delimiter = ',')]
    topics: Vec<String>,

    /// Auth scheme. `plaintext` is for local docker-compose and
    /// CI tests; `oidc` is required for the real GCN broker.
    #[arg(long, env = "BOOM_GW_GCN_AUTH", default_value = "oidc")]
    auth: AuthKind,

    #[arg(long, env = "BOOM_GW_GCN_CLIENT_ID")]
    gcn_client_id: Option<String>,

    #[arg(long, env = "BOOM_GW_GCN_CLIENT_SECRET")]
    gcn_client_secret: Option<String>,

    #[arg(long, env = "BOOM_GW_GCN_TOKEN_URL", default_value = DEFAULT_GCN_TOKEN_URL)]
    gcn_token_url: String,

    #[arg(long, env = "BOOM_GW_GCN_CA_CERT_PATH")]
    gcn_ca_cert_path: Option<PathBuf>,

    /// Coincidence window (seconds) for auto cross-matching. Each
    /// inbound trigger triggers a cross-match against superevents
    /// whose `t_0` is within ±`coincidence_window_sec` of the
    /// trigger time. Default 10 s matches the RAVEN GRB search.
    /// Set to 0 to disable auto cross-matching.
    #[arg(
        long,
        env = "BOOM_GW_GCN_COINCIDENCE_WINDOW_SEC",
        default_value_t = 10.0
    )]
    coincidence_window_sec: f64,

    /// librdkafka `debug` config (e.g. `"security,broker,topic"`).
    /// Forwarded directly; turns on noisy internal traces routed
    /// through the rdkafka tracing target. Off by default.
    #[arg(long, env = "BOOM_GW_GCN_KAFKA_DEBUG")]
    kafka_debug: Option<String>,

    #[arg(
        long,
        env = "BOOM_GW_MONGO_URI",
        default_value = "mongodb://localhost:27017"
    )]
    mongo_uri: String,

    #[arg(long, env = "BOOM_GW_MONGO_DB", default_value = DEFAULT_DB_NAME)]
    mongo_db: String,

    #[arg(long, env = "BOOM_GW_SKYMAP_STORAGE", default_value = "mongo")]
    skymap_storage: SkymapBackendKind,

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
    #[arg(long, env = "BOOM_GW_S3_ENDPOINT_URL")]
    s3_endpoint_url: Option<String>,
    #[arg(long, env = "BOOM_GW_S3_COMPRESS", default_value_t = true)]
    s3_compress: bool,
}

#[derive(Clone, Debug, clap::ValueEnum)]
enum AuthKind {
    Plaintext,
    Oidc,
}

fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,boom_gw=info,rdkafka=warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = Cli::parse();
    let rt = Runtime::new()?;

    // Build the archive + skymap storage handles up-front so any
    // misconfiguration fails fast instead of on the first message.
    let archive = rt.block_on(async {
        let mut cfg = ArchiveConfig::new(&cli.mongo_uri);
        cfg.database = cli.mongo_db.clone();
        Archive::connect(cfg).await
    })?;

    let s3_cfg = if matches!(cli.skymap_storage, SkymapBackendKind::S3) {
        let bucket = cli
            .s3_bucket
            .clone()
            .ok_or_else(|| anyhow::anyhow!("--s3-bucket required for s3 backend"))?;
        let access_key = cli
            .s3_access_key
            .clone()
            .ok_or_else(|| anyhow::anyhow!("--s3-access-key required for s3 backend"))?;
        let secret_key = cli
            .s3_secret_key
            .clone()
            .ok_or_else(|| anyhow::anyhow!("--s3-secret-key required for s3 backend"))?;
        Some(S3Config {
            bucket,
            key_prefix: cli.s3_key_prefix.clone(),
            region: cli.s3_region.clone(),
            access_key,
            secret_key,
            endpoint_url: cli.s3_endpoint_url.clone(),
            compress: cli.s3_compress,
            cache: None::<SkymapCacheConfig>,
        })
    } else {
        None
    };
    let storage =
        Arc::new(rt.block_on(async {
            build_storage(cli.skymap_storage, archive.database(), s3_cfg).await
        })?);

    let topics = if cli.topics.is_empty() {
        default_topics()
    } else {
        cli.topics.clone()
    };

    let auth = match cli.auth {
        AuthKind::Plaintext => GcnAuth::Plaintext,
        AuthKind::Oidc => GcnAuth::OidcOauthBearer {
            client_id: cli
                .gcn_client_id
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--gcn-client-id required for --auth oidc"))?,
            client_secret: cli
                .gcn_client_secret
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--gcn-client-secret required for --auth oidc"))?,
            token_url: cli.gcn_token_url.clone(),
            ca_cert_path: cli.gcn_ca_cert_path.clone(),
        },
    };

    let consumer = GcnAlertConsumer::new(GcnKafkaConfig {
        bootstrap_servers: cli.bootstrap_servers.clone(),
        group_id: cli.group_id.clone(),
        auto_offset_reset: cli.auto_offset_reset.clone(),
        poll_timeout: Duration::from_millis(1000),
        topics,
        auth,
        debug: cli.kafka_debug.clone(),
    });

    // Install a ctrl-c handler so SIGTERM / SIGINT cleanly stop
    // the poll loop. The stop_flag is shared with the consumer.
    let stop_flag = consumer.stop_flag();
    ctrlc::set_handler(move || {
        info!("received shutdown signal, draining...");
        stop_flag.store(true, Ordering::Relaxed);
    })?;

    info!(
        bootstrap_servers = %cli.bootstrap_servers,
        coincidence_window_sec = cli.coincidence_window_sec,
        "starting gw-gcn-consumer"
    );

    let archive_for_handler = archive.clone();
    let storage_for_handler = storage.clone();
    let coincidence_window = cli.coincidence_window_sec;

    consumer.run(|alert: GcnAlert| {
        if let Err(e) = rt.block_on(handle_alert(
            &archive_for_handler,
            &storage_for_handler,
            alert,
            coincidence_window,
        )) {
            error!("alert handler failed: {e}");
        }
        HandlerControl::Continue
    })?;

    info!("gw-gcn-consumer exiting cleanly");
    Ok(())
}

async fn handle_alert(
    archive: &Archive,
    storage: &Arc<SkymapStorage>,
    alert: GcnAlert,
    coincidence_window_sec: f64,
) -> anyhow::Result<()> {
    let topic = alert.topic.clone();
    let trigger = match alert.payload {
        boom_gw::gcn_consumer::GcnPayload::Grb(t) => t,
        boom_gw::gcn_consumer::GcnPayload::Boom(transients) => {
            return handle_boom_payload(archive, storage, &topic, transients).await;
        }
        boom_gw::gcn_consumer::GcnPayload::Frb(frb) => {
            return handle_frb_payload(archive, storage, &topic, frb).await;
        }
        boom_gw::gcn_consumer::GcnPayload::Neutrino(nu) => {
            return handle_neutrino_payload(archive, storage, &topic, nu).await;
        }
        boom_gw::gcn_consumer::GcnPayload::IceCubeLvkSearch(search) => {
            return handle_icecube_lvk_search(archive, &topic, search).await;
        }
    };
    info!(
        topic = %topic,
        instrument = %trigger.instrument,
        trigger_id = %trigger.trigger_id,
        trigger_time = trigger.trigger_time,
        "received GCN alert"
    );
    // Direct lib call into the shared `crate::ingest::ingest_grb_trigger`
    // — same handler the HTTP POST hits, but no network hop, no
    // JSON ser/deser, no JWT decode. Live ingest stays µs-scale.
    let trigger = match boom_gw::ingest::ingest_grb_trigger(archive, Some(storage), trigger).await {
        Ok((_created, doc)) => doc.trigger,
        Err(e) => {
            warn!("grb trigger ingest failed: {e}");
            return Err(e.into());
        }
    };

    if coincidence_window_sec <= 0.0 || trigger.position.is_none() {
        // Auto-match disabled, or the alert pre-localization. The
        // trigger is already persisted (above) in case downstream
        // jobs care.
        return Ok(());
    }

    // Find candidate superevents in ±window. We need both
    // `t_0 ∈ window` and a `skymap_summary` (otherwise there's
    // no FITS to integrate).
    let lo = trigger.trigger_time - coincidence_window_sec;
    let hi = trigger.trigger_time + coincidence_window_sec;
    let filter = doc! {
        "t_0": {"$gte": lo, "$lte": hi},
        "skymap_summary": {"$exists": true, "$ne": null},
    };
    let mut cursor = archive.superevents().find(filter).await?;
    use futures::stream::StreamExt;
    while let Some(s) = cursor.next().await {
        let s = match s {
            Ok(s) => s,
            Err(e) => {
                warn!("superevent fetch failed during scan: {e}");
                continue;
            }
        };
        if let Err(e) = compute_and_persist_cross_match(archive, storage, &trigger, &s).await {
            warn!(
                superevent = %s.id,
                trigger_id = %trigger.trigger_id,
                "auto cross-match failed: {e}"
            );
        }
    }
    Ok(())
}

async fn compute_and_persist_cross_match(
    archive: &Archive,
    storage: &Arc<SkymapStorage>,
    trigger: &boom_gw::grb::GrbTrigger,
    superevent: &boom_gw::archive::SupereventDoc,
) -> anyhow::Result<()> {
    let blob = storage.get(&superevent.id).await?;
    let contour_50 = storage.get_contour(&superevent.id, 50).await.ok();
    let contour_90 = storage.get_contour(&superevent.id, 90).await.ok();
    // Fetch the canonical GRB MOC. Should always exist by this
    // point because the same handler persisted it a few lines
    // back; if not (rare race), synthesize on the fly so the
    // cross-match still produces a result.
    let grb_moc_bytes = match storage
        .get_grb_skymap(&trigger.instrument, &trigger.trigger_id)
        .await
    {
        Ok(b) => b,
        Err(_) => boom_gw::grb::build_canonical_moc_fits(trigger)
            .map_err(|e| anyhow::anyhow!("grb canonical MOC build failed: {e}"))?,
    };

    // Look up the preferred event's FAR. Falls back to a
    // conservative 1e-7 Hz if the event is missing.
    let gw_far_hz = match archive
        .events()
        .find_one(doc! {"_id": &superevent.preferred_graceid})
        .await?
    {
        Some(ev) => ev.far,
        None => 1e-7,
    };

    // Auto cross-matches run with a modest 200-trial p-value
    // Monte Carlo — enough for a stable significance estimate
    // without making per-trigger latency painful. Operators can
    // re-run from the UI with more trials for a tighter result.
    let pvalue_opts = Some(boom_gw::crossmatch::PvalueOpts {
        n_trials: 200,
        far_gw_max_hz: 2.0 / 86400.0,
        seed: None,
    });
    let result = cross_match(
        trigger,
        superevent.t_0,
        gw_far_hz,
        &blob.bytes,
        &grb_moc_bytes,
        contour_50.as_deref(),
        contour_90.as_deref(),
        10.0,
        rates::GRB_RATE_HZ,
        pvalue_opts,
    )?;
    let doc = CrossMatchDoc::new(&superevent.id, trigger, result.clone());
    archive.upsert_cross_match(&doc).await?;
    info!(
        superevent = %superevent.id,
        trigger_id = %trigger.trigger_id,
        spatial_overlap = result.spatial_overlap,
        in_90cr = result.in_90cr,
        joint_far_per_year = ?result.joint_far_per_year,
        "auto cross-match persisted"
    );
    // Touch the unused-import suppressor.
    let _ = crossmatch::DEFAULT_CONE_DEPTH;
    Ok(())
}

/// BOOM optical-transient alerts. Each upstream envelope explodes
/// into one [`BoomAlertDoc`] per `data.targets[]` entry; we upsert
/// each into the `boom_alerts` collection AND synthesize the same
/// canonical MOC FITS we keep for GRB triggers (under
/// `instrument="BOOM"`, `trigger_id=alert_id`) so a later
/// superevent scan can include BOOM transients as cross-match
/// candidates uniformly. No auto cross-match loop here — that's
/// the operator's job via the Scan button on the Cross-matches
/// tab.
async fn handle_boom_payload(
    archive: &Archive,
    storage: &Arc<SkymapStorage>,
    topic: &str,
    transients: Vec<boom_gw::boom::BoomTransient>,
) -> anyhow::Result<()> {
    if transients.is_empty() {
        tracing::debug!(topic = %topic, "BOOM alert with no targets — nothing to persist");
        return Ok(());
    }
    for t in transients {
        info!(
            topic = %topic,
            alert_id = %t.alert_id,
            event_name = %t.event_name,
            classification = ?t.classification,
            "received BOOM transient"
        );
        let alert_id = t.alert_id.clone();
        if let Err(e) = boom_gw::ingest::ingest_boom_alert(archive, Some(storage), t).await {
            warn!(%alert_id, "boom alert ingest failed: {e}");
        }
    }
    Ok(())
}

async fn handle_frb_payload(
    archive: &Archive,
    storage: &Arc<SkymapStorage>,
    topic: &str,
    frb: boom_gw::frb::FrbAlert,
) -> anyhow::Result<()> {
    info!(
        topic = %topic,
        instrument = %frb.trigger.instrument,
        trigger_id = %frb.trigger.trigger_id,
        trigger_time = frb.trigger.trigger_time,
        snr = ?frb.snr,
        dm = ?frb.dm,
        "received FRB alert"
    );
    let trigger_id = frb.trigger.trigger_id.clone();
    if let Err(e) = boom_gw::ingest::ingest_frb_alert(archive, Some(storage), frb).await {
        warn!(%trigger_id, "frb alert ingest failed: {e}");
    }
    Ok(())
}

async fn handle_neutrino_payload(
    archive: &Archive,
    storage: &Arc<SkymapStorage>,
    topic: &str,
    nu: boom_gw::neutrino::NeutrinoAlert,
) -> anyhow::Result<()> {
    info!(
        topic = %topic,
        instrument = %nu.trigger.instrument,
        trigger_id = %nu.trigger.trigger_id,
        trigger_time = nu.trigger.trigger_time,
        pipeline = ?nu.pipeline,
        nu_energy = ?nu.nu_energy,
        "received neutrino alert"
    );
    let trigger_id = nu.trigger.trigger_id.clone();
    if let Err(e) = boom_gw::ingest::ingest_neutrino_alert(archive, Some(storage), nu).await {
        warn!(%trigger_id, "neutrino alert ingest failed: {e}");
    }
    Ok(())
}

async fn handle_icecube_lvk_search(
    archive: &Archive,
    topic: &str,
    search: boom_gw::icecube_lvk::IceCubeLvkSearch,
) -> anyhow::Result<()> {
    info!(
        topic = %topic,
        superevent_id = %search.superevent_id,
        n_coincident = search.n_events_coincident,
        pval_generic = ?search.pval_generic,
        pval_bayesian = ?search.pval_bayesian,
        "received IceCube LVK Nu Track Search result"
    );
    let superevent_id = search.superevent_id.clone();
    if let Err(e) = boom_gw::ingest::ingest_icecube_lvk_search(archive, None, search).await {
        warn!(%superevent_id, "lvk track search ingest failed: {e}");
    }
    Ok(())
}
