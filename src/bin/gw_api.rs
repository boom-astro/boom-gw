//! `gw-api` — read-only HTTP API over the boom-gw MongoDB archive.
//!
//! See `boom_gw::api` for the route table. Authentication uses
//! SCITokens bearer tokens validated against the configured IGWN
//! issuer allowlist; see [`boom_gw::auth`] for the policy.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use boom_gw::storage::skymap::{build_storage, S3Config, SkymapBackendKind, SkymapCacheConfig};
use boom_gw::{
    api, metrics, AlertPublisher, AlertPublisherConfig, Archive, ArchiveConfig, AuthConfig,
    JwksCache, DEFAULT_ALERT_TOPIC, DEFAULT_AUDIENCES, DEFAULT_DB_NAME, DEFAULT_ISSUERS,
    DEFAULT_REQUIRED_SCOPE,
};

fn comma_list(s: &str) -> Vec<String> {
    s.split(',')
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect()
}

fn default_issuers() -> String {
    DEFAULT_ISSUERS.join(",")
}

fn default_audiences() -> String {
    DEFAULT_AUDIENCES.join(",")
}

#[derive(Parser, Debug)]
#[command(name = "gw-api", about = "Read-only HTTP API over the boom-gw archive")]
struct Cli {
    /// MongoDB connection string for the archive.
    #[arg(
        long,
        env = "BOOM_GW_MONGO_URI",
        default_value = "mongodb://localhost:27017"
    )]
    mongo_uri: String,

    /// Database name inside the MongoDB instance.
    #[arg(long, env = "BOOM_GW_MONGO_DB", default_value = DEFAULT_DB_NAME)]
    mongo_db: String,

    /// HTTP listen address.
    #[arg(long, env = "BOOM_GW_API_BIND", default_value = "0.0.0.0:8080")]
    bind: String,

    /// Bootstrap servers for the public-alert Kafka cluster. When
    /// omitted, the API still accepts `POST /api/superevents/{id}/alerts`
    /// with `dry_run=true` but rejects real publishes.
    #[arg(long, env = "BOOM_GW_ALERT_SERVERS")]
    alert_servers: Option<String>,

    /// Topic name for public alerts.
    #[arg(long, env = "BOOM_GW_ALERT_TOPIC", default_value = DEFAULT_ALERT_TOPIC)]
    alert_topic: String,

    /// Enable the OpenTelemetry OTLP metrics exporter.
    #[arg(long, env = "BOOM_GW_METRICS_ENABLED", default_value_t = false)]
    metrics_enabled: bool,

    /// Deployment environment name reported on emitted metrics.
    #[arg(long, env = "BOOM_GW_DEPLOYMENT_ENV", default_value = "dev")]
    deployment_env: String,

    /// Comma-separated list of accepted token issuers. Defaults to
    /// the IGWN production + test CILogon + OSDF allowlist (matches
    /// GraceDB's `SCITOKEN_ISSUERS`).
    #[arg(long, env = "BOOM_GW_AUTH_ISSUERS", default_value_t = default_issuers())]
    auth_issuers: String,

    /// Comma-separated list of accepted audiences. Tokens must carry
    /// at least one of these in their `aud` claim. Default keeps
    /// SCITokens 2.0 `"ANY"` plus `"boom-gw"`.
    #[arg(long, env = "BOOM_GW_AUTH_AUDIENCES", default_value_t = default_audiences())]
    auth_audiences: String,

    /// Single scope every authenticated request must carry. Mirrors
    /// GraceDB's `SCITOKEN_SCOPE` (`gracedb.read` default).
    #[arg(long, env = "BOOM_GW_AUTH_SCOPE", default_value = DEFAULT_REQUIRED_SCOPE)]
    auth_scope: String,

    /// Comma-separated list of `sub` claims permitted to POST to
    /// `/api/superevents/{id}/alerts`. Other authenticated users
    /// can still read and annotate. An empty list (the default in
    /// dev) lets any authenticated user publish, which is **never**
    /// what you want in prod.
    #[arg(long, env = "BOOM_GW_ALERT_PUBLISHERS", default_value = "")]
    alert_publishers: String,

    /// Skip JWT signature validation. `iss`/`aud`/`exp`/`scope` are
    /// still enforced. For local development and CI integration
    /// tests where the CILogon JWKS endpoint is unreachable.
    #[arg(long, env = "BOOM_GW_API_AUTH_DEV_MODE", default_value_t = false)]
    auth_dev_mode: bool,

    /// Backend used to store the FITS sky-map blobs. `mongo`
    /// (default) writes to a `skymaps` collection on the same
    /// mongo database. `s3` writes to an S3-compatible object
    /// store (real AWS S3, MinIO, rustfs) — see the
    /// `--s3-*` flags.
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
    /// Override for S3-compatible endpoints (MinIO, rustfs, Wasabi).
    /// Leave unset when pointing at real AWS S3.
    #[arg(long, env = "BOOM_GW_S3_ENDPOINT_URL")]
    s3_endpoint_url: Option<String>,
    /// `true` → zstd-compress FITS bytes before writing to S3.
    /// Defaults `true` because real BAYESTAR FITS files compress
    /// well.
    #[arg(long, env = "BOOM_GW_S3_COMPRESS", default_value_t = true)]
    s3_compress: bool,
    /// Valkey URL for the optional S3 read cache. When unset,
    /// every S3 read is uncached.
    #[arg(long, env = "BOOM_GW_S3_CACHE_REDIS_URL")]
    s3_cache_redis_url: Option<String>,
    #[arg(long, env = "BOOM_GW_S3_CACHE_TTL_SECONDS", default_value_t = 30)]
    s3_cache_ttl_seconds: u64,

    /// Directory containing the built SPA bundle (`web/dist`). When
    /// set, gw-api serves it as the catch-all fallback for routes
    /// not matched by `/api/*`. Unset → API only (useful when the
    /// frontend is hosted separately or you're iterating in `vite
    /// dev` against this API).
    #[arg(long, env = "BOOM_GW_STATIC_DIR")]
    static_dir: Option<std::path::PathBuf>,
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,boom_gw=info,actix_web=info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = Cli::parse();

    let _meter_provider = if cli.metrics_enabled {
        Some(metrics::init_metrics(
            "gw-api".into(),
            uuid::Uuid::new_v4(),
            cli.deployment_env.clone(),
        )?)
    } else {
        None
    };

    let mut cfg = ArchiveConfig::new(&cli.mongo_uri);
    cfg.database = cli.mongo_db.clone();
    let archive = Archive::connect(cfg).await?;

    let alert_publisher = match cli.alert_servers.as_deref() {
        Some(servers) => {
            let mut cfg = AlertPublisherConfig::new(servers);
            cfg.topic = cli.alert_topic.clone();
            Some(AlertPublisher::new(cfg)?)
        }
        None => None,
    };

    let alert_publishers: HashSet<String> = comma_list(&cli.alert_publishers).into_iter().collect();
    if alert_publishers.is_empty() && !cli.auth_dev_mode {
        warn!(
            "alert publisher allowlist is empty; any authenticated user can POST public alerts. \
             Set BOOM_GW_ALERT_PUBLISHERS to a comma-separated list of `sub` values in production."
        );
    }

    let auth = AuthConfig {
        issuers: comma_list(&cli.auth_issuers),
        audiences: comma_list(&cli.auth_audiences),
        required_scope: cli.auth_scope.clone(),
        alert_publishers,
        dev_mode: cli.auth_dev_mode,
    };
    let jwks = JwksCache::new();
    if !auth.dev_mode {
        match jwks.warm(&auth.issuers).await {
            Ok(()) => info!(issuers = ?auth.issuers, "JWKS cache warmed for all issuers"),
            Err(e) => {
                // Non-fatal: the cache will lazily refresh on the
                // first inbound request that targets the issuer.
                warn!("JWKS warm-up failed: {e}; will retry lazily on first request");
            }
        }
    } else {
        warn!("BOOM_GW_API_AUTH_DEV_MODE=1 — signature validation disabled");
    }

    // Build the skymap storage backend. For S3 we require the
    // bucket + credentials; the CLI will refuse to start without
    // them rather than silently fall back to mongo.
    let s3_cfg = if matches!(cli.skymap_storage, SkymapBackendKind::S3) {
        let bucket = cli
            .s3_bucket
            .clone()
            .ok_or_else(|| anyhow::anyhow!("--s3-bucket is required when --skymap-storage=s3"))?;
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
                ttl: Duration::from_secs(cli.s3_cache_ttl_seconds),
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
    let storage = build_storage(cli.skymap_storage, archive.database(), s3_cfg).await?;
    info!(backend = ?cli.skymap_storage, "skymap storage initialized");
    let storage = Some(Arc::new(storage));

    api::run_server(
        archive,
        alert_publisher,
        storage,
        auth,
        jwks,
        &cli.bind,
        cli.static_dir.clone(),
    )
    .await?;
    Ok(())
}
