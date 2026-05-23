//! `gw-api` — read-only HTTP API over the boom-gw MongoDB archive.
//!
//! See `boom_gw::api` for the route table. Authentication uses
//! SCITokens bearer tokens validated against the configured IGWN
//! issuer allowlist; see [`boom_gw::auth`] for the policy.

use std::collections::HashSet;

use clap::Parser;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

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

    api::run_server(archive, alert_publisher, auth, jwks, &cli.bind).await?;
    Ok(())
}
