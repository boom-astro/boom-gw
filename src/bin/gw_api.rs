//! `gw-api` — read-only HTTP API over the boom-gw MongoDB archive.
//!
//! See `boom_gw::api` for the route table. The server is intended to
//! run behind an internal load balancer; auth and rate limiting are
//! the operator's responsibility for now.

use clap::Parser;
use tracing_subscriber::EnvFilter;

use boom_gw::{
    api, AlertPublisher, AlertPublisherConfig, Archive, ArchiveConfig, DEFAULT_ALERT_TOPIC,
    DEFAULT_DB_NAME,
};

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
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,boom_gw=info,actix_web=info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = Cli::parse();
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

    api::run_server(archive, alert_publisher, &cli.bind).await?;
    Ok(())
}
