//! `gw-consumer` — read GraceDB pipeline events from Kafka and print them.
//!
//! This is the BOOM equivalent of the `ligo.gracedb.kafka.GraceDbKafkaConsumer`
//! Python snippet. Authentication uses SCITokens over SASL/OAUTHBEARER; the
//! bearer token is acquired out-of-band (typically by `htgettoken`) and read
//! from `--token-file` or `$BEARER_TOKEN_FILE` / the WLCG discovery path.
//!
//! Example:
//!
//! ```text
//! htgettoken -a vault.ligo.org -i igwn
//! gw-consumer \
//!     --bootstrap-servers kafka-dev.ligo.org:9092 \
//!     --group-id "$USER-boom-consumer"
//! ```

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use boom_gw::{
    FileTokenSource, GwAlertConsumer, GwKafkaConfig, HandlerControl, TokenSource,
    DEFAULT_PIPELINE_TOPICS,
};

#[derive(Parser, Debug)]
#[command(
    name = "gw-consumer",
    about = "Consume GW pipeline events from the GraceDB Kafka topics via SCITokens"
)]
struct Cli {
    /// Kafka bootstrap broker list (comma-separated host:port).
    #[arg(long, default_value = "kafka-dev.ligo.org:9092")]
    bootstrap_servers: String,

    /// Topics to subscribe to. Defaults to the seven LVK pipeline topics.
    #[arg(long, value_delimiter = ',')]
    topics: Vec<String>,

    /// Kafka consumer group id.
    #[arg(long, default_value = "boom-gw-consumer")]
    group_id: String,

    /// Path to the SCITokens bearer-token file. If omitted, WLCG discovery
    /// rules apply (`$BEARER_TOKEN_FILE`, then `$XDG_RUNTIME_DIR/bt_u<uid>`,
    /// then `/tmp/bt_u<uid>`).
    #[arg(long)]
    token_file: Option<PathBuf>,

    /// CA bundle for the broker's TLS certificate. When omitted, rdkafka uses
    /// the system CA store.
    #[arg(long)]
    ca_cert_path: Option<PathBuf>,

    /// Disable TLS and use SASL_PLAINTEXT (only for local development brokers).
    #[arg(long, default_value_t = false)]
    no_tls: bool,

    /// Auto-offset-reset policy ("earliest" or "latest").
    #[arg(long, default_value = "latest")]
    auto_offset_reset: String,

    /// Poll timeout in milliseconds for each iteration.
    #[arg(long, default_value_t = 1000)]
    poll_timeout_ms: u64,

    /// Exit after receiving this many events (0 = unlimited).
    #[arg(long, default_value_t = 0)]
    max_events: u64,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_logging();

    let topics = if cli.topics.is_empty() {
        DEFAULT_PIPELINE_TOPICS
            .iter()
            .map(|s| s.to_string())
            .collect()
    } else {
        cli.topics.clone()
    };

    let token_source: Arc<dyn TokenSource> = match cli.token_file {
        Some(path) => Arc::new(FileTokenSource::new(path)),
        None => Arc::new(FileTokenSource::discover()?),
    };

    info!(
        topics = ?topics,
        bootstrap_servers = %cli.bootstrap_servers,
        group_id = %cli.group_id,
        use_tls = !cli.no_tls,
        "starting GW Kafka consumer"
    );

    // Smoke-test that the token source can produce a token before subscribing,
    // so a misconfiguration surfaces immediately rather than during the first
    // OAUTHBEARER refresh inside librdkafka.
    let _ = token_source
        .current_token()
        .map_err(|e| anyhow::anyhow!("token source not usable: {e}"))?;

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
    let mut count = 0u64;
    consumer.run(|result| match result {
        Ok(event) => {
            count += 1;
            println!(
                "{:>14.2}  graceid={:<10} pipeline={:<8} type={:<6} ifos={:<10} snr={:6.2} far={:.3e} mchirp={:?} end={:.6}",
                event.producer_timestamp,
                event.graceid,
                event.pipeline,
                event.message_type,
                event.ifos,
                event.snr,
                event.far,
                event.mchirp,
                event.end_time,
            );
            if max > 0 && count >= max {
                HandlerControl::Stop
            } else {
                HandlerControl::Continue
            }
        }
        Err(e) => {
            error!("decode error, skipping message: {e}");
            HandlerControl::Continue
        }
    })?;

    info!(events = count, "consumer exited cleanly");
    Ok(())
}

fn init_logging() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,boom=info,rdkafka=warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
