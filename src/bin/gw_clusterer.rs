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
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use boom_gw::{
    extract_gw_event, load_from_redis, save_to_redis, EventEnvelope, FileTokenSource,
    GwAlertConsumer, GwEvent, GwKafkaConfig, HandlerControl, PublisherConfig, SkipReason,
    SupereventCreator, SupereventPublisher, SupereventUpdate, TokenSource,
    DEFAULT_PIPELINE_TOPICS, DEFAULT_WINDOW_SECS,
};

#[derive(Parser, Debug)]
#[command(name = "gw-clusterer", about = "Cluster GW pipeline events into superevents")]
struct Cli {
    /// Replay envelope JSON files from this directory instead of consuming
    /// from Kafka. Files are sorted by `_producer_timestamp`.
    #[arg(long)]
    replay_dir: Option<PathBuf>,

    /// Kafka bootstrap servers (only used when --replay-dir is omitted).
    #[arg(long, default_value = "kafka-dev.ligo.org:9092")]
    bootstrap_servers: String,

    /// Topics to subscribe to (only used in live mode).
    #[arg(long, value_delimiter = ',')]
    topics: Vec<String>,

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
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_logging();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    // Construct the optional publisher and Redis connection up front so
    // misconfigurations surface before we start consuming.
    let publisher = match (cli.publish_topic.as_deref(), cli.publish_servers.as_deref()) {
        (Some(topic), Some(servers)) => Some(SupereventPublisher::new(PublisherConfig::new(
            servers, topic,
        ))?),
        (Some(_), None) => {
            anyhow::bail!("--publish-topic requires --publish-servers")
        }
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

    let mut creator = match &mut redis_conn {
        Some(conn) => rt
            .block_on(load_from_redis(conn, &cli.redis_prefix, Some(cli.window_secs)))?,
        None => SupereventCreator::new(cli.window_secs),
    };
    if creator.len() > 0 {
        info!(
            restored = creator.len(),
            prefix = %cli.redis_prefix,
            "restored open superevent state from redis"
        );
    }

    let mut writer = match &cli.out_jsonl {
        Some(p) => Some(std::io::BufWriter::new(fs::File::create(p)?)),
        None => None,
    };

    let mut handle = |event: GwEvent, creator: &mut SupereventCreator| -> anyhow::Result<()> {
        let update = creator.process(event.clone());
        print_update(&event, &update);
        if let Some(w) = writer.as_mut() {
            write_update_jsonl(w, &event, &update)?;
        }
        if let Some(pub_) = publisher.as_ref() {
            rt.block_on(pub_.publish(&update))?;
        }
        if let Some(conn) = redis_conn.as_mut() {
            rt.block_on(save_to_redis(conn, &cli.redis_prefix, creator))?;
        }
        Ok(())
    };

    if let Some(dir) = cli.replay_dir {
        let events = load_replay(&dir)?;
        info!(count = events.len(), dir = %dir.display(), "replaying events");
        for ev in events {
            handle(ev, &mut creator)?;
        }
    } else {
        let token_path = cli.token_file.ok_or_else(|| {
            anyhow::anyhow!(
                "live mode requires --token-file (or pass --replay-dir for offline mode)"
            )
        })?;
        let token_source: Arc<dyn TokenSource> =
            Arc::new(FileTokenSource::new(token_path));
        let _ = token_source.current_token()?;

        let topics: Vec<String> = if cli.topics.is_empty() {
            DEFAULT_PIPELINE_TOPICS
                .iter()
                .map(|s| s.to_string())
                .collect()
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
        consumer.run(|result| match result {
            Ok(event) => {
                count += 1;
                if let Err(e) = handle(event, &mut creator) {
                    error!("handler error: {e}");
                }
                if max > 0 && count >= max {
                    HandlerControl::Stop
                } else {
                    HandlerControl::Continue
                }
            }
            Err(e) => {
                error!("decode error, skipping: {e}");
                HandlerControl::Continue
            }
        })?;
    }

    print_final_summary(&creator);
    Ok(())
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
        SupereventUpdate::Skipped { superevent_id, reason, .. } => {
            println!(
                "SKIP     {se:<8}                              graceid={gid:<10} pipeline={pipe:<8} snr={snr:6.2} reason={reason:?}",
                se = superevent_id,
                gid = event.graceid,
                pipe = event.pipeline,
                snr = event.snr,
                reason = reason,
            );
        }
    }
}

fn write_update_jsonl(
    w: &mut impl std::io::Write,
    event: &GwEvent,
    update: &SupereventUpdate,
) -> anyhow::Result<()> {
    let (action, superevent_id, t_0): (&str, &str, f64) = match update {
        SupereventUpdate::Created { superevent } => ("create", superevent.id.as_str(), superevent.t_0),
        SupereventUpdate::PreferredUpdated { superevent, .. } => {
            ("prefer", superevent.id.as_str(), superevent.t_0)
        }
        SupereventUpdate::Skipped { superevent_id, reason: SkipReason::LowerSnr, .. } => {
            ("skip_lower_snr", superevent_id.as_str(), 0.0)
        }
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
    superevents.sort_by(|a, b| a.t_0.partial_cmp(&b.t_0).unwrap_or(std::cmp::Ordering::Equal));
    for s in superevents {
        let g_ids: Vec<&str> = s.g_events.iter().map(|e| e.graceid.as_str()).collect();
        println!(
            "{se:<8} t0={t0:>16.6}  window=[{ts:>16.6}, {te:>16.6}]  preferred={pref:<10} (snr={snr:.2})  events={n}: {ids:?}",
            se = s.id,
            t0 = s.t_0,
            ts = s.t_start,
            te = s.t_end,
            pref = s.preferred_event.graceid,
            snr = s.preferred_event.snr,
            n = s.g_events.len(),
            ids = g_ids,
        );
    }
}

/// Load events from a directory of `gw_dump` envelope JSON files. Sorted by
/// `_producer_timestamp` to mimic the order events would have arrived on the
/// wire.
fn load_replay(dir: &std::path::Path) -> anyhow::Result<Vec<GwEvent>> {
    let mut events: Vec<(f64, GwEvent)> = Vec::new();
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
        match extract_gw_event(&envelope) {
            Ok(event) => events.push((event.producer_timestamp, event)),
            Err(e) => error!("skip {}: extract failed: {e}", path.display()),
        }
    }
    events.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    // Deduplicate by graceid in case the dump captured the same event twice
    // (only the first occurrence is kept, matching the production-replay
    // semantics where the first emission wins).
    let mut seen: HashMap<String, ()> = HashMap::new();
    let deduped: Vec<GwEvent> = events
        .into_iter()
        .filter(|(_, e)| seen.insert(e.graceid.clone(), ()).is_none())
        .map(|(_, e)| e)
        .collect();
    Ok(deduped)
}

fn init_logging() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,boom=info,rdkafka=warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
