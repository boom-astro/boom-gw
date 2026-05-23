//! `gw-dump` — minimal one-shot dumper.
//!
//! Connects to the GraceDB Kafka with the same SCITokens auth as `gw_consumer`,
//! takes the first N messages, and writes both the JSON envelope and the
//! decoded coinc.xml payload to disk so we can inspect the on-the-wire shape
//! when the structured parser fails.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;

use boom_gw::kafka::{pipeline_topics_for_instance, DEFAULT_GRACEDB_INSTANCE};
use boom_gw::{
    decode_event_file, EventEnvelope, FileTokenSource, GwKafkaConfig, ScitokensContext, TokenSource,
};

#[derive(Parser, Debug)]
struct Cli {
    #[arg(long, default_value = "kafka-dev.ligo.org:9092")]
    bootstrap_servers: String,
    #[arg(long, value_delimiter = ',')]
    topics: Vec<String>,
    /// GraceDB instance namespace for default topic composition.
    #[arg(long, default_value = DEFAULT_GRACEDB_INSTANCE)]
    gracedb_instance: String,
    #[arg(long, default_value = "boom-gw-dump")]
    group_id: String,
    #[arg(long)]
    token_file: PathBuf,
    #[arg(long, default_value = "/tmp/gw-payloads")]
    out_dir: PathBuf,
    #[arg(long, default_value_t = 3)]
    max_messages: u64,
    #[arg(long, default_value = "earliest")]
    auto_offset_reset: String,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    fs::create_dir_all(&cli.out_dir)?;
    let topics: Vec<String> = if cli.topics.is_empty() {
        pipeline_topics_for_instance(&cli.gracedb_instance)
    } else {
        cli.topics.clone()
    };

    let token_source: Arc<dyn TokenSource> = Arc::new(FileTokenSource::new(cli.token_file));
    let _ = token_source.current_token()?;

    let config = GwKafkaConfig {
        bootstrap_servers: cli.bootstrap_servers,
        topics: topics.clone(),
        group_id: cli.group_id,
        use_tls: true,
        ca_cert_path: None,
        auto_offset_reset: cli.auto_offset_reset,
        poll_timeout: Duration::from_secs(2),
    };
    let context = ScitokensContext::new(token_source.clone());
    let consumer_cfg = boom_gw::GwAlertConsumer::new(config, token_source).client_config();
    let consumer: BaseConsumer<ScitokensContext> = consumer_cfg.create_with_context(context)?;
    let topic_refs: Vec<&str> = topics.iter().map(String::as_str).collect();
    consumer.subscribe(&topic_refs)?;

    let mut count: u64 = 0;
    while count < cli.max_messages {
        match consumer.poll(Duration::from_secs(2)) {
            Some(Ok(msg)) => {
                let Some(payload) = msg.payload() else {
                    continue;
                };
                let envelope_path = cli.out_dir.join(format!("msg_{count:04}.json"));
                let xml_path = cli.out_dir.join(format!("msg_{count:04}.xml"));
                fs::write(&envelope_path, payload)?;

                match EventEnvelope::from_json(payload) {
                    Ok(env) => match decode_event_file(&env.event_file) {
                        Ok(xml) => {
                            fs::write(&xml_path, &xml)?;
                            println!(
                                "msg {count} pipeline={} graceid={} envelope={} xml={} ({} bytes)",
                                env.pipeline,
                                env.graceid,
                                envelope_path.display(),
                                xml_path.display(),
                                xml.len()
                            );
                        }
                        Err(e) => println!("msg {count}: base64 decode failed: {e}"),
                    },
                    Err(e) => println!("msg {count}: envelope parse failed: {e}"),
                }
                count += 1;
            }
            Some(Err(e)) => eprintln!("kafka error: {e}"),
            None => {}
        }
    }
    Ok(())
}
