//! One-shot backfill: for every GRB trigger with a usable
//! localization, synthesize the canonical MOC FITS and persist
//! it to `SkymapStorage`. Same purpose as `backfill_contours`
//! but for the GRB side — meant to be run once after the
//! canonicalize-at-ingest code lands so that triggers ingested
//! before then get their MOCs retroactively, otherwise the
//! frontend Aladin overlay quietly skips them.

use std::sync::Arc;

use boom_gw::archive::GrbTriggerDoc;
use boom_gw::grb::build_canonical_moc_fits;
use boom_gw::storage::skymap::{build_storage, S3Config, SkymapBackendKind, SkymapCacheConfig};
use clap::Parser;
use futures::stream::StreamExt;
use mongodb::bson::doc;
use mongodb::Client;

#[derive(Parser)]
struct Cli {
    #[arg(long, default_value = "mongodb://localhost:27017")]
    mongo_uri: String,
    #[arg(long, default_value = "boom_gw")]
    mongo_db: String,
    #[arg(long, default_value = "mongo")]
    skymap_storage: SkymapBackendKind,
    #[arg(long)]
    s3_bucket: Option<String>,
    #[arg(long, default_value = "boom-gw")]
    s3_key_prefix: String,
    #[arg(long, default_value = "us-east-1")]
    s3_region: String,
    #[arg(long)]
    s3_access_key: Option<String>,
    #[arg(long)]
    s3_secret_key: Option<String>,
    #[arg(long)]
    s3_endpoint_url: Option<String>,
    /// Re-upsert even when a MOC already exists for the trigger.
    /// Use this after a synthesis-logic change to refresh every
    /// stored MOC.
    #[arg(long, default_value_t = false)]
    force: bool,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let client = Client::with_uri_str(&cli.mongo_uri).await?;
    let db = client.database(&cli.mongo_db);

    let s3 = if matches!(cli.skymap_storage, SkymapBackendKind::S3) {
        Some(S3Config {
            bucket: cli
                .s3_bucket
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--s3-bucket required for s3 backend"))?,
            key_prefix: cli.s3_key_prefix.clone(),
            region: cli.s3_region.clone(),
            access_key: cli
                .s3_access_key
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--s3-access-key required"))?,
            secret_key: cli
                .s3_secret_key
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--s3-secret-key required"))?,
            endpoint_url: cli.s3_endpoint_url.clone(),
            compress: true,
            cache: None::<SkymapCacheConfig>,
        })
    } else {
        None
    };
    let storage = Arc::new(build_storage(cli.skymap_storage, &db, s3).await?);

    let triggers = db.collection::<GrbTriggerDoc>("grb_triggers");
    // Iterate every trigger; the synthesis function does its own
    // "is there enough data to make a cone?" check.
    let mut cursor = triggers.find(doc! {}).await?;
    let (mut done, mut skipped, mut failed) = (0usize, 0usize, 0usize);
    while let Some(td) = cursor.next().await {
        let td = td?;
        let instrument = &td.trigger.instrument;
        let trigger_id = &td.trigger.trigger_id;
        if !cli.force {
            if storage.get_grb_skymap(instrument, trigger_id).await.is_ok() {
                skipped += 1;
                continue;
            }
        }
        match build_canonical_moc_fits(&td.trigger) {
            Ok(bytes) => {
                if let Err(e) = storage
                    .upsert_grb_skymap(instrument, trigger_id, bytes)
                    .await
                {
                    eprintln!("{instrument}/{trigger_id}: upsert failed: {e}");
                    failed += 1;
                    continue;
                }
                println!("{instrument}/{trigger_id}: stored canonical MOC");
                done += 1;
            }
            Err(e) => {
                // Pre-localization or otherwise unable to
                // synthesize. Common for early Fermi notices; not
                // an error.
                eprintln!("{instrument}/{trigger_id}: skipped — {e}");
                skipped += 1;
            }
        }
    }
    eprintln!("done: wrote={done} skipped={skipped} failed={failed}");
    Ok(())
}
