//! One-shot backfill: for each existing sky map in storage, compute
//! the 50% and 90% credible-region contour MOCs and write them
//! back alongside. Use this once after deploying the contour
//! pipeline so superevents created before the change get their
//! contours retroactively; from then on `gw-clusterer` writes
//! contours at attach time and this binary is unnecessary.
//!
//! Usage:
//!   cargo run --example backfill_contours -- \
//!     --skymap-storage s3 --s3-bucket boom-gw-skymaps \
//!     --s3-endpoint-url http://127.0.0.1:9000 \
//!     --s3-access-key minioadmin --s3-secret-key minioadmin \
//!     --mongo-uri 'mongodb://mongoadmin:devpassword@localhost:27017/admin?authSource=admin'

use std::sync::Arc;

use boom_gw::contour::compute_contour_moc;
use boom_gw::storage::skymap::{
    build_storage, S3Config, SkymapBackendKind, SkymapCacheConfig, SKYMAPS_COLLECTION,
};
use clap::Parser;
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
    /// Credible levels to backfill, as integer percents.
    #[arg(long, value_delimiter = ',', default_values_t = [50u8, 90u8])]
    levels: Vec<u8>,
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

    // Iterate superevents that have a skymap_summary; the storage
    // get() will fetch the FITS bytes.
    let superevents = db.collection::<mongodb::bson::Document>("superevents");
    let mut cursor = superevents
        .find(doc! {"skymap_summary": {"$exists": true, "$ne": null}})
        .await?;
    let mut n_done = 0usize;
    let mut n_fail = 0usize;
    use futures::stream::StreamExt;
    while let Some(doc) = cursor.next().await {
        let doc = doc?;
        let Some(id) = doc.get_str("_id").ok() else {
            continue;
        };
        let blob = match storage.get(id).await {
            Ok(b) => b,
            Err(e) => {
                eprintln!("{id}: get skymap failed: {e}");
                n_fail += 1;
                continue;
            }
        };
        for &level_pct in &cli.levels {
            let level = level_pct as f64 / 100.0;
            match compute_contour_moc(&blob.bytes, level) {
                Ok(bytes) => {
                    if let Err(e) = storage.upsert_contour(id, level_pct, bytes).await {
                        eprintln!("{id} @{level_pct}%: upsert failed: {e}");
                        n_fail += 1;
                        continue;
                    }
                    println!("{id} @{level_pct}%: wrote contour MOC");
                }
                Err(e) => {
                    eprintln!("{id} @{level_pct}%: compute failed: {e}");
                    n_fail += 1;
                }
            }
        }
        n_done += 1;
    }
    eprintln!(
        "done: {n_done} superevents processed ({n_fail} failures), collection={SKYMAPS_COLLECTION}"
    );
    Ok(())
}
