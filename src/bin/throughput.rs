//! `throughput` — boom-gw alert-ingest microbench. Inspired by
//! `boom`'s `tests/throughput/run.py`: spin up the stack, push N
//! synthetic alerts, measure wall time + alerts/sec.
//!
//! We benchmark four paths against a clean per-run mongo database:
//!
//!   * **HTTP — FRB**: `POST /api/frb-alerts` (loader path).
//!   * **HTTP — superevent**: `POST /api/superevents` (also exercises
//!     skymap upsert + contour derivation).
//!   * **Direct — FRB**: `crate::ingest::ingest_frb_alert` called in
//!     process (consumer path, after refactor #1).
//!   * **Scan**: `POST /api/superevents/{id}/scan-cross-matches` with
//!     pre-seeded triggers (exercises refactors #2 + #3).
//!
//! Usage:
//!     # gw-api must be running in dev mode against the same mongo
//!     # this binary points at (the dev defaults match).
//!     cargo run --release --bin throughput -- --n 1000
//!
//! Prints one line per scenario: `path | n=N | wall=…s | alerts/s`.

use std::time::Instant;

use clap::Parser;
use serde_json::json;

use boom_gw::archive::{FrbAlertDoc, GrbTriggerDoc};
use boom_gw::clustering::{SkyMapFits, Superevent};
use boom_gw::event::GwEvent;
use boom_gw::frb::{FrbAlert, CHIME_INSTRUMENT_LABEL};
use boom_gw::grb::{GrbTrigger, SkyPosition};
use boom_gw::storage::skymap::{build_storage, S3Config, SkymapBackendKind};
use boom_gw::{Archive, ArchiveConfig};
use igwn_ligolw::CoincInspiralEvent;

#[derive(Parser, Debug)]
#[command(name = "throughput", about = "boom-gw alert-ingest throughput bench")]
struct Cli {
    /// Number of alerts per scenario.
    #[arg(long, default_value_t = 500)]
    n: usize,
    /// Run scenarios this many times and report the best run.
    #[arg(long, default_value_t = 3)]
    repeats: usize,
    /// Mongo URI for the throughput-scoped database.
    #[arg(
        long,
        env = "BOOM_GW_MONGO_URI",
        default_value = "mongodb://mongoadmin:devpassword@localhost:27017/admin?authSource=admin"
    )]
    mongo_uri: String,
    /// API base URL (must be running in --auth-dev-mode).
    #[arg(long, env = "BOOM_GW_API_URL", default_value = "http://127.0.0.1:8080")]
    api_url: String,
    /// Dev JWT — same one `load_demo_data` ships, valid only when
    /// gw-api runs with `--auth-dev-mode`.
    #[arg(long, default_value = DEFAULT_DEV_TOKEN)]
    api_token: String,
    /// MinIO endpoint for the local S3 backend.
    #[arg(long, default_value = "http://127.0.0.1:9000")]
    s3_endpoint: String,
}

const DEFAULT_DEV_TOKEN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImRldiJ9.eyJpc3MiOiJodHRwczovL2NpbG9nb24ub3JnL2lnd24iLCJzdWIiOiJ0aHJvdWdocHV0LWJlbmNoIiwiYXVkIjoiQU5ZIiwic2NvcGUiOiJncmFjZWRiLnJlYWQgZ3JhY2VkYi53cml0ZSIsImV4cCI6NDAwMDAwMDAwMCwiaWF0IjoxNzMwMDAwMDAwfQ.YmVuY2g";

struct Scenario {
    label: &'static str,
    n: usize,
    wall_secs: f64,
}

impl Scenario {
    fn print(&self) {
        let per_sec = self.n as f64 / self.wall_secs;
        println!(
            "  {:<28} n={:<5} wall={:>7.3}s   {:>10.1} alerts/s",
            self.label, self.n, self.wall_secs, per_sec
        );
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Per-run database — keeps the bench from contending with
    // demo data and makes each invocation hermetic.
    let bench_db = format!("boom_gw_perf_{}", uuid::Uuid::new_v4().simple());
    let mut cfg = ArchiveConfig::new(&cli.mongo_uri);
    cfg.database = bench_db.clone();
    cfg.app_name = Some("throughput-bench".into());
    let archive = Archive::connect(cfg).await?;
    let storage = std::sync::Arc::new(
        build_storage(
            SkymapBackendKind::S3,
            archive.database(),
            Some(S3Config {
                bucket: "boom-gw-skymaps".into(),
                key_prefix: format!("perf/{bench_db}"),
                region: "us-east-1".into(),
                access_key: "boomgw".into(),
                secret_key: "boomgwsecret".into(),
                endpoint_url: Some(cli.s3_endpoint.clone()),
                compress: true,
                cache: None,
            }),
        )
        .await?,
    );

    let http = reqwest::Client::builder()
        .default_headers({
            let mut h = reqwest::header::HeaderMap::new();
            h.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&format!("Bearer {}", cli.api_token))?,
            );
            h
        })
        .build()?;
    // Sanity-check the API is up before we time anything.
    let r = http
        .get(format!("{}/api/health", cli.api_url.trim_end_matches('/')))
        .send()
        .await?;
    anyhow::ensure!(
        r.status().is_success(),
        "gw-api at {} not responding: {}",
        cli.api_url,
        r.status()
    );

    println!("boom-gw throughput bench");
    println!("  mongo db: {bench_db}");
    println!("  n per scenario: {}", cli.n);
    println!("  repeats: {} (best run reported)", cli.repeats);
    println!();
    let mut results: Vec<Scenario> = Vec::new();

    // ---- Scenario A: FRB ingest via HTTP POST ----
    let mut best = f64::INFINITY;
    for _ in 0..cli.repeats {
        wipe_frbs(&archive).await?;
        let t = Instant::now();
        for i in 0..cli.n {
            let frb = make_frb(i);
            http_post(&http, &cli.api_url, "/api/frb-alerts", &frb).await?;
        }
        best = best.min(t.elapsed().as_secs_f64());
    }
    results.push(Scenario {
        label: "HTTP /api/frb-alerts",
        n: cli.n,
        wall_secs: best,
    });

    // ---- Scenario B: FRB ingest via direct lib call ----
    // Same path the Kafka consumer takes after refactor #1.
    let mut best = f64::INFINITY;
    for _ in 0..cli.repeats {
        wipe_frbs(&archive).await?;
        let t = Instant::now();
        for i in 0..cli.n {
            let frb = make_frb(i);
            boom_gw::ingest::ingest_frb_alert(&archive, Some(storage.as_ref()), frb).await?;
        }
        best = best.min(t.elapsed().as_secs_f64());
    }
    results.push(Scenario {
        label: "direct ingest_frb_alert",
        n: cli.n,
        wall_secs: best,
    });

    // ---- Scenario C: superevent ingest via HTTP POST ----
    // Each POST persists the GW event, the superevent doc, the
    // skymap FITS, and derives 50%/90% contours — strictly heavier
    // than an alert POST, so we run fewer iterations.
    let n_super = (cli.n / 10).max(10);
    let mut best = f64::INFINITY;
    for _ in 0..cli.repeats {
        wipe_supereventy(&archive).await?;
        let t = Instant::now();
        for i in 0..n_super {
            let s = make_superevent(i);
            http_post(&http, &cli.api_url, "/api/superevents", &s).await?;
        }
        best = best.min(t.elapsed().as_secs_f64());
    }
    results.push(Scenario {
        label: "HTTP /api/superevents",
        n: n_super,
        wall_secs: best,
    });

    // ---- Scenario D: scan-cross-matches against a seeded superevent ----
    // First we pre-seed one superevent + N GRB triggers all inside
    // its ±60s window. Then we time only the scan POST (it computes
    // spatial overlap + RAVEN joint FAR for each candidate, then
    // upserts the results). p_value_trials=0 keeps the bench
    // focused on the per-candidate cross-match cost rather than
    // the MC, which is already separately tuneable.
    wipe_supereventy(&archive).await?;
    wipe_grbs(&archive).await?;
    let scan_superevent = make_superevent(0);
    let scan_id = scan_superevent.id.clone();
    http_post(&http, &cli.api_url, "/api/superevents", &scan_superevent).await?;
    let scan_t0 = scan_superevent.t_0;
    let grb_count = cli.n.min(200);
    for i in 0..grb_count {
        let trigger = make_grb_at(i, scan_t0, scan_superevent.preferred_event.coinc.snr);
        http_post(&http, &cli.api_url, "/api/grb-triggers", &trigger).await?;
    }
    let mut best = f64::INFINITY;
    for _ in 0..cli.repeats {
        let t = Instant::now();
        let url = format!(
            "{}/api/superevents/{}/scan-cross-matches",
            cli.api_url.trim_end_matches('/'),
            scan_id
        );
        let r = http
            .post(&url)
            .json(&json!({"time_window_sec": 120, "p_value_trials": 0}))
            .send()
            .await?;
        anyhow::ensure!(r.status().is_success(), "scan failed: {}", r.status());
        best = best.min(t.elapsed().as_secs_f64());
    }
    results.push(Scenario {
        label: "scan-cross-matches",
        n: grb_count,
        wall_secs: best,
    });

    println!();
    println!("Results (best of {} runs):", cli.repeats);
    for s in &results {
        s.print();
    }

    // Drop the bench database so we don't leave debris around.
    archive.database().drop().await?;
    println!();
    println!("Dropped bench db {bench_db}");
    Ok(())
}

async fn wipe_frbs(archive: &Archive) -> anyhow::Result<()> {
    archive
        .frb_alerts()
        .delete_many(mongodb::bson::doc! {})
        .await?;
    Ok(())
}

async fn wipe_grbs(archive: &Archive) -> anyhow::Result<()> {
    use boom_gw::archive::CROSS_MATCHES_COLLECTION;
    archive
        .grb_triggers()
        .delete_many(mongodb::bson::doc! {})
        .await?;
    archive
        .database()
        .collection::<mongodb::bson::Document>(CROSS_MATCHES_COLLECTION)
        .delete_many(mongodb::bson::doc! {})
        .await?;
    Ok(())
}

async fn wipe_supereventy(archive: &Archive) -> anyhow::Result<()> {
    archive
        .superevents()
        .delete_many(mongodb::bson::doc! {})
        .await?;
    archive.events().delete_many(mongodb::bson::doc! {}).await?;
    Ok(())
}

async fn http_post<T: serde::Serialize + ?Sized>(
    http: &reqwest::Client,
    base: &str,
    route: &str,
    body: &T,
) -> anyhow::Result<()> {
    let r = http
        .post(format!("{}{}", base.trim_end_matches('/'), route))
        .json(body)
        .send()
        .await?;
    let status = r.status();
    if !status.is_success() {
        let text = r.text().await.unwrap_or_default();
        anyhow::bail!("POST {route} → {status}: {text}");
    }
    let _ = r.bytes().await; // drain
    Ok(())
}

fn make_frb(i: usize) -> FrbAlert {
    let ra = (i as f64 * 37.0) % 360.0;
    let dec = ((i as f64 * 13.0) % 60.0) - 30.0;
    FrbAlert {
        trigger: GrbTrigger {
            trigger_id: format!("perf-frb-{i}"),
            instrument: CHIME_INSTRUMENT_LABEL.into(),
            trigger_time: 1_400_000_000.0 + i as f64,
            position: Some(SkyPosition::new(ra, dec, 36.0)),
            significance: 10.0,
            skymap_url: None,
            error_radius_deg: Some(0.01),
        },
        dm: Some(300.0),
        dm_error: Some(0.4),
        importance: Some(0.9),
        snr: Some(10.0),
        known_source: None,
        body: serde_json::json!({}),
    }
}

fn make_grb_at(i: usize, scan_t0: f64, _snr: f64) -> GrbTrigger {
    // Spread the triggers across a ±60s window centered on the
    // scan superevent's t_0 so the time-window filter accepts them
    // all.
    let dt = ((i as f64) * 0.6) - 60.0;
    let ra = (i as f64 * 7.0) % 360.0;
    let dec = ((i as f64 * 5.0) % 60.0) - 30.0;
    GrbTrigger {
        trigger_id: format!("perf-grb-{i}"),
        instrument: "Fermi-GBM-FIN".into(),
        trigger_time: scan_t0 + dt,
        position: Some(SkyPosition::new(ra, dec, 2.0 * 3600.0)),
        significance: 7.5,
        skymap_url: None,
        error_radius_deg: Some(2.0),
    }
}

fn make_superevent(i: usize) -> Superevent {
    let t0 = 1_400_000_000.0 + i as f64 * 1000.0;
    let graceid = format!("Gperf{i:04}");
    let event = GwEvent {
        pipeline: "gstlal".into(),
        graceid: graceid.clone(),
        producer_timestamp: t0,
        message_type: "new".into(),
        submitter: "throughput".into(),
        end_time: t0,
        ifos: "H1,L1".into(),
        snr: 12.0,
        far: 1e-10,
        mchirp: None,
        total_mass: None,
        coinc: CoincInspiralEvent {
            coinc_event_id: graceid.clone(),
            ifos: "H1,L1".into(),
            combined_far: 1e-10,
            snr: 12.0,
            mass: None,
            mchirp: None,
            end_time: t0,
            sngls: vec![],
        },
    };
    Superevent {
        id: format!("Sperf{i:04}"),
        t_0: t0,
        t_start: t0 - 2.5,
        t_end: t0 + 2.5,
        preferred_event: event.clone(),
        g_events: vec![event],
        skymap: Some(SkyMapFits {
            bytes: synthetic_skymap_fits((i as f64 * 9.0) % 360.0, 0.0, 5.0),
            elapsed_ms: 1000,
        }),
    }
}

/// Minimal IVOA multi-order skymap FITS — same shape the demo
/// loader writes. Kept inline so the bench binary doesn't depend
/// on loader internals.
fn synthetic_skymap_fits(ra_deg: f64, dec_deg: f64, radius_deg: f64) -> Vec<u8> {
    use moc::moc::range::{CellSelection, RangeMOC};
    use moc::qty::Hpx;
    const DEPTH: u8 = 6;
    let cone: RangeMOC<u64, Hpx<u64>> = RangeMOC::from_cone(
        ra_deg.to_radians(),
        dec_deg.to_radians(),
        radius_deg.to_radians(),
        DEPTH,
        2,
        CellSelection::All,
    );
    let pix: Vec<u64> = cone.flatten_to_fixed_depth_cells().collect();
    let n = pix.len() as f64;
    let cell_area = 4.0 * std::f64::consts::PI / (12.0 * (1u64 << (2 * DEPTH)) as f64);
    let density = 1.0 / (n * cell_area);
    let uniq_base: u64 = 4u64 << (2 * DEPTH);

    fn card(bytes: &[u8]) -> [u8; 80] {
        let mut c = [b' '; 80];
        let n = bytes.len().min(80);
        c[..n].copy_from_slice(&bytes[..n]);
        c
    }
    fn s(k: &str, v: &str) -> [u8; 80] {
        card(format!("{k:<8}= {v:<70}").as_bytes())
    }
    fn i(k: &str, v: i64) -> [u8; 80] {
        card(format!("{k:<8}= {v:>20}{:<50}", "").as_bytes())
    }
    fn b(k: &str, v: bool) -> [u8; 80] {
        card(format!("{k:<8}= {:>20}{:<50}", if v { "T" } else { "F" }, "").as_bytes())
    }
    let end = card(b"END");
    let mut out = Vec::new();
    out.extend_from_slice(&b("SIMPLE", true));
    out.extend_from_slice(&i("BITPIX", 8));
    out.extend_from_slice(&i("NAXIS", 0));
    out.extend_from_slice(&b("EXTEND", true));
    out.extend_from_slice(&end);
    while out.len() % 2880 != 0 {
        out.push(b' ');
    }
    out.extend_from_slice(&s("XTENSION", "'BINTABLE'"));
    out.extend_from_slice(&i("BITPIX", 8));
    out.extend_from_slice(&i("NAXIS", 2));
    out.extend_from_slice(&i("NAXIS1", 16));
    out.extend_from_slice(&i("NAXIS2", pix.len() as i64));
    out.extend_from_slice(&i("PCOUNT", 0));
    out.extend_from_slice(&i("GCOUNT", 1));
    out.extend_from_slice(&i("TFIELDS", 2));
    out.extend_from_slice(&s("TTYPE1", "'UNIQ    '"));
    out.extend_from_slice(&s("TFORM1", "'K       '"));
    out.extend_from_slice(&s("TTYPE2", "'PROBDENSITY'"));
    out.extend_from_slice(&s("TFORM2", "'D       '"));
    out.extend_from_slice(&s("TUNIT2", "'sr-1    '"));
    out.extend_from_slice(&s("PIXTYPE", "'HEALPIX '"));
    out.extend_from_slice(&s("ORDERING", "'NUNIQ   '"));
    out.extend_from_slice(&s("COORDSYS", "'C       '"));
    out.extend_from_slice(&i("MOCORDER", DEPTH as i64));
    out.extend_from_slice(&s("INDXSCHM", "'EXPLICIT'"));
    out.extend_from_slice(&end);
    while out.len() % 2880 != 0 {
        out.push(b' ');
    }
    for p in &pix {
        out.extend_from_slice(&((p + uniq_base) as i64).to_be_bytes());
        out.extend_from_slice(&density.to_be_bytes());
    }
    while out.len() % 2880 != 0 {
        out.push(0);
    }
    out
}

#[allow(dead_code)]
fn touch_unused() {
    // Keep these in the binary's namespace so a future test can
    // assert on docs returned by the bench paths.
    let _: Option<FrbAlertDoc> = None;
    let _: Option<GrbTriggerDoc> = None;
}
