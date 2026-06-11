//! `load_demo_data` — wipe the boom-gw archive and reseed it with a
//! comprehensive curated dataset.
//!
//! The shape mirrors what SkyPortal's `make load_demo_data` provides:
//! a one-shot binary that gives a developer enough realistic data to
//! exercise every UI path (multiple pipelines, multiple
//! significances, with-and-without skymap, retracted, external GRB
//! matches, BOOM optical-transient brackets, cross-matches,
//! annotations, alerts) without needing live Kafka feeds.
//!
//! Behavior:
//!   * Default: wipe the relevant collections, then upsert the seed
//!     set. Idempotent — run it twice, get the same shape.
//!   * `--wipe-only` clears the collections and exits. Used by
//!     `make db_clear`.
//!   * `--no-wipe` skips the clear step and just upserts the seed
//!     set; useful for additive demos.
//!
//! Skymap handling: this binary requires the same `--skymap-storage`
//! / `--s3-*` flags as `gw_api` and writes synthetic cone MOC FITS
//! files for the high-SNR and mid-SNR superevents. The MOC bytes are
//! generated with the `moc` crate (already a dep) using
//! `RangeMOC::from_cone`, so they are real IVOA MOC FITS and the
//! frontend's Aladin overlay accepts them.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Parser;
use igwn_ligolw::{CoincInspiralEvent, SnglInspiral};
use mongodb::bson::{doc, Bson};
use serde_json::json;
use tracing::info;
use tracing_subscriber::EnvFilter;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use boom_gw::archive::{
    AlertDoc, AnnotationDoc, CrossMatchDoc, ALERTS_COLLECTION, ANNOTATIONS_COLLECTION,
    BOOM_ALERTS_COLLECTION, CROSS_MATCHES_COLLECTION, EVENTS_COLLECTION, FRB_ALERTS_COLLECTION,
    GRB_TRIGGERS_COLLECTION, GROUPS_COLLECTION, GROUP_STREAMS_COLLECTION, GROUP_USERS_COLLECTION,
    ICECUBE_LVK_SEARCHES_COLLECTION, LOCALIZE_REQUESTS_COLLECTION, LOCALIZE_RESULTS_COLLECTION,
    NEUTRINO_ALERTS_COLLECTION, SCIENCE_FILTERS_COLLECTION, STREAM_USERS_COLLECTION,
    SUPEREVENTS_COLLECTION, USERS_COLLECTION,
};
use boom_gw::boom::{BoomPhotometry, BoomTransient};
use boom_gw::clustering::{SkyMapFits, Superevent};
use boom_gw::event::GwEvent;
use boom_gw::frb::{FrbAlert, CHIME_INSTRUMENT_LABEL, DSA110_INSTRUMENT_LABEL};
use boom_gw::grb::{CrossMatchResult, GrbTrigger, SkyPosition};
use boom_gw::icecube_lvk::{CoincidentTrackEvent, IceCubeLvkSearch};
use boom_gw::localizer::{LocalizeRequest, LocalizeResult, LocalizeStatus};
use boom_gw::neutrino::{NeutrinoAlert, ICECUBE_INSTRUMENT_LABEL, KM3NET_INSTRUMENT_LABEL};
use boom_gw::storage::skymap::{
    SkymapBackendKind, GRB_SKYMAPS_COLLECTION, SKYMAPS_COLLECTION, SKYMAP_CONTOURS_COLLECTION,
};
use boom_gw::{Archive, ArchiveConfig, DEFAULT_DB_NAME};

/// Cone MOC depth used for the synthetic skymaps. Matches
/// `grb::GRB_CONE_DEPTH` so set ops between a GW skymap and a GRB
/// cone are exact (no upsampling penalty).
const DEMO_MOC_DEPTH: u8 = 8;

/// Build a synthetic IVOA multi-order skymap FITS with uniform
/// PROBDENSITY over a cone. Mirrors what BAYESTAR emits (a
/// BINTABLE with UNIQ + PROBDENSITY columns) so the live scan +
/// contour paths read it identically to a real LVK skymap. Plain
/// `RangeMOC::from_cone(...).to_fits_ivoa()` produces a coverage
/// MOC with no probability column, which trips the scan's
/// `from_fits_multiordermap` reader on `UnexpectedKeyword("TTYPE1",
/// "MOCVER")` — that's why we hand-write the FITS here.
fn build_synthetic_prob_density_skymap(ra_deg: f64, dec_deg: f64, radius_deg: f64) -> Vec<u8> {
    use moc::moc::range::{CellSelection, RangeMOC};
    use moc::qty::Hpx;
    let depth = DEMO_MOC_DEPTH;
    let lon = ra_deg.to_radians();
    let lat = dec_deg.to_radians();
    let cone: RangeMOC<u64, Hpx<u64>> = RangeMOC::from_cone(
        lon,
        lat,
        radius_deg.to_radians(),
        depth,
        2,
        CellSelection::All,
    );
    let pix_indices: Vec<u64> = cone.flatten_to_fixed_depth_cells().collect();
    let n_cells = pix_indices.len() as f64;

    // HEALPix cell area at depth d: 4π / (12 · 4^d) sr. Uniform
    // density = 1 / (n_cells · cell_area_sr) so the total
    // probability mass integrates to 1 over the cone.
    let nside_sq = (1u64 << (2 * depth)) as f64;
    let cell_area_sr = 4.0 * std::f64::consts::PI / (12.0 * nside_sq);
    let density = 1.0 / (n_cells * cell_area_sr);

    // NUNIQ encoding: UNIQ = ipix + 4·4^depth.
    let uniq_base: u64 = 4u64 << (2 * depth);
    let nuniqs: Vec<u64> = pix_indices.iter().map(|&p| p + uniq_base).collect();

    write_ivoa_multiordermap_fits(&nuniqs, density, depth)
}

/// Hand-roll the FITS bytes for a HEALPix NUNIQ probability density
/// map. Cards are 80 bytes; headers + data are each padded to a
/// 2880-byte block. UNIQ is i64 big-endian, PROBDENSITY is f64
/// big-endian — FITS spec is big-endian regardless of host.
fn write_ivoa_multiordermap_fits(nuniqs: &[u64], density: f64, max_depth: u8) -> Vec<u8> {
    fn fixed_card(bytes: &[u8]) -> [u8; 80] {
        let mut c = [b' '; 80];
        let n = bytes.len().min(80);
        c[..n].copy_from_slice(&bytes[..n]);
        c
    }
    fn str_card(key: &str, quoted_value: &str) -> [u8; 80] {
        // 8-char key + "= " + value padded to col 80.
        fixed_card(format!("{key:<8}= {quoted_value:<70}").as_bytes())
    }
    fn int_card(key: &str, value: i64) -> [u8; 80] {
        // Integer right-justified in cols 11..30, then trailing
        // spaces to col 80.
        fixed_card(format!("{key:<8}= {value:>20}{:<50}", "").as_bytes())
    }
    fn bool_card(key: &str, value: bool) -> [u8; 80] {
        let v = if value { "T" } else { "F" };
        fixed_card(format!("{key:<8}= {v:>20}{:<50}", "").as_bytes())
    }
    let end_card = fixed_card(b"END");

    fn pad_to_block(bytes: &mut Vec<u8>, unit: usize) {
        let rem = bytes.len() % 2880;
        if rem != 0 {
            bytes.extend(std::iter::repeat_n(unit as u8, 2880 - rem));
        }
    }

    let mut bytes = Vec::new();
    // Primary HDU — empty.
    bytes.extend_from_slice(&bool_card("SIMPLE", true));
    bytes.extend_from_slice(&int_card("BITPIX", 8));
    bytes.extend_from_slice(&int_card("NAXIS", 0));
    bytes.extend_from_slice(&bool_card("EXTEND", true));
    bytes.extend_from_slice(&end_card);
    pad_to_block(&mut bytes, b' ' as usize);

    // BINTABLE HDU header.
    bytes.extend_from_slice(&str_card("XTENSION", "'BINTABLE'"));
    bytes.extend_from_slice(&int_card("BITPIX", 8));
    bytes.extend_from_slice(&int_card("NAXIS", 2));
    bytes.extend_from_slice(&int_card("NAXIS1", 16)); // 8B UNIQ + 8B PROBDENSITY
    bytes.extend_from_slice(&int_card("NAXIS2", nuniqs.len() as i64));
    bytes.extend_from_slice(&int_card("PCOUNT", 0));
    bytes.extend_from_slice(&int_card("GCOUNT", 1));
    bytes.extend_from_slice(&int_card("TFIELDS", 2));
    bytes.extend_from_slice(&str_card("TTYPE1", "'UNIQ    '"));
    bytes.extend_from_slice(&str_card("TFORM1", "'K       '"));
    bytes.extend_from_slice(&str_card("TTYPE2", "'PROBDENSITY'"));
    bytes.extend_from_slice(&str_card("TFORM2", "'D       '"));
    bytes.extend_from_slice(&str_card("TUNIT2", "'sr-1    '"));
    bytes.extend_from_slice(&str_card("PIXTYPE", "'HEALPIX '"));
    bytes.extend_from_slice(&str_card("ORDERING", "'NUNIQ   '"));
    bytes.extend_from_slice(&str_card("COORDSYS", "'C       '"));
    bytes.extend_from_slice(&int_card("MOCORDER", max_depth as i64));
    bytes.extend_from_slice(&str_card("INDXSCHM", "'EXPLICIT'"));
    bytes.extend_from_slice(&end_card);
    pad_to_block(&mut bytes, b' ' as usize);

    // BINTABLE data — UNIQ + PROBDENSITY per row, big-endian.
    for &nuniq in nuniqs {
        bytes.extend_from_slice(&(nuniq as i64).to_be_bytes());
        bytes.extend_from_slice(&density.to_be_bytes());
    }
    pad_to_block(&mut bytes, 0);

    bytes
}

#[derive(Parser, Debug)]
#[command(
    name = "load-demo-data",
    about = "Wipe the boom-gw archive and reseed it with a comprehensive curated dataset."
)]
struct Cli {
    #[arg(
        long,
        env = "BOOM_GW_MONGO_URI",
        default_value = "mongodb://mongoadmin:devpassword@localhost:27017/admin?authSource=admin"
    )]
    mongo_uri: String,

    #[arg(long, env = "BOOM_GW_MONGO_DB", default_value = DEFAULT_DB_NAME)]
    mongo_db: String,

    /// Skymap storage backend (matches gw-api's flag). Defaults to
    /// `s3` because that's what the dev stack runs.
    #[arg(long, env = "BOOM_GW_SKYMAP_STORAGE", default_value = "s3")]
    skymap_storage: SkymapBackendKind,

    #[arg(long, env = "BOOM_GW_S3_BUCKET", default_value = "boom-gw-skymaps")]
    s3_bucket: String,
    #[arg(long, env = "BOOM_GW_S3_KEY_PREFIX", default_value = "boom-gw")]
    s3_key_prefix: String,
    #[arg(long, env = "BOOM_GW_S3_REGION", default_value = "us-east-1")]
    s3_region: String,
    #[arg(long, env = "BOOM_GW_S3_ACCESS_KEY", default_value = "boomgw")]
    s3_access_key: String,
    #[arg(long, env = "BOOM_GW_S3_SECRET_KEY", default_value = "boomgwsecret")]
    s3_secret_key: String,
    #[arg(
        long,
        env = "BOOM_GW_S3_ENDPOINT_URL",
        default_value = "http://127.0.0.1:9000"
    )]
    s3_endpoint_url: String,

    /// Wipe the demo-managed collections and exit without reseeding.
    /// Used by `make db_clear`.
    #[arg(long, default_value_t = false)]
    wipe_only: bool,

    /// Skip the wipe step and just upsert the seed set on top of
    /// whatever is already present. Useful for additive demos.
    #[arg(long, default_value_t = false)]
    no_wipe: bool,

    /// Base URL of the running gw-api. The loader POSTs each
    /// external alert (BOOM, FRB, neutrino, IceCube LVK Nu Track
    /// Search) through the same REST surface operators and live
    /// consumers use — that way the loader can't drift from the
    /// production ingest schema. GW superevents + skymaps + GRB
    /// triggers stay direct-mongo for now (no equivalent POST
    /// route, or the route exists but the loader needs to set
    /// fields the route doesn't expose).
    #[arg(long, env = "BOOM_GW_API_URL", default_value = "http://127.0.0.1:8080")]
    api_url: String,

    /// Bearer token sent with each POST. Defaults to an unsigned
    /// dev JWT that satisfies the issuer/audience/scope checks
    /// when gw-api runs with `--auth-dev-mode`. Override only if
    /// you're seeding against a fully-authenticated deployment.
    #[arg(long, env = "BOOM_GW_API_TOKEN", default_value = DEFAULT_DEV_TOKEN)]
    api_token: String,
}

/// Unsigned dev JWT — issuer `https://cilogon.org/igwn`, audience
/// `ANY`, scope `gracedb.read gracedb.write`, never expires (exp
/// far in the future). Only valid when gw-api runs with
/// `--auth-dev-mode`, which is exactly what `make run` does.
const DEFAULT_DEV_TOKEN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImRldiJ9.eyJpc3MiOiJodHRwczovL2NpbG9nb24ub3JnL2lnd24iLCJzdWIiOiJsb2FkLWRlbW8tZGF0YSIsImF1ZCI6IkFOWSIsInNjb3BlIjoiZ3JhY2VkYi5yZWFkIGdyYWNlZGIud3JpdGUiLCJleHAiOjQwMDAwMDAwMDAsImlhdCI6MTczMDAwMDAwMH0.bG9hZGVy";

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,boom_gw=info,load_demo_data=info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = Cli::parse();

    let mut archive_cfg = ArchiveConfig::new(&cli.mongo_uri);
    archive_cfg.database = cli.mongo_db.clone();
    archive_cfg.app_name = Some("load-demo-data".into());
    let archive = Archive::connect(archive_cfg).await?;
    info!(db = %cli.mongo_db, "connected to archive");

    if !cli.no_wipe {
        wipe(&archive).await?;
        info!("wiped demo-managed collections");
    }

    if cli.wipe_only {
        println!("load_demo_data: --wipe-only complete, no seed data inserted");
        return Ok(());
    }

    // Every ingest type — GW superevents (incl. their inline
    // skymap + g-events), GRBs, BOOM, FRB, neutrino, LVK search
    // — is POSTed through gw-api so the loader exercises the
    // same handlers operators and the live Kafka consumer use.
    // Fail loud if the API isn't reachable; silently falling back
    // to direct mongo writes would defeat the whole point.
    // Annotations + public alerts + pre-seeded cross-matches
    // still go direct-to-archive (no POST routes for those yet).
    let api = ApiClient::new(&cli.api_url, &cli.api_token)?;
    api.ping().await.map_err(|e| {
        anyhow::anyhow!(
            "load_demo_data needs gw-api running at {} (POSTs all ingest through the REST surface): {e}",
            cli.api_url,
        )
    })?;

    let summary = seed(&archive, &api).await?;
    print_summary(&summary);

    // Drop the archive handle before the tokio runtime tears down
    // so the mongodb driver's connection pool gets a chance to
    // close cleanly. Without this, the multi-thread runtime can
    // drop the channel out from under an in-flight monitor task
    // and panic with `SendError { .. }` — cosmetic but distracting.
    drop(api);
    drop(archive);
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(())
}

/// Thin wrapper around `reqwest::Client` that prefills the
/// Authorization header and the API base URL. Each helper POSTs
/// to one route and returns the parsed envelope on success.
struct ApiClient {
    base: String,
    http: reqwest::Client,
}

impl ApiClient {
    fn new(base: &str, token: &str) -> anyhow::Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))?,
        );
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(10))
            .build()?;
        Ok(Self {
            base: base.trim_end_matches('/').to_string(),
            http,
        })
    }

    async fn ping(&self) -> anyhow::Result<()> {
        let r = self
            .http
            .get(format!("{}/api/health", self.base))
            .send()
            .await?;
        if !r.status().is_success() {
            anyhow::bail!("health check returned {}", r.status());
        }
        Ok(())
    }

    /// POST and assert 200 / 201. Caller passes the JSON-encodable
    /// body and the route suffix (without leading `/api`).
    async fn post<T: serde::Serialize + ?Sized>(
        &self,
        route: &str,
        body: &T,
    ) -> anyhow::Result<()> {
        self.post_json(route, body).await.map(|_| ())
    }

    /// Like [`Self::post`] but returns the response envelope's `data`
    /// field — used when the loader needs the server-assigned id (e.g.
    /// a freshly-created group).
    async fn post_json<T: serde::Serialize + ?Sized>(
        &self,
        route: &str,
        body: &T,
    ) -> anyhow::Result<serde_json::Value> {
        let r = self
            .http
            .post(format!("{}/api{}", self.base, route))
            .json(body)
            .send()
            .await?;
        let status = r.status();
        let text = r.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("POST {route} → {status}: {text}");
        }
        let env: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
        Ok(env.get("data").cloned().unwrap_or(serde_json::Value::Null))
    }
}

/// Collections this binary owns. Wiping these is safe; everything
/// `load_demo_data` writes is in here, and these are also the only
/// collections the live ingest path populates.
const DEMO_COLLECTIONS: &[&str] = &[
    EVENTS_COLLECTION,
    SUPEREVENTS_COLLECTION,
    LOCALIZE_REQUESTS_COLLECTION,
    LOCALIZE_RESULTS_COLLECTION,
    ANNOTATIONS_COLLECTION,
    ALERTS_COLLECTION,
    GRB_TRIGGERS_COLLECTION,
    CROSS_MATCHES_COLLECTION,
    BOOM_ALERTS_COLLECTION,
    FRB_ALERTS_COLLECTION,
    NEUTRINO_ALERTS_COLLECTION,
    ICECUBE_LVK_SEARCHES_COLLECTION,
    SCIENCE_FILTERS_COLLECTION,
    // Access control: wipe the per-instance + join rows (and users)
    // but NOT `roles`/`streams`, which `Archive::connect` reseeds.
    USERS_COLLECTION,
    GROUPS_COLLECTION,
    GROUP_USERS_COLLECTION,
    GROUP_STREAMS_COLLECTION,
    STREAM_USERS_COLLECTION,
    // The skymap-related collections only matter when the mongo
    // backend is in use; for the S3 backend they're a no-op. Both
    // paths are safe.
    SKYMAPS_COLLECTION,
    SKYMAP_CONTOURS_COLLECTION,
    GRB_SKYMAPS_COLLECTION,
];

async fn wipe(archive: &Archive) -> anyhow::Result<()> {
    for name in DEMO_COLLECTIONS {
        let coll = archive
            .database()
            .collection::<mongodb::bson::Document>(name);
        coll.delete_many(doc! {}).await?;
    }
    Ok(())
}

#[derive(Default, Debug)]
struct LoadSummary {
    superevents: usize,
    events: usize,
    grb_triggers: usize,
    boom_alerts: usize,
    frb_alerts: usize,
    neutrino_alerts: usize,
    icecube_lvk_searches: usize,
    cross_matches: usize,
    science_filters: usize,
    groups: usize,
    annotations: usize,
    alerts: usize,
    skymaps: usize,
    localize_jobs: usize,
}

fn print_summary(s: &LoadSummary) {
    println!("load_demo_data: seeded demo dataset:");
    println!("  superevents:       {}", s.superevents);
    println!("  gw events:         {}", s.events);
    println!("  skymaps:           {}", s.skymaps);
    println!("  grb triggers:      {}", s.grb_triggers);
    println!("  boom alerts:       {}", s.boom_alerts);
    println!("  frb alerts:        {}", s.frb_alerts);
    println!("  neutrino alerts:   {}", s.neutrino_alerts);
    println!("  icecube lvk search:{}", s.icecube_lvk_searches);
    println!("  cross-matches:     {}", s.cross_matches);
    println!("  science filters:   {}", s.science_filters);
    println!("  groups:            {}", s.groups);
    println!("  localize jobs:     {}", s.localize_jobs);
    println!("  annotations:       {}", s.annotations);
    println!("  public alerts:     {}", s.alerts);
}

/// Approximate GPS-time-now. The exact leap second tally only matters
/// when cross-matching against real instrument times — for the demo
/// the offset is "close enough" and the relative ordering of t_0
/// values is what the UI cares about.
fn gps_now() -> f64 {
    let unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs_f64();
    // unix → gps: subtract the offset between the two epochs (Jan 1
    // 1970 → Jan 6 1980 = 315964800 s) then add accumulated leap
    // seconds. We hard-code 18 because that's the value that has
    // applied since 2017-01-01 and the demo uses "now" timestamps.
    unix_secs - 315_964_800.0 + 18.0
}

/// Build the seed set. Everything is POSTed through gw-api except
/// the annotations + public-alerts + cross-matches that the API
/// doesn't expose POST routes for yet — those still go via
/// `archive` directly. Returns the per-collection counts.
async fn seed(archive: &Archive, api: &ApiClient) -> anyhow::Result<LoadSummary> {
    let mut summary = LoadSummary::default();
    let t_now = gps_now();
    // Stage the t_0 of each superevent inside the last 24 h, evenly
    // spaced. The relative spacing is what makes the demo
    // interesting (older events first) and 24 h is the default
    // "recent" window on the list views.
    let day = 86_400.0;
    let t_0_oldest = t_now - day;

    // ---- Superevent 1: high-SNR BNS-like, with skymap ----
    let s1_id = "S260524a".to_string();
    let s1_t0 = t_0_oldest + 1.0 * day / 6.0; // ~20 h ago
    let s1_event = mk_event("G260524a01", "gstlal", s1_t0, 20.4, 1.0e-12, 1.22, 2.8);
    let s1_skymap = build_synthetic_prob_density_skymap(197.45, -23.38, 7.5);
    let s1 = mk_superevent(&s1_id, s1_t0, &s1_event, vec![&s1_event], Some(&s1_skymap));
    api.post("/superevents", &s1).await?;
    summary.events += 1;
    summary.superevents += 1;
    summary.skymaps += 1;
    seed_localize_audit(
        &archive,
        &s1_id,
        &s1_event.graceid,
        &s1_event.pipeline,
        &s1_skymap,
        1421,
    )
    .await?;
    summary.localize_jobs += 1;

    // ---- Superevent 2: mid-SNR BBH-like, multi-pipeline ----
    // Two pipelines (gstlal + mbta) clustered into the same
    // superevent. gstlal has the higher SNR so it's the preferred
    // event. Both g-event docs are written via the same POST.
    let s2_id = "S260524b".to_string();
    let s2_t0 = t_0_oldest + 2.0 * day / 6.0; // ~16 h ago
    let s2_gstlal = mk_event("G260524b01", "gstlal", s2_t0, 12.6, 8.5e-10, 28.0, 64.0);
    let s2_mbta = mk_event(
        "G260524b02",
        "mbta",
        s2_t0 + 0.18, // arrived ~180 ms later
        11.9,
        2.1e-9,
        27.6,
        63.5,
    );
    let s2_skymap = build_synthetic_prob_density_skymap(83.6, 22.0, 12.0);
    let s2 = mk_superevent(
        &s2_id,
        s2_t0,
        &s2_gstlal,
        vec![&s2_gstlal, &s2_mbta],
        Some(&s2_skymap),
    );
    api.post("/superevents", &s2).await?;
    summary.events += 2;
    summary.superevents += 1;
    summary.skymaps += 1;
    // Two pipelines clustered → two localize jobs, both Ok (since
    // a single skymap was eventually attached). Shares pipeline
    // from each constituent g-event so the audit row shows where
    // the request came from.
    seed_localize_audit(
        &archive,
        &s2_id,
        &s2_gstlal.graceid,
        &s2_gstlal.pipeline,
        &s2_skymap,
        2113,
    )
    .await?;
    seed_localize_audit(
        &archive,
        &s2_id,
        &s2_mbta.graceid,
        &s2_mbta.pipeline,
        &s2_skymap,
        2247,
    )
    .await?;
    summary.localize_jobs += 2;

    // ---- Superevent 3: borderline / sub-threshold, no skymap ----
    // Exercises the UI "no skymap → 404" path. snr ≈ 8, far ≈ 1e-6.
    let s3_id = "S260524c".to_string();
    let s3_t0 = t_0_oldest + 3.0 * day / 6.0;
    let s3_event = mk_event("G260524c01", "pycbc", s3_t0, 8.1, 1.0e-6, 1.45, 3.1);
    let s3 = mk_superevent(&s3_id, s3_t0, &s3_event, vec![&s3_event], None);
    api.post("/superevents", &s3).await?;
    summary.events += 1;
    summary.superevents += 1;

    // ---- Superevent 4: retracted (preferred event message_type="retraction") ----
    let s4_id = "S260524d".to_string();
    let s4_t0 = t_0_oldest + 4.0 * day / 6.0;
    let mut s4_event = mk_event("G260524d01", "spiir", s4_t0, 9.7, 5.4e-8, 1.7, 3.9);
    s4_event.message_type = "retraction".into();
    let s4 = mk_superevent(&s4_id, s4_t0, &s4_event, vec![&s4_event], None);
    api.post("/superevents", &s4).await?;
    summary.events += 1;
    summary.superevents += 1;

    // ---- Superevent 5: high-SNR NSBH-ish, with skymap, GRB+BOOM matched ----
    // This is the one we hang the kilonova-style cross-match story
    // off — multiple GRB triggers inside ±60 s of t_0, plus BOOM
    // alerts bracketing t_0 in their (last_non_det, first_det) pair.
    let s5_id = "S260524e".to_string();
    let s5_t0 = t_0_oldest + 5.0 * day / 6.0;
    let s5_event = mk_event("G260524e01", "gstlal", s5_t0, 18.2, 4.4e-11, 2.6, 7.2);
    let s5_skymap = build_synthetic_prob_density_skymap(150.18, 15.28, 5.0);
    let s5 = mk_superevent(&s5_id, s5_t0, &s5_event, vec![&s5_event], Some(&s5_skymap));
    api.post("/superevents", &s5).await?;
    summary.events += 1;
    summary.superevents += 1;
    summary.skymaps += 1;
    seed_localize_audit(
        &archive,
        &s5_id,
        &s5_event.graceid,
        &s5_event.pipeline,
        &s5_skymap,
        1638,
    )
    .await?;
    summary.localize_jobs += 1;

    // ---- Annotations: one per superevent so the tab is non-empty ----
    for (sid, kind, payload) in [
        (
            s1_id.as_str(),
            "p_astro",
            json!({"BNS": 0.91, "NSBH": 0.04, "BBH": 0.00, "Terrestrial": 0.05}),
        ),
        (
            s2_id.as_str(),
            "p_astro",
            json!({"BNS": 0.00, "NSBH": 0.02, "BBH": 0.95, "Terrestrial": 0.03}),
        ),
        (
            s3_id.as_str(),
            "manual_note",
            json!({"text": "Borderline event — operator flagged for re-examination."}),
        ),
        (
            s4_id.as_str(),
            "manual_note",
            json!({"text": "Retracted by gstlal pipeline; data-quality veto."}),
        ),
        (
            s5_id.as_str(),
            "p_astro",
            json!({"BNS": 0.12, "NSBH": 0.78, "BBH": 0.00, "Terrestrial": 0.10}),
        ),
    ] {
        let payload_bson: Bson = mongodb::bson::to_bson(&payload)?;
        let ann = AnnotationDoc::new(sid, kind, "demo-loader", payload_bson);
        archive.insert_annotation(&ann).await?;
        summary.annotations += 1;
    }

    // ---- One public alert audit row for s2 ----
    let alert_body = json!({
        "alert_type": "preliminary",
        "superevent_id": s2_id,
        "time_created": s2_t0,
        "urls": {
            "skymap": format!("http://localhost:8080/api/superevents/{s2_id}/skymap"),
        },
        "event": {
            "significant": true,
            "time": s2_t0,
            "far": 8.5e-10,
            "instruments": ["H1", "L1"],
            "group": "CBC",
            "pipeline": "gstlal",
        },
    });
    let alert = AlertDoc::new(
        &s2_id,
        "preliminary",
        mongodb::bson::to_bson(&alert_body)?,
        true,
    );
    archive.insert_alert(&alert).await?;
    summary.alerts += 1;

    // ---- GRB triggers ----
    // Three of these are deliberately inside ±60 s of s5_t0 so the
    // cross-match scan returns something interesting; the rest are
    // far enough away to demonstrate "no nearby external event".
    let grb_triggers: Vec<(GrbTrigger, Option<f64>)> = vec![
        // Fermi-GBM-FIN inside the s5 window — bright burst, tight error.
        (
            GrbTrigger {
                trigger_id: "bn260524a".into(),
                instrument: "Fermi-GBM-FIN".into(),
                trigger_time: s5_t0 + 2.4,
                position: Some(SkyPosition::new(150.18, 15.28, 1.5 * 3600.0)),
                significance: 7.2,
                skymap_url: None,
                error_radius_deg: Some(1.5),
                far_hz: None,
            },
            Some(s5_t0),
        ),
        // Fermi-GBM-FLT inside the s5 window — earlier, looser position.
        (
            GrbTrigger {
                trigger_id: "bn260524a-flt".into(),
                instrument: "Fermi-GBM-FLT".into(),
                trigger_time: s5_t0 - 4.7,
                position: Some(SkyPosition::new(149.8, 14.9, 5.0 * 3600.0)),
                significance: 5.4,
                skymap_url: None,
                error_radius_deg: Some(5.0),
                far_hz: None,
            },
            Some(s5_t0),
        ),
        // Swift-BAT inside the s5 window — much tighter localization.
        (
            GrbTrigger {
                trigger_id: "01234567".into(),
                instrument: "Swift-BAT".into(),
                trigger_time: s5_t0 + 0.9,
                position: Some(SkyPosition::new(150.21, 15.30, 0.05 * 3600.0)),
                significance: 9.1,
                skymap_url: None,
                error_radius_deg: Some(0.05),
                far_hz: None,
            },
            Some(s5_t0),
        ),
        // Unrelated Fermi-GBM-FIN, far from any superevent.
        (
            GrbTrigger {
                trigger_id: "bn260524b".into(),
                instrument: "Fermi-GBM-FIN".into(),
                trigger_time: s2_t0 - 1_800.0,
                position: Some(SkyPosition::new(45.0, -60.0, 3.0 * 3600.0)),
                significance: 4.8,
                skymap_url: None,
                error_radius_deg: Some(3.0),
                far_hz: None,
            },
            None,
        ),
        // Unrelated Swift-BAT.
        (
            GrbTrigger {
                trigger_id: "07654321".into(),
                instrument: "Swift-BAT".into(),
                trigger_time: s1_t0 + 3_600.0,
                position: Some(SkyPosition::new(310.0, 5.0, 0.08 * 3600.0)),
                significance: 6.5,
                skymap_url: None,
                error_radius_deg: Some(0.08),
                far_hz: None,
            },
            None,
        ),
        // Fermi-GBM-GND, no nearby superevent.
        (
            GrbTrigger {
                trigger_id: "bn260523z".into(),
                instrument: "Fermi-GBM-GND".into(),
                trigger_time: s1_t0 - 2_400.0,
                position: Some(SkyPosition::new(12.5, 35.0, 4.0 * 3600.0)),
                significance: 5.1,
                skymap_url: None,
                error_radius_deg: Some(4.0),
                far_hz: None,
            },
            None,
        ),
    ];
    // POST each GRB trigger through `/api/grb-triggers` so the
    // canonical-MOC + upsert path is exercised end-to-end. The
    // route's body is an `untagged` enum that accepts a parsed
    // `GrbTrigger` directly.
    for (trigger, _associated_t0) in &grb_triggers {
        api.post("/grb-triggers", trigger).await?;
        summary.grb_triggers += 1;
    }

    // ---- BOOM optical-transient alerts ----
    // Bracket configurations exercise every branch of the
    // "(last_non_det, first_det) spans t_0?" kilonova time-match
    // criterion.
    let boom_alerts = vec![
        // Bracket spans s5_t0 (kilonova candidate).
        mk_boom_transient(
            "2026-05-24T18:14:00Z__ZTF26abccdef",
            s5_t0 + 14_400.0, // alert ~4 h after merger
            "ZTF26abccdef",
            150.20,
            15.27,
            Some(("kilonova", 0.78)),
            Some("GW LVK S260524e"),
            // last non-det 3 h before merger, first det 4 h after
            Some(s5_t0 - 10_800.0),
            Some(s5_t0 + 14_000.0),
        ),
        // Bracket entirely BEFORE the merger — should NOT match s5.
        mk_boom_transient(
            "2026-05-24T05:00:00Z__ZTF26axxxx01",
            s5_t0 - 86_400.0,
            "ZTF26axxxx01",
            120.0,
            -10.0,
            Some(("supernova_ia", 0.92)),
            None,
            Some(s5_t0 - 100_000.0),
            Some(s5_t0 - 80_000.0),
        ),
        // Bracket entirely AFTER the merger — should NOT match s5.
        mk_boom_transient(
            "2026-05-25T01:00:00Z__ZTF26axxxx02",
            s5_t0 + 86_400.0,
            "ZTF26axxxx02",
            200.0,
            -5.0,
            Some(("supernova_ii", 0.61)),
            None,
            Some(s5_t0 + 80_000.0),
            Some(s5_t0 + 100_000.0),
        ),
        // Only first detection (no preceding upper limits) — bracket
        // open on the lower side. Exercises the "first_det only" path.
        mk_boom_transient(
            "2026-05-24T10:00:00Z__ZTF26axxxx03",
            s2_t0 + 1_800.0,
            "ZTF26axxxx03",
            83.7,
            21.9,
            Some(("unknown", 0.40)),
            Some("GW LVK S260524b"),
            None,
            Some(s2_t0 + 600.0),
        ),
        // Cross-match summary already populated (won't be touched by
        // the scan endpoint).
        mk_boom_transient(
            "2026-05-24T22:00:00Z__ZTF26axxxx04",
            s5_t0 + 21_600.0,
            "ZTF26axxxx04",
            150.05,
            15.40,
            Some(("kilonova", 0.55)),
            Some("GW LVK S260524e, GRB Fermi-GBM-FIN bn260524a"),
            Some(s5_t0 - 7_200.0),
            Some(s5_t0 + 18_000.0),
        ),
    ];
    for transient in &boom_alerts {
        api.post("/boom-alerts", transient).await?;
        summary.boom_alerts += 1;
    }

    // ---- FRB alerts (CHIME + DSA110) ----
    // Two of these are inside ±60 s of s5_t0 so the scan endpoint
    // returns them alongside the GRBs; one is out-of-window
    // background.
    let frb_alerts = vec![
        // CHIME FRB inside the s5 window — point-localized arcminute
        // burst at the same RA/Dec as s5 so the cross-match overlap
        // will be high.
        mk_frb_alert(
            "chime_260524a",
            CHIME_INSTRUMENT_LABEL,
            s5_t0 - 1.8,
            150.18,
            15.28,
            Some(0.018),
            Some(13.5),
            Some(412.3),
            Some(0.99),
            None,
        ),
        // DSA110 FRB also inside the s5 window — different sky
        // position so its overlap is poor.
        mk_frb_alert(
            "dsa_260524a",
            DSA110_INSTRUMENT_LABEL,
            s5_t0 + 6.2,
            45.0,
            -10.0,
            Some(0.022),
            Some(9.7),
            Some(281.0),
            Some(0.94),
            None,
        ),
        // Background CHIME burst from earlier in the day — repeater.
        mk_frb_alert(
            "chime_repeater_42",
            CHIME_INSTRUMENT_LABEL,
            s2_t0 - 3_600.0,
            210.4,
            -33.7,
            Some(0.5),
            Some(8.2),
            Some(348.7),
            Some(0.88),
            Some("FRB-20180916B".to_string()),
        ),
    ];
    for alert in &frb_alerts {
        api.post("/frb-alerts", alert).await?;
        summary.frb_alerts += 1;
    }

    // ---- Neutrino alerts (IceCube + KM3NeT) ----
    let neutrino_alerts = vec![
        // IceCube Gold Track inside the s5 window, near the BNS
        // localization — exercises the highest-significance path.
        mk_icecube_alert(
            "240524_runevt_1",
            s5_t0 + 12.4,
            150.22,
            15.30,
            Some(0.4),
            "Gold Track Alert",
            "Track",
            Some(312.0),
            Some(0.86),
            Some("IceCube-DEMO-A"),
        ),
        // IceCube Bronze Track, off-source background.
        mk_icecube_alert(
            "240524_runevt_2",
            s2_t0 + 45.6,
            10.0,
            -25.0,
            Some(0.6),
            "Bronze Track Alert",
            "Track",
            Some(118.5),
            Some(0.42),
            Some("IceCube-DEMO-B"),
        ),
        // KM3NeT ORCA exceptional event near s5_t0 but with a
        // wider error region; spatial overlap will be lower than
        // the IceCube Gold but the temporal coincidence is tight.
        mk_km3net_alert(
            "km3_240524_1",
            s5_t0 + 8.1,
            148.7,
            14.9,
            Some(0.9),
            "orca_HE",
            Some(0.018),
            Some("KM3-DEMO-A"),
        ),
    ];
    for alert in &neutrino_alerts {
        api.post("/neutrino-alerts", alert).await?;
        summary.neutrino_alerts += 1;
    }

    // ---- IceCube LVK Nu Track Search results ----
    // Attach one to s1 (the BNS-like superevent) so its detail
    // page shows the coincidence-search section. The search ran
    // in a ±10 minute window around s1_t0 and found 2 coincident
    // tracks both inside the GW 90% region.
    let lvk_search = IceCubeLvkSearch {
        superevent_id: s1_id.clone(),
        alert_time: s1_t0 + 2_100.0,
        trigger_time: s1_t0,
        observation_start: Some(s1_t0 - 500.0),
        observation_stop: Some(s1_t0 + 500.0),
        observation_livetime: Some(1000.0),
        pval_generic: Some(0.0191),
        pval_bayesian: Some(0.0549),
        n_events_coincident: 2,
        coincident_events: vec![
            CoincidentTrackEvent {
                id: "138590_39138551".into(),
                event_dt: 12.91,
                localization: Some(SkyPosition::new(197.48, -23.15, 0.5 * 3600.0)),
                event_pval_generic: Some(0.0191),
                event_pval_bayesian: None,
            },
            CoincidentTrackEvent {
                id: "138590_39164579".into(),
                event_dt: 222.46,
                localization: Some(SkyPosition::new(197.20, -23.50, 0.5 * 3600.0)),
                event_pval_generic: Some(0.0928),
                event_pval_bayesian: Some(0.0656),
            },
        ],
        most_probable_direction: Some(SkyPosition::new(197.49, -23.18, 0.5 * 3600.0)),
        flux_sensitivity_range: Some([0.0277, 0.647]),
        sensitive_energy_range: Some([542.0, 23_000_000.0]),
        body: json!({}),
    };
    api.post(
        &format!(
            "/superevents/{}/icecube-lvk-searches",
            lvk_search.superevent_id
        ),
        &lvk_search,
    )
    .await?;
    summary.icecube_lvk_searches += 1;

    // ---- Pre-seeded cross-matches ----
    // These hang off s5 so the LocalizationTab + Aladin overlay
    // have something to render even before the scan endpoint is
    // hit. We deliberately mirror the three "states" the UI
    // distinguishes: un-associated, associated, and with a
    // populated p-value / remapped joint FAR.
    let s5_event_ref = &s5_event;
    let xmatches: Vec<(&GrbTrigger, CrossMatchResult)> = vec![
        (
            &grb_triggers[0].0,
            CrossMatchResult {
                time_offset_sec: grb_triggers[0].0.trigger_time - s5_t0,
                spatial_overlap: 0.81,
                in_50cr: true,
                in_90cr: true,
                joint_far_per_year: Some(2.7e-5),
                p_value: Some(2.3e-4),
                p_value_trials: Some(200),
                joint_far_remapped_per_year: Some(3.1e-5),
                targeted_joint_far_per_year: None,
                associated: true,
            },
        ),
        (
            &grb_triggers[1].0,
            CrossMatchResult {
                time_offset_sec: grb_triggers[1].0.trigger_time - s5_t0,
                spatial_overlap: 0.42,
                in_50cr: false,
                in_90cr: true,
                joint_far_per_year: Some(7.4e-3),
                p_value: None,
                p_value_trials: None,
                joint_far_remapped_per_year: None,
                targeted_joint_far_per_year: None,
                associated: false,
            },
        ),
        (
            &grb_triggers[2].0,
            CrossMatchResult {
                time_offset_sec: grb_triggers[2].0.trigger_time - s5_t0,
                spatial_overlap: 0.96,
                in_50cr: true,
                in_90cr: true,
                joint_far_per_year: Some(1.1e-6),
                p_value: Some(4.5e-5),
                p_value_trials: Some(500),
                joint_far_remapped_per_year: Some(2.0e-6),
                targeted_joint_far_per_year: None,
                associated: true,
            },
        ),
    ];
    let _ = s5_event_ref; // keep the binding alive without `unused` warn.
    for (trigger, result) in xmatches {
        let xm = CrossMatchDoc::new(&s5_id, trigger, result);
        archive.upsert_cross_match(&xm).await?;
        summary.cross_matches += 1;
    }

    // Pre-seeded cross-matches against the new external messengers
    // so the per-superevent table demonstrates the full multi-
    // messenger mix (GRB + FRB + neutrino) on s5 without having to
    // run the scan endpoint first.
    let chime_match = CrossMatchResult {
        time_offset_sec: frb_alerts[0].trigger.trigger_time - s5_t0,
        spatial_overlap: 0.74,
        in_50cr: true,
        in_90cr: true,
        joint_far_per_year: Some(8.3e-5),
        p_value: Some(6.1e-4),
        p_value_trials: Some(200),
        joint_far_remapped_per_year: Some(9.4e-5),
        targeted_joint_far_per_year: None,
        associated: true,
    };
    let icecube_match = CrossMatchResult {
        time_offset_sec: neutrino_alerts[0].trigger.trigger_time - s5_t0,
        spatial_overlap: 0.61,
        in_50cr: true,
        in_90cr: true,
        joint_far_per_year: Some(3.2e-4),
        p_value: Some(1.4e-3),
        p_value_trials: Some(200),
        joint_far_remapped_per_year: Some(3.8e-4),
        targeted_joint_far_per_year: None,
        associated: false,
    };
    for (trigger, result) in [
        (&frb_alerts[0].trigger, chime_match),
        (&neutrino_alerts[0].trigger, icecube_match),
    ] {
        let xm = CrossMatchDoc::new(&s5_id, trigger, result);
        archive.upsert_cross_match(&xm).await?;
        summary.cross_matches += 1;
    }

    // ---- Access control: groups, members, streams ----
    // Create a real group, grant it streams, and add the demo human
    // (cough052@ligo.org) as an admin member. Requires the loader's
    // sub (`load-demo-data`) to be a site admin — run gw-api with
    // BOOM_GW_SITE_ADMINS including it (the Makefile `run` target does).
    // Filters below are shared with this group, so any member sees
    // them; cough052 is also a site admin via the same env, so they'd
    // see everything regardless.
    const DEMO_HUMAN: &str = "cough052@ligo.org";
    let mma = api
        .post_json(
            "/groups",
            &json!({"name": "MMA team", "description": "Multi-messenger follow-up"}),
        )
        .await?;
    let mma_id = mma
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    summary.groups += 1;
    // Grant the group all five messenger streams, then add the human
    // (member-add auto-grants the group's streams to the user).
    for stream in [
        "gracedb_gw",
        "gcn_grb",
        "gcn_frb",
        "gcn_neutrino",
        "boom_optical",
    ] {
        api.post(
            &format!("/groups/{mma_id}/streams"),
            &json!({"stream_id": stream}),
        )
        .await?;
    }
    api.post(
        &format!("/groups/{mma_id}/members"),
        &json!({"sub": DEMO_HUMAN, "admin": true}),
    )
    .await?;

    // ---- Science filters ----
    // Saved filters shared with the MMA team group. Cuts are tuned to
    // the s5 cross-matches above so the filtered view is non-empty and
    // the tiers light up. The first restricts to the GRB + optical
    // streams to demonstrate stream-gating.
    let demo_filters = [
        json!({
            "name": "High-confidence (gold/silver)",
            "group_id": mma_id,
            "stream_ids": ["gcn_grb", "boom_optical", "gcn_neutrino"],
            "cuts": {
                "require_in_90cr": true,
                "joint_far_remapped_max_per_year": 1.0e-3,
            },
            "confidence_tiers": [
                { "name": "gold", "joint_far_remapped_max_per_year": 1.0e-5 },
                { "name": "silver", "joint_far_remapped_max_per_year": 1.0e-3 },
            ],
        }),
        json!({
            "name": "In 90% credible region",
            "group_id": mma_id,
            "cuts": { "require_in_90cr": true },
        }),
        json!({
            "name": "Strong spatial overlap (≥ 0.5)",
            "group_id": mma_id,
            "cuts": { "spatial_overlap_min": 0.5 },
        }),
    ];
    for f in &demo_filters {
        api.post("/science-filters", f).await?;
        summary.science_filters += 1;
    }

    Ok(summary)
}

/// Build a `GwEvent` with the right shape for a `EventDoc::from_event`
/// upsert. Most fields are synthesized; the only ones a UI test will
/// inspect are `graceid`, `pipeline`, `snr`, `far`, `mchirp`,
/// `end_time`, and the `coinc` sub-document.
fn mk_event(
    graceid: &str,
    pipeline: &str,
    end_time: f64,
    snr: f64,
    far: f64,
    mchirp: f64,
    total_mass: f64,
) -> GwEvent {
    let coinc = CoincInspiralEvent {
        coinc_event_id: graceid.into(),
        ifos: "H1,L1".into(),
        combined_far: far,
        snr,
        mass: Some(total_mass),
        mchirp: Some(mchirp),
        end_time,
        sngls: vec![
            SnglInspiral {
                event_id: format!("{graceid}-H1"),
                ifo: "H1".into(),
                end_time,
                snr: snr * 0.7,
                mass1: Some(total_mass / 2.0),
                mass2: Some(total_mass / 2.0),
                mchirp: Some(mchirp),
            },
            SnglInspiral {
                event_id: format!("{graceid}-L1"),
                ifo: "L1".into(),
                end_time: end_time + 0.005,
                snr: snr * 0.65,
                mass1: Some(total_mass / 2.0),
                mass2: Some(total_mass / 2.0),
                mchirp: Some(mchirp),
            },
        ],
    };
    GwEvent {
        pipeline: pipeline.into(),
        graceid: graceid.into(),
        producer_timestamp: end_time + 0.5,
        message_type: "new".into(),
        submitter: "demo-loader".into(),
        end_time,
        ifos: "H1,L1".into(),
        snr,
        far,
        mchirp: Some(mchirp),
        total_mass: Some(total_mass),
        coinc,
    }
}

/// Write a matched `LocalizeRequest` + `LocalizeResult` audit pair
/// so the Localization tab's "Localization jobs" table has a row to
/// show. In live production these rows arrive via the Kafka loop
/// gw_clusterer ↔ bayestar-service; the demo loader attaches the
/// skymap directly to the superevent and would otherwise leave the
/// audit collections empty (which prompts "why is the table empty
/// when there's clearly a skymap?" — exactly the kind of UX
/// surprise this seeds away).
async fn seed_localize_audit(
    archive: &Archive,
    superevent_id: &str,
    graceid: &str,
    pipeline: &str,
    skymap_bytes: &[u8],
    elapsed_ms: u64,
) -> anyhow::Result<()> {
    // Include graceid so multi-pipeline superevents (gstlal + mbta
    // clustered into the same superevent) get distinct audit rows
    // rather than colliding on the same `_id`.
    let request_id = format!("demo-loc-{superevent_id}-{graceid}");
    let req =
        LocalizeRequest::from_coinc_xml(&request_id, superevent_id, graceid, pipeline, b"<demo/>");
    let result = LocalizeResult {
        request_id,
        superevent_id: superevent_id.into(),
        graceid: graceid.into(),
        status: LocalizeStatus::Ok,
        skymap_fits: Some(BASE64.encode(skymap_bytes)),
        error_message: None,
        elapsed_ms,
    };
    archive.record_localize_request(&req).await?;
    archive.record_localize_result(&result).await?;
    Ok(())
}

/// Stitch a `Superevent` together. The window is the default ±2.5 s
/// (5 s total) used everywhere in the live pipeline. Returns the
/// in-memory `Superevent` shape so `archive.upsert_superevent()` can
/// derive the on-disk `SupereventDoc` itself — keeps the
/// preferred_event SNR / skymap_summary fields in sync.
fn mk_superevent(
    id: &str,
    t_0: f64,
    preferred: &GwEvent,
    members: Vec<&GwEvent>,
    skymap_bytes: Option<&[u8]>,
) -> Superevent {
    Superevent {
        id: id.into(),
        t_0,
        t_start: t_0 - 2.5,
        t_end: t_0 + 2.5,
        preferred_event: preferred.clone(),
        g_events: members.iter().map(|e| (*e).clone()).collect(),
        skymap: skymap_bytes.map(|b| SkyMapFits {
            bytes: b.to_vec(),
            elapsed_ms: 5_000,
        }),
    }
}

/// Construct a `BoomTransient` with the photometry rows the
/// detection_bracket walker has already been applied to, then
/// hand-set first/last detection times. We bypass `parse_boom_alert`
/// on purpose: the demo wants explicit control over the bracket so
/// every "where does t_0 fall?" case is covered.
#[allow(clippy::too_many_arguments)]
fn mk_boom_transient(
    alert_id: &str,
    alert_time: f64,
    event_name: &str,
    ra: f64,
    dec: f64,
    classification: Option<(&str, f64)>,
    cross_match_summary: Option<&str>,
    last_non_detection_time: Option<f64>,
    first_detection_time: Option<f64>,
) -> BoomTransient {
    // Two synthetic photometry rows, mostly so the UI's drill-in
    // table is non-empty. Times are reasonable strings even if the
    // detection bracket above doesn't recompute from them.
    let photometry = vec![
        BoomPhotometry {
            observation_start: Some("2026-05-23T20:00:00Z".into()),
            telescope: Some("Palomar 1.2m Oschin".into()),
            instrument: Some("ZTF".into()),
            filter: Some("ztfg".into()),
            mag: None,
            mag_error: None,
            mag_system: Some("AB".into()),
            limiting_mag: Some(20.5),
        },
        BoomPhotometry {
            observation_start: Some("2026-05-24T08:00:00Z".into()),
            telescope: Some("Palomar 1.2m Oschin".into()),
            instrument: Some("ZTF".into()),
            filter: Some("ztfr".into()),
            mag: Some(19.4),
            mag_error: Some(0.05),
            mag_system: Some("AB".into()),
            limiting_mag: Some(20.5),
        },
    ];
    let (classification, classification_score) = match classification {
        Some((label, score)) => (Some(label.to_string()), Some(score)),
        None => (None, None),
    };
    BoomTransient {
        alert_id: alert_id.into(),
        alert_time,
        event_name: event_name.into(),
        ra: Some(ra),
        dec: Some(dec),
        error_radius_deg: Some(0.001),
        classification,
        classification_score,
        cross_match_summary: cross_match_summary.map(String::from),
        photometry,
        first_detection_time,
        last_non_detection_time,
        body: json!({
            "alert_datetime": alert_id.split("__").next().unwrap_or(""),
            "mission": "Boom",
            "data": {
                "targets": [
                    {"event_name": event_name, "ra": ra, "dec": dec}
                ],
            },
        }),
    }
}

/// Build a synthetic FRB alert. The cross-match-relevant fields
/// (instrument, trigger_id, trigger_time, position, error_radius_deg,
/// significance) sit on the inner `trigger`; the source-specific
/// extras (dm, snr, importance, known_source) live as siblings.
fn mk_frb_alert(
    trigger_id: &str,
    instrument: &str,
    trigger_time: f64,
    ra: f64,
    dec: f64,
    error_radius_deg: Option<f64>,
    snr: Option<f64>,
    dm: Option<f64>,
    importance: Option<f64>,
    known_source: Option<String>,
) -> FrbAlert {
    let err_deg = error_radius_deg.unwrap_or(0.05);
    FrbAlert {
        trigger: GrbTrigger {
            trigger_id: trigger_id.to_string(),
            instrument: instrument.to_string(),
            trigger_time,
            position: Some(SkyPosition::new(ra, dec, err_deg * 3600.0)),
            significance: snr.unwrap_or(0.0),
            skymap_url: None,
            error_radius_deg,
            far_hz: None,
        },
        dm,
        dm_error: dm.map(|_| 0.4),
        importance,
        snr,
        known_source,
        body: json!({}),
    }
}

/// Build a synthetic IceCube single-neutrino alert. Sets
/// `significance` from `p_astro` because that's the field the
/// table sorts on.
fn mk_icecube_alert(
    trigger_id: &str,
    trigger_time: f64,
    ra: f64,
    dec: f64,
    error_radius_deg: Option<f64>,
    pipeline: &str,
    alert_topology: &str,
    nu_energy: Option<f64>,
    p_astro: Option<f64>,
    event_name: Option<&str>,
) -> NeutrinoAlert {
    let err_deg = error_radius_deg.unwrap_or(0.5);
    NeutrinoAlert {
        trigger: GrbTrigger {
            trigger_id: trigger_id.to_string(),
            instrument: ICECUBE_INSTRUMENT_LABEL.to_string(),
            trigger_time,
            position: Some(SkyPosition::new(ra, dec, err_deg * 3600.0)),
            significance: p_astro.unwrap_or(0.0),
            skymap_url: None,
            error_radius_deg,
            far_hz: None,
        },
        alert_topology: Some(alert_topology.to_string()),
        pipeline: Some(pipeline.to_string()),
        nu_energy,
        p_astro,
        p_value: None,
        far: Some(8.029e-8),
        healpix_url: None,
        event_name: event_name.map(str::to_string),
        body: json!({}),
    }
}

/// Build a synthetic KM3NeT alert. KM3NeT emits `p_value` rather
/// than `p_astro`; the scalar significance picks whichever is
/// present.
fn mk_km3net_alert(
    trigger_id: &str,
    trigger_time: f64,
    ra: f64,
    dec: f64,
    error_radius_deg: Option<f64>,
    pipeline: &str,
    p_value: Option<f64>,
    event_name: Option<&str>,
) -> NeutrinoAlert {
    let err_deg = error_radius_deg.unwrap_or(0.9);
    NeutrinoAlert {
        trigger: GrbTrigger {
            trigger_id: trigger_id.to_string(),
            instrument: KM3NET_INSTRUMENT_LABEL.to_string(),
            trigger_time,
            position: Some(SkyPosition::new(ra, dec, err_deg * 3600.0)),
            significance: p_value.unwrap_or(0.0),
            skymap_url: None,
            error_radius_deg,
            far_hz: None,
        },
        alert_topology: Some("Track".to_string()),
        pipeline: Some(pipeline.to_string()),
        nu_energy: None,
        p_astro: None,
        p_value,
        far: Some(8.029e-8),
        healpix_url: None,
        event_name: event_name.map(str::to_string),
        body: json!({}),
    }
}
