//! Canonical alert-ingest path.
//!
//! Every external-event ingest in the system — operator POSTs
//! through the REST API, the Kafka consumer
//! (`bin/gw_gcn_consumer`), the demo loader, integration tests —
//! lands here. The HTTP and Kafka clients used to maintain
//! parallel "persist + canonical MOC + upsert" implementations;
//! moving the shared logic into one place means they can't drift
//! out of sync, and the Kafka path stops paying the network +
//! JSON-serde + JWT-decode tax of going through HTTP.
//!
//! Shape: one `pub async fn ingest_*` per alert type, each taking
//! a typed alert plus the archive + optional skymap storage and
//! returning the upserted doc with a created/replaced flag.
//! Higher-level orchestration (HTTP handlers, Kafka consumer
//! handlers) stays one tier up — this module is pure
//! "given this alert, persist it correctly".

use crate::archive::{
    Archive, ArchiveError, BoomAlertDoc, CrossMatchDoc, FrbAlertDoc, GrbTriggerDoc,
    IceCubeLvkSearchDoc, NeutrinoAlertDoc, SupereventDoc,
};
use crate::boom::BoomTransient;
use crate::clustering::Superevent;
use crate::contour::{compute_contour_moc, compute_skymap_centroid};
use crate::crossmatch::{self, cross_match_with_contours, parse_contour_moc, PvalueOpts};
use crate::frb::FrbAlert;
use crate::grb::{build_canonical_moc_fits, CrossMatchResult, GrbTrigger};
use crate::icecube_lvk::IceCubeLvkSearch;
use crate::neutrino::NeutrinoAlert;
use crate::storage::skymap::{SkymapBlob, SkymapStorage, SkymapStorageError};
use mongodb::bson::doc;

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("archive error: {0}")]
    Archive(#[from] ArchiveError),
    #[error("skymap storage error: {0}")]
    Storage(#[from] crate::storage::skymap::SkymapStorageError),
    #[error("superevent id mismatch: body {body:?} != url {url:?}")]
    SuperEventIdMismatch { body: String, url: String },
    #[error("superevent {0} not found")]
    SupereventNotFound(String),
    #[error("superevent {0} has no attached skymap")]
    SkymapMissing(String),
    #[error("mongo error: {0}")]
    Mongo(#[from] mongodb::error::Error),
    #[error("join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}

/// Best-effort: synthesize the canonical GRB-shaped MOC for an
/// external trigger and stash it next to the source. Failure is
/// logged and swallowed — pre-localization alerts have no
/// position, and a later update will fill the MOC in.
async fn try_canonical_moc(storage: Option<&SkymapStorage>, trigger: &GrbTrigger, kind: &str) {
    let Some(storage) = storage else { return };
    match build_canonical_moc_fits(trigger) {
        Ok(bytes) => {
            if let Err(e) = storage
                .upsert_grb_skymap(&trigger.instrument, &trigger.trigger_id, bytes)
                .await
            {
                tracing::warn!("{kind} canonical MOC upsert failed: {e}");
            }
        }
        Err(e) => tracing::debug!("skipping {kind} canonical MOC: {e}"),
    }
}

/// Ingest one GRB trigger (Fermi-GBM, Swift-BAT, etc.). Canonical
/// MOC + archive upsert.
pub async fn ingest_grb_trigger(
    archive: &Archive,
    storage: Option<&SkymapStorage>,
    trigger: GrbTrigger,
) -> Result<(bool, GrbTriggerDoc), IngestError> {
    try_canonical_moc(storage, &trigger, "grb").await;
    let doc = GrbTriggerDoc::from_trigger(trigger);
    let created = archive.upsert_grb_trigger(&doc).await?;
    Ok((created, doc))
}

/// Ingest one BOOM optical-transient. Canonical MOC + archive
/// upsert. The Kafka envelope can carry 1..N transients; the
/// consumer calls this once per target.
pub async fn ingest_boom_alert(
    archive: &Archive,
    storage: Option<&SkymapStorage>,
    transient: BoomTransient,
) -> Result<(bool, BoomAlertDoc), IngestError> {
    let trigger = transient.as_trigger();
    if let Some(t) = trigger.as_ref() {
        try_canonical_moc(storage, t, "boom").await;
    }
    let doc = BoomAlertDoc::from_transient(transient);
    let created = archive.upsert_boom_alert(&doc).await?;
    Ok((created, doc))
}

/// Ingest one FRB alert (CHIME or DSA110). Canonical MOC +
/// archive upsert.
pub async fn ingest_frb_alert(
    archive: &Archive,
    storage: Option<&SkymapStorage>,
    alert: FrbAlert,
) -> Result<(bool, FrbAlertDoc), IngestError> {
    try_canonical_moc(storage, &alert.trigger, "frb").await;
    let doc = FrbAlertDoc::from_alert(alert);
    let created = archive.upsert_frb_alert(&doc).await?;
    Ok((created, doc))
}

/// Ingest one high-energy neutrino alert (IceCube
/// single-neutrino, KM3NeT). Canonical MOC + archive upsert.
pub async fn ingest_neutrino_alert(
    archive: &Archive,
    storage: Option<&SkymapStorage>,
    alert: NeutrinoAlert,
) -> Result<(bool, NeutrinoAlertDoc), IngestError> {
    try_canonical_moc(storage, &alert.trigger, "neutrino").await;
    let doc = NeutrinoAlertDoc::from_alert(alert);
    let created = archive.upsert_neutrino_alert(&doc).await?;
    Ok((created, doc))
}

/// Ingest one IceCube LVK Nu Track Search result. No canonical
/// MOC step — the alert is a coincidence-search result attached
/// to a specific superevent, not a free-standing trigger. The
/// body's own `superevent_id` must match the URL path id when
/// called from the HTTP handler.
pub async fn ingest_icecube_lvk_search(
    archive: &Archive,
    expected_superevent_id: Option<&str>,
    search: IceCubeLvkSearch,
) -> Result<(bool, IceCubeLvkSearchDoc), IngestError> {
    if let Some(url_id) = expected_superevent_id {
        if url_id != search.superevent_id {
            return Err(IngestError::SuperEventIdMismatch {
                body: search.superevent_id,
                url: url_id.to_string(),
            });
        }
    }
    let doc = IceCubeLvkSearchDoc::from_search(search);
    let created = archive.upsert_icecube_lvk_search(&doc).await?;
    Ok((created, doc))
}

/// Ingest one fully-formed superevent (operator + loader path).
/// Persists each constituent g-event, then upserts the
/// superevent doc with a centroid-enriched skymap summary, then
/// writes the FITS bytes + derived 50%/90% contour MOCs if the
/// superevent carries a skymap.
///
/// Production clustering still flows through `gw_clusterer` —
/// this is for explicit insertion (backfill, demo seeding) where
/// the caller already knows the superevent's exact shape.
pub async fn ingest_superevent(
    archive: &Archive,
    storage: Option<&SkymapStorage>,
    superevent: Superevent,
) -> Result<SupereventDoc, IngestError> {
    // 1. g-events
    for ev in &superevent.g_events {
        archive.record_event(ev).await?;
    }
    // 2. SupereventDoc — enriched with centroid from the 50%
    //    credible region so the frontend Aladin viewer can
    //    initial-center the localization.
    let mut sdoc = SupereventDoc::from_superevent(&superevent);
    if let (Some(sky), Some(summary)) = (&superevent.skymap, sdoc.skymap_summary.as_mut()) {
        if let Some((ra, dec)) = compute_skymap_centroid(&sky.bytes, 0.5) {
            summary.center_ra = Some(ra);
            summary.center_dec = Some(dec);
        }
    }
    archive
        .superevents()
        .replace_one(doc! {"_id": &sdoc.id}, &sdoc)
        .upsert(true)
        .await
        .map_err(ArchiveError::from)?;
    // 3. Skymap blob + derived contours, if a skymap is attached
    //    and storage is configured.
    if let (Some(sky), Some(storage)) = (&superevent.skymap, storage) {
        let blob = SkymapBlob {
            superevent_id: superevent.id.clone(),
            bytes: sky.bytes.clone(),
            elapsed_ms: sky.elapsed_ms,
        };
        storage.upsert(blob).await?;
        for level_pct in [50u8, 90u8] {
            let level = level_pct as f64 / 100.0;
            match compute_contour_moc(&sky.bytes, level) {
                Ok(moc) => {
                    if let Err(e) = storage.upsert_contour(&superevent.id, level_pct, moc).await {
                        tracing::warn!(
                            superevent_id = %superevent.id,
                            level_pct,
                            "contour upsert failed: {e}"
                        );
                    }
                }
                Err(e) => tracing::warn!(
                    superevent_id = %superevent.id,
                    level_pct,
                    "contour synthesis failed: {e}"
                ),
            }
        }
    } else if superevent.skymap.is_some() {
        tracing::warn!(
            superevent_id = %superevent.id,
            "skymap supplied but no SkymapStorage configured — bytes not persisted"
        );
    }
    Ok(sdoc)
}

/// Knobs the scan / rescan path picks up. Mirror of the fields
/// the HTTP `ScanCrossMatchBody` exposes, factored out so the
/// `gw_clusterer` hook can pass the same shape without depending
/// on the API layer.
#[derive(Debug, Clone, Copy)]
pub struct RescanOptions {
    /// Symmetric coincidence window applied to GRB / FRB /
    /// neutrino triggers (`trigger_time` ∈ [t_0 ± window]). BOOM
    /// alerts use the bracket criterion regardless. The default
    /// matches the API handler's default.
    pub time_window_sec: f64,
    /// Optional p-value Monte Carlo. `None` skips the MC; `Some`
    /// runs `n_trials` random-rotation MOC intersections per
    /// candidate and reports the empirical p-value.
    pub pvalue_opts: Option<PvalueOpts>,
}

impl Default for RescanOptions {
    fn default() -> Self {
        Self {
            time_window_sec: 10.0,
            pvalue_opts: None,
        }
    }
}

/// Re-run every cross-match for a superevent against every
/// candidate external trigger in its coincidence window. The
/// single source of truth for two callers:
///
///   * `POST /api/superevents/{id}/scan-cross-matches` (operator
///     clicks "Scan ±window" in the UI).
///   * `gw_clusterer`'s `SkymapAttached` hook (BAYESTAR returned
///     a refined map, so every previously-ingested external alert
///     in window needs its overlap recomputed against the new
///     localization — without this, the cross-match table stays
///     stale until the operator manually re-scans or a new
///     external alert arrives).
///
/// Returns the freshly-upserted `CrossMatchDoc`s sorted by best
/// joint FAR. Preserves any operator-set `associated` flag — a
/// re-scan shouldn't flip an analyst's commit just because the
/// numbers wobbled.
pub async fn rescan_superevent_cross_matches(
    archive: &Archive,
    storage: &SkymapStorage,
    superevent_id: &str,
    opts: RescanOptions,
) -> Result<Vec<CrossMatchDoc>, IngestError> {
    let superevent = archive
        .superevents()
        .find_one(doc! {"_id": superevent_id})
        .await?
        .ok_or_else(|| IngestError::SupereventNotFound(superevent_id.to_string()))?;
    let skymap_blob = match storage.get(superevent_id).await {
        Ok(b) => b,
        Err(SkymapStorageError::NotFound(_)) => {
            return Err(IngestError::SkymapMissing(superevent_id.to_string()));
        }
        Err(e) => return Err(e.into()),
    };
    // Parse contours once per scan, not once per candidate. Cheap
    // when there are no contours (None), real win when there are.
    let contour_50_parsed = storage
        .get_contour(superevent_id, 50)
        .await
        .ok()
        .as_deref()
        .map(parse_contour_moc)
        .transpose()
        .map_err(|e| {
            ArchiveError::from(mongodb::error::Error::custom(format!("contour 50: {e}")))
        })?;
    let contour_90_parsed = storage
        .get_contour(superevent_id, 90)
        .await
        .ok()
        .as_deref()
        .map(parse_contour_moc)
        .transpose()
        .map_err(|e| {
            ArchiveError::from(mongodb::error::Error::custom(format!("contour 90: {e}")))
        })?;
    let gw_far_hz = archive
        .events()
        .find_one(doc! {"_id": &superevent.preferred_graceid})
        .await
        .ok()
        .flatten()
        .map(|ev| ev.far)
        .unwrap_or(1e-7);

    let t_0 = superevent.t_0;
    let lo = t_0 - opts.time_window_sec;
    let hi = t_0 + opts.time_window_sec;
    let trigger_window = doc! {"trigger_time": {"$gte": lo, "$lte": hi}};
    let boom_filter = doc! {
        "first_detection_time": {"$gte": t_0},
        "last_non_detection_time": {"$lte": t_0},
    };

    // Phase 1a: fan out the four cursor walks in parallel. Each
    // closure owns its window doc (clones up-front) so the
    // borrow checker doesn't complain about overlapping borrows
    // when `tokio::try_join!` polls all four concurrently.
    let grb_window = trigger_window.clone();
    let collect_grb = async move {
        use futures::stream::StreamExt;
        let mut cursor = archive.grb_triggers().find(grb_window).await?;
        let mut out: Vec<GrbTrigger> = Vec::new();
        while let Some(td) = cursor.next().await {
            out.push(td?.trigger);
        }
        Ok::<_, mongodb::error::Error>(out)
    };
    let collect_boom = async move {
        use futures::stream::StreamExt;
        let mut cursor = archive.boom_alerts().find(boom_filter).await?;
        let mut out: Vec<GrbTrigger> = Vec::new();
        while let Some(ba) = cursor.next().await {
            if let Some(trigger) = ba?.transient.as_trigger() {
                out.push(trigger);
            }
        }
        Ok::<_, mongodb::error::Error>(out)
    };
    let frb_window = trigger_window.clone();
    let collect_frb = async move {
        use futures::stream::StreamExt;
        let mut cursor = archive.frb_alerts().find(frb_window).await?;
        let mut out: Vec<GrbTrigger> = Vec::new();
        while let Some(fa) = cursor.next().await {
            out.push(fa?.alert.trigger);
        }
        Ok::<_, mongodb::error::Error>(out)
    };
    let collect_nu = async move {
        use futures::stream::StreamExt;
        let mut cursor = archive.neutrino_alerts().find(trigger_window).await?;
        let mut out: Vec<GrbTrigger> = Vec::new();
        while let Some(na) = cursor.next().await {
            out.push(na?.alert.trigger);
        }
        Ok::<_, mongodb::error::Error>(out)
    };
    let (grb_v, boom_v, frb_v, nu_v) =
        tokio::try_join!(collect_grb, collect_boom, collect_frb, collect_nu)?;
    let mut candidates: Vec<GrbTrigger> =
        Vec::with_capacity(grb_v.len() + boom_v.len() + frb_v.len() + nu_v.len());
    candidates.extend(grb_v);
    candidates.extend(boom_v);
    candidates.extend(frb_v);
    candidates.extend(nu_v);

    // Phase 1b: fetch each candidate's canonical GRB MOC. Bounded
    // concurrency to avoid saturating the storage backend.
    use futures::stream::StreamExt;
    let mut moc_pairs: Vec<(GrbTrigger, Vec<u8>)> = Vec::with_capacity(candidates.len());
    let mut fetches = futures::stream::iter(candidates.into_iter().map(|trigger| async move {
        let bytes = match storage
            .get_grb_skymap(&trigger.instrument, &trigger.trigger_id)
            .await
        {
            Ok(b) => Some(b),
            Err(_) => match build_canonical_moc_fits(&trigger) {
                Ok(b) => {
                    let _ = storage
                        .upsert_grb_skymap(&trigger.instrument, &trigger.trigger_id, b.clone())
                        .await;
                    Some(b)
                }
                Err(e) => {
                    tracing::debug!(
                        instrument = %trigger.instrument,
                        trigger_id = %trigger.trigger_id,
                        "skipping rescan candidate without usable localization: {e}"
                    );
                    None
                }
            },
        };
        bytes.map(|b| (trigger, b))
    }))
    .buffer_unordered(8);
    while let Some(pair) = fetches.next().await {
        if let Some(p) = pair {
            moc_pairs.push(p);
        }
    }

    // Phase 2: parallel CPU via rayon inside spawn_blocking. Arc
    // the skymap + contours so per-thread borrows are cheap.
    use rayon::prelude::*;
    let skymap_bytes = std::sync::Arc::new(skymap_blob.bytes);
    let contour_50_arc = std::sync::Arc::new(contour_50_parsed);
    let contour_90_arc = std::sync::Arc::new(contour_90_parsed);
    let time_window_sec = opts.time_window_sec;
    let pvalue_opts = opts.pvalue_opts;
    let computed: Vec<(GrbTrigger, CrossMatchResult)> = tokio::task::spawn_blocking(move || {
        moc_pairs
            .into_par_iter()
            .filter_map(|(trigger, grb_moc_bytes)| {
                match cross_match_with_contours(
                    &trigger,
                    t_0,
                    gw_far_hz,
                    skymap_bytes.as_ref(),
                    &grb_moc_bytes,
                    contour_50_arc.as_ref().as_ref(),
                    contour_90_arc.as_ref().as_ref(),
                    time_window_sec,
                    crossmatch::rates::GRB_RATE_HZ,
                    pvalue_opts,
                ) {
                    Ok(r) => Some((trigger, r)),
                    Err(e) => {
                        tracing::warn!(
                            instrument = %trigger.instrument,
                            trigger_id = %trigger.trigger_id,
                            "cross-match in rescan failed: {e}"
                        );
                        None
                    }
                }
            })
            .collect()
    })
    .await?;

    // Phase 3: preserve operator-set `associated` + upsert.
    let mut results: Vec<CrossMatchDoc> = Vec::with_capacity(computed.len());
    for (trigger, result) in computed {
        let preserved_associated = archive
            .cross_matches()
            .find_one(doc! {
                "_id.superevent_id": superevent_id,
                "_id.instrument": &trigger.instrument,
                "_id.trigger_id": &trigger.trigger_id,
            })
            .await
            .ok()
            .flatten()
            .map(|d| d.result.associated)
            .unwrap_or(false);
        let mut doc = CrossMatchDoc::new(superevent_id, &trigger, result);
        doc.result.associated = preserved_associated;
        archive.upsert_cross_match(&doc).await?;
        results.push(doc);
    }

    // Best joint FAR first; missing FAR sorts to the bottom.
    results.sort_by(|a, b| {
        let av = a
            .result
            .joint_far_remapped_per_year
            .or(a.result.joint_far_per_year)
            .unwrap_or(f64::INFINITY);
        let bv = b
            .result
            .joint_far_remapped_per_year
            .or(b.result.joint_far_per_year)
            .unwrap_or(f64::INFINITY);
        av.partial_cmp(&bv).unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(results)
}
