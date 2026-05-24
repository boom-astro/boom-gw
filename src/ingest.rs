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
    Archive, ArchiveError, BoomAlertDoc, FrbAlertDoc, GrbTriggerDoc, IceCubeLvkSearchDoc,
    NeutrinoAlertDoc, SupereventDoc,
};
use crate::boom::BoomTransient;
use crate::clustering::Superevent;
use crate::contour::{compute_contour_moc, compute_skymap_centroid};
use crate::frb::FrbAlert;
use crate::grb::{build_canonical_moc_fits, GrbTrigger};
use crate::icecube_lvk::IceCubeLvkSearch;
use crate::neutrino::NeutrinoAlert;
use crate::storage::skymap::{SkymapBlob, SkymapStorage};
use mongodb::bson::doc;

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("archive error: {0}")]
    Archive(#[from] ArchiveError),
    #[error("skymap storage error: {0}")]
    Storage(#[from] crate::storage::skymap::SkymapStorageError),
    #[error("superevent id mismatch: body {body:?} != url {url:?}")]
    SuperEventIdMismatch { body: String, url: String },
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
                    if let Err(e) = storage
                        .upsert_contour(&superevent.id, level_pct, moc)
                        .await
                    {
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
