//! GW × GRB spatial/temporal cross-matching, RAVEN style.
//!
//! The spatial integral is the real work: given a BAYESTAR multi-
//! order skymap FITS for the GW event and a **canonical GRB MOC**
//! (synthesized at ingest from whatever shape the alert provided —
//! cone, ellipse, real HEALPix), compute the cumulative GW
//! localization probability contained in the GRB region. The CDS
//! `moc` crate's `sum_from_fits_multiordermap` integrates
//! PROBDENSITY × cell_area over every cell intersecting an arbitrary
//! MOC, so the math is the same regardless of what the GRB shape
//! actually was — that's the whole point of canonicalizing at
//! ingest (cf. SkyPortal's `gcn.get_skymap`).
//!
//! Algorithm and FAR formula ported from
//! `origen/crates/mm-correlator/src/spatial.rs`, with the
//! O4-calibration tests left behind (they live in origen's repo
//! against the observing-scenarios injection set).
//!
//! References:
//! * RAVEN: <https://doi.org/10.3847/1538-4357/aabfd2>
//! * IVOA Multi-Order Coverage HEALPix: <https://www.ivoa.net/documents/MOC/>
//! * BAYESTAR sky-map format: <https://lscsoft.docs.ligo.org/ligo.skymap/io/fits.html>

use std::io::{BufReader, Cursor};

use moc::deser::fits::multiordermap::sum_from_fits_multiordermap;
use moc::deser::fits::{from_fits_ivoa, MocIdxType, MocQtyType};
use moc::moc::range::RangeMOC;
use moc::qty::Hpx;
use thiserror::Error;

use crate::grb::{CrossMatchResult, GrbTrigger, SkyPosition};

/// Default Hpx depth used when building the cone MOC. 10 → NSIDE
/// 1024 → pixel size ~3.4′, comfortably below the smallest GRB
/// error circles we care about (Swift-BAT ~2′) and above what
/// BAYESTAR typically resolves (NSIDE 512 / depth 9 for most O4
/// triggers). `from_cone` will up-sample as needed for tiny radii.
pub const DEFAULT_CONE_DEPTH: u8 = 10;

/// Pre-computed event rates for the RAVEN joint-FAR formula.
/// Values follow `origen::mm_correlator::spatial::background_rates`
/// which in turn cites the RAVEN paper + LIGO-T2400116.
pub mod rates {
    /// Combined Fermi-GBM + Swift-BAT + SVOM ECLAIRs GRB rate,
    /// 325/year → events per second.
    pub const GRB_RATE_HZ: f64 = 325.0 / (365.25 * 24.0 * 3600.0);
    /// Sub-threshold GRB rate, 65/year → events per second.
    pub const SUBGRB_RATE_HZ: f64 = 65.0 / (365.25 * 24.0 * 3600.0);
}

#[derive(Debug, Error)]
pub enum CrossMatchError {
    #[error("GRB trigger has no localization (position is None)")]
    GrbWithoutPosition,
    #[error("GRB error radius must be positive; got {0}")]
    InvalidErrorRadius(f64),
    #[error("moc cone build failed: {0}")]
    Cone(String),
    #[error("skymap fits read / integration failed: {0}")]
    Fits(String),
    #[error("contour MOC fits read failed: {0}")]
    ContourFits(String),
}

/// Integrate the BAYESTAR localization probability inside a GRB
/// MOC region. Returns a probability in [0, 1].
///
/// `gw_skymap_fits` is the GW multi-order probability density map
/// (BAYESTAR output). `grb_moc_fits` is the canonical GRB MOC
/// FITS as stored by [`crate::storage::skymap::SkymapStorage::upsert_grb_skymap`]
/// — built at ingest from cone parameters, an ellipse, or a real
/// HEALPix posterior. We don't care which: the moc crate's
/// `sum_from_fits_multiordermap` integrates PROBDENSITY × cell area
/// over every cell of the GW map that intersects the MOC.
pub fn spatial_overlap(gw_skymap_fits: &[u8], grb_moc_fits: &[u8]) -> Result<f64, CrossMatchError> {
    let grb_moc = parse_grb_moc(grb_moc_fits)?;
    let reader = BufReader::new(Cursor::new(gw_skymap_fits));
    sum_from_fits_multiordermap(reader, &grb_moc)
        .map_err(|e| CrossMatchError::Fits(format!("{e:?}")))
}

/// Parse a MOC FITS payload into a `RangeMOC` — the canonical
/// in-memory representation we hand to set ops. Used by both the
/// spatial-overlap path and the p-value Monte Carlo.
pub fn parse_grb_moc(grb_moc_fits: &[u8]) -> Result<RangeMOC<u64, Hpx<u64>>, CrossMatchError> {
    let reader = BufReader::new(Cursor::new(grb_moc_fits));
    let moc_type = from_fits_ivoa(reader)
        .map_err(|e| CrossMatchError::ContourFits(format!("grb moc: {e:?}")))?;
    match moc_type {
        MocIdxType::U64(MocQtyType::Hpx(m)) => Ok(m.collect()),
        _ => Err(CrossMatchError::ContourFits(
            "GRB MOC was not a u64 HEALPix MOC".into(),
        )),
    }
}

/// Test whether `position` falls inside a previously-computed
/// credible-region MOC (we already pre-compute the 50% and 90%
/// regions when a sky map is attached — see `gw-clusterer`).
/// Returns `false` (not an error) when the position is missing,
/// so callers can fold both checks into a single chained query.
pub fn position_in_contour(
    contour_fits: &[u8],
    position: &SkyPosition,
) -> Result<bool, CrossMatchError> {
    use cdshealpix::nested::hash;
    use moc::deser::fits::{from_fits_ivoa, MocIdxType, MocQtyType};

    let reader = BufReader::new(Cursor::new(contour_fits));
    let moc_type =
        from_fits_ivoa(reader).map_err(|e| CrossMatchError::ContourFits(format!("{e:?}")))?;
    let hpx_moc: RangeMOC<u64, Hpx<u64>> = match moc_type {
        MocIdxType::U64(MocQtyType::Hpx(m)) => m.collect(),
        _ => {
            return Err(CrossMatchError::ContourFits(
                "contour MOC was not a u64 HEALPix MOC".into(),
            ));
        }
    };

    let depth = hpx_moc.depth_max();
    let lon = position.ra.to_radians();
    let lat = position.dec.to_radians();
    let ipix = hash(depth, lon, lat);
    Ok(hpx_moc.contains_val(&ipix))
}

/// Settings that control the empirical-p-value Monte Carlo. When
/// supplied, [`cross_match`] rotates the GRB skymap to `n_trials`
/// random sky positions and computes the corrected joint FAR via
/// [`crate::pvalue::far_remapped`]. When `None`, the cross-match
/// runs the classical RAVEN path only — useful when the caller
/// doesn't want to pay the Monte Carlo cost.
#[derive(Debug, Clone, Copy)]
pub struct PvalueOpts {
    /// Number of random rotations. Each rotation is O(nnz)
    /// arithmetic so a few hundred trials runs in milliseconds.
    pub n_trials: usize,
    /// Maximum GW pipeline FAR threshold in Hz, used by the
    /// remapped-FAR formula. Pass `2.0 / 86400.0` for the
    /// "two-per-day" calibration used by LIGO/Virgo.
    pub far_gw_max_hz: f64,
    /// Optional RNG seed for reproducibility.
    pub seed: Option<u64>,
}

/// Compute a full cross-match between one GW superevent and one
/// GRB trigger.
///
/// * `superevent_t0` — GW merger time in GPS seconds.
/// * `gw_far_hz` — preferred-event FAR in Hz (used in joint FAR).
/// * `gw_skymap_fits` — multi-order BAYESTAR PROBDENSITY map.
/// * `grb_moc_fits` — canonical GRB MOC FITS bytes (synthesized at
///   ingest by [`crate::grb::build_canonical_moc_fits`] and stored
///   via [`crate::storage::skymap::SkymapStorage::upsert_grb_skymap`]).
///   The cross-match never re-synthesizes the GRB shape — that's
///   ingest's job, so this stays format-agnostic.
/// * `contour_50` / `contour_90` — pre-computed GW credible-region
///   MOC FITS. `None` skips the in-CR flag but doesn't fail.
/// * `time_window_sec` — coincidence window in seconds.
/// * `grb_rate_hz` — assumed background GRB rate.
pub fn cross_match(
    trigger: &GrbTrigger,
    superevent_t0: f64,
    gw_far_hz: f64,
    gw_skymap_fits: &[u8],
    grb_moc_fits: &[u8],
    contour_50: Option<&[u8]>,
    contour_90: Option<&[u8]>,
    time_window_sec: f64,
    grb_rate_hz: f64,
    pvalue: Option<PvalueOpts>,
) -> Result<CrossMatchResult, CrossMatchError> {
    let position = trigger
        .position
        .ok_or(CrossMatchError::GrbWithoutPosition)?;
    let radius_deg = trigger
        .error_radius_deg
        .or_else(|| Some(position.error_radius_deg()))
        .filter(|r| r.is_finite() && *r > 0.0)
        .ok_or(CrossMatchError::InvalidErrorRadius(0.0))?;

    let time_offset_sec = trigger.trigger_time - superevent_t0;
    let spatial_overlap_val = spatial_overlap(gw_skymap_fits, grb_moc_fits)?;
    let in_50cr = match contour_50 {
        Some(bytes) => position_in_contour(bytes, &position)?,
        None => false,
    };
    let in_90cr = match contour_90 {
        Some(bytes) => position_in_contour(bytes, &position)?,
        None => false,
    };

    let joint_far_per_year =
        raven_joint_far_per_year(time_window_sec, grb_rate_hz, gw_far_hz, spatial_overlap_val);

    // Empirical p-value path — optional. Uses MOC set ops end to
    // end: we load the GW 90% contour MOC (already pre-computed
    // and stored alongside the skymap), build a cone MOC for the
    // GRB error region, intersect at every Monte Carlo rotation,
    // and count how often the intersection area meets-or-exceeds
    // the observed. O(n_ranges) per trial — fast at any depth.
    //
    // Falls back to None if no 90% contour is in storage; the
    // observed-overlap PROBDENSITY integral and classical RAVEN
    // FAR are still produced.
    let (p_value, p_value_trials, joint_far_remapped_per_year) = match pvalue {
        Some(opts) if opts.n_trials > 0 => match contour_90 {
            Some(bytes) => {
                let gw_moc = crate::pvalue::load_contour_moc(bytes)
                    .map_err(|e| CrossMatchError::Fits(format!("pvalue contour load: {e}")))?;
                let res = crate::pvalue::empirical_pvalue(
                    &gw_moc,
                    position.ra,
                    position.dec,
                    radius_deg,
                    opts.n_trials,
                    opts.seed,
                )
                .map_err(|e| CrossMatchError::Fits(format!("pvalue compute: {e}")))?;
                let remapped = crate::pvalue::far_remapped(
                    gw_far_hz,
                    grb_rate_hz,
                    time_window_sec,
                    res.p_value,
                    opts.far_gw_max_hz,
                );
                (Some(res.p_value), Some(res.n_trials), remapped)
            }
            None => (None, None, None),
        },
        _ => (None, None, None),
    };

    Ok(CrossMatchResult {
        time_offset_sec,
        spatial_overlap: spatial_overlap_val,
        in_50cr,
        in_90cr,
        joint_far_per_year,
        p_value,
        p_value_trials,
        joint_far_remapped_per_year,
    })
}

/// RAVEN spatiotemporal FAR, converted from per-second to per-year
/// for human readability. Returns `None` when spatial_overlap is 0
/// (formula diverges).
pub fn raven_joint_far_per_year(
    time_window_sec: f64,
    ext_rate_hz: f64,
    gw_far_hz: f64,
    spatial_overlap: f64,
) -> Option<f64> {
    if spatial_overlap <= 0.0 || !spatial_overlap.is_finite() {
        return None;
    }
    // temporal_far = Δt × R_ext × FAR_GW (in Hz)
    // spatiotemporal_far = temporal_far / spatial_overlap
    let temporal_far_hz = time_window_sec * ext_rate_hz * gw_far_hz;
    let joint_far_hz = temporal_far_hz / spatial_overlap;
    Some(joint_far_hz * 365.25 * 24.0 * 3600.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raven_far_is_none_when_no_overlap() {
        let far = raven_joint_far_per_year(10.0, rates::GRB_RATE_HZ, 1e-7, 0.0);
        assert!(far.is_none());
    }

    #[test]
    fn raven_far_scales_inversely_with_overlap() {
        let high = raven_joint_far_per_year(10.0, rates::GRB_RATE_HZ, 1e-7, 0.5).unwrap();
        let low = raven_joint_far_per_year(10.0, rates::GRB_RATE_HZ, 1e-7, 0.01).unwrap();
        // Smaller overlap → larger joint FAR (more likely chance coincidence).
        assert!(low > high * 10.0, "high={high}, low={low}");
    }

    fn fake_trigger(ra: f64, dec: f64, err_deg: f64) -> GrbTrigger {
        GrbTrigger {
            trigger_id: "X".into(),
            instrument: "I".into(),
            trigger_time: 0.0,
            position: Some(SkyPosition::new(ra, dec, err_deg * 3600.0)),
            significance: 0.0,
            skymap_url: None,
            error_radius_deg: Some(err_deg),
        }
    }

    #[test]
    fn spatial_overlap_rejects_garbage_gw_fits() {
        // Build a real GRB MOC then hand it bogus GW skymap bytes.
        let grb = crate::grb::build_canonical_moc_fits(&fake_trigger(0.0, 0.0, 1.0)).unwrap();
        let err = spatial_overlap(b"NOT A FITS FILE", &grb).unwrap_err();
        assert!(matches!(err, CrossMatchError::Fits(_)));
    }

    #[test]
    fn spatial_overlap_rejects_garbage_grb_moc() {
        let err = spatial_overlap(b"", b"NOT A MOC FITS").unwrap_err();
        assert!(matches!(err, CrossMatchError::ContourFits(_)));
    }

    /// End-to-end test against a real BAYESTAR skymap on disk. The
    /// path matches the contour-module ignored test; same opt-in
    /// activation:
    ///
    ///   BAYESTAR_FIXTURE=/tmp/S000000.fits \
    ///     cargo test --lib crossmatch::tests::real_bayestar_spatial -- --ignored
    ///
    /// Asserts (a) full-sphere GRB MOC recovers ≈1.0 of the GW
    /// probability mass, and (b) larger GRB cones integrate at
    /// least as much as smaller ones at the same center.
    #[test]
    #[ignore = "needs a real BAYESTAR FITS fixture on disk"]
    fn real_bayestar_spatial() {
        let path =
            std::env::var("BAYESTAR_FIXTURE").unwrap_or_else(|_| "/tmp/S000000.fits".to_string());
        let fits = std::fs::read(&path).expect("fixture");

        let full_sky_grb =
            crate::grb::build_canonical_moc_fits(&fake_trigger(180.0, 0.0, 180.0)).unwrap();
        let full_sky = spatial_overlap(&fits, &full_sky_grb).unwrap();
        assert!(
            (full_sky - 1.0).abs() < 0.05,
            "full-sky GRB MOC should integrate to ~1.0; got {full_sky}"
        );

        let small_grb =
            crate::grb::build_canonical_moc_fits(&fake_trigger(180.0, 0.0, 1.0)).unwrap();
        let large_grb =
            crate::grb::build_canonical_moc_fits(&fake_trigger(180.0, 0.0, 30.0)).unwrap();
        let small = spatial_overlap(&fits, &small_grb).unwrap();
        let large = spatial_overlap(&fits, &large_grb).unwrap();
        assert!(
            large >= small,
            "30° GRB cone should cover ≥ 1° cone at the same center; small={small}, large={large}"
        );
    }
}
