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
///
/// Convenience wrapper around [`parse_contour_moc`] +
/// [`position_in_parsed_contour`] for callers that have raw FITS
/// bytes and only need a single lookup. `scan_cross_matches` and
/// other batched callers should parse the MOC once via
/// [`parse_contour_moc`] and reuse the parsed form per candidate.
pub fn position_in_contour(
    contour_fits: &[u8],
    position: &SkyPosition,
) -> Result<bool, CrossMatchError> {
    let hpx_moc = parse_contour_moc(contour_fits)?;
    Ok(position_in_parsed_contour(&hpx_moc, position))
}

/// Parse a credible-region MOC FITS into the in-memory
/// `RangeMOC` form that the cross-match call site can hand to
/// [`position_in_parsed_contour`] and to
/// [`crate::pvalue::empirical_pvalue`] without re-parsing per
/// candidate. Scan endpoints with N candidates against the same
/// GW skymap save up to 2N parses by holding the result once.
pub fn parse_contour_moc(contour_fits: &[u8]) -> Result<RangeMOC<u64, Hpx<u64>>, CrossMatchError> {
    use moc::deser::fits::{from_fits_ivoa, MocIdxType, MocQtyType};
    let reader = BufReader::new(Cursor::new(contour_fits));
    let moc_type =
        from_fits_ivoa(reader).map_err(|e| CrossMatchError::ContourFits(format!("{e:?}")))?;
    match moc_type {
        MocIdxType::U64(MocQtyType::Hpx(m)) => Ok(m.collect()),
        _ => Err(CrossMatchError::ContourFits(
            "contour MOC was not a u64 HEALPix MOC".into(),
        )),
    }
}

/// Membership test against a pre-parsed credible-region MOC. The
/// hot-path companion to [`position_in_contour`].
///
/// `RangeMOC::contains_val` expects a pixel at the Quantity's
/// `MAX_DEPTH` (29 for `Hpx<u64>`), NOT the MOC's `depth_max` —
/// `contains_cell(depth, idx)` is the right entry point when we
/// have a `(depth, ipix)` pair.
pub fn position_in_parsed_contour(
    hpx_moc: &RangeMOC<u64, Hpx<u64>>,
    position: &SkyPosition,
) -> bool {
    use cdshealpix::nested::hash;
    let depth = hpx_moc.depth_max();
    let lon = position.ra.to_radians();
    let lat = position.dec.to_radians();
    let ipix = hash(depth, lon, lat);
    hpx_moc.contains_cell(depth, ipix)
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
    let c50_parsed = contour_50.map(parse_contour_moc).transpose()?;
    let c90_parsed = contour_90.map(parse_contour_moc).transpose()?;
    cross_match_with_contours(
        trigger,
        superevent_t0,
        gw_far_hz,
        gw_skymap_fits,
        grb_moc_fits,
        c50_parsed.as_ref(),
        c90_parsed.as_ref(),
        time_window_sec,
        grb_rate_hz,
        pvalue,
    )
}

/// Hot-path variant of [`cross_match`] for callers that have
/// already parsed the credible-region MOCs and want to reuse them
/// across many triggers — `scan_cross_matches` does this for
/// every candidate against the same GW skymap. Drops the
/// per-trigger contour parse cost from 3×N (50, 90, plus 90
/// again inside the p-value path) to 2 total.
#[allow(clippy::too_many_arguments)]
pub fn cross_match_with_contours(
    trigger: &GrbTrigger,
    superevent_t0: f64,
    gw_far_hz: f64,
    gw_skymap_fits: &[u8],
    grb_moc_fits: &[u8],
    contour_50: Option<&RangeMOC<u64, Hpx<u64>>>,
    contour_90: Option<&RangeMOC<u64, Hpx<u64>>>,
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
    let in_50cr = contour_50
        .map(|m| position_in_parsed_contour(m, &position))
        .unwrap_or(false);
    let in_90cr = contour_90
        .map(|m| position_in_parsed_contour(m, &position))
        .unwrap_or(false);

    let joint_far_per_year =
        raven_joint_far_per_year(time_window_sec, grb_rate_hz, gw_far_hz, spatial_overlap_val);

    // Empirical p-value path — optional. Reuses `contour_90` if
    // present so we don't pay the parse cost again here. The
    // pre-parsed `RangeMOC<u64, Hpx<u64>>` is exactly the shape
    // `pvalue::empirical_pvalue` consumes.
    let (p_value, p_value_trials, joint_far_remapped_per_year) = match pvalue {
        Some(opts) if opts.n_trials > 0 => match contour_90 {
            Some(gw_moc) => {
                let res = crate::pvalue::empirical_pvalue(
                    gw_moc,
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

    // RAVEN-targeted joint FAR: only defined when the trigger
    // reports a per-trigger FAR and the originating instrument
    // is one of the targeted-search-supported pipelines (Fermi-
    // GBM, Swift-BAT — `gwcelery.tasks.raven`'s SubGRBTargeted
    // analysis). Picks the per-pipeline default thresholds from
    // `TargetedJointFarOpts::{fermi,swift}_subgrb_default`.
    let targeted_joint_far_per_year = targeted_opts_for(trigger).and_then(|opts| {
        raven_targeted_joint_far_per_year(time_window_sec, opts, gw_far_hz, spatial_overlap_val)
    });

    Ok(CrossMatchResult {
        time_offset_sec,
        spatial_overlap: spatial_overlap_val,
        in_50cr,
        in_90cr,
        joint_far_per_year,
        p_value,
        p_value_trials,
        joint_far_remapped_per_year,
        targeted_joint_far_per_year,
        // Always-false at compute time; the operator commits via
        // PATCH /cross-matches/{instrument}/{trigger_id}.
        associated: false,
    })
}

/// Pick the right `TargetedJointFarOpts` for a trigger based on
/// its `instrument` label, when the trigger carries a per-trigger
/// FAR. Returns `None` if either the FAR is missing or the
/// instrument isn't in RAVEN's SubGRBTargeted catalogue —
/// callers should fall back to the untargeted FAR in that case.
fn targeted_opts_for(trigger: &GrbTrigger) -> Option<TargetedJointFarOpts> {
    let far = trigger.far_hz?;
    if trigger.instrument.starts_with("Fermi-GBM") {
        Some(TargetedJointFarOpts::fermi_subgrb_default(far))
    } else if trigger.instrument == "Swift-BAT" {
        Some(TargetedJointFarOpts::swift_subgrb_default(far))
    } else {
        None
    }
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

/// Knobs for RAVEN's *targeted* joint FAR (the `SubGRBTargeted`
/// path in `ligo.raven.search.coinc_far`). Used when both the GW
/// pipeline and the external pipeline are running below their
/// individual significance thresholds — e.g. Fermi/Swift looking
/// at "subthreshold" GRBs that they wouldn't publish on their
/// own, paired with a sub-threshold GW candidate. In that
/// regime the untargeted formula isn't right because neither
/// side is detecting independently; the joint significance has
/// to account for the "both below threshold" sample-space.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TargetedJointFarOpts {
    /// The external trigger's own FAR in Hz (Fermi-GBM's reported
    /// `FAR` field, Swift's catalog rate for this trigger class,
    /// etc.). Different from `ext_rate_hz` in the untargeted
    /// path: there the rate is a population-wide constant; here
    /// it's the per-trigger detection FAR.
    pub far_ext_hz: f64,
    /// Maximum GW pipeline FAR threshold considered by the
    /// targeted search. Defaults to 2/day for LIGO-Virgo —
    /// matches RAVEN's `far_gw_thresh = 2 / (3600 * 24)` for the
    /// SubGRBTargeted search.
    pub far_gw_thresh_hz: f64,
    /// Maximum external pipeline FAR threshold. RAVEN uses
    /// 1/10000 (per-second) for Fermi and 1/1000 for Swift in
    /// the SubGRBTargeted search. Caller picks based on the
    /// originating instrument.
    pub far_ext_thresh_hz: f64,
}

impl TargetedJointFarOpts {
    /// RAVEN's default thresholds for the Fermi SubGRBTargeted
    /// search — `far_gw_thresh = 2/day`, `far_ext_thresh = 1/10000`.
    pub fn fermi_subgrb_default(far_ext_hz: f64) -> Self {
        Self {
            far_ext_hz,
            far_gw_thresh_hz: 2.0 / 86400.0,
            far_ext_thresh_hz: 1.0 / 10_000.0,
        }
    }

    /// RAVEN's default thresholds for the Swift SubGRBTargeted
    /// search — `far_gw_thresh = 2/day`, `far_ext_thresh = 1/1000`.
    pub fn swift_subgrb_default(far_ext_hz: f64) -> Self {
        Self {
            far_ext_hz,
            far_gw_thresh_hz: 2.0 / 86400.0,
            far_ext_thresh_hz: 1.0 / 1_000.0,
        }
    }
}

/// Targeted (SubGRBTargeted) joint FAR per year, mirroring
/// RAVEN's `search.coinc_far(... joint_far_method='targeted' ...)`.
///
/// The math is the CDF of the product of two independent
/// uniformly-distributed random variables on `[0, threshold]` —
/// see <https://en.wikipedia.org/wiki/Product_distribution>:
///
/// ```text
/// z     = Δt × FAR_ext × FAR_GW
/// z_max = Δt × FAR_ext_thresh × FAR_GW_thresh
/// temporal_far_hz = z × (1 − log(z / z_max))
/// ```
///
/// `spatiotemporal_far = temporal_far / spatial_overlap`. Returns
/// `None` when the overlap is zero (same as the untargeted
/// path), when `z ≥ z_max` (caller is outside the targeted
/// search's sample space — fall back to the untargeted FAR), or
/// when `z` is non-finite.
pub fn raven_targeted_joint_far_per_year(
    time_window_sec: f64,
    opts: TargetedJointFarOpts,
    gw_far_hz: f64,
    spatial_overlap: f64,
) -> Option<f64> {
    if spatial_overlap <= 0.0 || !spatial_overlap.is_finite() {
        return None;
    }
    let z = time_window_sec * opts.far_ext_hz * gw_far_hz;
    let z_max = time_window_sec * opts.far_ext_thresh_hz * opts.far_gw_thresh_hz;
    if !z.is_finite() || z <= 0.0 || z_max <= 0.0 || z >= z_max {
        return None;
    }
    let temporal_far_hz = z * (1.0 - (z / z_max).ln());
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

    #[test]
    fn targeted_far_zero_overlap_returns_none() {
        let opts = TargetedJointFarOpts::fermi_subgrb_default(1e-6);
        assert!(raven_targeted_joint_far_per_year(10.0, opts, 1e-7, 0.0).is_none());
    }

    #[test]
    fn targeted_far_outside_sample_space_returns_none() {
        // FAR_ext = far_ext_thresh AND FAR_GW = far_gw_thresh →
        // z == z_max → log(z/z_max)=0 → temporal_far == z.
        // Bumping either above-threshold pushes z > z_max and
        // the targeted formula no longer applies.
        let opts = TargetedJointFarOpts {
            far_ext_hz: 2e-4, // > far_ext_thresh (1/10000 = 1e-4)
            far_gw_thresh_hz: 2.0 / 86400.0,
            far_ext_thresh_hz: 1.0 / 10_000.0,
        };
        let gw_far_above_thresh = 2.0 * (2.0 / 86400.0); // > far_gw_thresh
        assert!(
            raven_targeted_joint_far_per_year(10.0, opts, gw_far_above_thresh, 0.5).is_none(),
            "above both thresholds, targeted formula must decline"
        );
    }

    #[test]
    fn targeted_far_more_significant_than_untargeted_in_subthreshold() {
        // Operating point where the SubGRBTargeted analysis was
        // designed to help: both detectors well below threshold
        // (their reported FAR is small compared to the search
        // threshold), but the joint coincidence is meaningful.
        // The targeted formula should give a smaller (more
        // significant) joint FAR than the untargeted one in
        // that regime — that's the whole reason the targeted
        // analysis exists.
        let time_window = 10.0;
        let gw_far = 1e-7; // ~1 / 4 months — well below 2/day threshold
        let overlap = 0.3;
        let untargeted =
            raven_joint_far_per_year(time_window, rates::GRB_RATE_HZ, gw_far, overlap).unwrap();
        let opts = TargetedJointFarOpts::fermi_subgrb_default(1e-7);
        let targeted = raven_targeted_joint_far_per_year(time_window, opts, gw_far, overlap)
            .expect("targeted should be defined in this regime");
        assert!(
            targeted < untargeted,
            "targeted ({targeted}) should be more significant than untargeted ({untargeted}) in the SubGRBTargeted regime"
        );
    }

    #[test]
    fn targeted_far_matches_raven_reference_formula() {
        // Direct numerical reproduction of the formula in
        // `ligo.raven.search.coinc_far` for the targeted branch:
        //   z     = (th-tl) * far_ext * se_far
        //   z_max = (th-tl) * far_ext_thresh * far_gw_thresh
        //   temporal_far = z * (1 − log(z/z_max))
        let time_window = 10.0;
        let gw_far = 1e-7;
        let overlap = 0.4;
        let opts = TargetedJointFarOpts {
            far_ext_hz: 5e-6,
            far_gw_thresh_hz: 2.0 / 86400.0,
            far_ext_thresh_hz: 1.0 / 10_000.0,
        };
        let z = time_window * opts.far_ext_hz * gw_far;
        let z_max = time_window * opts.far_ext_thresh_hz * opts.far_gw_thresh_hz;
        let expected_temporal_hz = z * (1.0 - (z / z_max).ln());
        let expected_joint_per_year = (expected_temporal_hz / overlap) * 365.25 * 24.0 * 3600.0;
        let got = raven_targeted_joint_far_per_year(time_window, opts, gw_far, overlap).unwrap();
        let rel_err = (got - expected_joint_per_year).abs() / expected_joint_per_year.abs();
        assert!(
            rel_err < 1e-12,
            "reference mismatch: got {got}, expected {expected_joint_per_year} (rel err {rel_err})"
        );
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
            far_hz: None,
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

    // ---------------------------------------------------------------------
    // Property / invariant tests for the MOC set-ops.
    //
    // Instead of fixed fixtures, these sweep many randomized (center,
    // radius) configurations — built with a *fixed* RNG seed so CI is
    // deterministic — and assert invariants the spatial-overlap integral
    // and credible-region membership test must always satisfy, whatever
    // the geometry. They use the shared synthetic-skymap builder
    // (`crate::skymap_synth`), so the GW side is the same hand-written
    // multi-order FITS the live scan consumes.
    // ---------------------------------------------------------------------

    use crate::grb::SkyPosition;
    use crate::skymap_synth::build_uniform_cone_skymap;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    /// Canonical GRB MOC for a cone at `(ra, dec)` of radius `r` deg.
    fn grb_cone(ra: f64, dec: f64, r: f64) -> Vec<u8> {
        crate::grb::build_canonical_moc_fits(&fake_trigger(ra, dec, r)).unwrap()
    }

    /// Antipode of a sky position, for building provably-disjoint cones.
    fn antipode(ra: f64, dec: f64) -> (f64, f64) {
        ((ra + 180.0).rem_euclid(360.0), -dec)
    }

    #[test]
    fn overlap_is_always_a_valid_probability() {
        // For any GW cone and any GRB cone, the integral is a
        // probability mass in [0, 1] and never NaN/Inf.
        let mut rng = StdRng::seed_from_u64(0xB0017E57_60);
        for _ in 0..40 {
            let ra = rng.gen_range(0.0..360.0);
            let dec = rng.gen_range(-70.0..70.0);
            let gw_r = rng.gen_range(3.0..10.0);
            let gw = build_uniform_cone_skymap(ra, dec, gw_r, 7);

            let gra = rng.gen_range(0.0..360.0);
            let gdec = rng.gen_range(-70.0..70.0);
            let grb_r = rng.gen_range(0.5..8.0);
            let overlap = spatial_overlap(&gw, &grb_cone(gra, gdec, grb_r)).unwrap();

            assert!(
                overlap.is_finite() && (-1e-9..=1.0 + 1e-6).contains(&overlap),
                "overlap out of [0,1]: {overlap} (gw {ra},{dec} r{gw_r}; grb {gra},{gdec} r{grb_r})"
            );
        }
    }

    #[test]
    fn overlap_is_monotonic_in_grb_radius() {
        // Growing the GRB error region (same center) can only capture
        // more — never less — of the GW probability mass.
        let mut rng = StdRng::seed_from_u64(0xB0017E57_61);
        for _ in 0..25 {
            let ra = rng.gen_range(0.0..360.0);
            let dec = rng.gen_range(-70.0..70.0);
            let gw = build_uniform_cone_skymap(ra, dec, 8.0, 7);
            // Concentric GRB cones at the GW center, increasing radius.
            let mut prev = -1.0;
            for r in [0.5, 1.0, 2.0, 4.0, 8.0, 30.0] {
                let o = spatial_overlap(&gw, &grb_cone(ra, dec, r)).unwrap();
                assert!(
                    o >= prev - 1e-6,
                    "overlap dropped as radius grew: {o} < {prev} at r={r} (gw {ra},{dec})"
                );
                prev = o;
            }
        }
    }

    #[test]
    fn full_sky_grb_recovers_all_mass_and_antipodal_recovers_none() {
        // Containment: a GRB region covering the whole sky integrates
        // the entire (normalized) GW posterior → ≈1. Disjoint: a GRB
        // cone at the antipode shares no mass → ≈0.
        let mut rng = StdRng::seed_from_u64(0xB0017E57_62);
        for _ in 0..20 {
            let ra = rng.gen_range(0.0..360.0);
            let dec = rng.gen_range(-60.0..60.0);
            let gw = build_uniform_cone_skymap(ra, dec, 5.0, 7);

            let whole_sky = spatial_overlap(&gw, &grb_cone(ra, dec, 180.0)).unwrap();
            assert!(
                (whole_sky - 1.0).abs() < 0.05,
                "full-sky GRB should recover ≈1.0; got {whole_sky} (gw {ra},{dec})"
            );

            let (ara, adec) = antipode(ra, dec);
            let opposite = spatial_overlap(&gw, &grb_cone(ara, adec, 10.0)).unwrap();
            assert!(
                opposite < 1e-3,
                "antipodal GRB cone should recover ≈0; got {opposite} (gw {ra},{dec})"
            );
        }
    }

    #[test]
    fn credible_region_membership_matches_geometry() {
        // The GRB MOC's own center is inside it; the antipode is not.
        // Exercises parse + `position_in_parsed_contour` over random
        // cones (the same membership test used to set `in_50cr`/`in_90cr`).
        let mut rng = StdRng::seed_from_u64(0xB0017E57_63);
        for _ in 0..40 {
            let ra = rng.gen_range(0.0..360.0);
            let dec = rng.gen_range(-70.0..70.0);
            let r = rng.gen_range(1.0..15.0);
            let moc = parse_grb_moc(&grb_cone(ra, dec, r)).unwrap();

            assert!(
                position_in_parsed_contour(&moc, &SkyPosition::new(ra, dec, r * 3600.0)),
                "cone center ({ra},{dec}) must be inside its own r={r}° MOC"
            );
            let (ara, adec) = antipode(ra, dec);
            assert!(
                !position_in_parsed_contour(&moc, &SkyPosition::new(ara, adec, r * 3600.0)),
                "antipode of ({ra},{dec}) must be outside the r={r}° MOC"
            );
        }
    }

    #[test]
    fn canonical_moc_round_trips_for_random_cones() {
        // Every synthesized GRB MOC parses back to a non-empty u64
        // HEALPix MOC — the ingest→cross-match handoff never produces
        // an unreadable or empty region.
        let mut rng = StdRng::seed_from_u64(0xB0017E57_64);
        for _ in 0..40 {
            let ra = rng.gen_range(0.0..360.0);
            let dec = rng.gen_range(-85.0..85.0);
            let r = rng.gen_range(0.05..20.0);
            let parsed = parse_grb_moc(&grb_cone(ra, dec, r)).unwrap();
            assert!(
                parsed.depth_max() >= 1 && parsed.coverage_percentage() > 0.0,
                "round-tripped MOC is empty for ({ra},{dec}) r={r}°"
            );
        }
    }
}
