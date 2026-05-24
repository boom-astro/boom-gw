//! GW × GRB spatial/temporal cross-matching, RAVEN style.
//!
//! The spatial integral is the real work: given a BAYESTAR multi-
//! order skymap FITS and a GRB error circle (RA, Dec, radius),
//! compute the cumulative GW localization probability contained in
//! the circle. The CDS `moc` crate does the heavy lifting end to
//! end — `RangeMOC::from_cone` builds an HEALPix MOC for the
//! circle, and `sum_from_fits_multiordermap` integrates
//! PROBDENSITY × cell_area over every cell intersecting the MOC.
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
use moc::moc::range::{CellSelection, RangeMOC};
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
/// error circle. Returns a probability in [0, 1].
///
/// `position` is the GRB's best-fit RA/Dec; `radius_deg` is the
/// half-opening angle of the error circle (typically 1σ). For
/// very small radii (< 0.05°) we widen to that floor so the cone
/// MOC has at least one pixel at our default depth — without it,
/// `from_cone` can return an empty MOC and the integral comes out
/// to zero even when the position is dead-center on a high-prob
/// pixel.
pub fn spatial_overlap(
    skymap_fits: &[u8],
    position: &SkyPosition,
    radius_deg: f64,
) -> Result<f64, CrossMatchError> {
    if radius_deg <= 0.0 || !radius_deg.is_finite() {
        return Err(CrossMatchError::InvalidErrorRadius(radius_deg));
    }
    let radius_deg = radius_deg.max(0.05);
    let lon = position.ra.to_radians();
    let lat = position.dec.to_radians();
    let radius = radius_deg.to_radians();

    let cone_moc: RangeMOC<u64, Hpx<u64>> = RangeMOC::from_cone(
        lon,
        lat,
        radius,
        DEFAULT_CONE_DEPTH,
        // delta_depth=2 → moc samples internally at depth+2 then
        // collapses to depth. Matches the default the moc crate's
        // own cone_coverage_approx uses.
        2,
        CellSelection::All,
    );

    let reader = BufReader::new(Cursor::new(skymap_fits));
    sum_from_fits_multiordermap(reader, &cone_moc)
        .map_err(|e| CrossMatchError::Fits(format!("{e:?}")))
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

/// Compute a full cross-match between one GW superevent and one
/// GRB trigger. Pulls all the inputs the caller has already
/// loaded; persistence is the caller's job.
///
/// * `superevent_t0` — GW merger time in GPS seconds.
/// * `gw_far_hz` — preferred-event FAR in Hz (used in joint FAR).
/// * `skymap_fits` — multi-order BAYESTAR FITS bytes.
/// * `contour_50` / `contour_90` — pre-computed credible-region
///   MOC FITS bytes. `None` skips the in-CR flag but doesn't fail.
/// * `time_window_sec` — coincidence window in seconds (RAVEN
///   default for GRB is 10 s).
/// * `grb_rate_hz` — assumed background GRB rate; default to
///   [`rates::GRB_RATE_HZ`].
pub fn cross_match(
    trigger: &GrbTrigger,
    superevent_t0: f64,
    gw_far_hz: f64,
    skymap_fits: &[u8],
    contour_50: Option<&[u8]>,
    contour_90: Option<&[u8]>,
    time_window_sec: f64,
    grb_rate_hz: f64,
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
    let spatial_overlap = spatial_overlap(skymap_fits, &position, radius_deg)?;
    let in_50cr = match contour_50 {
        Some(bytes) => position_in_contour(bytes, &position)?,
        None => false,
    };
    let in_90cr = match contour_90 {
        Some(bytes) => position_in_contour(bytes, &position)?,
        None => false,
    };

    let joint_far_per_year =
        raven_joint_far_per_year(time_window_sec, grb_rate_hz, gw_far_hz, spatial_overlap);

    Ok(CrossMatchResult {
        time_offset_sec,
        spatial_overlap,
        in_50cr,
        in_90cr,
        joint_far_per_year,
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

    #[test]
    fn spatial_overlap_rejects_garbage_fits() {
        let pos = SkyPosition::new(10.0, 20.0, 0.0);
        let err = spatial_overlap(b"NOT A FITS FILE", &pos, 1.0).unwrap_err();
        assert!(matches!(err, CrossMatchError::Fits(_)));
    }

    #[test]
    fn spatial_overlap_rejects_bad_radius() {
        let pos = SkyPosition::new(10.0, 20.0, 0.0);
        assert!(matches!(
            spatial_overlap(b"", &pos, 0.0),
            Err(CrossMatchError::InvalidErrorRadius(_))
        ));
        assert!(matches!(
            spatial_overlap(b"", &pos, -1.0),
            Err(CrossMatchError::InvalidErrorRadius(_))
        ));
    }

    /// End-to-end test against a real BAYESTAR skymap on disk. The
    /// path matches the contour-module ignored test; same opt-in
    /// activation:
    ///
    ///   BAYESTAR_FIXTURE=/tmp/S000000.fits \
    ///     cargo test --lib crossmatch::tests::real_bayestar_spatial -- --ignored
    ///
    /// Asserts the physics-level invariant — full-sphere
    /// integration recovers the unit probability — plus
    /// monotonicity in cone radius. We don't pick magic numbers
    /// for partial coverage because the fixture's localization
    /// position is unknown to this test.
    #[test]
    #[ignore = "needs a real BAYESTAR FITS fixture on disk"]
    fn real_bayestar_spatial() {
        let path =
            std::env::var("BAYESTAR_FIXTURE").unwrap_or_else(|_| "/tmp/S000000.fits".to_string());
        let fits = std::fs::read(&path).expect("fixture");

        let any_position = SkyPosition::new(180.0, 0.0, 0.0);
        let full_sky = spatial_overlap(&fits, &any_position, 180.0).unwrap();
        assert!(
            (full_sky - 1.0).abs() < 0.05,
            "full-sky integral should normalize to ~1.0; got {full_sky}"
        );

        let small = spatial_overlap(&fits, &any_position, 1.0).unwrap();
        let larger = spatial_overlap(&fits, &any_position, 30.0).unwrap();
        assert!(
            larger >= small,
            "30° cone should cover ≥ 1° cone at the same center; small={small}, larger={larger}"
        );
    }
}
