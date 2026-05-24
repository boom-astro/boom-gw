//! Credible-region contour MOC computation for BAYESTAR sky maps.
//!
//! BAYESTAR emits multi-order HEALPix probability density maps in
//! the IVOA "MOC sky map" FITS shape (BINTABLE with UNIQ +
//! PROBDENSITY columns). Aladin Lite v3 cannot render these
//! directly — it speaks plain MOC FITS (a coverage map) and 2-D
//! image FITS, not the density variant. We bridge that gap by
//! computing a MOC of the smallest credible region whose cumulative
//! probability mass equals `level` (e.g. 0.5 for 50%, 0.9 for 90%).
//! Frontend overlays the resulting MOC as a contour.
//!
//! The CDS `moc` crate does the heavy lifting:
//! `from_fits_multiordermap` reads the density map and integrates
//! the highest-density cells until the requested mass is reached,
//! returning a `RangeMOC` covering exactly that region.
//! `to_fits_ivoa` writes it back out as a standard IVOA MOC FITS
//! that Aladin's `A.MOCFromURL` consumes happily.

use std::io::{BufReader, Cursor};

use moc::moc::range::RangeMOC;
use moc::moc::{RangeMOCIntoIterator, RangeMOCIterator};
use moc::qty::Hpx;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ContourError {
    #[error("level must be in (0, 1]; got {0}")]
    InvalidLevel(f64),
    #[error("moc fits read failed: {0}")]
    FitsRead(String),
    #[error("moc fits write failed: {0}")]
    FitsWrite(String),
}

/// Build a MOC FITS describing the smallest credible region with
/// cumulative probability `level` (0 < level ≤ 1). Returns the
/// IVOA MOC FITS bytes ready to ship to the frontend.
///
/// `level=0.9` → the 90% credible region. The moc crate iterates
/// cells in descending PROBDENSITY order, accumulating mass until
/// the threshold is reached.
pub fn compute_contour_moc(skymap_fits: &[u8], level: f64) -> Result<Vec<u8>, ContourError> {
    if !(level > 0.0 && level <= 1.0) {
        return Err(ContourError::InvalidLevel(level));
    }
    let reader = BufReader::new(Cursor::new(skymap_fits));
    let moc: RangeMOC<u64, Hpx<u64>> =
        moc::deser::fits::multiordermap::from_fits_multiordermap(
            reader, 0.0,    // cumul_from
            level,  // cumul_to
            false,  // asc=false → descend from highest density (credible region)
            false,  // strict
            false,  // no_split
            false,  // reverse_decent
        )
        .map_err(|e| ContourError::FitsRead(format!("{e:?}")))?;
    let mut out = Vec::new();
    moc.into_range_moc_iter()
        .to_fits_ivoa(None, None, &mut out)
        .map_err(|e| ContourError::FitsWrite(format!("{e:?}")))?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_zero_level() {
        let err = compute_contour_moc(b"", 0.0).unwrap_err();
        assert!(matches!(err, ContourError::InvalidLevel(_)));
    }

    #[test]
    fn rejects_level_above_one() {
        let err = compute_contour_moc(b"", 1.5).unwrap_err();
        assert!(matches!(err, ContourError::InvalidLevel(_)));
    }

    #[test]
    fn rejects_garbage_fits() {
        // A valid level but non-FITS bytes — moc should error out
        // cleanly rather than panic.
        let err = compute_contour_moc(b"NOT A FITS FILE", 0.9).unwrap_err();
        assert!(matches!(err, ContourError::FitsRead(_)));
    }

    /// Smoke test against a real BAYESTAR multi-order skymap. Only
    /// runs when the fixture exists on disk — that fixture is too
    /// large to check into the repo and is intentionally opt-in:
    ///
    ///   BAYESTAR_FIXTURE=/tmp/S000000.fits cargo test --lib contour::tests::real_bayestar_fixture -- --ignored
    ///
    /// Asserts that (a) the contour bytes are a non-empty FITS
    /// payload, (b) the 50% region is smaller than the 90%, which
    /// is the only invariant the user-visible behavior depends on.
    #[test]
    #[ignore = "needs a real BAYESTAR FITS fixture on disk"]
    fn real_bayestar_fixture() {
        let path = std::env::var("BAYESTAR_FIXTURE")
            .unwrap_or_else(|_| "/tmp/S000000.fits".to_string());
        let bytes = std::fs::read(&path)
            .unwrap_or_else(|e| panic!("could not read fixture {path}: {e}"));

        let moc_90 = compute_contour_moc(&bytes, 0.9).expect("90% contour");
        let moc_50 = compute_contour_moc(&bytes, 0.5).expect("50% contour");

        // Both outputs should be valid (non-trivially short) FITS
        // and start with the "SIMPLE" keyword.
        assert!(moc_90.len() > 100, "90% MOC unexpectedly small");
        assert!(moc_50.len() > 100, "50% MOC unexpectedly small");
        assert_eq!(&moc_90[..6], b"SIMPLE");
        assert_eq!(&moc_50[..6], b"SIMPLE");

        // The 50% credible region is a subset of the 90% — bytes
        // count is a coarse proxy but consistently holds for the
        // run-length encoded MOC format.
        assert!(
            moc_50.len() <= moc_90.len(),
            "50% MOC ({}) larger than 90% MOC ({})",
            moc_50.len(),
            moc_90.len()
        );
    }
}
