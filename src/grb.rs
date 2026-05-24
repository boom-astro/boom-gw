//! Gamma-ray burst trigger types — the "external event" peer of
//! [`crate::event::GwEvent`].
//!
//! Lifted from `origen/crates/mm-core` (the user's other multi-
//! messenger framework) and adapted to boom-gw's archive shape.
//! We intentionally keep the GRB pipeline separate from GW
//! clustering: the physics is different (single trigger vs. coinc
//! inspiral), the FAR scaling is different, and downstream
//! consumers want to query them independently. Cross-references
//! live in a join collection ([`crate::archive::CrossMatchDoc`]),
//! not in the GRB or superevent doc itself.
//!
//! The wire form here is the canonical representation: a Fermi GBM
//! alert (JSON or VOEvent), a Swift-BAT alert, or an operator-
//! supplied trigger from the API all normalize into a single
//! `GrbTrigger` before storage.

use moc::moc::range::{CellSelection, RangeMOC};
use moc::moc::{RangeMOCIntoIterator, RangeMOCIterator};
use moc::qty::Hpx;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A point on the sky with optional positional uncertainty. RA and
/// Dec are J2000 equatorial in degrees; `uncertainty_arcsec` is the
/// 1-σ statistical error or instrument default (e.g. 5° for
/// Fermi-GBM, ~2′ for Swift-BAT). Use [`Self::error_radius_deg`] to
/// get the degree-scale value the cross-matcher consumes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct SkyPosition {
    /// Right Ascension, degrees, [0, 360).
    pub ra: f64,
    /// Declination, degrees, [-90, 90].
    pub dec: f64,
    /// Position uncertainty in arcseconds.
    #[serde(default)]
    pub uncertainty_arcsec: f64,
}

impl SkyPosition {
    pub fn new(ra: f64, dec: f64, uncertainty_arcsec: f64) -> Self {
        Self {
            ra,
            dec,
            uncertainty_arcsec,
        }
    }

    /// Convert the stored arcsec uncertainty to degrees, which is
    /// the unit the cross-match cone integration uses.
    pub fn error_radius_deg(&self) -> f64 {
        self.uncertainty_arcsec / 3600.0
    }

    /// Great-circle angular separation to another position, in
    /// degrees. Uses the haversine formula — accurate enough for
    /// the few-arcsecond regime where small-angle approximations
    /// start to lose precision.
    pub fn angular_separation(&self, other: &SkyPosition) -> f64 {
        let phi1 = self.dec.to_radians();
        let phi2 = other.dec.to_radians();
        let d_phi = phi2 - phi1;
        let d_lambda = (other.ra - self.ra).to_radians();
        let a =
            (d_phi / 2.0).sin().powi(2) + phi1.cos() * phi2.cos() * (d_lambda / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().asin();
        c.to_degrees()
    }
}

/// One ingested GRB trigger. `trigger_id` is the instrument's
/// canonical ID (Fermi-GBM `bn250101000`, Swift-BAT `01234567`,
/// etc.) and combined with `instrument` forms the natural primary
/// key in the archive — duplicate updates to the same trigger
/// over-write rather than fan out.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GrbTrigger {
    /// Instrument-assigned trigger ID. We don't normalize across
    /// instruments — `bn250101000` from Fermi and `01234567` from
    /// Swift are distinct namespaces.
    pub trigger_id: String,
    /// Originating instrument, e.g. "Fermi-GBM-FLT",
    /// "Fermi-GBM-GND", "Fermi-GBM-FIN", "Swift-BAT".
    pub instrument: String,
    /// Trigger time in GPS seconds. The GBM parser converts
    /// MET → GPS internally.
    pub trigger_time: f64,
    /// Reported position + uncertainty. `None` if the alert was
    /// pre-localization (some early Fermi notices arrive without
    /// position).
    #[serde(default)]
    pub position: Option<SkyPosition>,
    /// Instrument-reported significance or reliability. Scale and
    /// meaning vary by instrument (Fermi reports `reliability` on
    /// 0–10, Swift reports raw SNR).
    #[serde(default)]
    pub significance: f64,
    /// URL to a more detailed sky map, when the alert includes
    /// one. Boom-gw does not fetch this today.
    #[serde(default)]
    pub skymap_url: Option<String>,
    /// Reported error radius in degrees (1-σ). Same value as
    /// `position.uncertainty_arcsec` after unit conversion; kept
    /// separate for parity with what the alert wire format
    /// declares.
    #[serde(default)]
    pub error_radius_deg: Option<f64>,
}

/// Default HEALPix depth for synthesized cone MOCs. Matches the
/// [`crate::pvalue::CONE_DEPTH`] constant — both must agree so MOC
/// set ops between an ingest-time cone and a Monte-Carlo rotated
/// cone are exact (no resolution up-sampling penalty).
pub const GRB_CONE_DEPTH: u8 = 10;

#[derive(Debug, Error)]
pub enum GrbSkymapError {
    #[error("trigger has no localization (position is None)")]
    NoPosition,
    #[error("trigger has no usable error_radius_deg: {0}")]
    NoErrorRadius(String),
    #[error("moc fits serialization failed: {0}")]
    FitsWrite(String),
}

/// Build the canonical MOC FITS bytes for a GRB trigger.
///
/// Today this only handles circular error regions — the common
/// case for Fermi-GBM and Swift-BAT notices. The function is the
/// single point that future ingest paths plug into when richer
/// localizations show up (HEALPix posteriors, elliptical cones,
/// multi-component maps): instead of teaching every consumer about
/// each new shape, we canonicalize at ingest into a MOC FITS that
/// the rest of the codebase treats as opaque bytes.
///
/// The returned bytes are written via the IVOA MOC FITS serializer
/// (`moc::ranges_to_fits_ivoa`), the same format we already store
/// for GW credible-region contours.
pub fn build_canonical_moc_fits(trigger: &GrbTrigger) -> Result<Vec<u8>, GrbSkymapError> {
    let position = trigger.position.ok_or(GrbSkymapError::NoPosition)?;
    let radius_deg = trigger
        .error_radius_deg
        .or_else(|| {
            let r = position.error_radius_deg();
            (r > 0.0 && r.is_finite()).then_some(r)
        })
        .ok_or_else(|| {
            GrbSkymapError::NoErrorRadius(format!(
                "instrument={}, trigger_id={}",
                trigger.instrument, trigger.trigger_id
            ))
        })?;
    if !radius_deg.is_finite() || radius_deg <= 0.0 {
        return Err(GrbSkymapError::NoErrorRadius(format!(
            "radius_deg={radius_deg}"
        )));
    }
    let lon = position.ra.to_radians();
    let lat = position.dec.to_radians();
    // Floor the cone radius the same way crate::pvalue::cone_moc
    // does so set ops between the two stay numerically consistent.
    let radius_rad = radius_deg.max(0.05).to_radians();
    let cone: RangeMOC<u64, Hpx<u64>> =
        RangeMOC::from_cone(lon, lat, radius_rad, GRB_CONE_DEPTH, 2, CellSelection::All);
    let mut out = Vec::new();
    cone.into_range_moc_iter()
        .to_fits_ivoa(None, None, &mut out)
        .map_err(|e| GrbSkymapError::FitsWrite(format!("{e:?}")))?;
    Ok(out)
}

/// Result of a single GW-superevent × GRB-trigger cross-match.
/// Persisted in the `superevent_grb_matches` collection and
/// returned by the cross-match API.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossMatchResult {
    /// Time delta = `trigger_time - superevent.t_0` in seconds.
    /// Positive means the GRB arrived after the GW merger time.
    pub time_offset_sec: f64,
    /// Cumulative GW localization probability inside the GRB error
    /// circle, computed by integrating PROBDENSITY over the cone
    /// MOC (RAVEN methodology). Range [0, 1].
    pub spatial_overlap: f64,
    /// `true` if the GRB best-fit position lies inside the GW
    /// 50% credible region, false otherwise. Cheap cosmetic flag
    /// for UI — operators care about this even when the integral
    /// itself is small.
    pub in_50cr: bool,
    /// Same for the 90% credible region.
    pub in_90cr: bool,
    /// Joint spatiotemporal FAR in events per year, computed via
    /// the classic RAVEN formula:
    /// `(Δt × R_ext × FAR_GW) / spatial_overlap`. Known to be
    /// biased — kept for parity with the legacy pipeline. `None`
    /// when spatial_overlap is zero.
    #[serde(default)]
    pub joint_far_per_year: Option<f64>,
    /// Empirical one-sided p-value for the observed overlap,
    /// from a Monte Carlo of N random sky rotations of the GRB
    /// skymap. `None` if the p-value wasn't requested for this
    /// match (e.g. on-demand path with `n_trials=0`).
    #[serde(default)]
    pub p_value: Option<f64>,
    /// Number of Monte Carlo trials that produced [`Self::p_value`].
    /// Lets the UI distinguish a tight 200-trial estimate from a
    /// coarse 20-trial one.
    #[serde(default)]
    pub p_value_trials: Option<usize>,
    /// Bias-corrected joint FAR (events per year) using the
    /// remapped p-value method. Better-calibrated than
    /// [`Self::joint_far_per_year`] but only available when the
    /// p-value Monte Carlo ran. `None` otherwise.
    #[serde(default)]
    pub joint_far_remapped_per_year: Option<f64>,
    /// Operator-flagged association — `true` once the analyst has
    /// reviewed this match and committed that it represents a
    /// real GW × external coincidence. The scan endpoint always
    /// emits matches with `associated = false`; the PATCH
    /// endpoint flips this in response to a star-click in the UI.
    /// Aladin overlays render every associated match plus any
    /// unassociated one with a small empirical p-value (see the
    /// LocalizationTab filter).
    #[serde(default)]
    pub associated: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn angular_separation_zero_for_same_position() {
        let p = SkyPosition::new(123.0, 45.0, 0.0);
        assert!((p.angular_separation(&p)).abs() < 1e-9);
    }

    #[test]
    fn angular_separation_one_degree() {
        // 1° in RA at the equator should be 1°.
        let p1 = SkyPosition::new(0.0, 0.0, 0.0);
        let p2 = SkyPosition::new(1.0, 0.0, 0.0);
        let sep = p1.angular_separation(&p2);
        assert!((sep - 1.0).abs() < 1e-6, "sep={sep}");
    }

    #[test]
    fn angular_separation_antipodes() {
        // RA difference of 180° at the equator → 180° on the sphere.
        let p1 = SkyPosition::new(0.0, 0.0, 0.0);
        let p2 = SkyPosition::new(180.0, 0.0, 0.0);
        let sep = p1.angular_separation(&p2);
        assert!((sep - 180.0).abs() < 1e-6, "sep={sep}");
    }

    #[test]
    fn error_radius_arcsec_to_degrees() {
        let p = SkyPosition::new(0.0, 0.0, 3600.0);
        assert!((p.error_radius_deg() - 1.0).abs() < 1e-9);
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
    fn build_moc_for_cone_emits_simple_fits() {
        let bytes = build_canonical_moc_fits(&fake_trigger(120.0, -20.0, 1.0))
            .expect("should produce MOC FITS");
        // Sanity: the IVOA MOC FITS writer always emits a SIMPLE
        // primary header.
        assert_eq!(&bytes[..6], b"SIMPLE");
        assert!(bytes.len() > 2880, "should have a real BINTABLE");
    }

    #[test]
    fn build_moc_rejects_missing_position() {
        let mut t = fake_trigger(0.0, 0.0, 1.0);
        t.position = None;
        assert!(matches!(
            build_canonical_moc_fits(&t),
            Err(GrbSkymapError::NoPosition)
        ));
    }

    #[test]
    fn build_moc_rejects_bad_radius() {
        let mut t = fake_trigger(0.0, 0.0, 1.0);
        t.error_radius_deg = Some(0.0);
        t.position = Some(SkyPosition::new(0.0, 0.0, 0.0));
        assert!(matches!(
            build_canonical_moc_fits(&t),
            Err(GrbSkymapError::NoErrorRadius(_))
        ));
    }
}
