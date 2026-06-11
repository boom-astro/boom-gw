//! Synthetic IVOA multi-order PROBDENSITY sky maps.
//!
//! A stand-in for a real BAYESTAR localization: a HEALPix NUNIQ
//! BINTABLE (UNIQ + PROBDENSITY columns) with uniform density over a
//! cone, normalized so the probability integrates to 1. The hand-
//! written FITS matches what BAYESTAR emits closely enough that the
//! live scan / contour / cross-match readers
//! (`moc::deser::fits::multiordermap::{sum,from}_fits_multiordermap`)
//! consume it identically to a real LVK sky map — which is why the
//! demo loader uses it to seed superevents, and why tests can use it
//! to exercise the geometry without a real fixture.
//!
//! `RangeMOC::from_cone(...).to_fits_ivoa()` would produce a *coverage*
//! MOC with no probability column, which the multi-order-map reader
//! rejects (`UnexpectedKeyword("TTYPE1", "MOCVER")`); that's why we
//! hand-write the BINTABLE here.

use moc::moc::range::{CellSelection, RangeMOC};
use moc::qty::Hpx;

/// Default NUNIQ depth for synthetic maps (NSIDE 256, ~13.7′ cells).
/// Matches the demo loader.
pub const DEFAULT_SYNTH_DEPTH: u8 = 8;

/// Build a synthetic multi-order PROBDENSITY FITS with uniform density
/// over a cone of `radius_deg` centered at `(ra_deg, dec_deg)`, at the
/// given NUNIQ `depth`. The density is set so the posterior integrates
/// to 1 over the cone.
pub fn build_uniform_cone_skymap(ra_deg: f64, dec_deg: f64, radius_deg: f64, depth: u8) -> Vec<u8> {
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
    let n_cells = (pix_indices.len().max(1)) as f64;

    // HEALPix cell area at depth d: 4π / (12 · 4^d) sr. Uniform density
    // = 1 / (n_cells · cell_area_sr) so the total mass integrates to 1.
    let nside_sq = (1u64 << (2 * depth)) as f64;
    let cell_area_sr = 4.0 * std::f64::consts::PI / (12.0 * nside_sq);
    let density = 1.0 / (n_cells * cell_area_sr);

    // NUNIQ encoding: UNIQ = ipix + 4·4^depth.
    let uniq_base: u64 = 4u64 << (2 * depth);
    let nuniqs: Vec<u64> = pix_indices.iter().map(|&p| p + uniq_base).collect();

    write_ivoa_multiordermap_fits(&nuniqs, density, depth)
}

/// Hand-roll the FITS bytes for a HEALPix NUNIQ probability-density
/// map. Cards are 80 bytes; headers + data are each padded to a
/// 2880-byte block. UNIQ is i64 big-endian, PROBDENSITY is f64
/// big-endian — FITS is big-endian regardless of host.
pub fn write_ivoa_multiordermap_fits(nuniqs: &[u64], density: f64, max_depth: u8) -> Vec<u8> {
    fn fixed_card(bytes: &[u8]) -> [u8; 80] {
        let mut c = [b' '; 80];
        let n = bytes.len().min(80);
        c[..n].copy_from_slice(&bytes[..n]);
        c
    }
    fn str_card(key: &str, quoted_value: &str) -> [u8; 80] {
        fixed_card(format!("{key:<8}= {quoted_value:<70}").as_bytes())
    }
    fn int_card(key: &str, value: i64) -> [u8; 80] {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crossmatch::spatial_overlap;
    use crate::grb::{build_canonical_moc_fits, GrbTrigger, SkyPosition};

    fn cone_trigger(ra: f64, dec: f64, err_deg: f64) -> GrbTrigger {
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
    fn synthetic_skymap_is_readable_and_full_sky_grb_recovers_all_mass() {
        // A GW cone, integrated against a whole-sky GRB MOC, must
        // recover ~all of the probability mass (the map normalizes to
        // 1). This also proves the hand-written FITS is accepted by
        // the same reader the live scan uses.
        let gw = build_uniform_cone_skymap(120.0, 30.0, 8.0, 7);
        let full_sky = build_canonical_moc_fits(&cone_trigger(120.0, 30.0, 180.0)).unwrap();
        let overlap = spatial_overlap(&gw, &full_sky).unwrap();
        assert!(
            (overlap - 1.0).abs() < 0.05,
            "full-sky GRB MOC should recover ≈1.0 of the GW mass; got {overlap}"
        );
    }
}
