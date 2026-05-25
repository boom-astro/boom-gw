//! Joint GW + external posterior sky map synthesis.
//!
//! Port of the math in
//! `gwcelery.tasks.external_skymaps.combine_skymaps_moc_moc`
//! (LIGO's joint-posterior task) — the element-wise product of
//! the GW PROBDENSITY map and the external trigger's
//! localization, renormalized so the total probability mass
//! integrates to 1.
//!
//! Today we only implement the case where the external side is
//! a coverage MOC (no per-cell probability density) — that's the
//! 95% of boom-gw inputs: every GRB / FRB / point-localized
//! neutrino we ingest is canonicalized at ingest into a cone MOC
//! by [`crate::grb::build_canonical_moc_fits`]. The math
//! simplifies to "restrict the GW density to cells inside the
//! external mask, renormalize." When/if we start fetching real
//! BAYESTAR-style external skymaps (IceCube `healpix_url`,
//! KM3NeT), we'd extend this to the full density × density
//! product the gwcelery code does.
//!
//! We deliberately do NOT include the BAYESTAR distance columns
//! (DISTMU / DISTSIGMA / DISTNORM) in the output — joint
//! distance synthesis is its own physics step and not what
//! `combine_skymaps_moc_moc` produces for the spatial-only
//! case anyway. Adding distance reweighting is a clean
//! follow-up once we have a use case.

use std::io::{BufReader, Cursor, Read};

use moc::deser::fits::{from_fits_ivoa, MocIdxType, MocQtyType};
use moc::moc::range::RangeMOC;
use moc::qty::Hpx;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum JointSkymapError {
    #[error("GW skymap FITS parse failed: {0}")]
    GwFits(String),
    #[error("external MOC FITS parse failed: {0}")]
    ExtMocFits(String),
    #[error("joint skymap is empty (no GW probability mass falls inside the external region)")]
    EmptyOverlap,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// One row of a multi-order PROBDENSITY map.
#[derive(Debug, Clone, Copy)]
struct Cell {
    /// NUNIQ-encoded HEALPix index. Encodes `(depth, ipix)` as
    /// `ipix + 4·4^depth`.
    uniq: u64,
    /// Probability density in steradian⁻¹ at this cell.
    probdensity: f64,
}

/// Compute the joint GW × external sky map. Returns a new
/// multi-order PROBDENSITY FITS containing only cells that fall
/// inside `ext_moc`, with each cell's density renormalized so
/// the integral over the sphere is 1.
///
/// `gw_fits` must be a BAYESTAR-style multi-order map with at
/// least UNIQ + PROBDENSITY columns (extra columns are ignored).
/// `ext_moc_fits` must be a plain IVOA HEALPix coverage MOC
/// (e.g. what [`crate::grb::build_canonical_moc_fits`] writes).
pub fn combine_gw_with_external_moc(
    gw_fits: &[u8],
    ext_moc_fits: &[u8],
) -> Result<Vec<u8>, JointSkymapError> {
    let cells = read_multiordermap_cells(gw_fits)?;
    let ext_moc = parse_ext_moc(ext_moc_fits)?;

    // Membership test by cell *center*: convert each GW cell's
    // NUNIQ → (lon, lat) via cdshealpix, hash to the ext MOC's
    // depth_max, then ask via `contains_cell(depth, ipix)` (NOT
    // `contains_val`, which expects pixels at the quantity's
    // MAX_DEPTH = 29 for u64 HEALPix). For boundary cells whose
    // center is just outside the external region we may
    // under-include, but for boom-gw's cone-shaped external
    // MOCs the per-cell footprint is much smaller than the cone
    // radius so the bias is negligible.
    let ext_depth = ext_moc.depth_max();
    let mut kept: Vec<Cell> = Vec::with_capacity(cells.len());
    let mut mass = 0.0_f64;
    for cell in cells {
        let (depth, ipix) = uniq_to_depth_ipix(cell.uniq);
        let (lon, lat) = cdshealpix::nested::center(depth, ipix);
        let ext_pix = cdshealpix::nested::hash(ext_depth, lon, lat);
        if !ext_moc.contains_cell(ext_depth, ext_pix) {
            continue;
        }
        let area = cell_area_sr(depth);
        mass += cell.probdensity * area;
        kept.push(cell);
    }

    if mass <= 0.0 || !mass.is_finite() {
        return Err(JointSkymapError::EmptyOverlap);
    }
    let scale = 1.0 / mass;
    for cell in &mut kept {
        cell.probdensity *= scale;
    }
    Ok(write_multiordermap_fits(&kept))
}

/// HEALPix cell area in steradians at the given NUNIQ depth.
fn cell_area_sr(depth: u8) -> f64 {
    // 4π / (12 · 4^depth)
    let n_cells = 12.0 * (1u64 << (2 * depth as u32)) as f64;
    4.0 * std::f64::consts::PI / n_cells
}

/// Decode a NUNIQ value into `(depth, ipix)`.
/// NUNIQ = ipix + 4·4^depth, so depth = ⌊log4(uniq/4)⌋ and
/// ipix = uniq − 4^(depth+1).
fn uniq_to_depth_ipix(uniq: u64) -> (u8, u64) {
    // depth where 4·4^depth ≤ uniq < 4·4^(depth+1)
    // ↔ ⌊log4(uniq/4)⌋. Compute via leading-zeros of uniq>>2.
    debug_assert!(uniq >= 4, "uniq below the depth-0 range");
    let bits = (uniq >> 2).ilog2();
    let depth = (bits / 2) as u8;
    let base = 4u64 << (2 * depth);
    let ipix = uniq - base;
    (depth, ipix)
}

fn parse_ext_moc(bytes: &[u8]) -> Result<RangeMOC<u64, Hpx<u64>>, JointSkymapError> {
    let reader = BufReader::new(Cursor::new(bytes));
    let moc_type =
        from_fits_ivoa(reader).map_err(|e| JointSkymapError::ExtMocFits(format!("{e:?}")))?;
    match moc_type {
        MocIdxType::U64(MocQtyType::Hpx(m)) => Ok(m.collect()),
        _ => Err(JointSkymapError::ExtMocFits(
            "external MOC was not a u64 HEALPix MOC".into(),
        )),
    }
}

/// Hand-parse the (UNIQ, PROBDENSITY) rows of a multi-order
/// PROBDENSITY FITS. Mirror of the writer in
/// `bin/load_demo_data.rs` and the spec the moc crate's private
/// `MultiOrderMapIterator` reads. Tolerates >2-column tables by
/// skipping the trailing bytes per row (handles real BAYESTAR
/// FITS which carry DISTMU / DISTSIGMA / DISTNORM after the
/// density column).
fn read_multiordermap_cells(bytes: &[u8]) -> Result<Vec<Cell>, JointSkymapError> {
    let mut reader = Cursor::new(bytes);
    // Skip primary HDU(s): each HDU is a header (one or more
    // 2880-byte blocks ending in `END`) followed by data padded
    // to a 2880-byte boundary. The primary in a multi-order
    // skymap is empty (NAXIS=0), so its data block is zero
    // bytes — we just need to advance past its header.
    skip_hdu_header(&mut reader)?;

    // Now read the BINTABLE HDU header.
    let header = read_hdu_header(&mut reader)?;
    let naxis1: u64 = header_int(&header, b"NAXIS1")?;
    let naxis2: u64 = header_int(&header, b"NAXIS2")?;
    // We require UNIQ + PROBDENSITY as the first two columns; we
    // *don't* require them to be the only columns. A trailing
    // skip handles BAYESTAR's DISTMU/DISTSIGMA/DISTNORM rows.
    expect_string_card(&header, b"TTYPE1", b"UNIQ")?;
    expect_string_card(&header, b"TFORM1", b"K")?;
    expect_string_card(&header, b"TTYPE2", b"PROBDENSITY")?;
    expect_string_card(&header, b"TFORM2", b"D")?;
    let bytes_per_row = naxis1 as usize;
    if bytes_per_row < 16 {
        return Err(JointSkymapError::GwFits(format!(
            "NAXIS1={bytes_per_row} too small to hold UNIQ + PROBDENSITY (need ≥ 16)"
        )));
    }
    let skip_per_row = bytes_per_row - 16;
    let mut cells = Vec::with_capacity(naxis2 as usize);
    let mut row_buf = [0u8; 16];
    let mut sink = vec![0u8; skip_per_row];
    for _ in 0..naxis2 {
        reader.read_exact(&mut row_buf)?;
        let uniq = i64::from_be_bytes(row_buf[0..8].try_into().unwrap()) as u64;
        let probdensity = f64::from_be_bytes(row_buf[8..16].try_into().unwrap());
        if skip_per_row > 0 {
            reader.read_exact(&mut sink)?;
        }
        cells.push(Cell { uniq, probdensity });
    }
    Ok(cells)
}

fn skip_hdu_header(reader: &mut Cursor<&[u8]>) -> Result<(), JointSkymapError> {
    let _ = read_hdu_header(reader)?;
    Ok(())
}

/// Read FITS header records (80-byte cards) from `reader` until
/// the `END` card. Returns the concatenated header bytes. Also
/// advances past the trailing 2880-byte alignment padding so the
/// reader cursor is at the start of the data block.
fn read_hdu_header(reader: &mut Cursor<&[u8]>) -> Result<Vec<u8>, JointSkymapError> {
    let mut header = Vec::with_capacity(2880);
    loop {
        let mut block = [0u8; 2880];
        reader.read_exact(&mut block)?;
        header.extend_from_slice(&block);
        // Scan the block for the END card (cols 1-3 of an
        // 80-byte card == "END").
        for chunk in block.chunks_exact(80) {
            if chunk.len() >= 3 && &chunk[0..3] == b"END" && chunk[3..80].iter().all(|b| *b == b' ')
            {
                return Ok(header);
            }
        }
    }
}

/// Look up a FITS keyword in `header`, parse its value as i64.
fn header_int(header: &[u8], keyword: &[u8]) -> Result<u64, JointSkymapError> {
    let v = find_card_value(header, keyword)?;
    let s = std::str::from_utf8(v)
        .map_err(|e| JointSkymapError::GwFits(format!("non-utf8 value for {keyword:?}: {e}")))?;
    s.trim()
        .parse::<u64>()
        .map_err(|e| JointSkymapError::GwFits(format!("parse {keyword:?}: {e}")))
}

/// Assert that the FITS string-card `keyword` has value `expected`
/// (whitespace + quote-stripped, case-sensitive prefix match).
fn expect_string_card(
    header: &[u8],
    keyword: &[u8],
    expected: &[u8],
) -> Result<(), JointSkymapError> {
    let v = find_card_value(header, keyword)?;
    let trimmed = v
        .iter()
        .copied()
        .filter(|b| *b != b'\'' && *b != b' ')
        .take(expected.len())
        .collect::<Vec<_>>();
    if trimmed.as_slice() == expected {
        Ok(())
    } else {
        Err(JointSkymapError::GwFits(format!(
            "expected {keyword:?}={expected:?}, got {:?}",
            String::from_utf8_lossy(v)
        )))
    }
}

/// Find the value portion of a header card by 8-char keyword.
/// Returns the bytes between `= ` (cols 9-10) and the next `/`
/// (if any) or end of card.
fn find_card_value<'a>(header: &'a [u8], keyword: &[u8]) -> Result<&'a [u8], JointSkymapError> {
    for chunk in header.chunks_exact(80) {
        // Keyword lives in cols 1-8, padded with spaces.
        let key_field = &chunk[0..8];
        let key_trim_len = key_field.iter().take_while(|b| **b != b' ').count();
        if &key_field[0..key_trim_len] == keyword
            && chunk.len() > 10
            && chunk[8] == b'='
            && chunk[9] == b' '
        {
            let value_field = &chunk[10..80];
            let end = value_field
                .iter()
                .position(|b| *b == b'/')
                .unwrap_or(value_field.len());
            return Ok(&value_field[..end]);
        }
    }
    Err(JointSkymapError::GwFits(format!(
        "keyword {:?} not found in header",
        String::from_utf8_lossy(keyword)
    )))
}

/// Serialize a `Vec<Cell>` back into a 2-column multi-order
/// PROBDENSITY FITS. Same format as the writer in
/// `bin/load_demo_data.rs` but with per-cell PROBDENSITY
/// instead of a uniform value, and `MOCORDER` set to the
/// deepest cell in the input.
fn write_multiordermap_fits(cells: &[Cell]) -> Vec<u8> {
    let max_depth = cells
        .iter()
        .map(|c| uniq_to_depth_ipix(c.uniq).0)
        .max()
        .unwrap_or(0);

    fn fixed_card(bytes: &[u8]) -> [u8; 80] {
        let mut c = [b' '; 80];
        let n = bytes.len().min(80);
        c[..n].copy_from_slice(&bytes[..n]);
        c
    }
    fn s(k: &str, v: &str) -> [u8; 80] {
        fixed_card(format!("{k:<8}= {v:<70}").as_bytes())
    }
    fn i(k: &str, v: i64) -> [u8; 80] {
        fixed_card(format!("{k:<8}= {v:>20}{:<50}", "").as_bytes())
    }
    fn b(k: &str, v: bool) -> [u8; 80] {
        fixed_card(format!("{k:<8}= {:>20}{:<50}", if v { "T" } else { "F" }, "").as_bytes())
    }
    let end = fixed_card(b"END");

    let mut out = Vec::with_capacity(2880 * 2 + cells.len() * 16);
    // Primary HDU.
    out.extend_from_slice(&b("SIMPLE", true));
    out.extend_from_slice(&i("BITPIX", 8));
    out.extend_from_slice(&i("NAXIS", 0));
    out.extend_from_slice(&b("EXTEND", true));
    out.extend_from_slice(&end);
    while out.len() % 2880 != 0 {
        out.push(b' ');
    }
    // BINTABLE.
    out.extend_from_slice(&s("XTENSION", "'BINTABLE'"));
    out.extend_from_slice(&i("BITPIX", 8));
    out.extend_from_slice(&i("NAXIS", 2));
    out.extend_from_slice(&i("NAXIS1", 16));
    out.extend_from_slice(&i("NAXIS2", cells.len() as i64));
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
    out.extend_from_slice(&i("MOCORDER", max_depth as i64));
    out.extend_from_slice(&s("INDXSCHM", "'EXPLICIT'"));
    out.extend_from_slice(&end);
    while out.len() % 2880 != 0 {
        out.push(b' ');
    }
    // Rows.
    for c in cells {
        out.extend_from_slice(&(c.uniq as i64).to_be_bytes());
        out.extend_from_slice(&c.probdensity.to_be_bytes());
    }
    while out.len() % 2880 != 0 {
        out.push(0);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use moc::moc::range::{CellSelection, RangeMOC};
    use moc::qty::Hpx;

    /// Build a small synthetic GW skymap (uniform-density cone)
    /// + a smaller external MOC fully contained in it, combine
    /// them, and verify the output integrates to 1 and is
    /// concentrated in the overlap region.
    #[test]
    fn combined_skymap_integrates_to_one_when_ext_inside_gw() {
        // Depth 6 → cells ~0.92° on a side. Make the external
        // cone several cells wide so the center-membership test
        // catches a meaningful number of GW cells (a 1°-radius
        // ext cone at this depth could fit between cell centers
        // entirely and the test would erroneously look like
        // "no overlap").
        let depth: u8 = 6;
        let gw_fits = synthetic_uniform_density_cone(160.0, 10.0, 8.0, depth);
        let ext_moc = synthetic_cone_moc(160.0, 10.0, 3.0, depth);

        let combined = combine_gw_with_external_moc(&gw_fits, &ext_moc)
            .expect("combine should succeed when overlap is non-empty");

        let cells = read_multiordermap_cells(&combined).expect("output is a valid FITS");
        assert!(!cells.is_empty());
        let total_mass: f64 = cells
            .iter()
            .map(|c| {
                let (d, _) = uniq_to_depth_ipix(c.uniq);
                c.probdensity * cell_area_sr(d)
            })
            .sum();
        // Should be 1.0 by construction (we renormalize).
        let rel_err = (total_mass - 1.0).abs();
        assert!(
            rel_err < 1e-9,
            "combined posterior mass = {total_mass}, expected 1.0 (rel err {rel_err})"
        );
    }

    #[test]
    fn combined_skymap_errors_on_no_overlap() {
        let depth: u8 = 6;
        let gw_fits = synthetic_uniform_density_cone(0.0, 0.0, 3.0, depth);
        // External cone on the opposite side of the sky — zero
        // overlap with the GW skymap.
        let ext_moc = synthetic_cone_moc(180.0, 0.0, 3.0, depth);
        let err = combine_gw_with_external_moc(&gw_fits, &ext_moc).unwrap_err();
        assert!(matches!(err, JointSkymapError::EmptyOverlap));
    }

    #[test]
    fn uniq_round_trip_at_several_depths() {
        for &d in &[0u8, 4, 6, 8, 10] {
            // Valid ipix range at depth d is [0, 12·4^d). Test
            // both endpoints + a sampling in between.
            let n_pix = 12u64 << (2 * d as u32);
            let probes: [u64; 4] = [0, 1, n_pix / 2, n_pix - 1];
            for &ipix in &probes {
                let uniq = ipix + (4u64 << (2 * d as u32));
                let (got_d, got_ipix) = uniq_to_depth_ipix(uniq);
                assert_eq!(got_d, d, "depth round-trip failed for uniq={uniq}");
                assert_eq!(got_ipix, ipix, "ipix round-trip failed for uniq={uniq}");
            }
        }
    }

    /// Hand-roll a synthetic uniform-density cone FITS. Same
    /// shape as `bin/load_demo_data::build_synthetic_prob_density_skymap`,
    /// kept self-contained so this test doesn't depend on a
    /// binary's internals.
    fn synthetic_uniform_density_cone(
        ra_deg: f64,
        dec_deg: f64,
        radius_deg: f64,
        depth: u8,
    ) -> Vec<u8> {
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
        let pix: Vec<u64> = cone.flatten_to_fixed_depth_cells().collect();
        let n = pix.len() as f64;
        let cell_area = cell_area_sr(depth);
        let density = 1.0 / (n * cell_area);
        let uniq_base: u64 = 4u64 << (2 * depth);
        let cells: Vec<Cell> = pix
            .into_iter()
            .map(|p| Cell {
                uniq: p + uniq_base,
                probdensity: density,
            })
            .collect();
        write_multiordermap_fits(&cells)
    }

    /// Plain coverage MOC for a cone — what
    /// `crate::grb::build_canonical_moc_fits` writes for a
    /// canonical GRB error region.
    fn synthetic_cone_moc(ra_deg: f64, dec_deg: f64, radius_deg: f64, depth: u8) -> Vec<u8> {
        use moc::moc::{RangeMOCIntoIterator, RangeMOCIterator};
        let cone: RangeMOC<u64, Hpx<u64>> = RangeMOC::from_cone(
            ra_deg.to_radians(),
            dec_deg.to_radians(),
            radius_deg.to_radians(),
            depth,
            2,
            CellSelection::All,
        );
        let mut out = Vec::new();
        cone.into_range_moc_iter()
            .to_fits_ivoa(None, None, &mut out)
            .expect("MOC FITS write");
        out
    }
}
