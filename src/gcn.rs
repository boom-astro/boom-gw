//! Parsers for GCN (Gamma-ray Coordinates Network) alert payloads.
//!
//! Phase 1 covers Fermi-GBM only — the dominant GRB source for
//! LIGO-Virgo joint analyses. Each parser normalizes its wire
//! format into a [`crate::grb::GrbTrigger`]; downstream archival
//! and cross-matching is format-agnostic from there.
//!
//! Two Fermi flavors are supported:
//!
//! * **Modern JSON notices** (`gcn.notices.fermi.gbm.*` Kafka
//!   topics) — `parse_fermi_gbm_json`.
//! * **Legacy VOEvent XML** (`gcn.classic.voevent.FERMI_GBM_*`) —
//!   `parse_fermi_voevent`. We use the same regex-y string
//!   extraction origen uses rather than pulling in `quick-xml`;
//!   the VOEvent payloads from Fermi are well-formed and tiny, and
//!   a real XML parser is overkill until we add Swift/SVOM VOEvent
//!   support.
//!
//! Ported and adapted from
//! `origen/crates/mm-gcn/src/parsers/grb.rs` (same author).

use serde_json::Value;
use thiserror::Error;
use tracing::warn;

use crate::grb::{GrbTrigger, SkyPosition};

/// Difference between the Fermi MET (Mission Elapsed Time, epoch
/// 2001-01-01 UTC) and GPS time (epoch 1980-01-06 UTC), in seconds.
/// Matches the constant used by `ligo.gracedb` for Fermi notice
/// time conversion.
const FERMI_MET_TO_GPS_OFFSET_SEC: f64 = 662_860_800.0;

/// Default error radius (degrees) for Fermi GBM alerts that don't
/// carry one. 5° is the GBM-FLT (flight) median; GND/FIN updates
/// usually arrive with a real value.
const FERMI_GBM_DEFAULT_ERR_DEG: f64 = 5.0;

#[derive(Debug, Error)]
pub enum GcnParseError {
    #[error("json parse failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("voevent payload missing required field {0}")]
    MissingVoeventField(&'static str),
    #[error("iso8601 time parse failed: {0}")]
    IsoTime(String),
}

/// Cumulative leap-seconds the GPS clock is **ahead** of UTC, by
/// the UTC year. GPS doesn't apply leap seconds; UTC does. Each
/// entry is the value valid from the listed introduction date
/// onward. Sourced from
/// <https://www.ietf.org/timezones/data/leap-seconds.list>. The
/// table only needs extending when a new leap-second is announced
/// by the IERS (extremely rare since 2017).
const LEAP_SECONDS: &[(i64, i64)] = &[
    // (UTC seconds since GPS epoch when the leap-second took
    // effect, total accumulated leap-seconds from that point on).
    // GPS epoch is 1980-01-06 00:00:00 UTC; before then GPS-UTC is 0.
    (46828800, 1),    // 1981-07-01
    (78364801, 2),    // 1982-07-01
    (109900802, 3),   // 1983-07-01
    (173059203, 4),   // 1985-07-01
    (252028804, 5),   // 1988-01-01
    (315187205, 6),   // 1990-01-01
    (346723206, 7),   // 1991-01-01
    (393984007, 8),   // 1992-07-01
    (425520008, 9),   // 1993-07-01
    (457056009, 10),  // 1994-07-01
    (504489610, 11),  // 1996-01-01
    (551750411, 12),  // 1997-07-01
    (599184012, 13),  // 1999-01-01
    (820108813, 14),  // 2006-01-01
    (914803214, 15),  // 2009-01-01
    (1025136015, 16), // 2012-07-01
    (1119744016, 17), // 2015-07-01
    (1167264017, 18), // 2017-01-01
];

/// Convert an ISO-8601 UTC timestamp (the form VOEvent
/// `<ISOTime>` carries) to GPS seconds. Examples of accepted
/// forms: `"2026-01-15T12:34:56"`, `"2026-01-15T12:34:56Z"`,
/// `"2026-01-15T12:34:56.789"`, `"2026-01-15T12:34:56.789Z"`.
///
/// We implement the conversion by hand (no chrono dep) — the
/// math is straightforward and the leap-second table is a small
/// constant. Caller is responsible for making sure the leap-second
/// table is up to date if cross-matching pre-2026 events.
pub fn iso8601_utc_to_gps(s: &str) -> Result<f64, GcnParseError> {
    // Strip trailing Z; we already assume UTC.
    let s = s.trim().trim_end_matches('Z');
    let (date_part, time_part) = s
        .split_once('T')
        .ok_or_else(|| GcnParseError::IsoTime(format!("missing T separator: {s}")))?;
    let date_bits: Vec<&str> = date_part.split('-').collect();
    if date_bits.len() != 3 {
        return Err(GcnParseError::IsoTime(format!(
            "bad date component {date_part}"
        )));
    }
    let year: i64 = date_bits[0]
        .parse()
        .map_err(|e| GcnParseError::IsoTime(format!("year: {e}")))?;
    let month: u32 = date_bits[1]
        .parse()
        .map_err(|e| GcnParseError::IsoTime(format!("month: {e}")))?;
    let day: u32 = date_bits[2]
        .parse()
        .map_err(|e| GcnParseError::IsoTime(format!("day: {e}")))?;
    let time_bits: Vec<&str> = time_part.split(':').collect();
    if time_bits.len() != 3 {
        return Err(GcnParseError::IsoTime(format!(
            "bad time component {time_part}"
        )));
    }
    let hour: u32 = time_bits[0]
        .parse()
        .map_err(|e| GcnParseError::IsoTime(format!("hour: {e}")))?;
    let minute: u32 = time_bits[1]
        .parse()
        .map_err(|e| GcnParseError::IsoTime(format!("minute: {e}")))?;
    let second: f64 = time_bits[2]
        .parse()
        .map_err(|e| GcnParseError::IsoTime(format!("second: {e}")))?;

    // Days since 1980-01-06 (the GPS epoch) up to the start of
    // `year` January. Uses the proleptic Gregorian calendar; for
    // our valid year range (1980+) this matches reality.
    let days_to_year_start = (1980..year)
        .map(|y| if is_leap_year(y) { 366 } else { 365 })
        .sum::<i64>();
    let days_in_months_before = days_before_month(month, is_leap_year(year));
    let days = days_to_year_start - 5 // GPS epoch is Jan 6, not Jan 1
        + days_in_months_before
        + (day as i64 - 1);
    let utc_seconds_since_gps_epoch =
        (days as f64) * 86400.0 + (hour as f64) * 3600.0 + (minute as f64) * 60.0 + second;
    // Add the leap-second offset for the GPS clock at this UTC.
    let leap = leap_seconds_at(utc_seconds_since_gps_epoch as i64);
    Ok(utc_seconds_since_gps_epoch + leap as f64)
}

fn is_leap_year(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_before_month(month: u32, leap: bool) -> i64 {
    let mut days_per_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if leap {
        days_per_month[1] = 29;
    }
    days_per_month
        .iter()
        .take((month - 1) as usize)
        .sum::<i64>()
}

fn leap_seconds_at(utc_seconds_since_gps_epoch: i64) -> i64 {
    let mut leaps = 0;
    for &(threshold, total) in LEAP_SECONDS {
        if utc_seconds_since_gps_epoch >= threshold {
            leaps = total;
        } else {
            break;
        }
    }
    leaps
}

/// Parse a Fermi GBM JSON notice. `instrument` should encode which
/// stage of the GBM pipeline produced the alert (e.g.
/// `"Fermi-GBM-FLT"` for flight, `"-GND"` for ground,
/// `"-FIN"` for final). Caller picks this based on the Kafka topic.
pub fn parse_fermi_gbm_json(payload: &str, instrument: &str) -> Result<GrbTrigger, GcnParseError> {
    let json: Value = serde_json::from_str(payload)?;

    let trigger_id = json["trigger_id"]
        .as_str()
        .or_else(|| json["triggerID"].as_str())
        .unwrap_or("unknown")
        .to_string();

    let trigger_time_met = json["trigger_time"]
        .as_f64()
        .or_else(|| json["triggerTime"].as_f64())
        .unwrap_or(0.0);
    let trigger_time = if trigger_time_met > 0.0 {
        trigger_time_met + FERMI_MET_TO_GPS_OFFSET_SEC
    } else {
        0.0
    };

    let ra = json["ra"].as_f64();
    let dec = json["dec"].as_f64();
    let error_radius_deg = json["error_radius"]
        .as_f64()
        .or_else(|| json["errorRadius"].as_f64());
    let position = match (ra, dec) {
        (Some(ra), Some(dec)) => {
            let err_deg = error_radius_deg.unwrap_or(FERMI_GBM_DEFAULT_ERR_DEG);
            Some(SkyPosition::new(ra, dec, err_deg * 3600.0))
        }
        _ => None,
    };

    let significance = json["reliability"]
        .as_f64()
        .or_else(|| json["most_likely_prob"].as_f64())
        .unwrap_or(0.0);
    let skymap_url = json["skymap_url"]
        .as_str()
        .or_else(|| json["skymap"].as_str())
        .map(|s| s.to_string());

    Ok(GrbTrigger {
        trigger_id,
        instrument: instrument.to_string(),
        trigger_time,
        position,
        significance,
        skymap_url,
        error_radius_deg,
    })
}

/// Parse a Fermi GBM VOEvent XML notice. We extract the half-dozen
/// fields we care about by direct string search — same approach
/// origen uses. The Fermi VOEvent payloads are tiny (a few KB) and
/// well-formed, so the extra dependency a real XML parser would
/// introduce isn't worth it yet.
pub fn parse_fermi_voevent(payload: &str, instrument: &str) -> Result<GrbTrigger, GcnParseError> {
    let trigger_id = extract_voevent_param(payload, "TrigID")
        .or_else(|| extract_voevent_param(payload, "Trigger_Number"))
        .unwrap_or_else(|| "unknown".to_string());

    // Coordinates can appear either as <C1>/<C2> child elements of
    // <Position2D> (Fermi GBM, Swift) or — rarely — as <Param>
    // attributes. Try both, with the Position2D form preferred since
    // that's what Fermi actually emits.
    let ra = extract_xml_element(payload, "C1")
        .or_else(|| extract_voevent_param(payload, "RA").and_then(|s| s.parse::<f64>().ok()));
    let dec = extract_xml_element(payload, "C2")
        .or_else(|| extract_voevent_param(payload, "Dec").and_then(|s| s.parse::<f64>().ok()));

    // Same story for the 1-σ error radius. Fermi GBM puts it inside
    // <Position2D> as a child element; some other VOEvent flavors
    // wrap it in a <Param>. Try the child element first.
    let error_radius_deg = extract_xml_element(payload, "Error2Radius").or_else(|| {
        extract_voevent_param(payload, "Error2Radius").and_then(|s| s.parse::<f64>().ok())
    });

    let position = match (ra, dec) {
        (Some(ra), Some(dec)) => {
            let err_deg = error_radius_deg.unwrap_or(FERMI_GBM_DEFAULT_ERR_DEG);
            Some(SkyPosition::new(ra, dec, err_deg * 3600.0))
        }
        _ => None,
    };

    let trigger_time = extract_voevent_isotime(payload)
        .and_then(|s| iso8601_utc_to_gps(&s).ok())
        .unwrap_or_else(|| {
            warn!("VOEvent ISOTime missing or unparseable; trigger_time=0");
            0.0
        });

    // `Burst_Signif` is the Fermi-reported detection significance in
    // σ — the closest analog to the JSON notices' `reliability`
    // field, and what operators look at to decide whether a trigger
    // is worth chasing.
    let significance = extract_voevent_param(payload, "Burst_Signif")
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);

    Ok(GrbTrigger {
        trigger_id,
        instrument: instrument.to_string(),
        trigger_time,
        position,
        significance,
        skymap_url: None,
        error_radius_deg,
    })
}

/// Derive a canonical instrument label from a Fermi VOEvent topic
/// name. Falls back to a generic suffix when the topic doesn't
/// match one of the known patterns.
pub fn fermi_instrument_for_voevent_topic(topic: &str) -> &'static str {
    if topic.contains("FLT_POS") {
        "Fermi-GBM-FLT"
    } else if topic.contains("GND_POS") {
        "Fermi-GBM-GND"
    } else if topic.contains("FIN_POS") {
        "Fermi-GBM-FIN"
    } else if topic.contains("SUBTHRESH") {
        "Fermi-GBM-SUBTHRESH"
    } else {
        "Fermi-GBM-VOEvent"
    }
}

// ===================== VOEvent string extraction =====================

/// Pull `value="X"` from a `<Param name="$name" value="X" />` line.
/// Tolerates either attribute order. Returns `None` when the param
/// isn't present.
fn extract_voevent_param(xml: &str, name: &str) -> Option<String> {
    let name_pattern = format!("name=\"{}\"", name);
    for line in xml.lines() {
        let line = line.trim();
        if line.contains(&name_pattern) && line.contains("value=\"") {
            if let Some(start) = line.find("value=\"") {
                let rest = &line[start + 7..];
                if let Some(end) = rest.find('"') {
                    return Some(rest[..end].to_string());
                }
            }
        }
    }
    None
}

/// Pull the text content of a simple XML element like `<C1>123.45</C1>`
/// and parse it as f64. Used for `Position2D` coordinates.
fn extract_xml_element(xml: &str, tag: &str) -> Option<f64> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    if let Some(start) = xml.find(&open) {
        let rest = &xml[start + open.len()..];
        if let Some(end) = rest.find(&close) {
            return rest[..end].trim().parse::<f64>().ok();
        }
    }
    None
}

fn extract_voevent_isotime(xml: &str) -> Option<String> {
    if let Some(start) = xml.find("<ISOTime>") {
        let rest = &xml[start + "<ISOTime>".len()..];
        if let Some(end) = rest.find("</ISOTime>") {
            return Some(rest[..end].trim().to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fermi_json_with_all_fields() {
        let payload = r#"{
            "trigger_id": "bn250101000",
            "trigger_time": 757382400.0,
            "ra": 123.456,
            "dec": -45.678,
            "error_radius": 2.5,
            "reliability": 7.5,
            "skymap_url": "https://example.org/skymap.fits"
        }"#;
        let grb = parse_fermi_gbm_json(payload, "Fermi-GBM-FIN").unwrap();
        assert_eq!(grb.trigger_id, "bn250101000");
        assert_eq!(grb.instrument, "Fermi-GBM-FIN");
        // MET → GPS conversion adds the epoch offset.
        assert!((grb.trigger_time - (757382400.0 + FERMI_MET_TO_GPS_OFFSET_SEC)).abs() < 1e-3);
        let pos = grb.position.expect("position should parse");
        assert_eq!(pos.ra, 123.456);
        assert_eq!(pos.dec, -45.678);
        // 2.5° → 9000″.
        assert!((pos.uncertainty_arcsec - 9000.0).abs() < 1e-6);
        assert_eq!(grb.error_radius_deg, Some(2.5));
        assert_eq!(grb.significance, 7.5);
        assert_eq!(
            grb.skymap_url.as_deref(),
            Some("https://example.org/skymap.fits")
        );
    }

    #[test]
    fn fermi_json_uses_default_error_radius() {
        let payload = r#"{
            "trigger_id": "bn250101000",
            "trigger_time": 1.0,
            "ra": 0.0,
            "dec": 0.0
        }"#;
        let grb = parse_fermi_gbm_json(payload, "Fermi-GBM-FLT").unwrap();
        let pos = grb.position.expect("position");
        assert!((pos.uncertainty_arcsec - FERMI_GBM_DEFAULT_ERR_DEG * 3600.0).abs() < 1e-6);
    }

    #[test]
    fn fermi_json_missing_position_returns_none() {
        let payload = r#"{
            "trigger_id": "abc",
            "trigger_time": 1.0
        }"#;
        let grb = parse_fermi_gbm_json(payload, "Fermi-GBM-FLT").unwrap();
        assert!(grb.position.is_none());
    }

    #[test]
    fn fermi_json_alternative_field_names() {
        // Some legacy payloads use camelCase.
        let payload = r#"{
            "triggerID": "12345",
            "triggerTime": 100.0,
            "ra": 1.0,
            "dec": 2.0,
            "errorRadius": 1.0
        }"#;
        let grb = parse_fermi_gbm_json(payload, "Fermi-GBM-FLT").unwrap();
        assert_eq!(grb.trigger_id, "12345");
        assert!(grb.position.is_some());
    }

    #[test]
    fn voevent_extracts_id_and_position() {
        let xml = r#"<voe:VOEvent>
<What>
<Param name="TrigID" value="987654" />
<Param name="RA" value="200.5" />
<Param name="Dec" value="-30.25" />
<Param name="Error2Radius" value="3.5" />
</What>
</voe:VOEvent>"#;
        let grb = parse_fermi_voevent(xml, "Fermi-GBM-FLT").unwrap();
        assert_eq!(grb.trigger_id, "987654");
        let pos = grb.position.unwrap();
        assert_eq!(pos.ra, 200.5);
        assert_eq!(pos.dec, -30.25);
        assert_eq!(grb.error_radius_deg, Some(3.5));
    }

    #[test]
    fn voevent_real_fermi_gbm_shape() {
        // Trimmed but structurally faithful copy of a real Fermi
        // GBM FIN_POS VOEvent — RA/Dec/Error2Radius live as child
        // elements of <Position2D>, not as <Param>s. Burst_Signif
        // is the significance.
        let xml = r#"<voe:VOEvent>
<What>
<Param name="TrigID" value="799078840" ucd="meta.id" />
<Param name="Burst_Signif" value="6.5" unit="sigma" />
</What>
<WhereWhen>
<Position2D unit="deg">
<Value2><C1>276.9500</C1><C2>1.8700</C2></Value2>
<Error2Radius>2.1200</Error2Radius>
</Position2D>
<ISOTime>2026-04-28T14:20:35.68</ISOTime>
</WhereWhen>
</voe:VOEvent>"#;
        let grb = parse_fermi_voevent(xml, "Fermi-GBM-FIN").unwrap();
        assert_eq!(grb.trigger_id, "799078840");
        let pos = grb.position.expect("position should parse");
        assert!((pos.ra - 276.95).abs() < 1e-3);
        assert!((pos.dec - 1.87).abs() < 1e-3);
        assert_eq!(grb.error_radius_deg, Some(2.12));
        assert!((grb.significance - 6.5).abs() < 1e-6);
        assert!(grb.trigger_time > 1.4e9, "GPS time should be parsed");
    }

    #[test]
    fn voevent_falls_back_to_position2d() {
        let xml = r#"<voe:VOEvent>
<Position2D>
<Value2><C1>10.0</C1><C2>20.0</C2></Value2>
</Position2D>
</voe:VOEvent>"#;
        let grb = parse_fermi_voevent(xml, "Fermi-GBM-FLT").unwrap();
        let pos = grb.position.unwrap();
        assert_eq!(pos.ra, 10.0);
        assert_eq!(pos.dec, 20.0);
    }

    #[test]
    fn iso8601_gps_epoch_is_zero() {
        // GPS epoch is itself 0 GPS seconds.
        let gps = iso8601_utc_to_gps("1980-01-06T00:00:00Z").unwrap();
        assert!(gps.abs() < 1e-6, "got {gps}");
    }

    #[test]
    fn iso8601_known_post_2017_leap_offset() {
        // Per IERS, after 2017-01-01 GPS is 18 s ahead of UTC.
        // The check is structural rather than picking a magic
        // value: at a midnight UTC, the GPS value modulo a day
        // should equal exactly the accumulated leap-seconds.
        let gps = iso8601_utc_to_gps("2026-05-23T00:00:00Z").unwrap();
        let offset = (gps as i64) % 86400;
        assert_eq!(
            offset, 18,
            "expected 18 s GPS-UTC offset at midnight; got {offset}"
        );
    }

    #[test]
    fn iso8601_pre_first_leap_has_no_offset() {
        // Between GPS epoch and the first leap-second (1981-07-01),
        // GPS and UTC march in lockstep.
        let gps = iso8601_utc_to_gps("1981-01-01T00:00:00Z").unwrap();
        let offset = (gps as i64) % 86400;
        assert_eq!(
            offset, 0,
            "expected no leap-second offset before 1981-07-01"
        );
    }

    #[test]
    fn iso8601_with_fractional_seconds() {
        let gps = iso8601_utc_to_gps("2026-05-23T00:00:00.5Z").unwrap();
        let base = iso8601_utc_to_gps("2026-05-23T00:00:00Z").unwrap();
        assert!((gps - base - 0.5).abs() < 1e-6);
    }

    #[test]
    fn iso8601_accepts_no_trailing_z() {
        let with_z = iso8601_utc_to_gps("2026-01-15T12:34:56Z").unwrap();
        let without_z = iso8601_utc_to_gps("2026-01-15T12:34:56").unwrap();
        assert!((with_z - without_z).abs() < 1e-6);
    }

    #[test]
    fn iso8601_rejects_garbage() {
        assert!(iso8601_utc_to_gps("not a date").is_err());
        assert!(iso8601_utc_to_gps("2026-01-15 12:34:56").is_err()); // space, not T
    }

    #[test]
    fn voevent_with_isotime_yields_nonzero_trigger_time() {
        let xml = r#"<voe:VOEvent>
<What><Param name="TrigID" value="42" /></What>
<WhereWhen>
<ISOTime>2026-05-23T00:00:00Z</ISOTime>
</WhereWhen>
</voe:VOEvent>"#;
        let grb = parse_fermi_voevent(xml, "Fermi-GBM-FLT").unwrap();
        assert!(
            grb.trigger_time > 1.43e9,
            "expected GPS-scale trigger_time; got {}",
            grb.trigger_time
        );
    }

    #[test]
    fn topic_to_instrument() {
        assert_eq!(
            fermi_instrument_for_voevent_topic("gcn.classic.voevent.FERMI_GBM_FLT_POS"),
            "Fermi-GBM-FLT"
        );
        assert_eq!(
            fermi_instrument_for_voevent_topic("gcn.classic.voevent.FERMI_GBM_GND_POS"),
            "Fermi-GBM-GND"
        );
        assert_eq!(
            fermi_instrument_for_voevent_topic("gcn.classic.voevent.FERMI_GBM_FIN_POS"),
            "Fermi-GBM-FIN"
        );
        assert_eq!(
            fermi_instrument_for_voevent_topic("gcn.classic.voevent.FERMI_GBM_SUBTHRESH"),
            "Fermi-GBM-SUBTHRESH"
        );
        assert_eq!(
            fermi_instrument_for_voevent_topic("anything.else"),
            "Fermi-GBM-VOEvent"
        );
    }
}
