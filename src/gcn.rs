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

    let ra = extract_voevent_param(payload, "RA")
        .and_then(|s| s.parse::<f64>().ok())
        .or_else(|| extract_xml_element(payload, "C1"));
    let dec = extract_voevent_param(payload, "Dec")
        .and_then(|s| s.parse::<f64>().ok())
        .or_else(|| extract_xml_element(payload, "C2"));
    let error_radius_deg =
        extract_voevent_param(payload, "Error2Radius").and_then(|s| s.parse::<f64>().ok());

    let position = match (ra, dec) {
        (Some(ra), Some(dec)) => {
            let err_deg = error_radius_deg.unwrap_or(FERMI_GBM_DEFAULT_ERR_DEG);
            Some(SkyPosition::new(ra, dec, err_deg * 3600.0))
        }
        _ => None,
    };

    // VOEvent ISOTime → GPS conversion is non-trivial (UTC ↔ GPS
    // leap-second handling). Parser punts to 0.0 until we have a
    // real need — the JSON path covers modern alerts.
    let trigger_time = 0.0;
    if extract_voevent_isotime(payload).is_some() {
        warn!("VOEvent ISOTime field present but GPS conversion not implemented; trigger_time=0");
    }

    Ok(GrbTrigger {
        trigger_id,
        instrument: instrument.to_string(),
        trigger_time,
        position,
        significance: 0.0,
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
