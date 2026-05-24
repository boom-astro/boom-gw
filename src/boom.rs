//! Parser for BOOM cross-matched optical-transient alerts
//! (GCN Kafka topic `gcn.notices.boom.alert`, schema v7+).
//!
//! The wire format groups multiple targets + multiple photometry
//! points inside one envelope keyed by `alert_datetime`. For
//! downstream display + storage we explode each *target* into its
//! own [`BoomTransient`] row keyed by `(alert_datetime, event_name)`
//! and carry the photometry filtered to that target. The full
//! upstream envelope is retained as opaque JSON so future schema
//! additions are surfaced in the UI without code changes.
//!
//! Schema reference:
//! `/Users/mcoughlin/Code/GCN/gcn-schema/gcn/notices/boom/alert.schema.json`

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::gcn::{iso8601_utc_to_gps, GcnParseError};
use crate::grb::{GrbTrigger, SkyPosition};

#[derive(Debug, Error)]
pub enum BoomParseError {
    #[error("json parse failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("alert envelope missing required field: {0}")]
    MissingField(&'static str),
    #[error("iso8601 time conversion failed: {0}")]
    IsoTime(#[from] GcnParseError),
}

/// One BOOM-flagged optical transient. A single upstream alert
/// envelope may contain several; the parser exploder yields one
/// `BoomTransient` per `data.targets[]` entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BoomTransient {
    /// Synthetic primary key: `{alert_datetime}__{event_name}`. The
    /// upstream JSON has no first-class alert id and the same
    /// (envelope time, event) pair never repeats in normal
    /// operation, so this is a stable natural key.
    pub alert_id: String,
    /// `alert_datetime` from the envelope, converted to GPS seconds.
    /// `0` if the field was missing or unparseable.
    pub alert_time: f64,
    /// Upstream survey transient name — `ZTF25acffkdr`, etc.
    pub event_name: String,
    /// Best-fit position (degrees). `None` only if the target
    /// envelope is malformed.
    pub ra: Option<f64>,
    pub dec: Option<f64>,
    /// Optical localization uncertainty in degrees. Optical alerts
    /// from ZTF/LSST are arcsec-scale; we use 0.001° (3.6″) as a
    /// reasonable default when the field is absent, matching
    /// SkyPortal's `get_skymap_cone` floor.
    pub error_radius_deg: Option<f64>,
    /// Highest-confidence classification label + score from the
    /// `classification_scores` dict. Picked because the dict can
    /// have arbitrary keys and the table view needs ONE label.
    pub classification: Option<String>,
    pub classification_score: Option<f64>,
    /// Comma-joined human-readable summary of the
    /// `gcn_crossmatch[]` array — e.g. `"GW LVK S251117bs"`.
    pub cross_match_summary: Option<String>,
    /// Per-photometry points for this target's `event_name`.
    /// Slimmed to a few columns; the full original photometry
    /// lives in [`Self::body`] for callers that need it.
    pub photometry: Vec<BoomPhotometry>,
    /// Full upstream alert envelope, opaque. Useful for replay
    /// + forward-compat with schema evolution.
    pub body: Value,
}

/// A single photometric measurement carried inside a BOOM alert.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BoomPhotometry {
    pub observation_start: Option<String>,
    pub telescope: Option<String>,
    pub instrument: Option<String>,
    pub filter: Option<String>,
    pub mag: Option<f64>,
    pub mag_error: Option<f64>,
    pub mag_system: Option<String>,
    pub limiting_mag: Option<f64>,
}

/// Default 1-σ optical localization radius (degrees) used when a
/// target has no explicit uncertainty. 3.6″ ≈ 0.001° — sub-pixel
/// on most modern surveys but small enough that a generated MOC
/// cone stays nontrivially small.
const DEFAULT_OPTICAL_ERROR_DEG: f64 = 0.001;

/// Instrument label every cross-match against a BOOM transient
/// uses. Single constant so the storage key, the cross-match key,
/// and any "show all BOOM matches" filter all agree.
pub const BOOM_INSTRUMENT_LABEL: &str = "BOOM";

impl BoomTransient {
    /// View this BOOM transient as a [`GrbTrigger`]-shaped record
    /// for the purposes of the cross-match math. The trigger_id is
    /// the synthetic BOOM `alert_id` and the instrument string is
    /// the fixed [`BOOM_INSTRUMENT_LABEL`], so the resulting
    /// `(instrument, trigger_id)` key namespace doesn't collide
    /// with any real Fermi/Swift trigger.
    pub fn as_trigger(&self) -> Option<GrbTrigger> {
        let (ra, dec) = (self.ra?, self.dec?);
        let radius_deg = self.error_radius_deg.unwrap_or(DEFAULT_OPTICAL_ERROR_DEG);
        Some(GrbTrigger {
            trigger_id: self.alert_id.clone(),
            instrument: BOOM_INSTRUMENT_LABEL.to_string(),
            trigger_time: self.alert_time,
            position: Some(SkyPosition::new(ra, dec, radius_deg * 3600.0)),
            significance: self.classification_score.unwrap_or(0.0),
            skymap_url: None,
            error_radius_deg: Some(radius_deg),
        })
    }
}

/// Parse a single GCN BOOM JSON alert and emit one
/// [`BoomTransient`] per target. Returns an empty Vec only when
/// the envelope's `data.targets` array is empty (early-warning
/// envelopes that haven't matched any optical transient yet).
pub fn parse_boom_alert(payload: &str) -> Result<Vec<BoomTransient>, BoomParseError> {
    let root: Value = serde_json::from_str(payload)?;
    let alert_datetime = root["alert_datetime"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let alert_time = if alert_datetime.is_empty() {
        0.0
    } else {
        iso8601_utc_to_gps(&alert_datetime).unwrap_or(0.0)
    };

    let targets = root["data"]["targets"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let photometry = root["data"]["photometry"]
        .as_array()
        .cloned()
        .unwrap_or_default();

    let mut out = Vec::with_capacity(targets.len());
    for t in targets {
        let event_name = t["event_name"].as_str().unwrap_or("unknown").to_string();
        let alert_id = format!("{alert_datetime}__{event_name}");
        let ra = t["ra"].as_f64();
        let dec = t["dec"].as_f64();
        let error_radius_deg = t["ra_dec_error"]
            .as_f64()
            .or_else(|| t["uncertainty_deg"].as_f64())
            .or(Some(DEFAULT_OPTICAL_ERROR_DEG));

        // Highest-scoring classification from
        // {label: score} dict. Ties broken by JSON-iteration order.
        let (classification, classification_score) = match t["classification_scores"].as_object() {
            Some(obj) => {
                let mut best: Option<(String, f64)> = None;
                for (k, v) in obj {
                    let s = v.as_f64().unwrap_or(0.0);
                    if best.as_ref().map(|(_, bs)| s > *bs).unwrap_or(true) {
                        best = Some((k.clone(), s));
                    }
                }
                match best {
                    Some((k, s)) => (Some(k), Some(s)),
                    None => (None, None),
                }
            }
            None => (None, None),
        };

        let cross_match_summary = t["gcn_crossmatch"].as_array().map(|xms| {
            xms.iter()
                .filter_map(|x| {
                    let ref_type = x["ref_type"].as_str()?;
                    let ref_instrument = x["ref_instrument"].as_str().unwrap_or("");
                    let ref_id = x["ref_ID"].as_str().unwrap_or("");
                    Some(
                        format!("{ref_type} {ref_instrument} {ref_id}")
                            .trim()
                            .to_string(),
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        });

        let target_photometry: Vec<BoomPhotometry> = photometry
            .iter()
            .filter(|p| p["event_name"].as_str() == Some(event_name.as_str()))
            .map(|p| BoomPhotometry {
                observation_start: p["observation_start"].as_str().map(String::from),
                telescope: p["telescope"].as_str().map(String::from),
                instrument: p["instrument"].as_str().map(String::from),
                filter: p["filter"].as_str().map(String::from),
                mag: p["mag"].as_f64(),
                mag_error: p["mag_error"].as_f64(),
                mag_system: p["mag_system"].as_str().map(String::from),
                limiting_mag: p["limiting_mag"].as_f64(),
            })
            .collect();

        out.push(BoomTransient {
            alert_id,
            alert_time,
            event_name,
            ra,
            dec,
            error_radius_deg,
            classification,
            classification_score,
            cross_match_summary,
            photometry: target_photometry,
            body: root.clone(),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA_EXAMPLE: &str = r#"{
  "$schema": "https://gcn.nasa.gov/schema/main/gcn/notices/boom/alert.schema.json",
  "alert_datetime": "2026-11-30T10:28:57Z",
  "mission": "Boom",
  "data": {
    "targets": [
      {
        "event_name": "ZTF25acffkdr",
        "ra": 150.175649,
        "dec": 15.2832185,
        "classification_scores": { "Type II": 0.9, "Type Ia": 0.07 },
        "gcn_crossmatch": [
          { "ref_type": "GW", "ref_instrument": "LVK", "ref_ID": "S251117bs" }
        ]
      }
    ],
    "photometry": [
      {
        "event_name": "ZTF25acffkdr",
        "observation_start": "2026-11-28T10:07:08.003Z",
        "telescope": "Palomar 1.2m Oschin",
        "instrument": "ZTF",
        "filter": "ztfg",
        "mag": 18.1,
        "mag_error": 0.02,
        "mag_system": "AB",
        "limiting_mag": 20.48
      },
      {
        "event_name": "ZTF25acffkdr",
        "observation_start": "2026-11-29T03:25:56.064Z",
        "telescope": "Liverpool Telescope",
        "instrument": "IOO",
        "filter": "sdssg",
        "mag": 17.78,
        "mag_system": "AB"
      }
    ]
  }
}"#;

    #[test]
    fn parses_one_target_with_classification_and_xmatch() {
        let out = parse_boom_alert(SCHEMA_EXAMPLE).unwrap();
        assert_eq!(out.len(), 1);
        let t = &out[0];
        assert_eq!(t.event_name, "ZTF25acffkdr");
        assert_eq!(t.alert_id, "2026-11-30T10:28:57Z__ZTF25acffkdr");
        assert!(
            t.alert_time > 1.4e9,
            "expected GPS scale, got {}",
            t.alert_time
        );
        assert_eq!(t.ra, Some(150.175649));
        assert_eq!(t.dec, Some(15.2832185));
        // Pick the higher of {Type II: 0.9, Type Ia: 0.07}.
        assert_eq!(t.classification.as_deref(), Some("Type II"));
        assert_eq!(t.classification_score, Some(0.9));
        assert_eq!(t.cross_match_summary.as_deref(), Some("GW LVK S251117bs"));
        // Photometry filtered to this target — both rows have the
        // same event_name.
        assert_eq!(t.photometry.len(), 2);
        assert_eq!(t.photometry[0].instrument.as_deref(), Some("ZTF"));
        assert_eq!(
            t.photometry[1].telescope.as_deref(),
            Some("Liverpool Telescope")
        );
    }

    #[test]
    fn empty_targets_yields_empty_vec() {
        let payload =
            r#"{"alert_datetime":"2026-01-01T00:00:00Z","data":{"targets":[],"photometry":[]}}"#;
        assert!(parse_boom_alert(payload).unwrap().is_empty());
    }

    #[test]
    fn missing_classification_scores_handled() {
        let payload = r#"{
            "alert_datetime": "2026-01-01T00:00:00Z",
            "data": {"targets":[{"event_name":"X","ra":1.0,"dec":2.0}],"photometry":[]}
        }"#;
        let out = parse_boom_alert(payload).unwrap();
        assert_eq!(out.len(), 1);
        assert!(out[0].classification.is_none());
        assert!(out[0].classification_score.is_none());
    }

    #[test]
    fn defaults_optical_error_when_absent() {
        let payload = r#"{
            "alert_datetime": "2026-01-01T00:00:00Z",
            "data": {"targets":[{"event_name":"X","ra":1.0,"dec":2.0}],"photometry":[]}
        }"#;
        let out = parse_boom_alert(payload).unwrap();
        assert!((out[0].error_radius_deg.unwrap() - DEFAULT_OPTICAL_ERROR_DEG).abs() < 1e-12);
    }

    #[test]
    fn rejects_invalid_json() {
        assert!(matches!(
            parse_boom_alert("not json"),
            Err(BoomParseError::Json(_))
        ));
    }
}
