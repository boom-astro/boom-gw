//! Fast-radio-burst alert types — the FRB peer of [`crate::grb`].
//!
//! FRBs share the GRB ingest shape (a single trigger time + a
//! point localization with circular or elliptical uncertainty),
//! so the cross-match math reuses [`crate::grb::GrbTrigger`]. The
//! type defined here carries the source-specific extras (DM, SNR,
//! importance, repeating-source name) that the operator needs to
//! see in the UI but the spatial × temporal cross-match call
//! doesn't.
//!
//! Schema references:
//! * `/Users/mcoughlin/Code/GCN/gcn-schema/gcn/notices/chime/frb.schema.json`
//! * `/Users/mcoughlin/Code/GCN/gcn-schema/gcn/notices/dsa110/frb.schema.json`
//!
//! Both schemas inherit `core/Alert.schema.json` +
//! `core/Localization.schema.json` + `core/DispersionMeasure.schema.json`,
//! so the field set is nearly identical. The two GCN topics
//! `gcn.notices.chime.frb` and `gcn.notices.dsa110.frb` reuse the
//! same parser; only the `instrument` label differs.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::gcn::{iso8601_utc_to_gps, GcnParseError};
use crate::grb::{GrbTrigger, SkyPosition};

#[derive(Debug, Error)]
pub enum FrbParseError {
    #[error("json parse failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("iso8601 time conversion failed: {0}")]
    IsoTime(#[from] GcnParseError),
    #[error("FRB alert missing required field: {0}")]
    MissingField(&'static str),
}

/// Default 1-σ localization radius used when an FRB alert reports
/// neither `ra_dec_error` nor a parsable error array. CHIME's
/// real-time pipeline localizations are typically arcminute-scale
/// (~0.01°), so 0.05° is a conservative fallback that won't fold in
/// the entire sky.
pub const FRB_DEFAULT_ERR_DEG: f64 = 0.05;

/// Instrument labels emitted by the FRB parsers. Kept as constants
/// so the consumer (which routes by Kafka topic) and the cross-
/// match storage layer (which keys on instrument string) can't
/// drift from each other.
pub const CHIME_INSTRUMENT_LABEL: &str = "CHIME-FRB";
pub const DSA110_INSTRUMENT_LABEL: &str = "DSA110-FRB";

/// Parsed FRB alert. The cross-match-relevant fields live on
/// [`Self::trigger`]; the source-specific fields are siblings so
/// the External Streams table can render them and the cross-match
/// path can ignore them.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FrbAlert {
    /// GRB-shaped trigger view used by the cross-match math.
    /// `instrument` is one of [`CHIME_INSTRUMENT_LABEL`] /
    /// [`DSA110_INSTRUMENT_LABEL`]; `trigger_id` is the upstream
    /// alert `id`; `trigger_time` is GPS seconds; `significance`
    /// holds the upstream `snr` (so the table can sort by it).
    ///
    /// Serialized with `#[serde(flatten)]` so the persisted doc
    /// keeps a flat layout — the scan filter on `trigger_time`
    /// and the list-filter on `instrument` both target root-level
    /// fields, same as the existing [`crate::grb::GrbTrigger`]
    /// storage layout.
    #[serde(flatten)]
    pub trigger: GrbTrigger,
    /// Dispersion measure in pc/cm^3. Distinguishes Galactic
    /// pulsars (low DM) from extragalactic FRBs (high DM).
    #[serde(default)]
    pub dm: Option<f64>,
    /// Reported 1-σ DM uncertainty in pc/cm^3.
    #[serde(default)]
    pub dm_error: Option<f64>,
    /// CHIME-style real-vs-RFI ML score in [0, 1]. DSA110 also
    /// emits this under the same key.
    #[serde(default)]
    pub importance: Option<f64>,
    /// Signal-to-noise ratio (dimensionless). Repeated on
    /// [`Self::trigger::significance`] for table sorting.
    #[serde(default)]
    pub snr: Option<f64>,
    /// Known source association (TNS name) when the burst is
    /// flagged as a repeater. Empty / unset for "new" FRBs.
    #[serde(default)]
    pub known_source: Option<String>,
    /// Full upstream alert envelope, opaque. Carried for replay +
    /// forward-compat with schema evolution.
    pub body: Value,
}

/// Parse one CHIME or DSA110 FRB alert (both share the same wire
/// shape — see schema cross-refs at the module head). `instrument`
/// should be one of [`CHIME_INSTRUMENT_LABEL`] /
/// [`DSA110_INSTRUMENT_LABEL`], picked by the caller based on the
/// Kafka topic.
pub fn parse_frb_alert(payload: &str, instrument: &str) -> Result<FrbAlert, FrbParseError> {
    let json: Value = serde_json::from_str(payload)?;

    let trigger_id = json["id"]
        .as_str()
        .ok_or(FrbParseError::MissingField("id"))?
        .to_string();

    let trigger_time = match json["trigger_time"].as_str() {
        Some(s) => iso8601_utc_to_gps(s)?,
        None => return Err(FrbParseError::MissingField("trigger_time")),
    };

    let ra = json["ra"].as_f64();
    let dec = json["dec"].as_f64();
    // `ra_dec_error` is an array `[major, minor, ...]` per both
    // CHIME and DSA110 schemas. We take the major axis as the
    // worst-case 1-σ radius — same convention the BAYESTAR
    // localization summary uses for elliptical error regions.
    let error_radius_deg = ra_dec_error_major_axis(&json["ra_dec_error"]);
    let position = match (ra, dec) {
        (Some(ra), Some(dec)) => Some(SkyPosition::new(
            ra,
            dec,
            error_radius_deg.unwrap_or(FRB_DEFAULT_ERR_DEG) * 3600.0,
        )),
        _ => None,
    };

    let snr = json["snr"].as_f64();
    let dm = json["dm"].as_f64();
    let dm_error = json["dm_error"].as_f64();
    let importance = json["importance"].as_f64();
    let known_source = json["known_source"].as_str().map(str::to_string);

    let trigger = GrbTrigger {
        trigger_id,
        instrument: instrument.to_string(),
        trigger_time,
        position,
        significance: snr.unwrap_or(0.0),
        skymap_url: None,
        error_radius_deg,
    };
    Ok(FrbAlert {
        trigger,
        dm,
        dm_error,
        importance,
        snr,
        known_source,
        body: json,
    })
}

/// Extract the **major axis** from a `ra_dec_error` JSON value.
/// Accepts either a `[major, minor, ...]` array (CHIME/DSA110) or
/// a single number (some envelope variants). Returns `None` if the
/// field is absent or unparseable.
fn ra_dec_error_major_axis(v: &Value) -> Option<f64> {
    if let Some(arr) = v.as_array() {
        // Take the largest finite element so an ordering swap by the
        // upstream emitter can't accidentally collapse the cone.
        arr.iter()
            .filter_map(|e| e.as_f64())
            .filter(|x| x.is_finite() && *x > 0.0)
            .fold(None, |acc: Option<f64>, x| {
                Some(acc.map(|a| a.max(x)).unwrap_or(x))
            })
    } else {
        v.as_f64().filter(|x| x.is_finite() && *x > 0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_chime_detection_example() {
        // Lifted verbatim from
        // /Users/mcoughlin/Code/GCN/gcn-schema/gcn/notices/chime/frb.detection.example.json.
        let payload = r#"{
            "alert_type": "initial",
            "trigger_time": "2024-09-18T07:19:10.765268Z",
            "trigger_time_error": 0.00786431,
            "id": "427325191",
            "snr": 12.6985,
            "ra": 346.7785,
            "dec": 12.6324,
            "ra_dec_error": [0.5038, 0.5989, 0],
            "dm": 279.42,
            "dm_error": 0.4044,
            "importance": 0.9871
        }"#;
        let frb = parse_frb_alert(payload, CHIME_INSTRUMENT_LABEL).unwrap();
        assert_eq!(frb.trigger.trigger_id, "427325191");
        assert_eq!(frb.trigger.instrument, "CHIME-FRB");
        assert!(frb.trigger.trigger_time > 1.4e9);
        assert_eq!(frb.dm, Some(279.42));
        assert_eq!(frb.snr, Some(12.6985));
        // Major-axis pick: 0.5989 > 0.5038.
        assert!((frb.trigger.error_radius_deg.unwrap() - 0.5989).abs() < 1e-9);
        let pos = frb.trigger.position.unwrap();
        assert!((pos.ra - 346.7785).abs() < 1e-9);
        assert!((pos.dec - 12.6324).abs() < 1e-9);
    }

    #[test]
    fn parses_dsa110_detection_example() {
        // Lifted verbatim from the dsa110 detection example.
        let payload = r#"{
            "alert_type": "initial",
            "trigger_time": "2024-09-18T07:19:10.765268Z",
            "id": "240918aaaa",
            "snr": 12.6986,
            "dm": 279.42,
            "event_duration": 1,
            "ra": 346.7785,
            "dec": 12.6325,
            "ra_dec_error": [0.016, 0.02],
            "importance": 0.9871
        }"#;
        let frb = parse_frb_alert(payload, DSA110_INSTRUMENT_LABEL).unwrap();
        assert_eq!(frb.trigger.trigger_id, "240918aaaa");
        assert_eq!(frb.trigger.instrument, "DSA110-FRB");
        // Major axis is 0.02 (max of {0.016, 0.02}).
        assert!((frb.trigger.error_radius_deg.unwrap() - 0.02).abs() < 1e-9);
    }

    #[test]
    fn parser_rejects_missing_trigger_time() {
        let payload = r#"{"id": "abc"}"#;
        let err = parse_frb_alert(payload, CHIME_INSTRUMENT_LABEL).unwrap_err();
        assert!(
            matches!(err, FrbParseError::MissingField("trigger_time")),
            "expected MissingField(trigger_time); got {err:?}"
        );
    }

    #[test]
    fn parser_rejects_missing_id() {
        let payload = r#"{"trigger_time": "2024-09-18T07:19:10Z"}"#;
        let err = parse_frb_alert(payload, CHIME_INSTRUMENT_LABEL).unwrap_err();
        assert!(
            matches!(err, FrbParseError::MissingField("id")),
            "expected MissingField(id); got {err:?}"
        );
    }

    #[test]
    fn parser_falls_back_to_default_radius_when_array_is_empty() {
        let payload = r#"{
            "id": "no-radius",
            "trigger_time": "2024-01-01T00:00:00Z",
            "ra": 10.0,
            "dec": 20.0,
            "ra_dec_error": []
        }"#;
        let frb = parse_frb_alert(payload, CHIME_INSTRUMENT_LABEL).unwrap();
        assert_eq!(frb.trigger.error_radius_deg, None);
        // The position is still emitted using FRB_DEFAULT_ERR_DEG so
        // the cross-match cone has something finite to work with.
        let pos = frb.trigger.position.unwrap();
        let expected_arcsec = FRB_DEFAULT_ERR_DEG * 3600.0;
        assert!((pos.uncertainty_arcsec - expected_arcsec).abs() < 1e-9);
    }

    #[test]
    fn ra_dec_error_major_axis_handles_scalar_form() {
        let v = serde_json::json!(0.7);
        assert_eq!(ra_dec_error_major_axis(&v), Some(0.7));
        // Zero / negative are rejected — same as the radius_deg
        // sanity check in [`crate::grb::build_canonical_moc_fits`].
        let v = serde_json::json!(0.0);
        assert_eq!(ra_dec_error_major_axis(&v), None);
    }
}
