//! Parser for IceCube LVK Coincident Neutrino Track Search
//! alerts (GCN topic `gcn.notices.icecube.lvk_nu_track_search`).
//!
//! Unlike the single-neutrino path in [`crate::neutrino`], these
//! alerts are **search results**: IceCube ran a track search
//! against a specific LVK superevent's localization within an
//! observation window, and reports per-superevent statistics
//! (`pval_generic`, `pval_bayesian`, `n_events_coincident`) plus
//! the individual coincident tracks. So there's no cross-match
//! against the GW localization to perform — the alert *is* the
//! cross-match. We attach it to the superevent it references and
//! surface it on the per-superevent page.
//!
//! Schema reference:
//! `/Users/mcoughlin/Code/GCN/gcn-schema/gcn/notices/icecube/lvk_nu_track_search.schema.json`

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::gcn::{iso8601_utc_to_gps, GcnParseError};
use crate::grb::SkyPosition;

#[derive(Debug, Error)]
pub enum IceCubeLvkParseError {
    #[error("json parse failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("iso8601 time conversion failed: {0}")]
    IsoTime(#[from] GcnParseError),
    #[error("LVK Nu Track Search alert missing required field: {0}")]
    MissingField(&'static str),
}

/// A single coincident IceCube track event picked out by the LVK
/// search. Each carries its own localization + per-event p-values.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CoincidentTrackEvent {
    /// Per-event identifier (the schema uses `id: array<string>`;
    /// we keep the first element since it's the canonical run/event
    /// key like `138590_39138551`).
    pub id: String,
    /// Seconds between the LVK alert time and this neutrino
    /// candidate. Negative = neutrino before merger.
    pub event_dt: f64,
    /// Track localization, when present. `None` only for malformed
    /// payloads.
    pub localization: Option<SkyPosition>,
    pub event_pval_generic: Option<f64>,
    pub event_pval_bayesian: Option<f64>,
}

/// Parsed IceCube LVK Nu Track Search alert. The wire form is
/// rich (flux sensitivities, observation livetime, etc.); we
/// surface the fields the operator cares about on individual
/// struct slots and stash the full envelope in [`Self::body`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IceCubeLvkSearch {
    /// The LVK superevent this search references — primary join
    /// key when attaching to a [`crate::clustering::Superevent`].
    pub superevent_id: String,
    /// `alert_datetime` from the envelope, GPS seconds.
    pub alert_time: f64,
    /// `trigger_time` (LVK merger time), GPS seconds. Always
    /// matches the parent superevent's `t_0` for valid alerts.
    pub trigger_time: f64,
    /// IceCube observation window the search ran over — GPS
    /// seconds. The window is the right scale for "was IceCube
    /// observing when this superevent happened?".
    pub observation_start: Option<f64>,
    pub observation_stop: Option<f64>,
    /// Seconds of live observation time inside the window, as
    /// reported by the upstream pipeline.
    pub observation_livetime: Option<f64>,
    /// p-value from the generic transient search.
    pub pval_generic: Option<f64>,
    /// p-value from the LLAMA Bayesian search.
    pub pval_bayesian: Option<f64>,
    /// Count of IceCube tracks the search found in space-time
    /// coincidence with the GW map.
    pub n_events_coincident: usize,
    /// Per-event details for the coincident tracks.
    pub coincident_events: Vec<CoincidentTrackEvent>,
    /// Combined most-probable source direction across all
    /// coincident tracks + the GW localization. `None` when
    /// `n_events_coincident == 0` (the schema is opt).
    pub most_probable_direction: Option<SkyPosition>,
    /// `[min, max]` time-integrated E^-2 flux sensitivity over
    /// the GW 90% region, in GeV/cm^2.
    pub flux_sensitivity_range: Option<[f64; 2]>,
    /// `[lower, upper]` energy sensitivity range in GeV.
    pub sensitive_energy_range: Option<[f64; 2]>,
    /// Full upstream alert envelope, opaque. Carried for replay
    /// + forward-compat with schema evolution.
    pub body: Value,
}

pub fn parse_icecube_lvk_track_search(
    payload: &str,
) -> Result<IceCubeLvkSearch, IceCubeLvkParseError> {
    let json: Value = serde_json::from_str(payload)?;

    let superevent_id = json["ref_ID"]
        .as_str()
        .ok_or(IceCubeLvkParseError::MissingField("ref_ID"))?
        .to_string();

    let alert_time = match json["alert_datetime"].as_str() {
        Some(s) => iso8601_utc_to_gps(s)?,
        None => return Err(IceCubeLvkParseError::MissingField("alert_datetime")),
    };
    let trigger_time = match json["trigger_time"].as_str() {
        Some(s) => iso8601_utc_to_gps(s)?,
        None => return Err(IceCubeLvkParseError::MissingField("trigger_time")),
    };

    // Observation window timestamps are optional in the schema —
    // some early-warning variants omit them. Fall back to None
    // rather than erroring, since the operator can still act on
    // the rest of the payload.
    let observation_start = json["observation_start"]
        .as_str()
        .and_then(|s| iso8601_utc_to_gps(s).ok());
    let observation_stop = json["observation_stop"]
        .as_str()
        .and_then(|s| iso8601_utc_to_gps(s).ok());
    let observation_livetime = json["observation_livetime"].as_f64();

    let pval_generic = json["pval_generic"].as_f64();
    let pval_bayesian = json["pval_bayesian"].as_f64();
    let n_events_coincident = json["n_events_coincident"]
        .as_u64()
        .map(|n| n as usize)
        .unwrap_or(0);

    let coincident_events = json["coincident_events"]
        .as_array()
        .map(|arr| arr.iter().map(parse_coincident_event).collect())
        .unwrap_or_default();

    let most_probable_direction = parse_localization(&json["most_probable_direction"]);

    let flux_sensitivity_range = json["neutrino_flux_sensitivity_range"]["flux_sensitivity"]
        .as_array()
        .and_then(|a| match a.as_slice() {
            [lo, hi] => Some([lo.as_f64()?, hi.as_f64()?]),
            _ => None,
        });
    let sensitive_energy_range = json["neutrino_flux_sensitivity_range"]["sensitive_energy_range"]
        .as_array()
        .and_then(|a| match a.as_slice() {
            [lo, hi] => Some([lo.as_f64()?, hi.as_f64()?]),
            _ => None,
        });

    Ok(IceCubeLvkSearch {
        superevent_id,
        alert_time,
        trigger_time,
        observation_start,
        observation_stop,
        observation_livetime,
        pval_generic,
        pval_bayesian,
        n_events_coincident,
        coincident_events,
        most_probable_direction,
        flux_sensitivity_range,
        sensitive_energy_range,
        body: json,
    })
}

fn parse_coincident_event(v: &Value) -> CoincidentTrackEvent {
    let id = v["id"]
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|e| e.as_str())
        .or_else(|| v["id"].as_str())
        .unwrap_or("")
        .to_string();
    let event_dt = v["event_dt"].as_f64().unwrap_or(0.0);
    CoincidentTrackEvent {
        id,
        event_dt,
        localization: parse_localization(&v["localization"]),
        event_pval_generic: v["event_pval_generic"].as_f64(),
        event_pval_bayesian: v["event_pval_bayesian"].as_f64(),
    }
}

/// Parse a `{ra, dec, ra_dec_error?}` sub-object. `ra_dec_error`
/// is taken as the 1-σ radius in degrees (the LVK Nu Track Search
/// schema declares it as a scalar number, same as the
/// single-neutrino path).
fn parse_localization(v: &Value) -> Option<SkyPosition> {
    let ra = v["ra"].as_f64()?;
    let dec = v["dec"].as_f64()?;
    let err_deg = v["ra_dec_error"]
        .as_f64()
        .filter(|x| x.is_finite() && *x > 0.0)
        .unwrap_or(0.5);
    Some(SkyPosition::new(ra, dec, err_deg * 3600.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_lvk_track_search_example() {
        // Lifted verbatim from the gcn-schema example.
        let payload = r#"{
            "type": "IceCube LVK Alert Nu Track Search",
            "ref_ID": "S230914ak",
            "alert_datetime": "2023-09-14T11:49:16.526Z",
            "trigger_time": "2023-09-14T11:14:01Z",
            "observation_start": "2023-09-14T11:05:41.000Z",
            "observation_stop": "2023-09-14T11:22:21.000Z",
            "observation_livetime": 1000,
            "pval_generic": 0.0191,
            "pval_bayesian": 0.0549,
            "n_events_coincident": 2,
            "coincident_events": [
                {
                    "event_dt": 12.91,
                    "localization": {"ra": 17.48, "dec": 16.15, "ra_dec_error": 0.5},
                    "id": ["138590_39138551"],
                    "event_pval_generic": 0.0191,
                    "event_pval_bayesian": null
                },
                {
                    "event_dt": 222.46,
                    "localization": {"ra": 13.82, "dec": 18.66, "ra_dec_error": 0.5},
                    "id": ["138590_39164579"],
                    "event_pval_generic": 0.0928,
                    "event_pval_bayesian": 0.0656
                }
            ],
            "most_probable_direction": {"ra": 17.49, "dec": 16.18},
            "neutrino_flux_sensitivity_range": {
                "flux_sensitivity": [0.0277, 0.647],
                "sensitive_energy_range": [542, 23000000]
            }
        }"#;
        let s = parse_icecube_lvk_track_search(payload).unwrap();
        assert_eq!(s.superevent_id, "S230914ak");
        assert_eq!(s.n_events_coincident, 2);
        assert_eq!(s.coincident_events.len(), 2);
        assert_eq!(s.coincident_events[0].id, "138590_39138551");
        assert!((s.pval_generic.unwrap() - 0.0191).abs() < 1e-9);
        assert!((s.pval_bayesian.unwrap() - 0.0549).abs() < 1e-9);
        assert_eq!(s.flux_sensitivity_range, Some([0.0277, 0.647]));
        assert_eq!(s.sensitive_energy_range, Some([542.0, 23_000_000.0]));
        // observation_livetime is a plain number (seconds), not ISO.
        assert_eq!(s.observation_livetime, Some(1000.0));
        // most_probable_direction has no ra_dec_error → falls back
        // to the default 0.5° localization radius.
        let mpd = s.most_probable_direction.unwrap();
        assert!((mpd.ra - 17.49).abs() < 1e-9);
    }

    #[test]
    fn rejects_payload_without_ref_id() {
        let payload =
            r#"{"alert_datetime": "2023-09-14T11:49:16Z", "trigger_time": "2023-09-14T11:14:01Z"}"#;
        let err = parse_icecube_lvk_track_search(payload).unwrap_err();
        assert!(matches!(err, IceCubeLvkParseError::MissingField("ref_ID")));
    }

    #[test]
    fn handles_alert_without_coincident_events() {
        // The "we ran the search, found nothing" branch — the
        // alert still goes out because the absence of coincident
        // tracks is itself a result the operator wants to record.
        let payload = r#"{
            "ref_ID": "S230914ak",
            "alert_datetime": "2023-09-14T11:49:16Z",
            "trigger_time": "2023-09-14T11:14:01Z",
            "n_events_coincident": 0,
            "coincident_events": []
        }"#;
        let s = parse_icecube_lvk_track_search(payload).unwrap();
        assert_eq!(s.n_events_coincident, 0);
        assert!(s.coincident_events.is_empty());
        assert!(s.most_probable_direction.is_none());
    }
}
