//! Neutrino alert types — the high-energy-neutrino peer of
//! [`crate::grb`].
//!
//! Covers two GCN topics:
//!
//! * `gcn.notices.icecube.single_neutrino_alerts` — Gold / Bronze
//!   track alerts emitted by IceCube's astrophysical-event
//!   selection pipelines. Single-event, point localization
//!   (sometimes with a separate `healpix_url`).
//! * `gcn.notices.km3net.alert` — KM3NeT's online-analysis
//!   triggers (ORCA exceptional events + multiplet alerts).
//!
//! Both inherit the common `gcn/notices/neutrino/Alert.schema.json`
//! so the shared field set (`id`, `event_name`, `trigger_time`,
//! `ra/dec/ra_dec_error`, `pipeline`, `far`, `healpix_url`) is
//! identical. The IceCube-specific `nu_energy` + `p_astro` and
//! the KM3NeT-specific `p_value` + `src_error_50` live as optional
//! sibling fields on [`NeutrinoAlert`].
//!
//! Schema references:
//! * `/Users/mcoughlin/Code/GCN/gcn-schema/gcn/notices/icecube/single_neutrino_alerts.schema.json`
//! * `/Users/mcoughlin/Code/GCN/gcn-schema/gcn/notices/km3net/alert.schema.json`

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::gcn::{iso8601_utc_to_gps, GcnParseError};
use crate::grb::{GrbTrigger, SkyPosition};

#[derive(Debug, Error)]
pub enum NeutrinoParseError {
    #[error("json parse failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("iso8601 time conversion failed: {0}")]
    IsoTime(#[from] GcnParseError),
    #[error("neutrino alert missing required field: {0}")]
    MissingField(&'static str),
}

/// Default 1-σ localization radius (degrees) used when an alert
/// lacks a parsable `ra_dec_error`. IceCube tracks typically come
/// in at ~0.5°, KM3NeT at ~1°. 0.5° is a reasonable lower-bound
/// fallback that won't fold in too much of the sky.
pub const NEUTRINO_DEFAULT_ERR_DEG: f64 = 0.5;

/// Instrument labels emitted by the neutrino parsers. Picked by
/// the consumer based on the originating Kafka topic.
pub const ICECUBE_INSTRUMENT_LABEL: &str = "IceCube";
pub const KM3NET_INSTRUMENT_LABEL: &str = "KM3NeT";

/// Parsed high-energy neutrino alert. The cross-match-relevant
/// fields live on [`Self::trigger`]; the source-specific fields
/// are siblings so the External Streams table can render them and
/// the cross-match call can ignore them.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NeutrinoAlert {
    /// GRB-shaped trigger view used by the cross-match math.
    /// `instrument` is one of [`ICECUBE_INSTRUMENT_LABEL`] /
    /// [`KM3NET_INSTRUMENT_LABEL`]; `trigger_id` is the upstream
    /// `id` (or the first element if `id` is an array);
    /// `trigger_time` is GPS seconds; `significance` carries the
    /// most descriptive scalar score the alert reports (see
    /// [`NeutrinoAlert::scalar_significance`]).
    ///
    /// `#[serde(flatten)]` so the persisted doc has a flat layout
    /// matching [`crate::grb::GrbTriggerDoc`] — the list filter
    /// (`?instrument=…`) and the scan-window filter on
    /// `trigger_time` both look at root-level fields.
    #[serde(flatten)]
    pub trigger: GrbTrigger,
    /// IceCube event topology — "Track" / "Shower" / "Multiplet".
    /// `None` when the field is absent (KM3NeT multiplet alerts
    /// already carry the topology on the parent envelope).
    #[serde(default)]
    pub alert_topology: Option<String>,
    /// Pipeline tag. IceCube: "Gold Track Alert" / "Bronze Track
    /// Alert" / "Cascade Alert". KM3NeT: "orca_HE" / "multiplet"
    /// etc. Preserved verbatim — the UI groups on it.
    #[serde(default)]
    pub pipeline: Option<String>,
    /// IceCube-only: most probable neutrino energy in TeV.
    #[serde(default)]
    pub nu_energy: Option<f64>,
    /// IceCube-only: probability the event is astrophysical.
    #[serde(default)]
    pub p_astro: Option<f64>,
    /// KM3NeT-only: occurrence probability of the alert.
    #[serde(default)]
    pub p_value: Option<f64>,
    /// Reported false-alarm rate (Hz). IceCube and KM3NeT both
    /// emit this when available.
    #[serde(default)]
    pub far: Option<f64>,
    /// Out-of-band skymap URL. Today we don't fetch it — the
    /// localization comes from the in-line `ra/dec/ra_dec_error`.
    #[serde(default)]
    pub healpix_url: Option<String>,
    /// Survey transient name. IceCube reports "IceCube-230416A";
    /// KM3NeT reports "KM3-240901A". Useful for the table view
    /// even though the cross-match math doesn't use it.
    #[serde(default)]
    pub event_name: Option<String>,
    /// Full upstream alert envelope, opaque. Carried for replay +
    /// forward-compat with schema evolution.
    pub body: Value,
}

impl NeutrinoAlert {
    /// Pick the most informative scalar significance from the
    /// available alert fields. IceCube alerts emit `p_astro`;
    /// KM3NeT emits `p_value`; either way, the score is a
    /// probability in [0, 1]. Falls back to 0 when neither field
    /// is present (which is what the GRB path already does).
    pub fn scalar_significance(&self) -> f64 {
        self.p_astro.or(self.p_value).unwrap_or(0.0)
    }
}

/// Parse an IceCube single-neutrino alert (`gcn.notices.icecube.
/// single_neutrino_alerts` topic). The schema is shared between
/// Gold and Bronze track alerts and cascade alerts; the
/// `pipeline` field on the alert tells you which.
pub fn parse_icecube_single_neutrino_alert(
    payload: &str,
) -> Result<NeutrinoAlert, NeutrinoParseError> {
    let json: Value = serde_json::from_str(payload)?;

    // `id` is declared as an array of strings in the IceCube
    // schema. We use the first element as the trigger key — it's
    // the canonical run/event identifier (e.g. "138590_39138551")
    // and survives schema-allowed reorderings of the array.
    let trigger_id =
        extract_id_first_string(&json["id"]).ok_or(NeutrinoParseError::MissingField("id"))?;

    let trigger_time = match json["trigger_time"].as_str() {
        Some(s) => iso8601_utc_to_gps(s)?,
        None => return Err(NeutrinoParseError::MissingField("trigger_time")),
    };

    let position = build_position(&json);
    let error_radius_deg = json["ra_dec_error"]
        .as_f64()
        .filter(|x| x.is_finite() && *x > 0.0);

    let alert_topology = json["alert_topology"].as_str().map(str::to_string);
    let pipeline = json["pipeline"].as_str().map(str::to_string);
    let nu_energy = json["nu_energy"].as_f64();
    let p_astro = json["p_astro"].as_f64();
    let far = json["far"].as_f64();
    let healpix_url = json["healpix_url"].as_str().map(str::to_string);
    let event_name = extract_id_first_string(&json["event_name"]);

    let alert = NeutrinoAlert {
        trigger: GrbTrigger {
            trigger_id,
            instrument: ICECUBE_INSTRUMENT_LABEL.to_string(),
            trigger_time,
            position,
            // Filled in post-hoc once we know the source-specific
            // scalar — see assignment after the struct literal.
            significance: 0.0,
            skymap_url: healpix_url.clone(),
            error_radius_deg,
            // IceCube single-neutrino notices carry a `far`
            // field — propagate it so the targeted joint-FAR
            // path can use it downstream.
            far_hz: far,
        },
        alert_topology,
        pipeline,
        nu_energy,
        p_astro,
        p_value: None,
        far,
        healpix_url,
        event_name,
        body: json,
    };
    Ok(with_scalar_significance(alert))
}

/// Parse a KM3NeT alert (`gcn.notices.km3net.alert` topic).
/// Shares the neutrino-alert base schema with IceCube, but emits
/// `p_value` instead of `p_astro` and ships `src_error_50` (a 50%
/// CL radius — wider than the 1-σ value we want, so we prefer the
/// base `ra_dec_error` when both are present).
pub fn parse_km3net_alert(payload: &str) -> Result<NeutrinoAlert, NeutrinoParseError> {
    let json: Value = serde_json::from_str(payload)?;

    // KM3NeT declares `id` as a single string per
    // /Users/mcoughlin/Code/GCN/gcn-schema/gcn/notices/km3net/alert.schema.json.
    let trigger_id = json["id"]
        .as_str()
        .map(str::to_string)
        .or_else(|| extract_id_first_string(&json["id"]))
        .ok_or(NeutrinoParseError::MissingField("id"))?;

    let trigger_time = match json["trigger_time"].as_str() {
        Some(s) => iso8601_utc_to_gps(s)?,
        None => return Err(NeutrinoParseError::MissingField("trigger_time")),
    };

    let position = build_position(&json);
    let error_radius_deg = json["ra_dec_error"]
        .as_f64()
        .filter(|x| x.is_finite() && *x > 0.0);

    let alert_topology = json["alert_topology"].as_str().map(str::to_string);
    let pipeline = json["pipeline"].as_str().map(str::to_string);
    let p_value = json["p_value"].as_f64();
    let far = json["far"].as_f64();
    let healpix_url = json["healpix_url"].as_str().map(str::to_string);
    let event_name = json["event_name"].as_str().map(str::to_string);

    let alert = NeutrinoAlert {
        trigger: GrbTrigger {
            trigger_id,
            instrument: KM3NET_INSTRUMENT_LABEL.to_string(),
            trigger_time,
            position,
            significance: 0.0,
            skymap_url: healpix_url.clone(),
            error_radius_deg,
            // KM3NeT notices carry a `far` field too — same
            // semantics as IceCube.
            far_hz: far,
        },
        alert_topology,
        pipeline,
        nu_energy: None,
        p_astro: None,
        p_value,
        far,
        healpix_url,
        event_name,
        body: json,
    };
    Ok(with_scalar_significance(alert))
}

/// Common `ra` / `dec` / `ra_dec_error` → `SkyPosition` helper.
/// Both IceCube and KM3NeT use the same field names + a scalar
/// `ra_dec_error` (degrees).
fn build_position(json: &Value) -> Option<SkyPosition> {
    let ra = json["ra"].as_f64()?;
    let dec = json["dec"].as_f64()?;
    let err_deg = json["ra_dec_error"]
        .as_f64()
        .filter(|x| x.is_finite() && *x > 0.0)
        .unwrap_or(NEUTRINO_DEFAULT_ERR_DEG);
    Some(SkyPosition::new(ra, dec, err_deg * 3600.0))
}

/// Walk a JSON value that may be a string OR an array-of-strings
/// and yield the first usable string. The IceCube schema declares
/// both `id` and `event_name` as arrays, while KM3NeT keeps them
/// as plain strings.
fn extract_id_first_string(v: &Value) -> Option<String> {
    if let Some(s) = v.as_str() {
        return Some(s.to_string());
    }
    if let Some(arr) = v.as_array() {
        for el in arr {
            if let Some(s) = el.as_str() {
                return Some(s.to_string());
            }
        }
    }
    None
}

/// Populate [`GrbTrigger::significance`] from whichever scalar the
/// alert provided. Done after the struct literal so the math is
/// in one place across both source-specific parsers.
fn with_scalar_significance(mut a: NeutrinoAlert) -> NeutrinoAlert {
    a.trigger.significance = a.scalar_significance();
    a
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_icecube_gold_bronze_example() {
        // Lifted verbatim from the gcn-schema repo.
        let payload = r#"{
            "mission": "IceCube",
            "instrument": "IC86",
            "messenger": "Neutrino",
            "pipeline": "Bronze Track Alert",
            "record_number": 1,
            "event_name": ["IceCube-230416A"],
            "id": ["137840_57034692"],
            "alert_datetime": "2023-04-16T05:42:00.0Z",
            "alert_type": "initial",
            "alert_tense": "current",
            "alert_topology": "Track",
            "number_of_events": 1,
            "ra": 345.82,
            "dec": 9.01,
            "ra_dec_error": 0.5,
            "containment_probability": 0.9,
            "systematic_included": false,
            "healpix_url": "https://example.org/run00140078.fits.gz",
            "trigger_time": "2023-04-16T05:22:26.150574Z",
            "nu_energy": 127.29,
            "p_astro": 0.34064,
            "far": 8.029e-8
        }"#;
        let nu = parse_icecube_single_neutrino_alert(payload).unwrap();
        assert_eq!(nu.trigger.trigger_id, "137840_57034692");
        assert_eq!(nu.trigger.instrument, "IceCube");
        assert_eq!(nu.alert_topology.as_deref(), Some("Track"));
        assert_eq!(nu.pipeline.as_deref(), Some("Bronze Track Alert"));
        assert_eq!(nu.event_name.as_deref(), Some("IceCube-230416A"));
        assert_eq!(nu.nu_energy, Some(127.29));
        assert_eq!(nu.p_astro, Some(0.34064));
        // scalar_significance prefers p_astro for IceCube.
        assert!((nu.trigger.significance - 0.34064).abs() < 1e-9);
        let pos = nu.trigger.position.unwrap();
        assert!((pos.ra - 345.82).abs() < 1e-9);
        assert!((pos.dec - 9.01).abs() < 1e-9);
        // 0.5° × 3600 → 1800 arcsec.
        assert!((pos.uncertainty_arcsec - 1800.0).abs() < 1e-6);
    }

    #[test]
    fn parses_km3net_example() {
        // Lifted verbatim from the gcn-schema repo.
        let payload = r#"{
            "messenger": "Neutrino",
            "mission": "KM3NeT",
            "instrument": "ORCA024",
            "pipeline": "orca_HE",
            "alert_tense": "current",
            "alert_type": "initial",
            "record_number": 1,
            "alert_datetime": "2024-09-01T12:01:00.00Z",
            "id": "1",
            "event_name": "KM3-240901A",
            "trigger_time": "2024-09-01T01:16:47.0Z",
            "ra": 10.82,
            "dec": 20.01,
            "ra_dec_error": 0.9,
            "healpix_url": "https://opendata.km3net.de/",
            "far": 8.029e-8,
            "alert_topology": "Track",
            "number_of_events": 1,
            "p_value": 0.0234,
            "src_error_50": 0.49
        }"#;
        let nu = parse_km3net_alert(payload).unwrap();
        assert_eq!(nu.trigger.trigger_id, "1");
        assert_eq!(nu.trigger.instrument, "KM3NeT");
        assert_eq!(nu.event_name.as_deref(), Some("KM3-240901A"));
        assert_eq!(nu.pipeline.as_deref(), Some("orca_HE"));
        // scalar_significance falls back to p_value for KM3NeT.
        assert!((nu.trigger.significance - 0.0234).abs() < 1e-9);
        assert_eq!(nu.nu_energy, None);
    }

    #[test]
    fn icecube_parser_rejects_missing_id() {
        let payload = r#"{"trigger_time": "2024-01-01T00:00:00Z"}"#;
        let err = parse_icecube_single_neutrino_alert(payload).unwrap_err();
        assert!(matches!(err, NeutrinoParseError::MissingField("id")));
    }

    #[test]
    fn km3net_parser_rejects_missing_trigger_time() {
        let payload = r#"{"id": "1"}"#;
        let err = parse_km3net_alert(payload).unwrap_err();
        assert!(matches!(
            err,
            NeutrinoParseError::MissingField("trigger_time")
        ));
    }

    #[test]
    fn position_falls_back_to_default_radius_when_error_missing() {
        let payload = r#"{
            "id": "no-radius",
            "trigger_time": "2024-01-01T00:00:00Z",
            "ra": 12.0,
            "dec": 34.0
        }"#;
        let nu = parse_icecube_single_neutrino_alert(payload).unwrap();
        let pos = nu.trigger.position.unwrap();
        let expected = NEUTRINO_DEFAULT_ERR_DEG * 3600.0;
        assert!((pos.uncertainty_arcsec - expected).abs() < 1e-6);
        // But the standalone error_radius_deg stays None — the
        // alert genuinely didn't report one.
        assert_eq!(nu.trigger.error_radius_deg, None);
    }
}
