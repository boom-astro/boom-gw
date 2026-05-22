//! Conversion from a Kafka message envelope into a typed GW event for the
//! clustering layer.

use thiserror::Error;

use crate::envelope::{decode_event_file, EnvelopeError, EventEnvelope};
use ligo_lw::{parse_bytes, CoincInspiralEvent, Error as LigoError};

/// A single GW event distilled from a coinc.xml payload, carrying the envelope
/// metadata needed for downstream routing.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GwEvent {
    /// Source pipeline (gstlal, mbta, pycbc, ...).
    pub pipeline: String,
    /// GraceDB-assigned event ID (e.g. "G123456").
    pub graceid: String,
    /// Wall-clock producer timestamp from the envelope.
    pub producer_timestamp: f64,
    /// Envelope message type (e.g. "new", "update").
    pub message_type: String,
    /// Submitter username from the envelope.
    pub submitter: String,
    /// GPS end time of the coincidence in seconds (combined from `end_time`
    /// and `end_time_ns`).
    pub end_time: f64,
    /// IFOs participating in the coincidence (as recorded on the wire).
    pub ifos: String,
    /// Network combined SNR.
    pub snr: f64,
    /// Combined false-alarm rate (Hz).
    pub far: f64,
    /// Chirp mass in solar masses if reported.
    pub mchirp: Option<f64>,
    /// Total mass in solar masses if reported.
    pub total_mass: Option<f64>,
    /// Detailed CoincInspiralEvent for downstream consumers that need the
    /// constituent single-detector triggers.
    pub coinc: CoincInspiralEvent,
}

#[derive(Debug, Error)]
pub enum GwEventError {
    #[error("envelope decode error: {0}")]
    Envelope(#[from] EnvelopeError),
    #[error("ligo-lw parse error: {0}")]
    LigoLw(#[from] LigoError),
    #[error("coinc.xml contained no coinc_inspiral rows for graceid {graceid}")]
    NoCoincInspiralRow { graceid: String },
}

/// Parse a single Kafka message into a [`GwEvent`].
pub fn extract_gw_event(envelope: &EventEnvelope) -> Result<GwEvent, GwEventError> {
    let xml_bytes = decode_event_file(&envelope.event_file)?;
    let doc = parse_bytes(&xml_bytes)?;
    let coincs = doc.coinc_inspirals()?;
    let coinc = coincs
        .into_iter()
        .next()
        .ok_or_else(|| GwEventError::NoCoincInspiralRow {
            graceid: envelope.graceid.clone(),
        })?;

    Ok(GwEvent {
        pipeline: envelope.pipeline.clone(),
        graceid: envelope.graceid.clone(),
        producer_timestamp: envelope.producer_timestamp,
        message_type: envelope.message_type.clone(),
        submitter: envelope.submitter.clone(),
        end_time: coinc.end_time,
        ifos: coinc.ifos.clone(),
        snr: coinc.snr,
        far: coinc.combined_far,
        mchirp: coinc.mchirp,
        total_mass: coinc.mass,
        coinc,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use base64::Engine as _;

    const SAMPLE_COINC: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/ligo-lw/tests/fixtures/sample_coinc.xml"
    ));

    fn synthesize_envelope_json(pipeline: &str, graceid: &str, payload_xml: &str) -> Vec<u8> {
        let b64 = BASE64.encode(payload_xml.as_bytes());
        format!(
            r#"{{
                "type": "new",
                "_producer_timestamp": 1700000000.5,
                "graceid": "{graceid}",
                "pipeline": "{pipeline}",
                "submitter": "ci@ligo.org",
                "eventFile": ["coinc.xml", "{b64}"]
            }}"#
        )
        .into_bytes()
    }

    #[test]
    fn end_to_end_envelope_to_gw_event() {
        let raw = synthesize_envelope_json("gstlal", "G42", SAMPLE_COINC);
        let envelope = EventEnvelope::from_json(&raw).expect("envelope parses");
        let event = extract_gw_event(&envelope).expect("extraction succeeds");

        assert_eq!(event.pipeline, "gstlal");
        assert_eq!(event.graceid, "G42");
        assert_eq!(event.ifos, "H1,L1,V1");
        assert!((event.snr - 17.6).abs() < 1e-9);
        assert!((event.far - 1.23e-9).abs() < 1e-20);
        // end_time = 1187008882 + 0.42
        assert!((event.end_time - 1_187_008_882.42).abs() < 1e-6);
        assert_eq!(event.coinc.sngls.len(), 3);
    }

    #[test]
    fn raw_xml_payload_path_also_works() {
        let raw = format!(
            r#"{{
                "type": "new",
                "_producer_timestamp": 0.0,
                "graceid": "G99",
                "pipeline": "mbta",
                "submitter": "x",
                "eventFile": ["coinc.xml", {payload}]
            }}"#,
            payload = serde_json::to_string(SAMPLE_COINC).unwrap()
        )
        .into_bytes();
        let envelope = EventEnvelope::from_json(&raw).unwrap();
        let event = extract_gw_event(&envelope).unwrap();
        assert_eq!(event.pipeline, "mbta");
        assert_eq!(event.graceid, "G99");
    }
}
