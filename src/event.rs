//! Conversion from a Kafka message envelope into a typed GW event for the
//! clustering layer.

use thiserror::Error;

use crate::envelope::{decode_event_file, EnvelopeError, EventEnvelope};
use igwn_ligolw::{parse_bytes, CoincInspiralEvent, Error as LigoError};

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
    Ok(extract_gw_event_with_xml(envelope)?.0)
}

/// Same as [`extract_gw_event`], but also returns the raw coinc.xml
/// bytes that were embedded in the envelope. The bytes are exactly
/// what the producer published (post base64-decode), suitable for
/// forwarding to a downstream BAYESTAR service without a re-encode.
pub fn extract_gw_event_with_xml(
    envelope: &EventEnvelope,
) -> Result<(GwEvent, Vec<u8>), GwEventError> {
    let xml_bytes = decode_event_file(&envelope.event_file)?;
    let doc = parse_bytes(&xml_bytes)?;
    let coincs = doc.coinc_inspirals()?;
    let coinc = coincs
        .into_iter()
        .next()
        .ok_or_else(|| GwEventError::NoCoincInspiralRow {
            graceid: envelope.graceid.clone(),
        })?;

    let event = GwEvent {
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
    };
    Ok((event, xml_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use base64::Engine as _;

    // Hand-written coinc.xml exercising the table set the extractor walks
    // (process, sngl_inspiral, coinc_event, coinc_event_map, coinc_inspiral)
    // with values the assertions below are keyed to. The published
    // igwn-ligolw test fixtures do not include coinc_inspiral, so we keep
    // a focused sample inline rather than shipping a separate file.
    const SAMPLE_COINC: &str = r#"<?xml version='1.0' encoding='utf-8'?>
<!DOCTYPE LIGO_LW SYSTEM "http://ldas-sw.ligo.caltech.edu/doc/ligolwAPI/html/ligolw_dtd.txt">
<LIGO_LW>
  <Table Name="process:table">
    <Column Name="process:process_id" Type="ilwd:char"/>
    <Column Name="process:program" Type="lstring"/>
    <Stream Name="process:table" Type="Local" Delimiter=",">
      "process:process_id:0","gstlal_inspiral"
    </Stream>
  </Table>
  <Table Name="sngl_inspiral:table">
    <Column Name="sngl_inspiral:event_id" Type="ilwd:char"/>
    <Column Name="sngl_inspiral:ifo" Type="lstring"/>
    <Column Name="sngl_inspiral:end_time" Type="int_4s"/>
    <Column Name="sngl_inspiral:end_time_ns" Type="int_4s"/>
    <Column Name="sngl_inspiral:snr" Type="real_8"/>
    <Column Name="sngl_inspiral:mass1" Type="real_8"/>
    <Column Name="sngl_inspiral:mass2" Type="real_8"/>
    <Column Name="sngl_inspiral:mchirp" Type="real_8"/>
    <Stream Name="sngl_inspiral:table" Type="Local" Delimiter=",">
      "sngl_inspiral:event_id:0","H1",1187008882,420000000,12.3,1.4,1.4,1.2188,
      "sngl_inspiral:event_id:1","L1",1187008882,415000000,11.0,1.45,1.35,1.215,
      "sngl_inspiral:event_id:2","V1",1187008882,430000000,5.5,1.42,1.38,1.217
    </Stream>
  </Table>
  <Table Name="coinc_event:table">
    <Column Name="coinc_event:coinc_event_id" Type="ilwd:char"/>
    <Column Name="coinc_event:process_id" Type="ilwd:char"/>
    <Column Name="coinc_event:nevents" Type="int_4u"/>
    <Stream Name="coinc_event:table" Type="Local" Delimiter=",">
      "coinc_event:coinc_event_id:0","process:process_id:0",3
    </Stream>
  </Table>
  <Table Name="coinc_event_map:table">
    <Column Name="coinc_event_map:coinc_event_id" Type="ilwd:char"/>
    <Column Name="coinc_event_map:event_id" Type="ilwd:char"/>
    <Column Name="coinc_event_map:table_name" Type="lstring"/>
    <Stream Name="coinc_event_map:table" Type="Local" Delimiter=",">
      "coinc_event:coinc_event_id:0","sngl_inspiral:event_id:0","sngl_inspiral",
      "coinc_event:coinc_event_id:0","sngl_inspiral:event_id:1","sngl_inspiral",
      "coinc_event:coinc_event_id:0","sngl_inspiral:event_id:2","sngl_inspiral"
    </Stream>
  </Table>
  <Table Name="coinc_inspiral:table">
    <Column Name="coinc_inspiral:coinc_event_id" Type="ilwd:char"/>
    <Column Name="coinc_inspiral:ifos" Type="lstring"/>
    <Column Name="coinc_inspiral:end_time" Type="int_4s"/>
    <Column Name="coinc_inspiral:end_time_ns" Type="int_4s"/>
    <Column Name="coinc_inspiral:mass" Type="real_8"/>
    <Column Name="coinc_inspiral:mchirp" Type="real_8"/>
    <Column Name="coinc_inspiral:snr" Type="real_8"/>
    <Column Name="coinc_inspiral:combined_far" Type="real_8"/>
    <Stream Name="coinc_inspiral:table" Type="Local" Delimiter=",">
      "coinc_event:coinc_event_id:0","H1,L1,V1",1187008882,420000000,2.85,1.218,17.6,1.23e-9
    </Stream>
  </Table>
</LIGO_LW>
"#;

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
