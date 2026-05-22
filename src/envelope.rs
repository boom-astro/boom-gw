//! Deserializer for the GraceDB Kafka envelope.
//!
//! Each message that lands on a pipeline topic (`gstlal`, `mbta`, ...) is a
//! JSON object with the shape:
//!
//! ```jsonc
//! {
//!     "type": "...",                  // message type tag
//!     "_producer_timestamp": 1.7e9,    // producer wall-clock time in seconds
//!     "graceid": "G123456",            // GraceDB event ID
//!     "pipeline": "gstlal",            // emitting pipeline
//!     "submitter": "user@ligo.org",    // submitter username
//!     "eventFile": [
//!         "coinc.xml",                  // filename
//!         "<base64-encoded XML>"        // payload
//!     ]
//! }
//! ```
//!
//! The payload is base64-encoded XML (LIGO_LW). We tolerate the alternative
//! shape where `eventFile[1]` is already a byte array, which can happen during
//! local testing if the producer skipped the base64 step.
//!
//! The `_producer_timestamp` field name is preserved verbatim because it
//! includes the leading underscore.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use serde::Deserialize;
use thiserror::Error;

/// Top-level JSON envelope. Field names match the Python client.
#[derive(Debug, Clone, Deserialize)]
pub struct EventEnvelope {
    #[serde(rename = "type")]
    pub message_type: String,
    #[serde(rename = "_producer_timestamp")]
    pub producer_timestamp: f64,
    pub graceid: String,
    pub pipeline: String,
    pub submitter: String,
    #[serde(rename = "eventFile")]
    pub event_file: EventFile,
}

/// `eventFile` payload. JSON serialises it as a two-element array; we model
/// it as a tuple-shaped struct via `serde`'s tuple-style deserialisation.
#[derive(Debug, Clone)]
pub struct EventFile {
    pub filename: String,
    /// Raw payload. Stored as `String` because the wire format is base64;
    /// callers should pass through [`decode_event_file`] to get the bytes.
    pub payload_b64: String,
}

impl<'de> Deserialize<'de> for EventFile {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        // Accept either ["name", "<base64>"] or {"filename": "...", "payload_b64": "..."}.
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Tuple(String, String),
            Object {
                filename: String,
                #[serde(alias = "payload", alias = "payload_b64", alias = "bytes")]
                payload: String,
            },
        }
        match Repr::deserialize(de).map_err(D::Error::custom)? {
            Repr::Tuple(filename, payload_b64) => Ok(EventFile {
                filename,
                payload_b64,
            }),
            Repr::Object { filename, payload } => Ok(EventFile {
                filename,
                payload_b64: payload,
            }),
        }
    }
}

#[derive(Debug, Error)]
pub enum EnvelopeError {
    #[error("failed to parse Kafka envelope JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("failed to base64-decode eventFile payload: {0}")]
    Base64(#[from] base64::DecodeError),
}

impl EventEnvelope {
    pub fn from_json(bytes: &[u8]) -> Result<Self, EnvelopeError> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// Decode the `eventFile` payload into raw bytes. Tolerates the payload being
/// already in raw form by falling back when base64 decoding fails on what
/// looks like XML.
pub fn decode_event_file(file: &EventFile) -> Result<Vec<u8>, EnvelopeError> {
    // Fast path: if the payload starts with `<` it is almost certainly raw XML
    // that was injected without base64 encoding, so skip the decode attempt.
    if file.payload_b64.trim_start().starts_with('<') {
        return Ok(file.payload_b64.as_bytes().to_vec());
    }
    Ok(BASE64.decode(file.payload_b64.as_bytes())?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_tuple_payload() {
        let raw = br#"{
            "type": "new",
            "_producer_timestamp": 1234567890.5,
            "graceid": "G42",
            "pipeline": "gstlal",
            "submitter": "alice",
            "eventFile": ["coinc.xml", "PGZvby8+"]
        }"#;
        let env = EventEnvelope::from_json(raw).expect("envelope parses");
        assert_eq!(env.pipeline, "gstlal");
        assert_eq!(env.event_file.filename, "coinc.xml");
        assert_eq!(env.event_file.payload_b64, "PGZvby8+");

        let bytes = decode_event_file(&env.event_file).unwrap();
        assert_eq!(bytes, b"<foo/>");
    }

    #[test]
    fn parses_object_payload_with_aliases() {
        let raw = br#"{
            "type": "new",
            "_producer_timestamp": 100.0,
            "graceid": "G1",
            "pipeline": "mbta",
            "submitter": "bob",
            "eventFile": {"filename": "coinc.xml", "payload": "PGE+"}
        }"#;
        let env = EventEnvelope::from_json(raw).unwrap();
        let bytes = decode_event_file(&env.event_file).unwrap();
        assert_eq!(bytes, b"<a>");
    }

    #[test]
    fn falls_back_to_raw_xml_payload() {
        let raw = br#"{
            "type": "new",
            "_producer_timestamp": 0.0,
            "graceid": "G2",
            "pipeline": "pycbc",
            "submitter": "x",
            "eventFile": ["coinc.xml", "<LIGO_LW></LIGO_LW>"]
        }"#;
        let env = EventEnvelope::from_json(raw).unwrap();
        let bytes = decode_event_file(&env.event_file).unwrap();
        assert_eq!(bytes, b"<LIGO_LW></LIGO_LW>");
    }
}
