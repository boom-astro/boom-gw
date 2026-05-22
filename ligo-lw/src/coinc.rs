//! Convenience accessors for the tables that the GW alert-manager pipeline
//! actually reads from a coinc.xml file: `coinc_inspiral`, `coinc_event`,
//! `coinc_event_map`, and `sngl_inspiral`.

use crate::document::Document;
use crate::error::{Error, Result};

/// A single coincident inspiral event distilled from a coinc.xml document.
/// This is the shape the alert-manager clustering layer consumes.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CoincInspiralEvent {
    pub coinc_event_id: String,
    /// IFO string as recorded by the pipeline (e.g. `"H1,L1"`).
    pub ifos: String,
    /// Combined false-alarm rate (Hz).
    pub combined_far: f64,
    /// Combined SNR (network).
    pub snr: f64,
    /// Total mass (solar masses), if reported by the pipeline.
    pub mass: Option<f64>,
    /// Chirp mass (solar masses), if reported by the pipeline.
    pub mchirp: Option<f64>,
    /// End time as seconds since the GPS epoch, combining `end_time` and
    /// `end_time_ns` if both are present.
    pub end_time: f64,
    /// Single-detector triggers linked to this coincidence.
    pub sngls: Vec<SnglInspiral>,
}

/// A single-detector inspiral trigger.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnglInspiral {
    pub event_id: String,
    pub ifo: String,
    pub end_time: f64,
    pub snr: f64,
    pub mass1: Option<f64>,
    pub mass2: Option<f64>,
    pub mchirp: Option<f64>,
}

impl Document {
    /// Extract every `coinc_inspiral` row in the document, joining in the
    /// linked `sngl_inspiral` rows via `coinc_event_map` when present.
    pub fn coinc_inspirals(&self) -> Result<Vec<CoincInspiralEvent>> {
        let coinc_inspiral = self.require_table("coinc_inspiral")?;
        let mut events = Vec::with_capacity(coinc_inspiral.rows.len());

        let sngls = self.sngls_by_id();
        let map = self.coinc_event_map();

        for row_idx in 0..coinc_inspiral.rows.len() {
            let coinc_event_id = require_id(coinc_inspiral, row_idx, "coinc_event_id")?;
            let ifos = require_str(coinc_inspiral, row_idx, "ifos")?;
            // Prefer combined_far, fall back to false_alarm_rate (some pipelines
            // only emit the latter).
            let combined_far = optional_f64(coinc_inspiral, row_idx, "combined_far")
                .or_else(|| optional_f64(coinc_inspiral, row_idx, "false_alarm_rate"))
                .ok_or_else(|| Error::MissingColumn {
                    table: coinc_inspiral.name.clone(),
                    column: "combined_far".into(),
                })?;
            let snr = require_f64(coinc_inspiral, row_idx, "snr")?;
            let mass = optional_f64(coinc_inspiral, row_idx, "mass");
            let mchirp = optional_f64(coinc_inspiral, row_idx, "mchirp");
            let end_time = combined_end_time(coinc_inspiral, row_idx)?;

            let linked = map.get(&coinc_event_id).cloned().unwrap_or_default();
            let mut sngls_for_event: Vec<SnglInspiral> = linked
                .into_iter()
                .filter_map(|sngl_id| sngls.get(&sngl_id).cloned())
                .collect();

            // Fall back to all sngl_inspiral rows when no map is present
            // (some pipelines omit coinc_event_map for single-event coincs).
            if sngls_for_event.is_empty() && map.is_empty() {
                sngls_for_event = sngls.values().cloned().collect();
            }

            events.push(CoincInspiralEvent {
                coinc_event_id,
                ifos,
                combined_far,
                snr,
                mass,
                mchirp,
                end_time,
                sngls: sngls_for_event,
            });
        }
        Ok(events)
    }

    /// `coinc_event_id` -> list of linked `event_id` values from
    /// `coinc_event_map`, or an empty map if the table is absent.
    fn coinc_event_map(&self) -> std::collections::HashMap<String, Vec<String>> {
        let mut out: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        let Some(table) = self.tables.get("coinc_event_map") else {
            return out;
        };
        for row in 0..table.rows.len() {
            let Ok(ce_id) = require_id(table, row, "coinc_event_id") else {
                continue;
            };
            let Ok(event_id) = require_id(table, row, "event_id") else {
                continue;
            };
            out.entry(ce_id).or_default().push(event_id);
        }
        out
    }

    /// `event_id` -> SnglInspiral, or an empty map if the table is absent.
    fn sngls_by_id(&self) -> std::collections::HashMap<String, SnglInspiral> {
        let mut out: std::collections::HashMap<String, SnglInspiral> =
            std::collections::HashMap::new();
        let Some(table) = self.tables.get("sngl_inspiral") else {
            return out;
        };
        for row in 0..table.rows.len() {
            let event_id = match require_id(table, row, "event_id") {
                Ok(v) => v,
                Err(_) => continue,
            };
            let ifo = require_str(table, row, "ifo").unwrap_or_default();
            let snr = require_f64(table, row, "snr").unwrap_or(0.0);
            let end_time = combined_end_time(table, row).unwrap_or(0.0);
            let mass1 = optional_f64(table, row, "mass1");
            let mass2 = optional_f64(table, row, "mass2");
            let mchirp = optional_f64(table, row, "mchirp");
            out.insert(
                event_id.clone(),
                SnglInspiral {
                    event_id,
                    ifo,
                    end_time,
                    snr,
                    mass1,
                    mass2,
                    mchirp,
                },
            );
        }
        out
    }
}

fn require_str(table: &crate::document::Table, row: usize, column: &str) -> Result<String> {
    let cell = table.require_cell(row, column)?;
    cell.as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| Error::TypeMismatch {
            table: table.name.clone(),
            column: column.to_string(),
            expected: "string",
            actual: cell.type_name(),
        })
}

/// Read an identifier column that may be either a legacy `ilwd:char` string
/// (e.g. `"sngl_inspiral:event_id:0"`) or an `int_8s` integer (the modern
/// scheme used by gstlal/MBTA and other O4+ pipelines). The integer form is
/// rendered as its decimal representation so downstream consumers can treat
/// IDs uniformly as strings.
fn require_id(table: &crate::document::Table, row: usize, column: &str) -> Result<String> {
    let cell = table.require_cell(row, column)?;
    match cell {
        crate::value::Value::Str(s) => Ok(s.clone()),
        crate::value::Value::Int(i) => Ok(i.to_string()),
        crate::value::Value::UInt(u) => Ok(u.to_string()),
        other => Err(Error::TypeMismatch {
            table: table.name.clone(),
            column: column.to_string(),
            expected: "string-or-int identifier",
            actual: other.type_name(),
        }),
    }
}

fn require_f64(table: &crate::document::Table, row: usize, column: &str) -> Result<f64> {
    let cell = table.require_cell(row, column)?;
    cell.as_f64().ok_or_else(|| Error::TypeMismatch {
        table: table.name.clone(),
        column: column.to_string(),
        expected: "real",
        actual: cell.type_name(),
    })
}

fn optional_f64(table: &crate::document::Table, row: usize, column: &str) -> Option<f64> {
    table.cell(row, column).and_then(|v| v.as_f64())
}

/// Combine `end_time` and `end_time_ns` into seconds since the GPS epoch.
/// `end_time_ns` is treated as a nanosecond offset and folded in if present.
fn combined_end_time(table: &crate::document::Table, row: usize) -> Result<f64> {
    let seconds = require_f64(table, row, "end_time")?;
    if let Some(ns_cell) = table.cell(row, "end_time_ns") {
        if let Some(ns) = ns_cell.as_f64() {
            return Ok(seconds + ns * 1e-9);
        }
    }
    Ok(seconds)
}
