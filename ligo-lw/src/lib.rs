//! Rust-native parser for the LIGO_LW XML format.
//!
//! LIGO_LW is the XML container used across the LVK collaboration for tabular
//! event data (single-detector triggers, coincidences, sky map metadata, etc).
//! This crate parses LIGO_LW documents into typed Rust structures and provides
//! convenience accessors for the tables that the low-latency alert-manager
//! pipeline cares about (`sngl_inspiral`, `coinc_inspiral`, `coinc_event`,
//! `coinc_event_map`, `process`).
//!
//! The parser is intentionally tolerant of the LIGO_LW format quirks observed
//! in real coinc.xml files emitted by the production pipelines (gstlal, mbta,
//! pycbc, spiir, aframe, cwb, mly): mixed whitespace, trailing delimiters,
//! quoted strings with embedded commas, and ilwd:char identifiers.

mod coinc;
mod document;
mod error;
mod parser;
mod stream;
mod types;
mod value;

pub use coinc::{CoincInspiralEvent, SnglInspiral};
pub use document::{Column, Document, Param, Table};
pub use error::{Error, Result};
pub use parser::parse_bytes;
pub use types::LigoType;
pub use value::Value;
