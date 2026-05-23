//! Pluggable storage backends for the boom-gw blob-heavy data.
//!
//! Currently a single subsystem lives here: [`skymap`], which
//! supports both MongoDB and S3-compatible object stores behind a
//! single [`skymap::SkymapStorage`] enum. The layout mirrors BOOM
//! proper's `utils/cutouts.rs` (PR #313): one struct per backend,
//! both behind an enum that dispatches at runtime so call sites
//! never branch on the storage backend.

pub mod skymap;
