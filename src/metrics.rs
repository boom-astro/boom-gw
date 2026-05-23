//! Common OpenTelemetry metrics utilities for boom-gw.
//!
//! Mirrors BOOM proper's `src/utils/o11y/metrics.rs`: each binary
//! has its own globally-named [`Meter`] (so metrics never merge or
//! collide across applications), and [`init_metrics`] builds an OTLP
//! gRPC exporter that pushes to an OpenTelemetry Collector. The
//! Collector then re-exports to Prometheus (or any other backend).
//!
//! Configuration mirrors BOOM's:
//!
//! * `OTEL_EXPORTER_OTLP_ENDPOINT` selects the collector endpoint;
//!   defaults to `http://localhost:4317` (the standard OTLP gRPC
//!   port) for local non-containerized development.
//! * Temporality is `Cumulative` — Prometheus's delta-temporality
//!   support is still experimental; cumulative is the natural choice
//!   for that backend.
//!
//! When the binary chooses not to call [`init_metrics`], the global
//! meter provider is a no-op and every `Counter::add` call costs ~no
//! time, so leaving the instrumentation in place under a unit-test or
//! `cargo run`-without-a-collector workflow is safe.

use std::sync::LazyLock;

use opentelemetry::{metrics::Meter, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    metrics::{SdkMeterProvider, Temporality},
    Resource,
};

/// Meter used by the clusterer (`gw-clusterer` binary) for event
/// ingestion, superevent updates, localization submit/result, and
/// alert publishes.
pub static CLUSTERER_METER: LazyLock<Meter> =
    LazyLock::new(|| opentelemetry::global::meter("boom-gw-clusterer-meter"));

/// Meter used by the API (`gw-api` binary) for HTTP request counts
/// and any handler-level instrumentation.
pub static API_METER: LazyLock<Meter> =
    LazyLock::new(|| opentelemetry::global::meter("boom-gw-api-meter"));

/// Counters used by the clusterer hot path. Reaching for these lazily
/// keeps the metric definition next to the [`CLUSTERER_METER`] above
/// while letting code at every call site refer to the counter
/// directly without re-declaring it.
pub mod clusterer {
    use super::CLUSTERER_METER;
    use opentelemetry::metrics::Counter;
    use std::sync::LazyLock;

    /// Number of GW pipeline events boom-gw has ingested. Labelled by
    /// `pipeline` (gstlal / mbta / ...) and `result` (`ok` /
    /// `decode_error`).
    pub static EVENTS_INGESTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
        CLUSTERER_METER
            .u64_counter("boom_gw.clusterer.event.ingested")
            .with_unit("{event}")
            .with_description("Number of GW pipeline events boom-gw ingested.")
            .build()
    });

    /// Number of superevent updates emitted by the clusterer.
    /// Labelled by `kind` (`created`/`preferred_updated`/`skipped`/
    /// `skymap_attached`).
    pub static SUPEREVENT_UPDATES: LazyLock<Counter<u64>> = LazyLock::new(|| {
        CLUSTERER_METER
            .u64_counter("boom_gw.clusterer.superevent.update")
            .with_unit("{update}")
            .with_description("Superevent updates produced by clustering.")
            .build()
    });

    /// Localize requests submitted by the clusterer. Labelled by
    /// `result` (`ok`/`error`).
    pub static LOCALIZE_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
        CLUSTERER_METER
            .u64_counter("boom_gw.clusterer.localize.request")
            .with_unit("{request}")
            .with_description("LocalizeRequest publishes.")
            .build()
    });

    /// Localize results received by the clusterer. Labelled by
    /// `status` (`ok`/`error`/`orphan` for results that did not
    /// match any open superevent).
    pub static LOCALIZE_RESULTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
        CLUSTERER_METER
            .u64_counter("boom_gw.clusterer.localize.result")
            .with_unit("{result}")
            .with_description("LocalizeResults consumed.")
            .build()
    });

    /// Errors persisting state (Redis / Mongo) on the hot path.
    /// Labelled by `sink` (`redis`/`mongo_event`/`mongo_superevent`/
    /// `mongo_localize_request`/`mongo_localize_result`).
    pub static ARCHIVE_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
        CLUSTERER_METER
            .u64_counter("boom_gw.clusterer.archive.error")
            .with_unit("{error}")
            .with_description("Errors persisting clusterer state.")
            .build()
    });
}

/// Counters used by the public-alert publisher.
pub mod alert {
    use super::CLUSTERER_METER;
    use opentelemetry::metrics::Counter;
    use std::sync::LazyLock;

    /// Public alerts assembled. Labelled by `alert_type`
    /// (PRELIMINARY/INITIAL/UPDATE/RETRACTION) and `result`
    /// (`built`/`published`/`publish_error`).
    pub static ALERTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
        CLUSTERER_METER
            .u64_counter("boom_gw.alert.publish")
            .with_unit("{alert}")
            .with_description("Public alerts assembled / published.")
            .build()
    });
}

/// Errors that can occur while wiring up the OTel exporter.
#[derive(Debug, thiserror::Error)]
pub enum InitMetricsError {
    #[error("failed to build the OTLP exporter")]
    Exporter(#[from] opentelemetry_otlp::ExporterBuildError),
}

/// Initialize the OTel metrics system, pushing to an OTLP gRPC
/// collector every 60 s. `instance_id` and `deployment_env`
/// distinguish this instance from any other running copy of the same
/// service, exactly as in BOOM proper.
pub fn init_metrics(
    service_name: String,
    instance_id: uuid::Uuid,
    deployment_env: String,
) -> Result<SdkMeterProvider, InitMetricsError> {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());

    let resource = Resource::builder()
        .with_service_name(service_name)
        .with_attributes([
            KeyValue::new("service.instance.id", instance_id.to_string()),
            KeyValue::new("service.namespace", "boom-gw"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("deployment.environment.name", deployment_env),
        ])
        .build();

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_temporality(Temporality::Cumulative)
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;

    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_periodic_exporter(exporter)
        .build();

    opentelemetry::global::set_meter_provider(meter_provider.clone());
    Ok(meter_provider)
}
