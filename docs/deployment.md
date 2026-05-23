# boom-gw deployment

This document describes how to bring boom-gw up on a single host with
Docker Compose, how the observability stack is wired, and how to
scale ingestion horizontally. The layout mirrors BOOM proper as
closely as the boom-gw scope allows.

## Stack

The reference deployment is `docker-compose.yaml` at the repository
root. It defines three layers:

1. **Data plane** — MongoDB, Valkey (Redis-compatible), and a
   single-node Kafka broker in KRaft mode. Each has a healthcheck
   and is labelled `autoheal=true`. A `willfarrell/autoheal` sidecar
   restarts containers that go unhealthy.
2. **Observability** — an OpenTelemetry Collector receives OTLP gRPC
   on `:4317` from every boom-gw process and forwards to
   Prometheus's OTLP receiver (enabled with
   `--web.enable-otlp-receiver`). Sidecar exporters for Kafka,
   MongoDB, and Valkey are scraped directly by Prometheus.
3. **Application plane** (`--profile app`) — the `bayestar-service`
   Python microservice, the Rust `gw-clusterer`, and the Rust
   `gw-api`. All three push OTLP metrics to the Collector. The
   `gw-api` binds `:8080`; the others have no inbound ports.

## Bring-up

```sh
# Data plane only (useful for local development of the binaries):
BOOM_GW_MONGO_PASSWORD=changeme docker compose up -d

# Full stack:
BOOM_GW_MONGO_PASSWORD=changeme docker compose --profile app up -d
```

Required environment variables (the compose file fails loudly if
they are missing):

| Variable | Purpose |
|----------|---------|
| `BOOM_GW_MONGO_PASSWORD` | Root password for the MongoDB container. |
| `BOOM_GW_MONGO_USERNAME` | Root username (default `mongoadmin`). |
| `BOOM_GW_DEPLOYMENT_ENV` | OTel `deployment.environment.name` resource attribute (default `dev`). |
| `BOOM_GW_BAYESTAR_IMAGE` | Image tag for the bayestar-service container. |
| `BOOM_GW_BAYESTAR_STUB` | `1` (default) runs the canned-FITS stub; unset to run real BAYESTAR. |
| `BOOM_GW_API_PORT` | Host-side port for the API (default `8080`). |

## Observability

`OTEL_EXPORTER_OTLP_ENDPOINT` is the single dial. In Compose it's
set to `http://otel-collector:4317` for the application containers.
Outside Compose, set it to whichever OTLP gRPC endpoint your
environment exposes (or leave it unset for local development; the
process will buffer until a collector becomes reachable).

Counter names emitted by boom-gw:

| Name | Labels | Source |
|------|--------|--------|
| `boom_gw.clusterer.event.ingested` | `pipeline`, `result` (`ok`/`decode_error`) | `gw-clusterer` |
| `boom_gw.clusterer.superevent.update` | `kind` (`created`/`preferred_updated`/`skipped`/`skymap_attached`) | `gw-clusterer` |
| `boom_gw.clusterer.localize.request` | `result` (`ok`/`error`) | `gw-clusterer` |
| `boom_gw.clusterer.localize.result` | `status` (`ok`/`error`/`orphan`) | `gw-clusterer` |
| `boom_gw.clusterer.archive.error` | `sink` (`redis`/`mongo_event`/`mongo_superevent`/...) | `gw-clusterer` |
| `boom_gw.alert.publish` | `alert_type`, `result` (`published`/`publish_error`) | library, both binaries |
| `boom_gw.api.request` | `method`, `status_code` | `gw-api` |

## HA and horizontal scaling

boom-gw scales via the same Kafka-consumer-group mechanism BOOM
proper uses. There is no leader election, no Redis-fenced lock, no
DLQ — the consumer-group rebalancing built into rdkafka is the only
coordination primitive. The trade-off matches BOOM's: simple, but it
assumes the operator does not run more clusterer instances than the
ingestion topics have partitions.

To scale ingestion:

1. Bump the partition count on each GraceDB pipeline topic you
   subscribe to (boom-gw's default is the seven LVK pipelines).
2. Run additional `gw-clusterer` containers with the same
   `--group-id`. rdkafka will redistribute partitions on assignment.
3. Each instance writes to the same MongoDB / Valkey, so the archive
   and Redis state remain coherent.

The localizer-result consumer (`LocalizerResultConsumer`) and the
alert-publisher producer use their *own* consumer groups derived
from the process ID, so scaling clusterers does not produce
duplicate localize requests or duplicate alerts; each instance only
attaches FITS for the superevents it currently owns and only emits
alerts for the superevents that flow through its in-memory window.

The `gw-api` is stateless and scales by running multiple replicas
behind any L7 load balancer.

### What we deliberately don't do (and where the gaps are)

* **No dead-letter topic**. When boom-gw fails to decode a message
  or fails to write to the archive, it logs, increments a counter
  (`boom_gw.clusterer.event.ingested{result="decode_error"}` or
  `boom_gw.clusterer.archive.error{sink=...}`), and continues. This
  matches BOOM proper's behaviour exactly. If a class of message
  fails repeatedly the counter will surface it in Prometheus; the
  operator drains by hand from there.
* **No leader election**. Two clusterer instances assigned the same
  partition by rdkafka would not happen — that is the consumer
  group's invariant — but if it did, both would write to MongoDB
  via upserts keyed by `_id`, so the data layer would converge.
  Localization requests for the same `superevent_id` would collide,
  but the `request_id` differs by graceid so the bayestar-service
  would still respond correctly.
* **No Helm / Kustomize / Terraform**. The compose file is the
  reference deployment, same as BOOM proper. The Kubernetes
  manifests under [`deploy/nrp/`](../deploy/nrp/) are the
  NRP-specific equivalent (modeled on
  [boom-deploy-nrp](https://github.com/boom-astro/boom-deploy-nrp));
  they are plain manifests, not a Helm chart.
