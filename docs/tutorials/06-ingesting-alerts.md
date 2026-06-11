# 6. Ingesting external alerts

In production, external multi-messenger triggers reach boom-gw through
the `gw-gcn-consumer`, which subscribes to GCN Kafka topics and POSTs
normalized alerts to the REST API. You can push the same alerts in by
hand — useful for testing, backfills, or bridging a source GCN doesn't
carry. This tutorial ingests one of each kind and watches it become
cross-matchable.

Prerequisite: the stack from [tutorial 1](01-getting-started.md). All
ingest endpoints require an authenticated principal — use the dev
token from the [tutorials index](README.md) (`export TOKEN=…; alias
gw=…`).

A few conventions shared across all external triggers:

- **`trigger_time`** is **GPS seconds** (the same clock as a
  superevent's `t_0`), so the coincidence-window math lines up.
- **`position`** is `{ "ra", "dec", "uncertainty_arcsec" }` in
  degrees / arcseconds.
- `instrument` and `trigger_id` form the natural key — re-POSTing the
  same pair **upserts** (e.g. a Fermi flight → ground → final
  refinement updates in place rather than fanning out).

## GRB triggers

`POST /api/grb-triggers` accepts either a **raw** GCN payload (with a
`format` hint, parsed server-side) or a **pre-parsed** `GrbTrigger`.
The pre-parsed form is the simplest to hand-write:

```sh
gw -X POST http://127.0.0.1:8080/api/grb-triggers \
  -H 'Content-Type: application/json' -d '{
    "trigger_id": "bn990123tut",
    "instrument": "Fermi-GBM-FIN",
    "trigger_time": 1400000005.0,
    "position": { "ra": 150.2, "dec": 15.3, "uncertainty_arcsec": 5400 },
    "significance": 8.1,
    "error_radius_deg": 1.5
  }'
```

The raw path (what the GCN bridge actually uses) looks like:

```sh
gw -X POST http://127.0.0.1:8080/api/grb-triggers \
  -H 'Content-Type: application/json' -d '{
    "format": "fermi_gbm_json",
    "instrument": "Fermi-GBM-FLT",
    "payload": "{ ...the raw Fermi GBM JSON notice... }"
  }'
```

At ingest, boom-gw synthesizes a **canonical MOC** for the trigger
(from the cone / ellipse / HEALPix the alert provided) and stores it,
so the cross-match integral later is shape-agnostic. Fetch it back:

```sh
gw http://127.0.0.1:8080/api/grb-triggers/Fermi-GBM-FIN/bn990123tut/skymap -o grb.fits
```

List what's ingested:

```sh
gw http://127.0.0.1:8080/api/grb-triggers | jq '.data[] | {instrument, trigger_id}'
```

## FRB alerts

`POST /api/frb-alerts` — the body is a GRB-shaped trigger plus FRB
fields (dispersion measure, etc.). `instrument` is `CHIME-FRB` or
`DSA110-FRB`:

```sh
gw -X POST http://127.0.0.1:8080/api/frb-alerts \
  -H 'Content-Type: application/json' -d '{
    "trigger_id": "chime_tut01",
    "instrument": "CHIME-FRB",
    "trigger_time": 1400000003.0,
    "position": { "ra": 150.0, "dec": 15.2, "uncertainty_arcsec": 1800 },
    "significance": 12.5,
    "error_radius_deg": 0.5,
    "dm": 279.4,
    "body": {}
  }'
```

(`body` carries the original alert payload verbatim for forward-compat;
`{}` is fine when you're hand-crafting one.)

## Neutrino alerts

`POST /api/neutrino-alerts` — `instrument` is `IceCube` or `KM3NeT`,
with the trigger fields plus neutrino-specific ones:

```sh
gw -X POST http://127.0.0.1:8080/api/neutrino-alerts \
  -H 'Content-Type: application/json' -d '{
    "trigger_id": "icecube_tut01",
    "instrument": "IceCube",
    "trigger_time": 1400000007.0,
    "position": { "ra": 150.3, "dec": 15.1, "uncertainty_arcsec": 1440 },
    "significance": 4.2,
    "error_radius_deg": 0.4,
    "alert_topology": "Track",
    "pipeline": "Gold Track Alert",
    "nu_energy": 250.0,
    "p_astro": 0.85,
    "body": {}
  }'
```

(IceCube also runs the *targeted* LVK Neutrino Track Search against a
specific superevent — that's a different endpoint,
`POST /api/superevents/{id}/icecube-lvk-searches`, surfaced on the
**Nu searches** tab in [tutorial 2](02-superevents-and-localizations.md).)

## Optical (BOOM) transients

`POST /api/boom-alerts` ingests an optical-transient alert (the kind
BOOM's own pipeline cross-matches and forwards). `instrument` is
`BOOM`:

```sh
gw -X POST http://127.0.0.1:8080/api/boom-alerts \
  -H 'Content-Type: application/json' -d '{
    "alert_id": "ZTF_tut01",
    "event_name": "ZTF26tut",
    "alert_time": 1400000010.0,
    "ra": 150.25, "dec": 15.30,
    "error_radius_deg": 0.01,
    "classification": "kilonova candidate",
    "classification_score": 0.7,
    "photometry": [], "body": {},
    "last_non_detection_time": 1400000000.0,
    "first_detection_time": 1400000020.0
  }'
```

The `last_non_detection_time` / `first_detection_time` bracket is the
kilonova turn-on criterion: a `scan-cross-matches` only picks up an
optical transient whose first detection is *after* the superevent's
`t_0` and whose last non-detection is *before* it.

## Watch it become cross-matchable

Everything you just ingested sits near RA 150°, Dec 15° around GPS
~1.4e9 — close to a superevent if one shares that window. Ingest a
matching superevent (or use one of the demo's), then **scan** it
([tutorial 3](03-cross-matching.md)):

```sh
gw -X POST http://127.0.0.1:8080/api/superevents/<ID>/scan-cross-matches \
  -H 'Content-Type: application/json' \
  -d '{"time_window_sec": 60, "p_value_trials": 200}' | jq '.data | length'
```

Each ingested trigger within `±time_window_sec` of the superevent's
`t_0` gets a cross-match computed and persisted, ready for science
filters ([tutorial 4](04-science-filters.md)) and associations.

## How this maps to production

| You did (by hand) | Production equivalent |
|-------------------|-----------------------|
| `POST /api/grb-triggers` (raw) | `gw-gcn-consumer` parsing a Fermi/Swift GCN notice and POSTing it |
| `POST /api/frb-alerts` | GCN CHIME/DSA110 topic → consumer |
| `POST /api/neutrino-alerts` | GCN IceCube/KM3NeT topic → consumer |
| `POST /api/boom-alerts` | BOOM's optical pipeline forwarding a cross-matched transient |
| `scan-cross-matches` | runs automatically as triggers and superevents arrive |

The ingest endpoints require authentication today; the `Upload data`
ACL is reserved for finer-grained gating of who may push alerts.

---

That's the tour. From here: the [API route table](../../src/api.rs)
(top of `src/api.rs`) is the full endpoint reference, and
[deployment.md](../deployment.md) covers running this for real.
</content>
