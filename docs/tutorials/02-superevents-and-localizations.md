# 2. Superevents & localizations

A **superevent** is boom-gw's unit of a gravitational-wave detection:
one or more pipeline g-events (gstlal, mbta, pycbc, …) clustered in
time, with a preferred event, a localization sky map, and the
multi-messenger context that accumulates around it. This tutorial
tours the superevent list and the per-event detail page using the demo
data.

Prerequisite: the stack from [tutorial 1](01-getting-started.md), and
you're signed in.

## The superevent list

Click **Superevents** in the nav. The demo seeds five:

| id | character |
|----|-----------|
| `S260524a` | BNS-like, has a sky map |
| `S260524b`–`S260524d` | various — some without sky maps |
| `S260524e` | the rich one: sky map **plus** pre-computed cross-matches against GRB, FRB, and neutrino triggers |

Each row shows the preferred g-event's SNR, the time of coalescence
`t_0`, and whether a localization is attached. Click **`S260524e`**.

The same list over the API:

```sh
gw http://127.0.0.1:8080/api/superevents | jq '.data[] | {id:._id, snr:.preferred_snr, t_0}'
```

## The detail page: six tabs

The superevent page has six tabs:

### Overview

The constituent **g-events** (one row per pipeline contribution), the
preferred event, FAR, chirp mass / total mass when present, and the
clustering window `[t_start, t_end]`. This is the provenance of the
superevent — which pipelines saw it and when.

> g-events are *private*: anonymous visitors see a "sign in to view"
> placeholder here, mirroring GraceDB's posture. Everything else on the
> page is public.

### Localization

The sky map, rendered two ways:

- An **Aladin Lite** all-sky view with the GW **50%** and **90%
  credible-region** contours overlaid. Any associated external events
  (tutorial 3) drop pins here too.
- A summary line with the representative center (the sphere-average of
  the 50% region's cells) and the map's storage size.

The raw artifacts are downloadable from the API:

```sh
# Multi-order BAYESTAR PROBDENSITY FITS
gw http://127.0.0.1:8080/api/superevents/S260524e/skymap -o s260524e.fits

# Credible-region MOC (level = 50 or 90)
gw "http://127.0.0.1:8080/api/superevents/S260524e/contour?level=90" -o cr90.fits
```

The contour MOCs are pre-computed when a sky map is attached, so the
cross-match path (tutorial 3) can test "is this GRB inside the 90%
region?" without re-deriving them per query.

### Annotations

Free-form, append-only notes attached to the superevent — a
`p_astro` from a downstream classifier, an ML score, an operator note.
Corrections are new annotations with a later timestamp, never edits.

```sh
# Read
gw http://127.0.0.1:8080/api/superevents/S260524e/annotations | jq .data
# Write
gw -X POST http://127.0.0.1:8080/api/superevents/S260524e/annotations \
  -H 'Content-Type: application/json' \
  -d '{"kind":"operator_note","author":"you@example.org",
       "payload":{"text":"Looks promising — scheduling ToO."}}'
```

### Alerts

The public alerts boom-gw has assembled for this superevent (the
machine-readable notices it would publish downstream). Assembling and
publishing an alert is gated behind the `Publish alerts` ACL — see
[tutorial 5](05-access-control.md).

### Cross-matches

The multi-messenger heart of the system: GW × external coincidences,
their RAVEN joint FAR, empirical p-value, and confidence tiers. This
gets its own walkthrough in [tutorial 3](03-cross-matching.md).

### Nu searches

IceCube **LVK Neutrino Track Search** results attached to this
superevent — the targeted neutrino follow-up of a GW trigger, with
each coincident track's p-value and direction. `S260524e` (the
demo's `S260524a` in some seeds carries this) shows two coincident
tracks inside the GW 90% region.

```sh
gw http://127.0.0.1:8080/api/superevents/S260524e/icecube-lvk-searches | jq .data
```

## Where superevents come from

In production, `gw-clusterer` consumes the GraceDB pipeline Kafka
topics, decodes each `coinc.xml`, and clusters events into superevents
using the same time-window / SNR-preferred policy as the LVK
low-latency infrastructure. A localization request goes out to the
BAYESTAR service, and the returned sky map is attached. The demo
loader stands in for that pipeline by POSTing fully-formed superevents
(with inline sky maps) to `POST /api/superevents`.

---

Next: [Multi-messenger cross-matching](03-cross-matching.md).
</content>
