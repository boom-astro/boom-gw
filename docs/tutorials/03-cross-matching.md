# 3. Multi-messenger cross-matching

When a gravitational-wave superevent and an external trigger (a GRB,
FRB, neutrino, or optical transient) arrive close in time and on the
sky, boom-gw quantifies how likely that's a real association versus a
chance coincidence. The method is **RAVEN-style**: a spatiotemporal
joint false-alarm rate, plus an empirical p-value from a Monte Carlo.
This tutorial computes and reads those numbers on the demo data.

Prerequisite: the stack from [tutorial 1](01-getting-started.md).
We'll work on **`S260524e`**, which already has cross-matches seeded.

## The metrics, briefly

For each (superevent × external-event) pair boom-gw computes:

| Metric | Meaning |
|--------|---------|
| `time_offset_sec` | `trigger_time − t_0` — the temporal separation |
| `spatial_overlap` | GW localization probability contained in the external error region (the BAYESTAR × MOC integral), in `[0,1]` |
| `in_50cr` / `in_90cr` | whether the external position falls in the GW 50% / 90% credible region |
| `joint_far_per_year` | classic RAVEN spatiotemporal joint FAR, events/yr (smaller = more significant) |
| `p_value` | empirical one-sided p-value from rotating the external sky map to random positions |
| `joint_far_remapped_per_year` | the **bias-corrected** joint FAR using the empirical p-value — the headline number |
| `targeted_joint_far_per_year` | RAVEN *SubGRBTargeted* FAR, when both sides are sub-threshold (Fermi-GBM / Swift-BAT only) |

The expensive parts — the sky-map integral and the Monte Carlo — are
**geometry-only**: they don't depend on the coincidence window or the
assumed background rate. Only the FAR arithmetic does. (That property
is what makes per-user science filters cheap in
[tutorial 4](04-science-filters.md).)

## View the seeded cross-matches

Open **`S260524e`** → the **Cross-matches** tab. You'll see a ranked
table (most significant first) with rows for Swift-BAT, Fermi-GBM,
CHIME-FRB, and IceCube. Each row shows the messenger category (a
color-coded γ / FRB / ν / opt chip), Δt, spatial overlap, CR
membership, p-value, and the remapped joint FAR.

Over the API:

```sh
gw http://127.0.0.1:8080/api/superevents/S260524e/cross-matches \
  | jq '.data[] | {inst:.instrument, dt:.time_offset_sec,
        overlap:.spatial_overlap, in90:.in_90cr,
        p:.p_value, far_yr:.joint_far_remapped_per_year}'
```

## Scan for coincidences yourself

The seeded matches were produced by the same path you can trigger from
the UI. On the Cross-matches tab:

1. Set **Time window (± sec)** — RAVEN's GRB convention is ±10 s;
   widen it (e.g. 86400) to sweep up late-time optical companions.
2. Set **p-value trials** — the number of random sky rotations behind
   the Monte Carlo (200 is a reasonable default; more = tighter
   estimate).
3. Click **Scan ±Ns**.

gw-api pulls every ingested external event with arrival time inside
the window, computes spatial overlap + Monte Carlo p-value + remapped
joint FAR for each, persists them, and returns the list ranked by
significance. It's idempotent — re-scanning replaces prior matches in
place.

The same scan over the API:

```sh
gw -X POST http://127.0.0.1:8080/api/superevents/S260524e/scan-cross-matches \
  -H 'Content-Type: application/json' \
  -d '{"time_window_sec": 10, "p_value_trials": 200}' | jq '.data | length'
```

To compute a single pair on demand (without scanning everything):

```sh
gw -X POST http://127.0.0.1:8080/api/superevents/S260524e/cross-matches \
  -H 'Content-Type: application/json' \
  -d '{"instrument":"Swift-BAT","trigger_id":"01234567","time_window_sec":10}'
```

## Joint sky maps

For a given pair you can fetch the **combined GW × external
posterior** as a FITS sky map — the product of the two localizations,
useful for tiling the overlap region:

```sh
gw "http://127.0.0.1:8080/api/superevents/S260524e/joint-skymap/Swift-BAT/01234567" \
  -o joint.fits
```

## Committing an association

The computed metrics are *objective*. Whether a pair is a real
**association** is a judgment call. The classic flow is an operator
"starring" a row: on the Cross-matches table, click the star to flip
the `associated` flag. Associated matches (and any unassociated one
with a small p-value) are the ones the Aladin overlay on the
Localization tab renders.

Over the API:

```sh
gw -X PATCH \
  "http://127.0.0.1:8080/api/superevents/S260524e/cross-matches/Swift-BAT/01234567" \
  -H 'Content-Type: application/json' -d '{"associated": true}'
```

> This single global `associated` flag is the *old* model — one
> operator, one answer for everyone. The next tutorial introduces
> **science filters**, which let different users define their own
> association criteria and confidence tiers over the same stored
> metrics.

---

Next: [Science filters](04-science-filters.md).
</content>
