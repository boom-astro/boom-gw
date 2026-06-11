# Science filters: per-user multi-messenger associations

## Motivation

Today boom-gw produces a **single, global answer** to the question
"which external events are associated with this GW superevent?" The
cross-match math runs once with one set of parameters, and a single
operator-owned boolean (`associated`) records the verdict for
everyone.

That does not match how multi-messenger science actually works.
Different groups draw the GRB+GW (or neutrino+GW, FRB+GW, optical+GW)
association boundary in different places: a kilonova-hunting team
wants a loose spatial cut and a wide time window to sweep up late
optical companions, while a population-statistics team wants a tight
joint-FAR threshold and rejects anything that is not in the 90%
credible region. Both are looking at the same superevent and the same
candidate external events; they disagree only on the **cuts**.

The goal of this design is to make the *decision* a per-user (and
per-group) function of saved cuts — the boom / SkyPortal "filter"
model — while keeping the expensive *physics* computed once and
shared. Two users running two filters over the same superevent should
be able to surface two different sets of associations at two
different confidence levels.

## Where the code stands today

The objective metrics already exist and are stored:

- [`crossmatch.rs`](../src/crossmatch.rs) computes, per
  (superevent × external-event) pair: `spatial_overlap`, the
  empirical `p_value`, the classic RAVEN `joint_far_per_year`, the
  bias-corrected `joint_far_remapped_per_year`, and the
  SubGRBTargeted `targeted_joint_far_per_year`.
- [`CrossMatchResult`](../src/grb.rs) carries those fields and
  [`CrossMatchDoc`](../src/archive.rs) persists them, keyed by
  `(superevent_id, instrument, trigger_id)`.
- [`scan_cross_matches`](../src/api.rs) runs the math once for a
  superevent using the parameters in `ScanCrossMatchBody`
  (`time_window_sec`, `p_value_trials`, `far_gw_max_hz`) and writes
  every result with `associated = false`.
- [`patch_cross_match`](../src/api.rs) flips the single shared
  `associated` boolean (`$set: { associated }`).

So the metrics are objective and reusable, but the verdict
(`associated`) and the parameters that produced the metrics
(`time_window_sec`, rate prior) are global.

## Key insight: the expensive parts are window-independent

This is what makes per-user filters cheap. Of the stored metrics:

- `spatial_overlap` — the BAYESTAR × GRB-MOC integral — depends only
  on **geometry** (the two sky maps). It does not depend on the time
  window or the assumed background rate.
- `p_value` — the Monte Carlo over random sky rotations — likewise
  depends only on geometry, error radius, and trial count.
- `joint_far_per_year`, `targeted_joint_far_per_year`, and the
  remapped FAR are **closed-form arithmetic** over `spatial_overlap`,
  `gw_far_hz`, the assumed external rate, and `time_window_sec`. See
  `raven_joint_far_per_year`, `raven_targeted_joint_far_per_year`,
  and `far_remapped` in [`crossmatch.rs`](../src/crossmatch.rs) —
  each is a handful of multiplications and a log.

Therefore: compute the heavy geometry **once per pair** and store it.
Each filter can then re-derive its own joint-FAR for its own time
window and rate prior, and apply its own threshold cuts, at query
time for essentially free — no MOC integral, no Monte Carlo.

The one subtlety is that `time_window_sec` is currently both (a) the
bound on which external events get scanned and (b) an input to the
FAR formula. To support per-user windows cleanly we separate these:
metrics are computed at a generous **canonical maximum window**, and
the stored `time_offset_sec` lets any filter narrow the window after
the fact (drop pairs outside the user's window, recompute FAR at the
user's window). A filter window wider than the canonical maximum is
the only case that requires a rescan; the UI should surface that.

## Objective vs. subjective: the data split

### Objective metric store (existing, lightly extended)

`CrossMatchDoc` stays the system of record for what is true about a
pair regardless of who is looking. To support cheap per-filter FAR
recomputation it must store the **raw inputs**, not just the derived
FARs:

- `spatial_overlap`, `p_value`, `p_value_trials` (already present)
- `time_offset_sec` (already present)
- `gw_far_hz` — the preferred-event GW FAR (currently only an input;
  persist it)
- `ext_rate_hz` and, when available, the trigger's own `far_hz` — so
  the targeted FAR can be re-derived per filter
- `instrument`, and an `event_type` classifier (`grb`, `neutrino`,
  `frb`, `optical`) for filter selection

The derived `joint_far_*` fields stay too (they are the canonical-
window values, useful as defaults and for sorting), but filters that
change the window recompute rather than read them.

The global `associated` boolean is **retired as the answer** (see
Migration). It may remain as a deprecated field during transition.

### Subjective filter store (new collection `science_filters`)

A filter is the boom / SkyPortal analog: event-type selection +
threshold cuts + named confidence tiers, owned by a user and
optionally shared with a group.

```jsonc
{
  "id": "...",
  "owner": "cough052@umn.edu",
  "group": "umn-kilonova",          // null = private
  "name": "Kilonova companions (loose)",
  "active": true,                    // evaluated on the stream?

  "event_types": ["grb", "optical"],
  "instruments": ["Fermi-GBM", "Swift-BAT"],   // null = any in type

  "gw_constraints": {                // optional pre-cuts on the GW side
    "far_max_hz": 3.2e-8,            // ~1/year
    "has_ns": true,
    "classes": ["BNS", "NSBH"]
  },

  "cuts": {
    "time_window_sec": 86400,        // this filter's window (recompute FAR)
    "spatial_overlap_min": 0.1,
    "p_value_max": 0.05,
    "joint_far_remapped_max_per_year": 12.0,
    "require_in_90cr": false
  },

  "confidence_tiers": [              // ordered, most-significant first
    { "name": "gold",   "joint_far_remapped_max_per_year": 1.0 },
    { "name": "silver", "joint_far_remapped_max_per_year": 12.0 }
  ],

  "notify": {                        // stream-mode side effects
    "kafka_topic": "umn-kilonova-associations",
    "email": ["team@..."]
  }
}
```

"Different confidence levels" falls out of `confidence_tiers`: a pass
is tagged with the most-significant tier whose threshold it clears.
Tiers can key off `joint_far_remapped_per_year` (default), the
targeted FAR, or `p_value` — the schema allows whichever cut the
group trusts.

### Per-filter human override (new collection `association_verdicts`)

Human confirmation must not mutate a shared row. A verdict is scoped
to `(filter_id, superevent_id, instrument, trigger_id)`:

```jsonc
{ "filter_id": "...", "superevent_id": "...",
  "instrument": "Fermi-GBM", "trigger_id": "...",
  "verdict": "confirmed",       // confirmed | rejected | unset
  "by": "cough052@umn.edu", "at": "..." }
```

A confirmation in my filter is invisible in your filter. This
replaces the role the global `associated` flag played.

## Evaluation: two modes, one filter definition

Both modes read the same filter and the same objective metrics — the
SkyPortal pattern of "a filter is a saved query that can run
interactively or on the stream."

### Query-time (interactive)

`GET /api/superevents/{id}/cross-matches?filter_id={fid}`

Applies the filter's `gw_constraints` and `cuts` server-side (a Mongo
query over the stored metrics, plus on-the-fly FAR recompute for the
filter's window), drops the non-passing pairs, and tags each
surviving row with its confidence tier and any human verdict. This is
what makes two users see two different lists of the same superevent.
Cheap, recomputable, no extra storage.

The existing unfiltered [`list_cross_matches`](../src/api.rs) stays
as the "show me everything objective" view.

### Stream-time (alerting)

When a new external event or superevent arrives and its metrics are
computed (the [`gcn_consumer`](../src/gcn_consumer.rs) / scan path),
every `active` filter is evaluated against the new pairs. Each pass
emits a passed-filter record and fires the filter's `notify` side
effects (Kafka topic, email) — parallel to the existing alert
publisher in [`publisher.rs`](../src/publisher.rs) /
[`alert.rs`](../src/alert.rs). This is the real-time trigger path:
the kilonova team's filter pages them the moment a gold-tier optical
counterpart lands in the window.

## API surface

```
GET    /api/science-filters                 list (own + group-visible)
POST   /api/science-filters                 create
GET    /api/science-filters/{fid}           read
PATCH  /api/science-filters/{fid}           edit (cuts, active, tiers)
DELETE /api/science-filters/{fid}           delete
POST   /api/science-filters/{fid}/preview   dry-run over recent superevents

GET    /api/superevents/{id}/cross-matches?filter_id={fid}
                                            filtered + tier-tagged view
PUT    /api/science-filters/{fid}/verdicts/{instrument}/{trigger_id}
                                            confirm / reject one pair
```

`preview` is the SkyPortal "test this filter before saving" affordance
— run the candidate cuts over the last N superevents and report what
would have passed, so an analyst can tune thresholds without
committing.

## Frontend

The React/Redux scaffolding already exists:
[`ducks/crossMatches.ts`](../web/src/ducks/crossMatches.ts) and
[`CrossMatchesPanel.tsx`](../web/src/components/CrossMatchesPanel.tsx).
Two additions:

1. A **filter-builder page** (mirroring SkyPortal's filter editor):
   event-type pickers, cut sliders, confidence-tier rows, a live
   `preview` count, save/share-with-group.
2. A **filter selector** on the superevent view that swaps the
   cross-match table to the `?filter_id=` endpoint and renders the
   confidence-tier badges (gold / silver / …) plus a confirm/reject
   control wired to the verdict endpoint.

## Migration from the global `associated` flag

1. Backfill `gw_far_hz` / `ext_rate_hz` onto existing
   `CrossMatchDoc`s (from the source superevent + trigger) so older
   matches support per-filter FAR recompute.
2. Ship `science_filters` + `association_verdicts` collections and
   the read endpoints; the unfiltered list keeps working unchanged.
3. Seed a built-in **"Legacy RAVEN" filter** whose cuts reproduce the
   current scan defaults (`time_window_sec = 10`, the existing FAR
   thresholds). Migrate every existing `associated = true` row into a
   `confirmed` verdict under that filter, so today's curated
   associations are preserved verbatim.
4. Mark `associated` deprecated; once the SPA reads the filtered
   endpoint exclusively, drop it.

## Open questions

- **Scope default** — are filters user-private by default and opt-in
  shared, or group-owned from the start? (SkyPortal is group-first.)
- **Confidence semantics** — do tiers cut on remapped joint-FAR only,
  or should a group be able to define a tier as a boolean combination
  (e.g. "in 90% CR **and** p < 0.01")? The schema above is
  single-metric per tier; a small expression grammar is the more
  flexible alternative.
- **Stream cost** — how many `active` filters do we expect, and do we
  evaluate all of them synchronously on ingest or fan out? The cuts
  are cheap, but `notify` side effects are not.
- **GW-side classification** — `gw_constraints.classes` / `has_ns`
  assume a per-class probability vector, but `SupereventDoc` today
  stores only a single `classification: Option<String>` label plus a
  `classification_score: Option<f64>` ([archive.rs:459](../src/archive.rs#L459)).
  Either the filter cuts limit themselves to that label+score, or the
  superevent schema gains a `p_astro`-style class map first.
</content>
</invoke>
