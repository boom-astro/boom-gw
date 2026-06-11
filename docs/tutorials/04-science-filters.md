# 4. Science filters

The cross-match metrics from [tutorial 3](03-cross-matching.md) are
objective and shared. Whether a given pair counts as an *association*,
and at what *confidence*, is a science choice that different groups
make differently — a kilonova-hunting team wants a loose spatial cut
and a wide time window; a population-statistics team wants a tight
joint-FAR threshold and rejects anything outside the 90% region.

A **science filter** is a saved set of cuts over the stored metrics
plus named **confidence tiers**. It re-thresholds the same objective
data per user, so two people running two filters surface two different
sets of associations from the same superevent — no physics is
recomputed. (Background and rationale: [science-filters.md](../science-filters.md).)

Prerequisite: the stack from [tutorial 1](01-getting-started.md),
signed in. The demo seeds **3 filters** shared with the "MMA team"
group.

## Anatomy of a filter

A filter has:

- **Cuts** (all optional — an unset cut isn't applied):
  - `instruments` — restrict to specific instrument labels;
  - `time_window_sec` — keep matches with `|Δt| ≤` this;
  - `spatial_overlap_min`;
  - `p_value_max`;
  - `joint_far_remapped_max_per_year`;
  - `require_in_90cr`.
- **Confidence tiers** — named bands on the remapped joint FAR, e.g.
  *gold* ≤ 1×10⁻⁵/yr, *silver* ≤ 1×10⁻³/yr. A passing match is tagged
  with the tightest tier it clears.
- A **group** it's shared with and the **streams** it draws from
  (covered in [tutorial 5](05-access-control.md)).

## Look at the demo filters

Click **Science filters** in the nav. You'll see three, all owned by
`load-demo-data` and shared with **MMA team**:

| Filter | What it does |
|--------|--------------|
| **High-confidence (gold/silver)** | `require_in_90cr` + remapped FAR ≤ 1×10⁻³/yr; tiers gold (≤1e-5) / silver (≤1e-3); restricted to the GRB, optical, and neutrino streams |
| **In 90% credible region** | just `require_in_90cr` — a simple membership filter, no tiers |
| **Strong spatial overlap (≥ 0.5)** | `spatial_overlap_min = 0.5` |

Each row shows its cut chips, tier chips, and owner/group.

Over the API:

```sh
gw http://127.0.0.1:8080/api/science-filters \
  | jq '.data[] | {name, cuts, tiers:[.confidence_tiers[]?.name]}'
```

## Build your own

Click **New filter** and fill in the dialog:

1. **Name** it (e.g. "My GRB filter").
2. Pick a **Group** to share it with, or leave it **Private (no
   group)**. Sharing makes it visible to that group's members; the
   stream multi-select then offers that group's streams.
3. Set any **cuts** — leave a field blank to skip that cut.
4. Toggle **Require external position in GW 90% credible region** if
   you want that boolean cut.
5. Add **confidence tiers** — a name and a max remapped FAR each. They
   re-sort most-significant-first automatically.
6. **Create**.

The equivalent API call:

```sh
gw -X POST http://127.0.0.1:8080/api/science-filters \
  -H 'Content-Type: application/json' -d '{
    "name": "My GRB filter",
    "cuts": { "require_in_90cr": true,
              "joint_far_remapped_max_per_year": 1e-3 },
    "confidence_tiers": [
      { "name": "gold",   "joint_far_remapped_max_per_year": 1e-5 },
      { "name": "silver", "joint_far_remapped_max_per_year": 1e-3 }
    ]
  }' | jq '.data | {id:._id, name}'
```

You can edit (owner, group admin, or anyone with the `Manage science
filters` ACL) or delete it from the same page.

## Apply a filter to a superevent

Open **`S260524e`** → **Cross-matches**. At the top of the candidates
table there's a **Science filter** dropdown. Pick **High-confidence
(gold/silver)**.

The table now shows only the rows that pass the cuts, each tagged with
a **gold** or **silver** confidence chip, and it drops the rest. Try
the other filters and watch the row set change — that's the whole
point: the metrics are fixed, the *verdict* is per-filter.

The same filtered view over the API (append `?filter_id=`):

```sh
FID=$(gw http://127.0.0.1:8080/api/science-filters \
  | jq -r '.data[] | select(.name|startswith("High-confidence")) | ._id')

gw "http://127.0.0.1:8080/api/superevents/S260524e/cross-matches?filter_id=$FID" \
  | jq '.data[] | {inst:.instrument, tier:.confidence_tier}'
```

You'll notice the **CHIME-FRB** row is gone. That's not a cut — it's
**stream gating**: the High-confidence filter draws only from the GRB,
optical, and neutrino streams, so FRB matches are excluded, and a user
who can't access a stream never sees its matches. Streams are the
subject of the next tutorial.

---

Next: [Access control: users, groups, streams, roles](05-access-control.md).
</content>
