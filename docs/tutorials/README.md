# boom-gw tutorials

A hands-on tour of boom-gw, built around the bundled demo dataset.
Each tutorial is self-contained but they build on each other, so if
you're new, start at the top.

boom-gw ingests LIGO/Virgo/KAGRA gravitational-wave superevents and
external multi-messenger triggers (GRBs, FRBs, neutrinos, optical
transients), clusters and localizes them, cross-matches GW × external
events RAVEN-style, lets users define their own **science filters** to
decide what counts as an association, and gates everything behind a
SkyPortal-style **users / groups / streams / roles** access model.

## The tutorials

1. [Getting started](01-getting-started.md) — bring up the local
   stack, load the demo dataset, sign in, and tour the UI.
2. [Superevents & localizations](02-superevents-and-localizations.md)
   — browse superevents, view sky maps and credible regions, read
   annotations and coincidence searches.
3. [Multi-messenger cross-matching](03-cross-matching.md) — scan for
   coincident external events, read the RAVEN joint FAR and empirical
   p-value, view joint sky maps, and commit associations.
4. [Science filters](04-science-filters.md) — build a saved filter
   with cuts and confidence tiers, then apply it to a superevent's
   cross-matches.
5. [Access control: users, groups, streams, roles](05-access-control.md)
   — create a group, add members, grant streams, share a filter, and
   see how ACLs gate the UI and API.
6. [Ingesting external alerts](06-ingesting-alerts.md) — push GRB,
   FRB, neutrino, and optical alerts in through the REST API.

## Prerequisites

You'll need a local stack. The repo's `Makefile` automates it; the
full recipe is in [Getting started](01-getting-started.md), but the
short version is:

```sh
make db_init          # data plane: mongo, valkey, kafka, minio
make run              # gw-api on http://127.0.0.1:8080   (terminal 2)
make load_demo_data   # seed the demo dataset             (terminal 3)
make web              # vite dev server on :5173           (terminal 4)
```

Then open **http://localhost:5173** and sign in (dev mode) as
`cough052@ligo.org`.

## Conventions used in these tutorials

- **UI steps** assume the Vite dev server at http://localhost:5173.
- **API examples** use `curl` against gw-api at
  `http://127.0.0.1:8080`. In dev mode (`BOOM_GW_API_AUTH_DEV_MODE=1`,
  which `make run` sets) you can authenticate with the unsigned dev
  token the demo loader uses:

  ```sh
  export TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImRldiJ9.eyJpc3MiOiJodHRwczovL2NpbG9nb24ub3JnL2lnd24iLCJzdWIiOiJsb2FkLWRlbW8tZGF0YSIsImF1ZCI6IkFOWSIsInNjb3BlIjoiZ3JhY2VkYi5yZWFkIGdyYWNlZGIud3JpdGUiLCJleHAiOjQwMDAwMDAwMDAsImlhdCI6MTczMDAwMDAwMH0.bG9hZGVy"
  alias gw='curl -s -H "Authorization: Bearer $TOKEN"'
  gw http://127.0.0.1:8080/api/superevents | jq .
  ```

  This token's `sub` is `load-demo-data`, which `make run` lists in
  `BOOM_GW_SITE_ADMINS`, so it has the **Super admin** role — handy for
  tutorials, never for production. The same dev token is **only**
  accepted because gw-api is running with `--auth-dev-mode`; turn that
  off and signed CILogon/SciTokens are required.

- Every API response is the envelope `{"message": ..., "data": ...}`.
  The examples pipe through `jq .data` where it aids readability.

## Reference docs (not tutorials)

- [deployment.md](../deployment.md) — production-shaped deployment,
  observability, and scaling.
- [science-filters.md](../science-filters.md) — the design rationale
  behind the science-filter layer.
- [multi-messenger-comparison.md](../multi-messenger-comparison.md) —
  how boom-gw's approach compares to other MMA tooling.
</content>
