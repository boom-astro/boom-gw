# boom-gw

Exploratory work on using [BOOM](https://github.com/boom-astro/boom) technology for gravitational-wave event clustering.

boom-gw ingests LIGO/Virgo/KAGRA gravitational-wave superevents and
external multi-messenger triggers (GRBs, FRBs, neutrinos, optical
transients), clusters and localizes them, cross-matches GW × external
events RAVEN-style, lets users define their own science filters to
decide what counts as an association, and gates everything behind a
SkyPortal-style users / groups / streams / roles access model.

## Documentation

- **[Tutorials](docs/tutorials/README.md)** — a hands-on tour built on
  the bundled demo dataset. Start here.
  1. [Getting started](docs/tutorials/01-getting-started.md)
  2. [Superevents & localizations](docs/tutorials/02-superevents-and-localizations.md)
  3. [Multi-messenger cross-matching](docs/tutorials/03-cross-matching.md)
  4. [Science filters](docs/tutorials/04-science-filters.md)
  5. [Access control: users, groups, streams, roles](docs/tutorials/05-access-control.md)
  6. [Ingesting external alerts](docs/tutorials/06-ingesting-alerts.md)
- [Deployment](docs/deployment.md) — production-shaped bring-up,
  observability, and scaling.
- [Science filters design](docs/science-filters.md) — the rationale
  behind the per-user association layer.
- The top of [`src/api.rs`](src/api.rs) is the full REST endpoint
  reference.

## Quick start

```sh
make db_init          # data plane (mongo, valkey, kafka, minio)
make run              # gw-api on http://127.0.0.1:8080   (terminal 2)
make load_demo_data   # seed the demo dataset             (terminal 3)
make web              # vite dev server on :5173           (terminal 4)
```

Then open http://localhost:5173 and sign in (dev mode) as
`cough052@ligo.org`. Full walkthrough:
[Getting started](docs/tutorials/01-getting-started.md).
</content>
