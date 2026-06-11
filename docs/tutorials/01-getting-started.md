# 1. Getting started

This tutorial brings up boom-gw on your machine, loads a realistic
demo dataset, signs you in, and gives you a quick tour of the web UI.
By the end you'll have a running stack the rest of the tutorials build
on.

## What you'll run

boom-gw has four moving parts in dev:

- a **data plane** — MongoDB (history), Valkey (in-flight state),
  a single-broker Kafka, and MinIO (S3 sky-map storage), all in Docker;
- **gw-api** — the Rust HTTP service (REST API + serves the SPA);
- the **demo loader** — a one-shot binary that seeds a curated dataset
  through the live API;
- the **Vite dev server** — the React frontend, with hot reload.

You need Docker, a Rust toolchain (`cargo`), and Node (`npm`).

## Step 1 — data plane

```sh
make db_init
```

This `docker compose up`s mongo, valkey, the broker, and MinIO, then
creates the `boom-gw-skymaps` bucket. It's idempotent — run it again
any time.

## Step 2 — gw-api

In a second terminal:

```sh
make run
```

This builds and runs `gw_api` against the data plane in **dev-auth
mode** (no CILogon round-trip) with the S3 sky-map backend pointed at
MinIO. It binds `http://127.0.0.1:8080`. The recipe also sets
`BOOM_GW_SITE_ADMINS` to `load-demo-data,cough052@ligo.org` so those
two principals become **Super admin** on first sign-in (the
access-control bootstrap — see tutorial 5).

Confirm it's up:

```sh
curl -s http://127.0.0.1:8080/api/health
# {"message":"success","data":{"status":"ok"}}
```

## Step 3 — load the demo dataset

In a third terminal:

```sh
make load_demo_data
```

The loader wipes its managed collections and reseeds, **POSTing
everything through the live REST API** (so it exercises the same
handlers operators do). You'll see a summary:

```
load_demo_data: seeded demo dataset:
  superevents:       5
  gw events:         6
  skymaps:           3
  grb triggers:      6
  boom alerts:       5
  frb alerts:        3
  neutrino alerts:   3
  icecube lvk search:1
  cross-matches:     5
  science filters:   3
  groups:            1
  ...
```

What you now have:

- **5 superevents** (`S260524a`…`S260524e`) spanning BNS-like,
  borderline, and sub-threshold cases. `S260524e` is the rich one —
  it has a sky map plus pre-computed cross-matches against GRB, FRB,
  and neutrino triggers.
- **6 external GRB triggers** (Fermi-GBM at several refinement stages,
  Swift-BAT), **3 FRB alerts** (CHIME, DSA110), **3 neutrino alerts**
  (IceCube, KM3NeT), and **5 optical (BOOM) transients**.
- A group **"MMA team"** with all five messenger streams, plus
  **3 science filters** shared with it.

> The loader needs gw-api already running (step 2) because it ingests
> through the API. If you see "needs gw-api running", start `make run`
> first.

## Step 4 — the frontend

In a fourth terminal:

```sh
make web
```

Vite serves the SPA on **http://localhost:5173** and proxies `/api/*`
to gw-api. Open it in a browser.

> Prefer one command? `make stack_up` runs `db_init` + prints the
> recommended terminal split. The long-running processes (`run`,
> `web`) and the loader still go in their own terminals.

## Step 5 — sign in

The landing page shows a **Sign in** button. Because gw-api is in dev
mode, the login page offers a **dev-login** form. Sign in as:

```
cough052@ligo.org
```

This mints a session cookie and, on first sign-in, provisions the user
with the **Super admin** role (it's in `BOOM_GW_SITE_ADMINS`). You can
sign in as any other string too — those users start with the **Full
user** role and see only what they're a member of.

## Step 6 — a quick tour

The top nav bar exposes the main sections:

| Nav item | What's there |
|----------|--------------|
| **Superevents** | The GW superevent list and per-event detail (sky maps, g-events, cross-matches, annotations). Tutorial 2. |
| **External streams** | Ingested GRB / FRB / neutrino / optical triggers. Tutorial 6. |
| **Science filters** | Saved cut-sets + confidence tiers. Tutorial 4. |
| **Groups** | Your collaboration groups and their members/streams. Tutorial 5. |
| **Users / Streams** | Admin pages — only visible if you hold the `Manage users` / `Manage streams` ACL (you do, as Super admin). Tutorial 5. |
| **System health** | Ingest-stream freshness + localization-job stats. |

Click **Superevents**, then open **`S260524e`** — that's the
best-populated event and the subject of the next few tutorials.

## What just happened (the data flow)

In production the same picture runs continuously:

```
GraceDB Kafka ─▶ gw-clusterer ─▶ superevents ─▶ MongoDB
GCN Kafka     ─▶ gw-gcn-consumer ─▶ GRB/FRB/ν/optical triggers
                                       │
                        cross-match (RAVEN) ◀── on demand / scan
                                       │
                  science filters ─▶ per-user associations ─▶ alerts
```

The demo loader stands in for the live Kafka consumers by POSTing the
same documents through the REST API.

---

Next: [Superevents & localizations](02-superevents-and-localizations.md).
</content>
