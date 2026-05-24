# boom-gw web

React + TypeScript SPA for the boom-gw API. Modeled on SkyPortal's
GcnEvent UI: a paginated superevent list, a per-superevent detail page
with tabs (Overview / Localization / Annotations / Alerts), and a
sticky properties drawer.

## Stack

- React 18.3, MUI v7, Redux Toolkit (RTK), react-router-dom v7
- Vite 6 build, strict TypeScript
- Aladin Lite v3 (CDN) for the FITS skymap viewer

The stack mirrors SkyPortal's modern frontend with two intentional
divergences: TypeScript instead of JSX (fresh project, no migration
cost), and Aladin Lite for the localization viewer instead of
SkyPortal's D3 Mollweide globe (Aladin consumes the MOC FITS
directly, so we skip the backend contour pre-compute step).

## Dev loop

In one terminal, run gw-api:

```sh
# from repo root
cargo run --bin gw_api -- \
  --mongo-uri mongodb://localhost:27017 \
  --bind 0.0.0.0:8080 \
  --auth-dev-mode
```

In another, run Vite:

```sh
cd web
npm install
npm run dev
```

Vite serves on http://localhost:5173 and proxies `/api/*` →
http://localhost:8080, so the browser sees the API as same-origin.

## Auth

Get a SCITokens bearer JWT with:

```sh
htgettoken -a vault.ligo.org -i igwn
cat "$BEARER_TOKEN_FILE"
```

Paste the token into the login page. It's persisted in
`localStorage` under `boom-gw.token` and sent as `Authorization:
Bearer ...` on every API request. On a 401 the token is wiped and
the user is bounced back to login.

## Production build

```sh
npm run build   # → web/dist/
```

To serve the SPA from gw-api itself (same-origin, no separate web
server):

```sh
cargo run --bin gw_api -- --static-dir web/dist  # or BOOM_GW_STATIC_DIR
```

gw-api mounts the bundle as a catch-all behind `/api/*` and uses
`index.html` as the SPA-deep-link fallback so `/superevents/S250101a`
resolves on hard reload.

## What's in v1

- Superevents list with sort + pagination
- Superevent detail: Overview (g-events), Localization (Aladin Lite +
  localize req/result), Annotations (read-only), Alerts (read-only)
- Properties drawer with skymap size + elapsed-ms
- Token expiry warning in the app bar

## Tests

End-to-end Playwright suite under `e2e/`, modeled on SkyPortal's
browser-driven test approach but using Playwright instead of
Selenium. Every API request is mocked at the network layer
(`page.route(...)`), so the suite runs hermetically — no gw-api,
no mongo, no S3, no SCITokens IdP, no docker.

```sh
npm install
npm run test:e2e:install   # one-time: download chromium (~150 MB)
npm run test:e2e           # headless, ~7 s
npm run test:e2e:ui        # interactive Playwright UI
```

Run a single spec:

```sh
npx playwright test e2e/login.spec.ts
```

The Aladin Lite localization viewer is exercised via a stubbed
`window.A` (see `stubAladin` in [e2e/fixtures.ts](e2e/fixtures.ts))
— we assert that the SPA fires the right `/contour?level=50,90`
requests rather than that Aladin actually paints anything. A
visual-render smoke against a real running stack is intentionally
left for a manual check in the browser plus
`web/scripts/smoke.sh`.

## What's deferred

- Phase 2: annotation submit form, alert assemble/publish button
  (gated on the gw-api alert-publisher allowlist)
- Phase 3: SSE/websocket for live updates, deeper filtering, plots
  (distance posterior, sky probability contours)
