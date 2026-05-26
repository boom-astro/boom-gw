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

The SPA uses an HttpOnly session cookie (`boom_gw_session`) minted
by gw-api after a successful login. Two ways to get one:

1. **CILogon OIDC** ("Sign in with LIGO.org" button): redirects to
   CILogon, which delegates to the LIGO.org Shibboleth IdP, then
   comes back to `/api/auth/callback` and drops the session cookie.
   Requires a CILogon OIDC client registered at
   <https://cilogon.org/oauth2/register> with the boom-gw redirect
   URI; set `BOOM_GW_OIDC_CLIENT_ID` + `BOOM_GW_OIDC_CLIENT_SECRET`
   on the gw-api process to enable.

2. **Dev login** (only with `--auth-dev-mode` on gw-api): the
   LoginPage shows a `sub` field that POSTs to
   `/api/auth/dev-login` to mint a session for an arbitrary
   principal. Used by `make run` and Playwright.

CLI clients and CI can still use a SCITokens bearer JWT via
`Authorization: Bearer <jwt>` — the middleware accepts either form.

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
