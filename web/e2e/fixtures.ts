// Shared E2E test helpers.
//
// All tests mock the gw-api at the network layer instead of hitting
// a real backend — that keeps the suite hermetic (no docker, no
// mongo, no S3, no SCITokens IdP) and fast enough to run on every
// PR. The shape of every mock here matches the actual ApiEnvelope
// + types in `src/types/api.ts`; if the wire format drifts the
// Rust integration tests will catch it long before these do.

import { Page } from "@playwright/test";

/**
 * Mock `/api/auth/me` to report an authenticated principal. The
 * SPA's `loadMe()` thunk hits this on App mount; once it returns
 * 200, App drops the spinner and renders the protected routes.
 *
 * Implementation note: the cookie-session model means tests don't
 * need to set anything on the document — they just mock the `me`
 * lookup. This is intentional — the SPA never touches a token, so
 * "logged in" is whatever shape `/api/auth/me` returns.
 */
export async function loginAs(page: Page, overrides: Record<string, unknown> = {}) {
  const principal = {
    sub: "test@playwright",
    iss: "https://cilogon.org",
    scopes: ["gracedb.read"],
    ...overrides,
  };
  await page.route("**/api/auth/me", (route) =>
    route.fulfill({ json: { message: "ok", data: principal } }),
  );
  await page.route("**/api/auth/config", (route) =>
    route.fulfill({
      json: {
        message: "ok",
        data: {
          dev_mode: false,
          oidc_enabled: true,
          oidc_login_url: "/api/auth/login",
        },
      },
    }),
  );
  return principal;
}


/**
 * Stub `window.A` (Aladin Lite) with the minimum surface the
 * AladinViewer touches: `init` is a resolved Promise, `aladin()`
 * returns an object with `addMOC`, and `MOCFromURL` synchronously
 * returns a sentinel. This lets the viewer race past its
 * waitForAladin/init steps and immediately fire the contour
 * fetches — which is what most Localization tests want to observe.
 */
export async function stubAladin(page: Page) {
  await page.addInitScript(() => {
    type A = Record<string, unknown>;
    const inst: A = {
      addMOC: () => undefined,
      gotoRaDec: () => undefined,
    };
    (window as A).A = {
      init: Promise.resolve(),
      aladin: () => inst,
      MOCFromURL: () => ({}),
    };
  });
}

export const FIXTURE_SUPEREVENTS = [
  {
    _id: "S250101a",
    t_0: 1356134418.0,
    t_start: 1356134416.0,
    t_end: 1356134420.0,
    preferred_graceid: "G123456",
    preferred_snr: 12.3,
    g_event_graceids: ["G123456", "G123457"],
    skymap_summary: { bytes_size: 777600, elapsed_ms: 1421 },
  },
  {
    _id: "S250102b",
    t_0: 1356220818.0,
    t_start: 1356220816.0,
    t_end: 1356220820.0,
    preferred_graceid: "G123458",
    preferred_snr: 8.7,
    g_event_graceids: ["G123458"],
  },
];

export const FIXTURE_EVENTS = [
  {
    _id: "G123456",
    pipeline: "gstlal",
    producer_timestamp: 1356134418.0,
    message_type: "new",
    submitter: "gstlal",
    end_time: 1356134418.0,
    ifos: "H1,L1,V1",
    snr: 12.3,
    far: 1e-9,
    mchirp: 1.4,
    total_mass: 2.8,
  },
  {
    _id: "G123457",
    pipeline: "pycbc",
    producer_timestamp: 1356134419.0,
    message_type: "new",
    submitter: "pycbc",
    end_time: 1356134419.0,
    ifos: "H1,L1",
    snr: 9.1,
    far: 5e-8,
    mchirp: 1.5,
    total_mass: 2.9,
  },
];

/**
 * Install a complete set of API mocks. Any test that needs custom
 * behavior for a particular endpoint should call this first, then
 * override with its own `page.route(...)` registration — Playwright
 * matches routes in last-registered-first order.
 */
export async function mockApi(
  page: Page,
  overrides: {
    superevents?: unknown[];
    events?: unknown[];
    failContour?: boolean;
  } = {},
) {
  const superevents = overrides.superevents ?? FIXTURE_SUPEREVENTS;
  const events = overrides.events ?? FIXTURE_EVENTS;

  await page.route("**/api/health", (route) =>
    route.fulfill({ json: { message: "ok", data: { status: "ok" } } }),
  );
  // Default health-dashboard payload — populated enough that the
  // SystemHealthPage renders without "no data" placeholders. Tests
  // that care about specific numbers re-route this themselves.
  await page.route("**/api/health/dashboard", (route) =>
    route.fulfill({
      json: {
        message: "ok",
        data: {
          generated_at: new Date().toISOString(),
          streams: {
            gracedb_gw: {
              total: 12,
              last_ingested_at: null,
              count_1h: null,
            },
            gcn_grb: {
              total: 7,
              last_ingested_at: new Date(Date.now() - 60_000).toISOString(),
              count_1h: 2,
            },
            gcn_frb: {
              total: 3,
              last_ingested_at: new Date(Date.now() - 60_000).toISOString(),
              count_1h: 1,
            },
            gcn_neutrino: {
              total: 1,
              last_ingested_at: new Date(Date.now() - 60_000).toISOString(),
              count_1h: 0,
            },
            gcn_boom: {
              total: 0,
              last_ingested_at: null,
              count_1h: null,
            },
          },
          localize: {
            pending: 5,
            total_results: 100,
            total_errors: 3,
            total_skipped: 250,
          },
          recent_errors: [
            {
              request_id: "S000001-G0000001",
              superevent_id: "S000001",
              graceid: "G0000001",
              error_message: "BAYESTAR ValueError: mixed lengths",
              elapsed_ms: 51234,
            },
          ],
          config: {
            _id: "default",
            stream_stale_sec: {
              gracedb_gw: 900,
              gcn_grb: 86400,
              gcn_frb: 86400,
              gcn_neutrino: 86400,
              gcn_boom: 3600,
            },
          },
        },
      },
    }),
  );
  await page.route("**/api/superevents?*", (route) =>
    route.fulfill({ json: { message: "ok", data: superevents } }),
  );
  await page.route("**/api/superevents/*", (route) => {
    const url = new URL(route.request().url());
    const id = url.pathname.split("/").pop();
    const doc = superevents.find((s: { _id: string }) => s._id === id);
    if (!doc) {
      return route.fulfill({
        status: 404,
        json: { message: "not found", data: null },
      });
    }
    return route.fulfill({ json: { message: "ok", data: doc } });
  });
  await page.route("**/api/events?*", (route) =>
    route.fulfill({ json: { message: "ok", data: events } }),
  );
  await page.route("**/api/superevents/*/annotations?*", (route) =>
    route.fulfill({ json: { message: "ok", data: [] } }),
  );
  await page.route("**/api/superevents/*/alerts?*", (route) =>
    route.fulfill({ json: { message: "ok", data: [] } }),
  );
  await page.route("**/api/localize-requests?*", (route) =>
    route.fulfill({ json: { message: "ok", data: [] } }),
  );
  await page.route("**/api/localize-results?*", (route) =>
    route.fulfill({ json: { message: "ok", data: [] } }),
  );
  await page.route("**/api/grb-triggers?*", (route) =>
    route.fulfill({ json: { message: "ok", data: [] } }),
  );
  // GRB list view now hits the per-trigger_id summaries endpoint
  // (one row per trigger after the Fermi-GBM stage collapse).
  // ExternalStreamsPage also fetches a count for server-side
  // pagination. Empty defaults; tests that need populated rows
  // override these.
  await page.route("**/api/grb-trigger-summaries?*", (route) =>
    route.fulfill({ json: { message: "ok", data: [] } }),
  );
  await page.route("**/api/grb-trigger-summaries/count*", (route) =>
    route.fulfill({ json: { message: "ok", data: { count: 0 } } }),
  );
  // /api/superevents/count powers server-side pagination on the
  // Superevents list. Default to the list's length so the page
  // footer is consistent with what's rendered; tests that override
  // the list with custom data can re-register this route too.
  await page.route("**/api/superevents/count*", (route) =>
    route.fulfill({
      json: { message: "ok", data: { count: superevents.length } },
    }),
  );
  await page.route("**/api/boom-alerts?*", (route) =>
    route.fulfill({ json: { message: "ok", data: [] } }),
  );
  await page.route("**/api/frb-alerts?*", (route) =>
    route.fulfill({ json: { message: "ok", data: [] } }),
  );
  await page.route("**/api/neutrino-alerts?*", (route) =>
    route.fulfill({ json: { message: "ok", data: [] } }),
  );
  await page.route("**/api/superevents/*/icecube-lvk-searches*", (route) =>
    route.fulfill({ json: { message: "ok", data: [] } }),
  );
  // Scan endpoint defaults to a successful empty-list response —
  // tests that care override.
  await page.route("**/api/superevents/*/scan-cross-matches", (route) =>
    route.fulfill({ json: { message: "ok", data: [] } }),
  );
  // PATCH /cross-matches/{instrument}/{trigger_id} echoes a no-op
  // result; tests that assert on the body override.
  await page.route(
    "**/api/superevents/*/cross-matches/*/*",
    (route) => {
      if (route.request().method() === "PATCH") {
        return route.fulfill({
          json: {
            message: "ok",
            data: {
              _id: {
                superevent_id: "S",
                instrument: "X",
                trigger_id: "Y",
              },
              superevent_id: "S",
              instrument: "X",
              trigger_id: "Y",
              time_offset_sec: 0,
              spatial_overlap: 0,
              in_50cr: false,
              in_90cr: false,
              associated: false,
              computed_at: { $date: { $numberLong: String(Date.now()) } },
            },
          },
        });
      }
      return route.continue();
    },
  );
  // Cross-matches default to an empty list. Tests that care about
  // the populated path or POST behavior re-register their own
  // route after calling mockApi().
  await page.route("**/api/superevents/*/cross-matches**", (route) => {
    if (route.request().method() === "POST") {
      return route.fulfill({
        status: 201,
        json: {
          message: "ok",
          data: {
            _id: {
              superevent_id: "S250101a",
              instrument: "Fermi-GBM-FIN",
              trigger_id: "bn250101000",
            },
            superevent_id: "S250101a",
            instrument: "Fermi-GBM-FIN",
            trigger_id: "bn250101000",
            time_offset_sec: 0.5,
            spatial_overlap: 0.42,
            in_50cr: true,
            in_90cr: true,
            joint_far_per_year: 1.5e-3,
            computed_at: { $date: { $numberLong: String(Date.now()) } },
          },
        },
      });
    }
    return route.fulfill({ json: { message: "ok", data: [] } });
  });
  await page.route("**/api/superevents/*/contour*", (route) => {
    if (overrides.failContour) {
      return route.fulfill({
        status: 404,
        json: { message: "contour not found", data: null },
      });
    }
    // A minimal-but-valid-enough FITS header so the fetch resolves;
    // Aladin Lite will still try and fail to render it, but the
    // SPA only cares that the fetch succeeded.
    return route.fulfill({
      contentType: "application/fits",
      body: Buffer.from("SIMPLE  =                    T\nEND\n"),
    });
  });
}

