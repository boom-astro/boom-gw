import { expect, test } from "@playwright/test";
import { loginAs, mockApi } from "./fixtures";

test.describe("External streams page", () => {
  test.beforeEach(async ({ page }) => {
    await loginAs(page);
    await mockApi(page);
  });

  test("nav button opens the page with both tabs", async ({ page }) => {
    await page.goto("/superevents");
    await page.getByRole("button", { name: "External streams" }).click();
    await expect(page).toHaveURL(/\/external-streams$/);
    await expect(page.getByRole("tab", { name: "GRB triggers" })).toBeVisible();
    await expect(page.getByRole("tab", { name: "BOOM alerts" })).toBeVisible();
    await expect(page.getByRole("tab", { name: "FRB alerts" })).toBeVisible();
    await expect(
      page.getByRole("tab", { name: "Neutrino alerts" }),
    ).toBeVisible();
  });

  test("empty GRB table renders the empty-state hint", async ({ page }) => {
    // The list now fetches `/api/grb-trigger-summaries` (one row
    // per trigger_id after the Fermi-GBM stage-collapse), plus a
    // count endpoint for server-side pagination. Both stay empty
    // by default — the empty-state hint should still render.
    await page.route("**/api/grb-trigger-summaries?*", (route) =>
      route.fulfill({ json: { message: "ok", data: [] } }),
    );
    await page.route("**/api/grb-trigger-summaries/count*", (route) =>
      route.fulfill({ json: { message: "ok", data: { count: 0 } } }),
    );
    await page.goto("/external-streams");
    await expect(page.getByText(/No GRB triggers yet/)).toBeVisible();
  });

  test("populated GRB table renders rows", async ({ page }) => {
    // Summary shape: one row per trigger_id, with `stages` carrying
    // the per-stage refinement chain. The list view shows the
    // best-stage instrument + the stage count; per-stage detail
    // lives on the drill-down page.
    await page.route("**/api/grb-trigger-summaries?*", (route) =>
      route.fulfill({
        json: {
          message: "ok",
          data: [
            {
              _id: "bn250101",
              best_instrument: "Fermi-GBM-FIN",
              ra: 135.2,
              dec: -15.4,
              error_radius_deg: 2.5,
              trigger_time: 1463529618.5,
              max_significance: 7.5,
              stage_count: 3,
              stages: [],
              latest_ingest: { $date: { $numberLong: String(Date.now()) } },
            },
          ],
        },
      }),
    );
    await page.route("**/api/grb-trigger-summaries/count*", (route) =>
      route.fulfill({ json: { message: "ok", data: { count: 1 } } }),
    );
    await page.goto("/external-streams");
    await expect(page.getByText("bn250101")).toBeVisible();
    await expect(page.getByText("Fermi-GBM-FIN")).toBeVisible();
  });

  test("BOOM alerts tab renders alerts with classification", async ({
    page,
  }) => {
    await page.route("**/api/boom-alerts?*", (route) =>
      route.fulfill({
        json: {
          message: "ok",
          data: [
            {
              _id: "2026-11-30T10:28:57Z__ZTF25acffkdr",
              alert_id: "2026-11-30T10:28:57Z__ZTF25acffkdr",
              alert_time: 1480497015.0,
              event_name: "ZTF25acffkdr",
              ra: 150.175649,
              dec: 15.2832185,
              classification: "Type II",
              classification_score: 0.9,
              cross_match_summary: "GW LVK S251117bs",
              // Bracket: last non-detection at 09:29:57Z, first
              // detection at 10:29:57Z (one hour later). The page
              // formats both with the GPS→UTC helper.
              last_non_detection_time: 1480493415.0,
              first_detection_time: 1480497015.0,
              ingested_at: { $date: { $numberLong: String(Date.now()) } },
            },
          ],
        },
      }),
    );
    await page.goto("/external-streams");
    await page.getByRole("tab", { name: "BOOM alerts" }).click();
    await expect(page.getByText("ZTF25acffkdr")).toBeVisible();
    await expect(page.getByText("Type II")).toBeVisible();
    await expect(page.getByText("GW LVK S251117bs")).toBeVisible();
    // Bracket columns drive the scan-cross-match time filter; the
    // operator has to see them to sanity-check coverage. The
    // header text is wrapped in a Tooltip span, so the column's
    // accessible name is the tooltip body — match the visible
    // label via getByText instead.
    await expect(page.getByText("Last non-det", { exact: true })).toBeVisible();
    await expect(page.getByText("First det", { exact: true })).toBeVisible();
    // last_non_detection_time = first_detection_time - 3600 s, so
    // the rendered cell is exactly one hour before the alert/first-
    // detection timestamp the page already shows.
    await expect(page.getByText("2026-12-05 08:09:57Z")).toBeVisible();
  });

  test("FRB alerts tab renders DM + SNR + known source", async ({ page }) => {
    await page.route("**/api/frb-alerts?*", (route) =>
      route.fulfill({
        json: {
          message: "ok",
          data: [
            {
              _id: { instrument: "CHIME-FRB", trigger_id: "427325191" },
              instrument: "CHIME-FRB",
              trigger_id: "427325191",
              trigger_time: 1410873570.0,
              position: {
                ra: 346.78,
                dec: 12.63,
                uncertainty_arcsec: 2156.0,
              },
              significance: 12.7,
              error_radius_deg: 0.599,
              dm: 279.4,
              dm_error: 0.4,
              importance: 0.987,
              snr: 12.7,
              known_source: "FRB-20240918A",
              ingested_at: { $date: { $numberLong: String(Date.now()) } },
            },
          ],
        },
      }),
    );
    await page.goto("/external-streams");
    await page.getByRole("tab", { name: "FRB alerts" }).click();
    await expect(page.getByText("CHIME-FRB")).toBeVisible();
    await expect(page.getByText("427325191")).toBeVisible();
    // DM and SNR appear with their own columns so the operator can
    // sanity-check "is this likely extragalactic?".
    await expect(page.getByText("279.4")).toBeVisible();
    await expect(page.getByText("FRB-20240918A")).toBeVisible();
  });

  test("Neutrino alerts tab renders pipeline + topology + p", async ({
    page,
  }) => {
    await page.route("**/api/neutrino-alerts?*", (route) =>
      route.fulfill({
        json: {
          message: "ok",
          data: [
            {
              _id: { instrument: "IceCube", trigger_id: "137840_57034692" },
              instrument: "IceCube",
              trigger_id: "137840_57034692",
              trigger_time: 1366090946.0,
              position: {
                ra: 345.82,
                dec: 9.01,
                uncertainty_arcsec: 1800.0,
              },
              significance: 0.341,
              error_radius_deg: 0.5,
              alert_topology: "Track",
              pipeline: "Gold Track Alert",
              nu_energy: 127.3,
              p_astro: 0.341,
              event_name: "IceCube-230416A",
              ingested_at: { $date: { $numberLong: String(Date.now()) } },
            },
          ],
        },
      }),
    );
    await page.goto("/external-streams");
    await page.getByRole("tab", { name: "Neutrino alerts" }).click();
    await expect(page.getByText("IceCube-230416A")).toBeVisible();
    await expect(page.getByText("Gold Track Alert")).toBeVisible();
    await expect(page.getByText("Track", { exact: true })).toBeVisible();
    // p_astro rendered to three decimal places by the cell formatter.
    await expect(page.getByText("0.341")).toBeVisible();
    await expect(page.getByText("127.3")).toBeVisible();
  });
});
