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
  });

  test("empty GRB table renders the empty-state hint", async ({ page }) => {
    await page.goto("/external-streams");
    await expect(page.getByText(/No GRB triggers yet/)).toBeVisible();
  });

  test("populated GRB table renders rows", async ({ page }) => {
    await page.route("**/api/grb-triggers?*", (route) =>
      route.fulfill({
        json: {
          message: "ok",
          data: [
            {
              _id: { instrument: "Fermi-GBM-FIN", trigger_id: "bn250101" },
              instrument: "Fermi-GBM-FIN",
              trigger_id: "bn250101",
              trigger_time: 1463529618.5,
              position: {
                ra: 135.2,
                dec: -15.4,
                uncertainty_arcsec: 9000.0,
              },
              significance: 7.5,
              error_radius_deg: 2.5,
              ingested_at: { $date: { $numberLong: String(Date.now()) } },
            },
          ],
        },
      }),
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
  });
});
