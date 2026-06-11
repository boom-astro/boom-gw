import { expect, test } from "@playwright/test";
import { loginAs, mockApi } from "./fixtures";

test.describe("Cross-matches tab", () => {
  test.beforeEach(async ({ page }) => {
    await loginAs(page);
    await mockApi(page);
  });

  test("renders empty-state copy when no matches yet", async ({ page }) => {
    await page.goto("/superevents/S250101a");
    await page.getByRole("tab", { name: "Cross-matches" }).click();
    await expect(page.getByText(/no cross-matches yet/i)).toBeVisible();
    // The Scan controls are present even with no results.
    await expect(page.getByLabel("Time window (± sec)")).toBeVisible();
    await expect(page.getByLabel("p-value trials")).toBeVisible();
    await expect(page.getByRole("button", { name: /Scan/ })).toBeEnabled();
  });

  test("renders rows from the GET response and shows associated state", async ({
    page,
  }) => {
    await page.route("**/api/superevents/*/cross-matches**", (route) => {
      if (route.request().method() === "POST") {
        return route.continue();
      }
      return route.fulfill({
        json: {
          message: "ok",
          data: [
            {
              _id: {
                superevent_id: "S250101a",
                instrument: "Swift-BAT",
                trigger_id: "01234567",
              },
              superevent_id: "S250101a",
              instrument: "Swift-BAT",
              trigger_id: "01234567",
              time_offset_sec: 4.3,
              spatial_overlap: 0.612,
              in_50cr: true,
              in_90cr: true,
              joint_far_per_year: 5e-4,
              associated: true,
              computed_at: { $date: { $numberLong: String(Date.now()) } },
            },
            {
              _id: {
                superevent_id: "S250101a",
                instrument: "Fermi-GBM-FIN",
                trigger_id: "bn250101000",
              },
              superevent_id: "S250101a",
              instrument: "Fermi-GBM-FIN",
              trigger_id: "bn250101000",
              time_offset_sec: -2.1,
              spatial_overlap: 1e-5,
              in_50cr: false,
              in_90cr: false,
              joint_far_per_year: 1.2,
              associated: false,
              computed_at: { $date: { $numberLong: String(Date.now()) } },
            },
          ],
        },
      });
    });

    await page.goto("/superevents/S250101a");
    await page.getByRole("tab", { name: "Cross-matches" }).click();

    await expect(page.getByText("01234567")).toBeVisible();
    await expect(page.getByText("bn250101000")).toBeVisible();
    await expect(page.getByText(/2 results.*1 associated/)).toBeVisible();
  });

  test("Scan button POSTs to /scan-cross-matches and refreshes the list", async ({
    page,
  }) => {
    const scanCalls: Array<{ body: unknown }> = [];
    await page.route(
      "**/api/superevents/*/scan-cross-matches",
      async (route) => {
        let body: unknown = undefined;
        try {
          body = JSON.parse(route.request().postData() ?? "{}");
        } catch {
          // Ignore — undefined body is fine.
        }
        scanCalls.push({ body });
        return route.fulfill({
          json: {
            message: "ok",
            data: [
              {
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
                p_value: 0.01,
                p_value_trials: 200,
                joint_far_remapped_per_year: 1.5e-12,
                associated: false,
                computed_at: { $date: { $numberLong: String(Date.now()) } },
              },
            ],
          },
        });
      },
    );

    await page.goto("/superevents/S250101a");
    await page.getByRole("tab", { name: "Cross-matches" }).click();
    await page.getByLabel("Time window (± sec)").fill("30");
    await page.getByLabel("p-value trials").fill("500");
    await page.getByRole("button", { name: /Scan/ }).click();

    await expect.poll(() => scanCalls.length).toBe(1);
    expect(scanCalls[0].body).toMatchObject({
      time_window_sec: 30,
      p_value_trials: 500,
    });
    await expect(page.getByText("bn250101000")).toBeVisible();
  });

  test("star toggle PATCHes /cross-matches/{instrument}/{trigger_id}", async ({
    page,
  }) => {
    await page.route("**/api/superevents/*/cross-matches**", (route) => {
      const method = route.request().method();
      if (method === "GET") {
        return route.fulfill({
          json: {
            message: "ok",
            data: [
              {
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
                in_50cr: false,
                in_90cr: true,
                p_value: 0.01,
                joint_far_remapped_per_year: 1.5e-12,
                associated: false,
                computed_at: { $date: { $numberLong: String(Date.now()) } },
              },
            ],
          },
        });
      }
      return route.continue();
    });

    let patchedBody: unknown = null;
    await page.route(
      "**/api/superevents/*/cross-matches/*/*",
      async (route) => {
        try {
          patchedBody = JSON.parse(route.request().postData() ?? "{}");
        } catch {
          // Ignore — undefined body is fine.
        }
        return route.fulfill({
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
              in_50cr: false,
              in_90cr: true,
              p_value: 0.01,
              joint_far_remapped_per_year: 1.5e-12,
              associated: true,
              computed_at: { $date: { $numberLong: String(Date.now()) } },
            },
          },
        });
      },
    );

    await page.goto("/superevents/S250101a");
    await page.getByRole("tab", { name: "Cross-matches" }).click();
    await expect(page.getByText("bn250101000")).toBeVisible();
    await page.getByRole("button", { name: "Mark as an association" }).click();

    await expect.poll(() => patchedBody).toMatchObject({ associated: true });
    // After the patch, the row should now offer the un-associate
    // tooltip.
    await expect(
      page.getByRole("button", { name: "Un-associate this match" }),
    ).toBeVisible();
  });

  test("surfaces API errors in an alert", async ({ page }) => {
    await page.route("**/api/superevents/*/scan-cross-matches", (route) =>
      route.fulfill({
        status: 503,
        json: { message: "skymap storage not configured", data: null },
      }),
    );

    await page.goto("/superevents/S250101a");
    await page.getByRole("tab", { name: "Cross-matches" }).click();
    await page.getByRole("button", { name: /Scan/ }).click();
    await expect(
      page.getByText(/skymap storage not configured/i),
    ).toBeVisible();
  });
});
