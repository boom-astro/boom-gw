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
    // The compute form should be present even with no results.
    await expect(page.getByLabel("Trigger ID")).toBeVisible();
    await expect(page.getByRole("button", { name: "Compute" })).toBeDisabled();
  });

  test("renders rows from the GET response", async ({ page }) => {
    // Override the default empty-list route from mockApi() to
    // return a couple of seeded matches.
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
    // CR membership chips.
    await expect(page.getByText("50% CR")).toBeVisible();
    await expect(page.getByText("outside")).toBeVisible();
    // Result count in the header.
    await expect(page.getByText(/2 results/)).toBeVisible();
  });

  test("compute form fires POST and prepends the result", async ({ page }) => {
    const calls: Array<{ method: string; url: string; body?: unknown }> = [];
    await page.route("**/api/superevents/*/cross-matches**", async (route) => {
      const method = route.request().method();
      const url = route.request().url();
      let body: unknown = undefined;
      if (method === "POST") {
        try {
          body = JSON.parse(route.request().postData() ?? "{}");
        } catch {
          // Ignore — empty body is fine for the test.
        }
        calls.push({ method, url, body });
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
      calls.push({ method, url });
      return route.fulfill({ json: { message: "ok", data: [] } });
    });

    await page.goto("/superevents/S250101a");
    await page.getByRole("tab", { name: "Cross-matches" }).click();

    await page.getByLabel("Trigger ID").fill("bn250101000");
    await page.getByRole("button", { name: "Compute" }).click();

    await expect
      .poll(() => calls.filter((c) => c.method === "POST").length)
      .toBe(1);
    const post = calls.find((c) => c.method === "POST")!;
    expect(post.body).toMatchObject({
      instrument: "Fermi-GBM-FIN",
      trigger_id: "bn250101000",
    });

    // Result row should appear in the table.
    await expect(page.getByText("bn250101000")).toBeVisible();
  });

  test("surfaces API errors in an alert", async ({ page }) => {
    await page.route("**/api/superevents/*/cross-matches**", (route) => {
      if (route.request().method() === "POST") {
        return route.fulfill({
          status: 404,
          json: { message: "not found", data: null },
        });
      }
      return route.fulfill({ json: { message: "ok", data: [] } });
    });

    await page.goto("/superevents/S250101a");
    await page.getByRole("tab", { name: "Cross-matches" }).click();
    await page.getByLabel("Trigger ID").fill("does_not_exist");
    await page.getByRole("button", { name: "Compute" }).click();
    await expect(page.getByText(/not found/i)).toBeVisible();
  });
});
