import { expect, test } from "@playwright/test";
import { loginAs, mockApi, stubAladin } from "./fixtures";

test.describe("SuperEventPage", () => {
  test.beforeEach(async ({ page }) => {
    await loginAs(page);
    await mockApi(page);
  });

  test("Overview tab lists linked g-events with pipeline and SNR", async ({
    page,
  }) => {
    await page.goto("/superevents/S250101a");
    // Wait for the doc fetch to land — the heading should show the id.
    await expect(page.getByRole("heading", { name: "S250101a" })).toBeVisible();
    // Overview is the default tab. Scope to the events table since
    // GraceIDs also appear in the right-hand properties drawer.
    const eventsTable = page.getByRole("table").filter({ hasText: "Pipeline" });
    await expect(
      eventsTable.getByRole("cell", { name: "G123456" }),
    ).toBeVisible();
    await expect(
      eventsTable.getByRole("cell", { name: "gstlal" }),
    ).toBeVisible();
    await expect(
      eventsTable.getByRole("cell", { name: "H1,L1,V1" }),
    ).toBeVisible();
  });

  test("Localization tab fetches the contour MOC endpoint", async ({
    page,
  }) => {
    // Stub window.A so the viewer's waitForAladin resolves
    // immediately and the contour fetches fire. We're only asserting
    // on the fetch path here, not the visual render.
    await stubAladin(page);
    // Capture contour requests so we can assert on the URL the SPA
    // actually issues. Register BEFORE the page.route in mockApi
    // wins (Playwright matches last-registered-first, so this is
    // installed after the goto).
    const contourCalls: string[] = [];
    await page.route("**/api/superevents/*/contour*", (route) => {
      contourCalls.push(route.request().url());
      return route.fulfill({
        contentType: "application/fits",
        body: Buffer.from("SIMPLE  =                    T\nEND\n"),
      });
    });

    await page.goto("/superevents/S250101a");
    await page.getByRole("tab", { name: "Localization" }).click();

    await expect
      .poll(() => contourCalls.length, { timeout: 5000 })
      .toBeGreaterThanOrEqual(2);

    const urls = contourCalls.join("\n");
    expect(urls).toMatch(/contour\?level=50/);
    expect(urls).toMatch(/contour\?level=90/);
  });

  test("Localization tab surfaces 'no skymap' when summary is missing", async ({
    page,
  }) => {
    await page.goto("/superevents/S250102b");
    await page.getByRole("tab", { name: "Localization" }).click();
    await expect(page.getByText(/no skymap attached yet/i)).toBeVisible();
  });

  test("Annotations and Alerts tabs render empty-state copy", async ({
    page,
  }) => {
    await page.goto("/superevents/S250101a");
    await page.getByRole("tab", { name: "Annotations" }).click();
    await expect(page.getByText(/no annotations yet/i)).toBeVisible();
    await page.getByRole("tab", { name: "Alerts" }).click();
    await expect(
      page.getByText(/no alerts assembled for this superevent yet/i),
    ).toBeVisible();
  });

  test("Properties drawer shows the skymap size and elapsed_ms chip", async ({
    page,
  }) => {
    await page.goto("/superevents/S250101a");
    // 777600 bytes / 1024 ≈ 759 KB · 1421 ms
    await expect(page.getByText(/KB · 1421 ms/)).toBeVisible();
  });
});
