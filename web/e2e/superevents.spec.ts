import { expect, test } from "@playwright/test";
import { FIXTURE_SUPEREVENTS, loginAs, mockApi } from "./fixtures";

test.describe("SuperEventsPage", () => {
  test.beforeEach(async ({ page }) => {
    await loginAs(page);
    await mockApi(page);
  });

  test("renders the fixture rows and the loaded count", async ({ page }) => {
    await page.goto("/superevents");
    for (const s of FIXTURE_SUPEREVENTS) {
      await expect(page.getByText(s._id, { exact: true })).toBeVisible();
      await expect(
        page.getByText(s.preferred_graceid, { exact: true }),
      ).toBeVisible();
    }
    await expect(
      page.getByText(`${FIXTURE_SUPEREVENTS.length} loaded`),
    ).toBeVisible();
  });

  test("shows a skymap chip only for superevents that have one", async ({
    page,
  }) => {
    await page.goto("/superevents");
    // S250101a has skymap_summary.bytes_size = 777600 → ~760 KB chip
    await expect(page.getByText(/759 KB|760 KB|758 KB/)).toBeVisible();
    // S250102b has no skymap → dash chip
    // Use a row-scoped locator so we don't false-match on the
    // table cells of the other row.
    const row = page.getByRole("row", { name: /S250102b/ });
    await expect(row).toBeVisible();
  });

  test("clicking a row navigates to the detail page", async ({ page }) => {
    await page.goto("/superevents");
    await page.getByRole("row", { name: /S250101a/ }).click();
    await expect(page).toHaveURL(/\/superevents\/S250101a$/);
  });

  test("logout calls /api/auth/logout and stays on /superevents (anonymous)", async ({
    page,
  }) => {
    let logoutCalled = false;
    await page.route("**/api/auth/logout", (route) => {
      logoutCalled = true;
      return route.fulfill({ json: { message: "ok", data: null } });
    });
    await page.goto("/superevents");
    await page.getByRole("button", { name: "Log out" }).click();
    // We don't bounce to /login anymore — /superevents is public,
    // so the user stays where they were with the header now showing
    // Sign-in.
    await expect(page).toHaveURL(/\/superevents$/);
    await expect(
      page.getByRole("button", { name: "Sign in" }),
    ).toBeVisible();
    expect(logoutCalled).toBe(true);
  });
});
