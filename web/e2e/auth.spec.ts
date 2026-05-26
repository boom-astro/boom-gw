import { expect, test } from "@playwright/test";
import { seedMeOnce } from "./fixtures";

test.describe("Auth session lifecycle", () => {
  test("an expired session shows Sign-in instead of the principal", async ({
    page,
  }) => {
    // First /api/auth/me hit succeeds; subsequent hits return null
    // (anonymous). Models the cookie-session world: the SPA learns
    // it's signed out on the next mount, not from any single 401.
    await seedMeOnce(page);
    await page.route("**/api/health", (route) =>
      route.fulfill({ json: { message: "ok", data: { status: "ok" } } }),
    );
    await page.route("**/api/superevents?*", (route) =>
      route.fulfill({ json: { message: "ok", data: [] } }),
    );

    await page.goto("/superevents");
    // First load is authenticated — principal shown, no Sign-in.
    await expect(page.getByText(/test@playwright/)).toBeVisible();
    // Reload — /api/auth/me now returns null, header switches to Sign-in.
    await page.reload();
    await expect(
      page.getByRole("button", { name: "Sign in" }),
    ).toBeVisible();
    // Still on /superevents — anonymous browsing works.
    await expect(page).toHaveURL(/\/superevents$/);
  });
});
