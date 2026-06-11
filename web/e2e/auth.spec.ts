import { expect, test } from "@playwright/test";
import { loginAs } from "./fixtures";

test.describe("Auth session lifecycle", () => {
  test("an expired session shows Sign-in instead of the principal", async ({
    page,
  }) => {
    // Start authenticated. `loginAs` mocks /api/auth/me to return a
    // principal and /api/auth/config to report a normal env.
    await loginAs(page);
    await page.route("**/api/health", (route) =>
      route.fulfill({ json: { message: "ok", data: { status: "ok" } } }),
    );
    await page.route("**/api/superevents?*", (route) =>
      route.fulfill({ json: { message: "ok", data: [] } }),
    );

    await page.goto("/superevents");
    // First load is authenticated — principal shown in header.
    await expect(page.getByText(/test@playwright/)).toBeVisible();

    // Simulate the session cookie expiring server-side: swap the
    // /api/users/me mock (the SPA's hydration source) to report
    // anonymous on the next call. Using `page.unroute` + a fresh
    // `page.route` is more reliable than a single served-once counter
    // — React strict-mode may double-fire `loadMe()` on a single
    // mount, and a counter would trip on the second call rather than
    // on the reload.
    await page.unroute("**/api/users/me");
    await page.route("**/api/users/me", (route) =>
      route.fulfill({ json: { message: "ok", data: null } }),
    );

    await page.reload();
    // After reload the SPA learns it's signed out — header switches
    // to the Sign-in button — but still on /superevents (anonymous
    // browsing works now).
    await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();
    await expect(page).toHaveURL(/\/superevents$/);
  });
});
