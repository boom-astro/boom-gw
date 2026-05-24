import { expect, test } from "@playwright/test";
import { seedTokenOnce } from "./fixtures";

test.describe("Auth interceptor behavior", () => {
  test("a 401 response clears the stored token and triggers re-login", async ({
    page,
  }) => {
    // Seed via evaluate (not addInitScript) so the reload doesn't
    // re-seed the token after the interceptor clears it.
    await seedTokenOnce(page);
    // Force every superevents list call to return 401 — mimics an
    // expired/rejected token coming back from gw-api.
    await page.route("**/api/superevents?*", (route) =>
      route.fulfill({
        status: 401,
        json: { message: "unauthorized: token expired", data: null },
      }),
    );
    // Other endpoints can still 200 — the LoginPage redirect happens
    // on App-level state, not on any one fetch.
    await page.route("**/api/health", (route) =>
      route.fulfill({ json: { message: "ok", data: { status: "ok" } } }),
    );

    await page.goto("/superevents");
    // The interceptor in src/api.ts wipes localStorage on any 401.
    await expect
      .poll(
        () =>
          page.evaluate(() => window.localStorage.getItem("boom-gw.token")),
        { timeout: 5000 },
      )
      .toBeNull();
    // The cleared token only kicks the user back to /login on a
    // navigation event (the App reads state at render time). A
    // manual reload simulates exactly what a real user would do
    // after seeing an error.
    await page.reload();
    await expect(page).toHaveURL(/\/login$/);
  });
});
