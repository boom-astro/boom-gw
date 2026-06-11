import { expect, test } from "@playwright/test";
import { mockApi } from "./fixtures";

// The login UI is now config-driven by `/api/auth/config`. These
// tests stub that endpoint to exercise both the dev-login form and
// the OIDC button.

async function mockUnauthenticated(
  page: Parameters<typeof mockApi>[0],
  cfg: { dev_mode: boolean; oidc_enabled: boolean },
) {
  await page.route("**/api/auth/me", (route) =>
    route.fulfill({ json: { message: "ok", data: null } }),
  );
  // The SPA hydrates from the enriched profile; anonymous → null.
  await page.route("**/api/users/me", (route) =>
    route.fulfill({ json: { message: "ok", data: null } }),
  );
  await page.route("**/api/auth/config", (route) =>
    route.fulfill({
      json: {
        message: "ok",
        data: {
          ...cfg,
          oidc_login_url: cfg.oidc_enabled ? "/api/auth/login" : null,
        },
      },
    }),
  );
}

test.describe("LoginPage", () => {
  test("anonymous visitors see /superevents with a Sign-in button", async ({
    page,
  }) => {
    await mockUnauthenticated(page, { dev_mode: true, oidc_enabled: false });
    await mockApi(page);
    await page.goto("/superevents");
    // No redirect — anonymous now reaches the public page.
    await expect(page).toHaveURL(/\/superevents$/);
    await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();
  });

  test("Sign-in button in the header navigates to /login", async ({ page }) => {
    await mockUnauthenticated(page, { dev_mode: true, oidc_enabled: true });
    await mockApi(page);
    await page.goto("/superevents");
    await page.getByRole("button", { name: "Sign in" }).click();
    await expect(page).toHaveURL(/\/login$/);
  });

  test("renders the LIGO.org button when OIDC is configured", async ({
    page,
  }) => {
    await mockUnauthenticated(page, { dev_mode: false, oidc_enabled: true });
    await mockApi(page);
    await page.goto("/login");
    const btn = page.getByRole("button", { name: /Sign in with LIGO\.org$/ });
    await expect(btn).toBeVisible();
    await expect(btn).toBeEnabled();
  });

  test("dev-login mints a session and lands on /superevents", async ({
    page,
  }) => {
    await mockUnauthenticated(page, { dev_mode: true, oidc_enabled: false });
    await mockApi(page);
    const principal = {
      sub: "alice@example.org",
      iss: "dev-login",
      scopes: ["gracedb.read"],
    };
    // On successful dev-login the SPA re-hydrates from /api/users/me;
    // flip it from anonymous to a profile when the cookie is minted.
    await page.route("**/api/auth/dev-login", async (route) => {
      await page.unroute("**/api/users/me");
      await page.route("**/api/users/me", (r) =>
        r.fulfill({
          json: {
            message: "ok",
            data: { ...principal, acls: [], groups: [], streams: [] },
          },
        }),
      );
      await route.fulfill({ json: { message: "ok", data: principal } });
    });

    await page.goto("/login");
    await page.getByLabel(/Dev login: sub/i).fill("alice@example.org");
    await page.getByRole("button", { name: /Dev sign-in$/ }).click();
    await expect(page).toHaveURL(/\/superevents$/);
  });

  test("dev-login surfaces a friendly error when the endpoint 404s", async ({
    page,
  }) => {
    await mockUnauthenticated(page, { dev_mode: true, oidc_enabled: false });
    await mockApi(page);
    await page.route("**/api/auth/dev-login", (route) =>
      route.fulfill({
        status: 404,
        json: { message: "dev login disabled", data: null },
      }),
    );

    await page.goto("/login");
    await page.getByLabel(/Dev login: sub/i).fill("alice@example.org");
    await page.getByRole("button", { name: /Dev sign-in$/ }).click();
    await expect(
      page.getByText(
        /Dev login is disabled\. Start gw-api with --auth-dev-mode/i,
      ),
    ).toBeVisible();
    await expect(page).toHaveURL(/\/login$/);
  });
});
