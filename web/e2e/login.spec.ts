import { expect, test } from "@playwright/test";
import { fakeJwt, mockApi } from "./fixtures";

test.describe("LoginPage", () => {
  test("redirects unauthenticated visitors to /login", async ({ page }) => {
    await mockApi(page);
    await page.goto("/superevents");
    await expect(page).toHaveURL(/\/login$/);
    await expect(
      page.getByRole("heading", { name: "boom-gw" }),
    ).toBeVisible();
  });

  test("decodes a pasted token and surfaces the principal", async ({
    page,
  }) => {
    await mockApi(page);
    await page.goto("/login");
    const token = fakeJwt({ sub: "alice@example.org" });
    await page.getByLabel("Bearer token").fill(token);
    // The login form decodes claims on the fly and surfaces them
    // in an info Alert — this is what catches a paste mistake
    // before the user clicks Sign in.
    await expect(page.getByText("alice@example.org")).toBeVisible();
    await expect(page.getByText("gracedb.read")).toBeVisible();
  });

  test("rejects an expired token", async ({ page }) => {
    await mockApi(page);
    await page.goto("/login");
    const expired = fakeJwt({ exp: 1 });
    await page.getByLabel("Bearer token").fill(expired);
    await page.getByRole("button", { name: "Sign in" }).click();
    await expect(
      page.getByText(/expired — mint a fresh one/i),
    ).toBeVisible();
    // Should NOT have navigated away.
    await expect(page).toHaveURL(/\/login$/);
  });

  test("accepts a valid token and lands on /superevents", async ({ page }) => {
    await mockApi(page);
    await page.goto("/login");
    await page.getByLabel("Bearer token").fill(fakeJwt());
    await page.getByRole("button", { name: "Sign in" }).click();
    await expect(page).toHaveURL(/\/superevents$/);
    await expect(
      page.getByRole("heading", { name: "Superevents" }),
    ).toBeVisible();
  });
});
