import { expect, test } from "@playwright/test";
import { loginAs, mockApi } from "./fixtures";

test.describe("SystemHealthPage", () => {
  test.beforeEach(async ({ page }) => {
    await loginAs(page);
    await mockApi(page);
  });

  test("renders all three panels with fixture data", async ({ page }) => {
    await page.goto("/system-health");
    await expect(
      page.getByRole("heading", { name: "System Health" }),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Ingest streams" }),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Localize queue" }),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Recent BAYESTAR errors" }),
    ).toBeVisible();

    // One row per stream in the ingest table.
    await expect(page.getByText("GraceDB (GW)")).toBeVisible();
    await expect(page.getByText("GCN — GRB")).toBeVisible();
    await expect(page.getByText("GCN — FRB")).toBeVisible();
    await expect(page.getByText("GCN — Neutrino")).toBeVisible();
    await expect(page.getByText("BOOM alerts")).toBeVisible();

    // Localize tiles. Pending=5, errors=3, total=100 → error rate 3.0%.
    const localizePanel = page
      .getByRole("heading", { name: "Localize queue" })
      .locator("..");
    await expect(localizePanel.getByText("5", { exact: true })).toBeVisible();
    await expect(localizePanel.getByText("100", { exact: true })).toBeVisible();
    await expect(localizePanel.getByText("3.0%")).toBeVisible();
    // Gate tiles: 250 skipped; 105 submitted (100 results + 5 pending)
    // / 355 considered = 29.6% submitted.
    await expect(localizePanel.getByText("250", { exact: true })).toBeVisible();
    await expect(localizePanel.getByText("29.6%")).toBeVisible();

    // Recent error row.
    await expect(page.getByText("S000001-G0000001")).toBeVisible();
    await expect(
      page.getByText("BAYESTAR ValueError: mixed lengths"),
    ).toBeVisible();
  });

  test("nav button reaches the page from /superevents", async ({ page }) => {
    await page.goto("/superevents");
    await page.getByRole("button", { name: "System health" }).click();
    await expect(page).toHaveURL(/\/system-health$/);
    await expect(
      page.getByRole("heading", { name: "System Health" }),
    ).toBeVisible();
  });

  test("shows error banner when dashboard endpoint 500s", async ({ page }) => {
    await page.unroute("**/api/health/dashboard");
    await page.route("**/api/health/dashboard", (route) =>
      route.fulfill({ status: 500, json: { message: "boom", data: null } }),
    );
    await page.goto("/system-health");
    await expect(page.getByRole("alert")).toBeVisible();
  });
});
