import { expect, test } from "@playwright/test";
import { loginAs, mockApi } from "./fixtures";

test.describe("Groups page", () => {
  test.beforeEach(async ({ page }) => {
    await mockApi(page);
  });

  test("create a group via the dialog (Manage groups)", async ({ page }) => {
    await loginAs(page, { acls: ["Manage groups"] });
    let posted: Record<string, unknown> | null = null;
    await page.route("**/api/groups", async (route) => {
      if (route.request().method() === "POST") {
        posted = JSON.parse(route.request().postData() ?? "{}");
        return route.fulfill({
          status: 201,
          json: {
            message: "ok",
            data: {
              id: "g1",
              name: posted?.name,
              description: "",
              admin: true,
            },
          },
        });
      }
      return route.fulfill({ json: { message: "ok", data: [] } });
    });
    // Group detail fetch after create-navigation.
    await page.route("**/api/groups/g1", (route) =>
      route.fulfill({
        json: {
          message: "ok",
          data: {
            id: "g1",
            name: "MMA team",
            admin: true,
            members: [],
            streams: [],
          },
        },
      }),
    );

    await page.goto("/groups");
    await page.getByRole("button", { name: "New group" }).click();
    await page.getByLabel("Name").fill("MMA team");
    await page.getByRole("button", { name: "Create" }).click();

    await expect.poll(() => posted?.name).toBe("MMA team");
    await expect(page).toHaveURL(/\/groups\/g1$/);
  });

  test("no New group button without Manage groups", async ({ page }) => {
    await loginAs(page); // no acls
    await page.goto("/groups");
    await expect(page.getByText(/not in any groups yet/i)).toBeVisible();
    await expect(page.getByRole("button", { name: "New group" })).toHaveCount(
      0,
    );
  });
});

test.describe("Admin nav gating", () => {
  test.beforeEach(async ({ page }) => {
    await mockApi(page);
  });

  test("admin links hidden and routes redirect without ACLs", async ({
    page,
  }) => {
    await loginAs(page); // no acls
    await page.goto("/superevents");
    await expect(
      page.getByRole("button", { name: "Users", exact: true }),
    ).toHaveCount(0);
    await expect(
      page.getByRole("button", { name: "Streams", exact: true }),
    ).toHaveCount(0);
    // Deep-link is bounced back to /superevents.
    await page.goto("/admin/users");
    await expect(page).toHaveURL(/\/superevents$/);
  });

  test("admin links show with the ACLs", async ({ page }) => {
    await loginAs(page, { acls: ["Manage users", "Manage streams"] });
    await page.goto("/superevents");
    await expect(
      page.getByRole("button", { name: "Users", exact: true }),
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Streams", exact: true }),
    ).toBeVisible();
    await page.getByRole("button", { name: "Users", exact: true }).click();
    await expect(page).toHaveURL(/\/admin\/users$/);
    await expect(page.getByRole("heading", { name: "Users" })).toBeVisible();
  });
});
