import { expect, test } from "@playwright/test";
import { loginAs, mockApi } from "./fixtures";

const OWNER = "test@playwright";

function filterDoc(overrides: Record<string, unknown> = {}) {
  return {
    _id: "f1",
    owner: OWNER,
    group_id: "umn-mma",
    name: "GRB+GW gold/silver",
    active: false,
    cuts: {
      instruments: ["Fermi-GBM", "Swift-BAT"],
      time_window_sec: 10,
      spatial_overlap_min: 0.1,
      p_value_max: 0.05,
      joint_far_remapped_max_per_year: 12,
      require_in_90cr: true,
    },
    confidence_tiers: [
      { name: "gold", joint_far_remapped_max_per_year: 1 },
      { name: "silver", joint_far_remapped_max_per_year: 12 },
    ],
    ...overrides,
  };
}

test.describe("Science filters page", () => {
  test.beforeEach(async ({ page }) => {
    await loginAs(page);
    await mockApi(page);
  });

  test("shows empty state then creates a filter via the dialog", async ({
    page,
  }) => {
    let postedBody: Record<string, unknown> | null = null;
    await page.route("**/api/science-filters**", async (route) => {
      if (route.request().method() === "POST") {
        postedBody = JSON.parse(route.request().postData() ?? "{}");
        return route.fulfill({
          status: 201,
          json: {
            message: "ok",
            data: filterDoc({ name: postedBody?.name ?? "New" }),
          },
        });
      }
      return route.fulfill({ json: { message: "ok", data: [] } });
    });

    await page.goto("/science-filters");
    await expect(page.getByText(/no science filters yet/i)).toBeVisible();

    await page.getByRole("button", { name: "New filter" }).click();
    await page.getByLabel("Name").fill("My GRB filter");
    await page.getByLabel("Max joint FAR / yr").fill("12");
    await page.getByRole("button", { name: "Add tier" }).click();
    await page.getByLabel("Tier name").fill("gold");
    await page.getByRole("button", { name: "Create" }).click();

    await expect.poll(() => postedBody?.name).toBe("My GRB filter");
    expect(postedBody?.cuts).toMatchObject({
      joint_far_remapped_max_per_year: 12,
    });
    // The created filter shows up in the table (POST echoes the
    // submitted name).
    await expect(
      page.getByRole("cell", { name: "My GRB filter" }),
    ).toBeVisible();
  });

  test("owner can edit; tiers and cuts render as chips", async ({ page }) => {
    await page.route("**/api/science-filters**", (route) =>
      route.fulfill({ json: { message: "ok", data: [filterDoc()] } }),
    );

    await page.goto("/science-filters");
    // exact:true so the tier chips don't collide with the substring
    // in the filter name "GRB+GW gold/silver".
    await expect(page.getByText("gold", { exact: true })).toBeVisible();
    await expect(page.getByText("silver", { exact: true })).toBeVisible();
    await expect(page.getByText("in 90% CR")).toBeVisible();
    // Owner → edit button enabled.
    await expect(
      page.getByRole("button", { name: "Edit filter" }),
    ).toBeEnabled();
  });
});

test.describe("Filter selector on the cross-matches tab", () => {
  test.beforeEach(async ({ page }) => {
    await loginAs(page);
    await mockApi(page);
  });

  test("applying a filter shows only passing rows with tier badges", async ({
    page,
  }) => {
    await page.route("**/api/science-filters**", (route) =>
      route.fulfill({ json: { message: "ok", data: [filterDoc()] } }),
    );

    // Cross-matches: the filtered request (filter_id present) returns
    // a single passing row tagged gold; the unfiltered request returns
    // two rows.
    await page.route("**/api/superevents/*/cross-matches**", (route) => {
      if (route.request().method() !== "GET") return route.continue();
      const filtered = route.request().url().includes("filter_id=");
      const base = {
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
        p_value: 0.001,
        joint_far_remapped_per_year: 0.5,
        associated: false,
        computed_at: { $date: { $numberLong: String(Date.now()) } },
      };
      const data = filtered
        ? [{ ...base, confidence_tier: "gold" }]
        : [
            base,
            {
              ...base,
              _id: { ...base._id, trigger_id: "junk1" },
              trigger_id: "junk1",
              spatial_overlap: 1e-5,
              in_90cr: false,
              joint_far_remapped_per_year: 200,
            },
          ];
      return route.fulfill({ json: { message: "ok", data } });
    });

    await page.goto("/superevents/S250101a");
    await page.getByRole("tab", { name: "Cross-matches" }).click();

    // Unfiltered: both rows present.
    await expect(page.getByText("bn250101000")).toBeVisible();
    await expect(page.getByText("junk1")).toBeVisible();

    // Select the science filter.
    await page.getByRole("combobox", { name: "Science filter" }).click();
    await page
      .getByRole("option", { name: "GRB+GW gold/silver" })
      .click();

    // Filtered: junk row gone, gold tier chip shown. exact:true so
    // the chip doesn't collide with the filter name now shown in the
    // selector ("GRB+GW gold/silver").
    await expect(page.getByText("junk1")).toHaveCount(0);
    await expect(page.getByText("bn250101000")).toBeVisible();
    await expect(page.getByText("gold", { exact: true })).toBeVisible();
  });
});
