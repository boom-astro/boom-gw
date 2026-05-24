// Playwright E2E config. The tests run against a Vite dev server
// that's auto-started by Playwright (no separate gw-api boot, no
// docker, no real auth) — every API request is intercepted and
// mocked at the network layer so the suite is hermetic and fast.
// One-off interactive smoke against a real running stack is still
// a worthwhile manual step (see web/scripts/smoke.sh + the SPA in
// a browser), but is intentionally out of scope here.

import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  workers: process.env.CI ? 2 : undefined,
  reporter: process.env.CI ? "github" : "list",
  use: {
    baseURL: "http://127.0.0.1:5173",
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  webServer: {
    command: "npm run dev -- --host 127.0.0.1 --port 5173 --strictPort",
    url: "http://127.0.0.1:5173",
    timeout: 60_000,
    reuseExistingServer: !process.env.CI,
    stdout: "pipe",
    stderr: "pipe",
  },
});
