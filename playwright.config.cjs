// @ts-check
const { defineConfig } = require("@playwright/test");

const PORT = Number(process.env.E2E_PORT || 8123);
const BASE_URL = process.env.E2E_BASE_URL || `http://127.0.0.1:${PORT}`;

module.exports = defineConfig({
  testDir: "tests/e2e",
  timeout: 60_000,
  expect: { timeout: 10_000 },
  reporter: [["list"], ["html", { outputFolder: "reports/playwright-report", open: "never" }]],
  outputDir: "reports/playwright-results",
  use: {
    baseURL: BASE_URL,
    trace: "on-first-retry",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
    viewport: { width: 1280, height: 720 },
  },
  webServer: {
    command: `python -m uvicorn libraryreach.api.main:app --host 127.0.0.1 --port ${PORT}`,
    url: `${BASE_URL}/health`,
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
    env: {
      ...process.env,
      PYTHONPATH: [process.env.PYTHONPATH, require("node:path").join(process.cwd(), "src")].filter(Boolean).join(require("node:path").delimiter),
      LIBRARYREACH_E2E_FIXTURES: "1",
      LIBRARYREACH_SCENARIO: "weekday",
      PYTHONUNBUFFERED: "1"
    },
  },
});
