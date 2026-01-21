const fs = require("node:fs");
const path = require("node:path");
const { test, expect } = require("@playwright/test");
const { waitForConsoleMapDisplayed } = require("./e2e_utils");

const OUT_DIR = path.join(process.cwd(), "docs", "screenshots");

function ensureOutDir() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
}

async function prep(page, { audience = "public" } = {}) {
  await page.addInitScript((a) => {
    localStorage.setItem("lr_theme", "light");
    localStorage.setItem("lr_density", "comfort");
    localStorage.setItem("lr_audience", a);
    localStorage.setItem("lr_foldout_sources", "open");
    localStorage.setItem("lr_foldout_controls", "open");
  }, audience);
}

async function shot(page, name) {
  ensureOutDir();
  await page.screenshot({ path: path.join(OUT_DIR, name), fullPage: true });
}

test.describe("Showcase screenshots @screenshots", () => {
  test("desktop: home", async ({ page }) => {
    await prep(page, { audience: "public" });
    await page.goto("/");
    await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
    await expect(page.locator("#sourcesCardHome .sources-card")).toBeVisible();
    await shot(page, "home.png");
  });

  test("desktop: results", async ({ page }) => {
    await prep(page, { audience: "policy" });
    await page.goto("/results");
    await expect(page.locator("#chartScore svg")).toBeVisible();
    await expect(page.locator("#chartDeserts svg")).toBeVisible();
    await expect(page.locator("#chartOutreach svg")).toBeVisible();
    await shot(page, "results.png");
  });

  test("desktop: brief", async ({ page }) => {
    await prep(page, { audience: "public" });
    await page.goto("/brief");
    await expect(page.locator("#sourcesCardBrief .sources-card")).toBeVisible();
    await shot(page, "brief.png");
  });

  test("desktop: method", async ({ page }) => {
    await prep(page, { audience: "public" });
    await page.goto("/method");
    await expect(page.locator("#sourcesCardMethod .sources-card")).toBeVisible();
    await shot(page, "method.png");
  });

  test("desktop: console (e2e map style)", async ({ page }) => {
    await prep(page, { audience: "library" });
    await page.goto("/console?e2e=1");
    await expect(page.locator("#legend")).toBeVisible();
    await waitForConsoleMapDisplayed(page);
    const sourcesFoldout = page.locator('.foldout[data-foldout="sources"]');
    if (await sourcesFoldout.evaluate((n) => n.classList.contains("collapsed")).catch(() => false)) {
      await page.getByRole("button", { name: "Data & Provenance" }).click();
    }
    await expect(page.locator("#sourcesCardConsole .sources-card")).toBeVisible();
    await page.locator('.foldout[data-foldout="highlights"] .foldout-header').click();
    await shot(page, "console.png");
  });

  test.describe("mobile", () => {
    test.use({ viewport: { width: 390, height: 844 } });

    test("mobile: console bottom sheet", async ({ page }) => {
      await prep(page, { audience: "library" });
      await page.goto("/console?e2e=1");
      await expect(page.locator("#sheetHandle")).toBeVisible();
      await expect(page.locator("#legend")).toBeVisible();
      await waitForConsoleMapDisplayed(page);
      await shot(page, "console-mobile.png");
    });
  });
});
