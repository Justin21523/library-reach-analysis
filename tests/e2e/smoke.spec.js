const { test, expect } = require("@playwright/test");

test.describe("LibraryReach pages (fixture mode)", () => {
  test.beforeEach(async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.setItem("lr_theme", "light");
      localStorage.setItem("lr_density", "comfort");
      localStorage.setItem("lr_audience", "public");
    });
  });

  test("home loads and shows hero + sources card", async ({ page }) => {
    await page.goto("/");
    await expect(page.getByRole("heading", { level: 1 })).toContainText("到得了");
    await expect(page.locator("#sourcesCardHome .sources-card")).toBeVisible();
  });

  test("method loads", async ({ page }) => {
    await page.goto("/method");
    await expect(page.getByRole("heading", { level: 1 })).toContainText("可解釋");
    await expect(page.locator("#sourcesCardMethod .sources-card")).toBeVisible();
  });

  test("results loads and renders charts", async ({ page }) => {
    await page.goto("/results");
    await expect(page.getByRole("heading", { level: 1 })).toContainText("三張圖");
    await expect(page.locator("#chartScore svg")).toBeVisible();
    await expect(page.locator("#chartDeserts svg")).toBeVisible();
    await expect(page.locator("#chartOutreach svg")).toBeVisible();
  });

  test("brief loads", async ({ page }) => {
    await page.goto("/brief");
    await expect(page.getByRole("heading", { level: 1 })).toContainText("公共溝通摘要");
    await expect(page.locator("#sourcesCardBrief .sources-card")).toBeVisible();
  });

  test("console loads in e2e mode and shows legend + action drawer", async ({ page }) => {
    await page.goto("/console?e2e=1");
    await expect(page.locator("#legend")).toBeVisible();
    await expect(page.locator("#actionDrawer")).toBeVisible();
    await expect(page.locator("#cities")).toBeVisible();
  });
});
