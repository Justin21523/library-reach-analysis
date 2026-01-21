const { expect } = require("@playwright/test");

async function waitForConsoleMapDisplayed(page, { timeoutMs = 30_000 } = {}) {
  const map = page.locator("#map");
  const canvas = page.locator('#map canvas.maplibregl-canvas, #map canvas');

  await expect(map).toBeVisible({ timeout: timeoutMs });
  await expect(canvas.first()).toBeVisible({ timeout: timeoutMs });

  await page.waitForFunction(
    () => {
      const canvas = document.querySelector("#map canvas");
      if (!canvas) return false;
      const rect = canvas.getBoundingClientRect();
      if (!(rect.width > 0 && rect.height > 0)) return false;

      const flag = document.documentElement?.dataset?.e2eMapRendered;
      return flag ? flag === "1" : true;
    },
    null,
    { timeout: timeoutMs }
  );
}

module.exports = { waitForConsoleMapDisplayed };
