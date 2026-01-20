function el(id) {
  return document.getElementById(id);
}

function applyTheme(theme) {
  const t = theme === "dark" ? "dark" : "light";
  document.documentElement.setAttribute("data-theme", t);
  const btn = el("themeToggle");
  if (btn) {
    btn.textContent = t === "dark" ? "Dark" : "Light";
    btn.setAttribute("aria-pressed", t === "dark" ? "true" : "false");
    btn.title = t === "dark" ? "Switch to light theme" : "Switch to dark theme";
  }
}

function initTheme() {
  const stored = localStorage.getItem("lr_theme");
  const theme = stored === "dark" ? "dark" : "light";
  applyTheme(theme);
  const btn = el("themeToggle");
  if (btn) {
    btn.addEventListener("click", () => {
      const cur = document.documentElement.getAttribute("data-theme") === "dark" ? "dark" : "light";
      const next = cur === "dark" ? "light" : "dark";
      localStorage.setItem("lr_theme", next);
      applyTheme(next);
    });
  }
}

async function main() {
  initTheme();
  try {
    const health = await window.lrApi.getHealth();
    if (window.lrRenderSourcesCard) window.lrRenderSourcesCard("sourcesCardMethod", { title: "資料來源（本次輸出依據）", health });
  } catch (e) {
    console.warn(e);
  }
}

main();

