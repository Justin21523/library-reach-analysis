(() => {
  function applyDensity(mode) {
    const m = mode === "compact" ? "compact" : "comfort";
    if (m === "compact") document.documentElement.setAttribute("data-density", "compact");
    else document.documentElement.removeAttribute("data-density");
    try {
      localStorage.setItem("lr_density", m);
    } catch {
      // ignore
    }
    const btn = document.getElementById("densityToggle");
    if (btn) {
      btn.textContent = m === "compact" ? "Compact" : "Comfort";
      btn.setAttribute("aria-pressed", m === "compact" ? "true" : "false");
      btn.title = m === "compact" ? "Switch to comfort spacing" : "Switch to compact spacing";
    }
  }

  function initDensity() {
    let stored = "comfort";
    try {
      stored = localStorage.getItem("lr_density") || "comfort";
    } catch {
      // ignore
    }
    applyDensity(stored);

    const btn = document.getElementById("densityToggle");
    if (btn) {
      btn.addEventListener("click", () => {
        const cur = document.documentElement.getAttribute("data-density") === "compact" ? "compact" : "comfort";
        const next = cur === "compact" ? "comfort" : "compact";
        applyDensity(next);
      });
    }
  }

  initDensity();
  window.lrApplyDensity = applyDensity;
})();

