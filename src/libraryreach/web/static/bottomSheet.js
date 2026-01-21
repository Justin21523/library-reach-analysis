(function () {
  function getEl(id) {
    return document.getElementById(id);
  }

  function clamp(n, lo, hi) {
    return Math.max(lo, Math.min(hi, n));
  }

  function isMobileSheet() {
    return window.matchMedia && window.matchMedia("(max-width: 980px)").matches;
  }

  function applySheetMode(sidebar, mode) {
    const m = mode === "collapsed" || mode === "full" || mode === "mid" ? mode : "mid";
    sidebar.dataset.sheet = m;
    sidebar.style.removeProperty("--sheet-height");
    try {
      localStorage.setItem("lr_sheet", m);
    } catch {
      // ignore
    }
  }

  function readSavedMode() {
    try {
      return localStorage.getItem("lr_sheet") || "mid";
    } catch {
      return "mid";
    }
  }

  function initBottomSheet() {
    const sidebar = getEl("sidebar");
    const handle = getEl("sheetHandle");
    if (!sidebar || !handle) return;

    const sync = () => {
      if (!isMobileSheet()) {
        sidebar.style.removeProperty("--sheet-height");
        sidebar.removeAttribute("data-sheet");
        return;
      }
      if (!sidebar.dataset.sheet) applySheetMode(sidebar, readSavedMode());
    };
    sync();
    window.addEventListener("resize", sync);

    handle.addEventListener("click", () => {
      if (!isMobileSheet()) return;
      const cur = sidebar.dataset.sheet || "mid";
      const next = cur === "collapsed" ? "mid" : cur === "mid" ? "full" : "mid";
      applySheetMode(sidebar, next);
    });

    let dragging = false;
    let startY = 0;
    let startH = 0;
    let pointerId = null;

    const onMove = (e) => {
      if (!dragging) return;
      const y = e.clientY;
      if (typeof y !== "number") return;
      const dy = y - startY;
      const h = clamp(startH - dy, Math.floor(window.innerHeight * 0.18), Math.floor(window.innerHeight * 0.88));
      sidebar.style.setProperty("--sheet-height", `${h}px`);
    };

    const onUp = () => {
      if (!dragging) return;
      dragging = false;
      if (pointerId !== null) {
        try {
          handle.releasePointerCapture(pointerId);
        } catch {
          // ignore
        }
      }
      pointerId = null;

      const h = Number.parseFloat(getComputedStyle(sidebar).height || "0");
      const ratio = h / Math.max(1, window.innerHeight);
      const mode = ratio < 0.3 ? "collapsed" : ratio < 0.68 ? "mid" : "full";
      applySheetMode(sidebar, mode);
    };

    handle.addEventListener("pointerdown", (e) => {
      if (!isMobileSheet()) return;
      dragging = true;
      pointerId = e.pointerId ?? null;
      startY = e.clientY;
      startH = Number.parseFloat(getComputedStyle(sidebar).height || "0");
      try {
        handle.setPointerCapture(e.pointerId);
      } catch {
        // ignore
      }
    });
    handle.addEventListener("pointermove", onMove);
    handle.addEventListener("pointerup", onUp);
    handle.addEventListener("pointercancel", onUp);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initBottomSheet);
  } else {
    initBottomSheet();
  }
})();
