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

async function fetchJson(path, options = {}) {
  if (window.lrApi?.fetchJson) return window.lrApi.fetchJson(path, options);
  const res = await fetch(path, options);
  if (!res.ok) throw new Error(`${path} -> ${res.status}`);
  return res.json();
}

function fmt(n, digits = 1) {
  const v = Number(n);
  if (!Number.isFinite(v)) return "—";
  return v.toFixed(digits);
}

function escapeHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function mean(values) {
  let sum = 0;
  let n = 0;
  for (const v of values || []) {
    const x = Number(v);
    if (!Number.isFinite(x)) continue;
    sum += x;
    n += 1;
  }
  return n > 0 ? sum / n : null;
}

function filterCities(set, city) {
  return set.size ? set.has(String(city)) : true;
}

function toBuckets(summary) {
  const b = summary?.metrics?.score_buckets || {};
  return { low: Number(b.low || 0), mid: Number(b.mid || 0), high: Number(b.high || 0) };
}

function cssVar(name, fallback = "") {
  try {
    const v = getComputedStyle(document.documentElement).getPropertyValue(name);
    const s = String(v || "").trim();
    return s || fallback;
  } catch {
    return fallback;
  }
}

function slugify(s) {
  return String(s || "")
    .toLowerCase()
    .replaceAll(/[^a-z0-9]+/g, "-")
    .replaceAll(/(^-|-$)/g, "")
    .slice(0, 80);
}

function renderAssumptions(preset) {
  const node = el("resultsAssumptions");
  if (!node) return;
  if (!preset) {
    node.textContent = "未指定 preset（顯示 pipeline baseline）。";
    return;
  }
  const scenario = preset.scenario || "weekday";
  const cities = Array.isArray(preset.cities) ? preset.cities : [];
  const patch = preset.patch || {};
  const threshold = patch?.planning?.deserts?.threshold_score;
  const grid = patch?.spatial?.grid?.cell_size_m;
  const radius = patch?.planning?.outreach?.coverage_radius_m;
  const topN = patch?.planning?.outreach?.top_n_per_city;

  node.innerHTML = `
    <div class="assumption-card">
      <div class="assumption-title">政策假設（從 URL 讀取）</div>
      <div class="assumption-grid">
        <div class="assumption-k">Scenario</div><div>${escapeHtml(scenario)}</div>
        <div class="assumption-k">Cities</div><div>${cities.length ? escapeHtml(cities.join(", ")) : "—"}</div>
        <div class="assumption-k">Desert threshold</div><div>${threshold === undefined ? "—" : `${fmt(threshold, 0)} / 100`}</div>
        <div class="assumption-k">Grid size</div><div>${grid === undefined ? "—" : `${fmt(grid, 0)} m`}</div>
        <div class="assumption-k">Outreach radius</div><div>${radius === undefined ? "—" : `${fmt(radius, 0)} m`}</div>
        <div class="assumption-k">Top N / city</div><div>${topN === undefined ? "—" : fmt(topN, 0)}</div>
      </div>
      <div class="assumption-note muted">
        提示：這些參數會影響 deserts 判定與外展建議排序；可用控制台調整並用 Copy Link 分享。
      </div>
    </div>
  `;
}

function svgBarChart({ labels, values, colors, height = 140 }) {
  const w = 640;
  const h = height;
  const pad = 18;
  const max = Math.max(1, ...values.map((v) => Number(v) || 0));
  const barW = (w - pad * 2) / values.length;
  const bars = values
    .map((v, i) => {
      const val = Number(v) || 0;
      const bh = Math.round(((h - pad * 2 - 24) * val) / max);
      const x = pad + i * barW + 10;
      const y = h - pad - 18 - bh;
      const bw = Math.max(18, barW - 20);
      const fill = colors?.[i] || cssVar("--chart-bar", "rgba(31,90,166,0.35)");
      const label = labels[i] || "";
      return `
        <rect x="${x}" y="${y}" width="${bw}" height="${bh}" rx="10" fill="${fill}"></rect>
        <text x="${x + bw / 2}" y="${h - pad}" text-anchor="middle" font-size="12" fill="currentColor">${label}</text>
        <text x="${x + bw / 2}" y="${y - 6}" text-anchor="middle" font-size="12" fill="currentColor">${val}</text>
      `;
    })
    .join("");
  return `<svg class="chart-svg" viewBox="0 0 ${w} ${h}" width="100%" height="${h}" role="img">${bars}</svg>`;
}

function svgHorizontalBars(rows, { height = 220, valueKey = "count", barColor = null } = {}) {
  const w = 860;
  const rowH = 22;
  const pad = 18;
  const maxRows = Math.min(rows.length, Math.floor((height - pad * 2) / rowH));
  const data = rows.slice(0, maxRows);
  const max = Math.max(1, ...data.map((r) => Number(r[valueKey]) || 0));
  const fill = barColor || cssVar("--chart-bar", "rgba(31,90,166,0.35)");
  const items = data
    .map((r, i) => {
      const v = Number(r[valueKey]) || 0;
      const barW = Math.round(((w - 260) * v) / max);
      const y = pad + i * rowH;
      return `
        <text x="${pad}" y="${y + 14}" font-size="12" fill="currentColor">${escapeHtml(r.label)}</text>
        <rect x="240" y="${y + 4}" width="${barW}" height="12" rx="8" fill="${fill}"></rect>
        <text x="${240 + barW + 8}" y="${y + 14}" font-size="12" fill="currentColor">${v}</text>
      `;
    })
    .join("");
  return `<svg class="chart-svg" viewBox="0 0 ${w} ${height}" width="100%" height="${height}" role="img">${items}</svg>`;
}

function svgWithTheme(svgMarkup) {
  // Apply default text color via currentColor (theme-aware via CSS variable).
  return svgMarkup.replace("<svg ", `<svg style="color:var(--chart-text);" `);
}

function renderResultsFromSummary({ summary, note }) {
  const m = summary?.metrics || {};
  const buckets = toBuckets(summary);
  const desertsCity = (summary?.deserts_by_city || []).slice(0, 8).map((r) => ({
    label: r.city,
    count: Number(r.desert_count || 0),
  }));
  const top = (summary?.outreach_top || []).slice(0, 10).map((r) => ({
    label: `${r.city ?? "-"} · ${r.name ?? r.id ?? "site"}`,
    count: Number(r.outreach_score || 0),
  }));

  const summaryEl = el("resultsSummary");
  if (summaryEl) {
    const yb = m.youbike_station_count;
    const ybText = yb === null || yb === undefined ? "—" : fmt(yb, 0);
    summaryEl.textContent =
      `${note} 平均可達性 ${m.avg_accessibility_score === null || m.avg_accessibility_score === undefined ? "—" : fmt(m.avg_accessibility_score, 1)}，` +
      `deserts ${fmt(m.deserts_count, 0)}，外展建議 ${fmt(m.outreach_count, 0)}。`;
    if (yb !== null && yb !== undefined) {
      summaryEl.textContent += ` YouBike 站點 ${ybText}。`;
    } else {
      summaryEl.textContent += ` YouBike 站點 ${ybText}（可在設定啟用）。`;
    }
  }

  const chart1 = el("chartScore");
  if (chart1) {
    chart1.innerHTML = svgWithTheme(
      svgBarChart({
        labels: ["Low", "Mid", "High"],
        values: [buckets.low, buckets.mid, buckets.high],
        colors: [
          cssVar("--map-score-low", "#d55e00"),
          cssVar("--map-score-mid", "#e69f00"),
          cssVar("--map-score-high", "#009e73"),
        ],
        height: 170,
      })
    );
  }

  const chart2 = el("chartDeserts");
  if (chart2) {
    if (desertsCity.length === 0) {
      chart2.textContent = "目前沒有 deserts 可視化資料。";
    } else {
      chart2.innerHTML = svgWithTheme(
        svgHorizontalBars(desertsCity, {
          height: 220,
          valueKey: "count",
          barColor: cssVar("--map-desert", "rgba(225,29,72,0.65)"),
        })
      );
    }
  }

  const chart3 = el("chartOutreach");
  if (chart3) {
    if (top.length === 0) {
      chart3.textContent = "目前沒有外展建議資料。";
    } else {
      chart3.innerHTML = svgWithTheme(
        svgHorizontalBars(top, { height: 260, valueKey: "count", barColor: cssVar("--map-outreach", "rgba(0,114,178,0.75)") })
      );
    }
  }
}

function serializeSvg(svgEl) {
  const clone = svgEl.cloneNode(true);
  clone.setAttribute("xmlns", "http://www.w3.org/2000/svg");
  const color = getComputedStyle(document.documentElement).getPropertyValue("--chart-text").trim() || "rgba(15,23,42,0.74)";
  clone.setAttribute("style", `color:${color};`);
  const css = `
    text{font-family: ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;}
  `;
  const style = document.createElementNS("http://www.w3.org/2000/svg", "style");
  style.textContent = css;
  clone.insertBefore(style, clone.firstChild);
  const s = new XMLSerializer().serializeToString(clone);
  return `<?xml version="1.0" encoding="UTF-8"?>${s}`;
}

async function svgToPngBlob(svgEl, { scale = 2 } = {}) {
  const svgText = serializeSvg(svgEl);
  const svgUrl = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svgText)}`;
  const img = new Image();
  img.decoding = "async";
  await new Promise((resolve, reject) => {
    img.onload = resolve;
    img.onerror = reject;
    img.src = svgUrl;
  });
  const width = Math.max(1, Math.floor((svgEl.viewBox?.baseVal?.width || img.width) * scale));
  const height = Math.max(1, Math.floor((svgEl.viewBox?.baseVal?.height || img.height) * scale));
  const canvas = document.createElement("canvas");
  canvas.width = width;
  canvas.height = height;
  const ctx = canvas.getContext("2d");
  ctx.fillStyle = document.documentElement.getAttribute("data-theme") === "dark" ? "#0b1220" : "#ffffff";
  ctx.fillRect(0, 0, width, height);
  ctx.drawImage(img, 0, 0, width, height);
  return await new Promise((resolve) => canvas.toBlob(resolve, "image/png"));
}

function downloadBlob(blob, filename) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function downloadChartPng(chartId) {
  const host = el(chartId);
  const svg = host?.querySelector("svg");
  if (!svg) throw new Error("Chart not ready");
  const blob = await svgToPngBlob(svg);
  if (!blob) throw new Error("PNG export not supported");
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `libraryreach-${chartId}.png`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function copyChart(chartId) {
  const host = el(chartId);
  const svg = host?.querySelector("svg");
  if (!svg) throw new Error("Chart not ready");
  const blob = await svgToPngBlob(svg);
  if (blob && window.ClipboardItem && navigator.clipboard?.write) {
    await navigator.clipboard.write([new ClipboardItem({ "image/png": blob })]);
    return "png";
  }
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(serializeSvg(svg));
    return "svg";
  }
  throw new Error("Clipboard not available");
}

function initChartTools() {
  for (const btn of document.querySelectorAll(".chart-action")) {
    btn.addEventListener("click", async () => {
      const action = btn.getAttribute("data-action");
      const chartId = btn.getAttribute("data-chart");
      if (!chartId) return;
      btn.disabled = true;
      const old = btn.textContent;
      btn.textContent = action === "download" ? "匯出中…" : "複製中…";
      try {
        if (action === "download") await downloadChartPng(chartId);
        else await copyChart(chartId);
        btn.textContent = "完成";
        window.setTimeout(() => (btn.textContent = old), 900);
      } catch (e) {
        console.warn(e);
        btn.textContent = "失敗";
        window.setTimeout(() => (btn.textContent = old), 1200);
      } finally {
        btn.disabled = false;
      }
    });
  }
}

async function buildSharePackZip({ preset, summaryText }) {
  if (!window.JSZip) throw new Error("JSZip not loaded");

  const zip = new window.JSZip();
  const meta = {
    generated_at: new Date().toISOString(),
    url: window.location.href,
    preset: preset || null,
  };
  zip.file("meta.json", JSON.stringify(meta, null, 2));
  zip.file("summary.txt", String(summaryText || "").trim() || "—");

  // assumptions + sources
  try {
    const health = await fetchJson("/health");
    zip.file("health.json", JSON.stringify(health, null, 2));
  } catch {
    // ignore
  }
  try {
    const sources = await (window.lrApi?.fetchJson ? window.lrApi.fetchJson("/sources") : fetchJson("/sources"));
    zip.file("sources_index.json", JSON.stringify(sources, null, 2));
  } catch {
    // ignore
  }

  // charts
  for (const id of ["chartScore", "chartDeserts", "chartOutreach"]) {
    const host = el(id);
    const svg = host?.querySelector("svg");
    if (!svg) continue;
    const blob = await svgToPngBlob(svg);
    if (!blob) continue;
    zip.file(`${id}.png`, blob);
  }

  const out = await zip.generateAsync({ type: "blob" });
  const stamp = new Date().toISOString().slice(0, 10);
  const sc = preset?.scenario ? slugify(preset.scenario) : "baseline";
  downloadBlob(out, `libraryreach-sharepack-${stamp}-${sc}.zip`);
}

async function main() {
  initTheme();
  initChartTools();

  const preset = lrReadPresetFromUrl();
  let comparePayload = null;
  const summaryEl = el("resultsSummary");
  if (preset) {
    const consoleLink = el("openConsoleFromResults");
    const briefLink = el("openBriefFromResults");
    const heroConsole = el("heroConsoleFromResults");
    const heroBrief = el("heroBriefFromResults");
    const stickyConsole = el("stickyConsoleFromResults");
    const stickyBrief = el("stickyBriefFromResults");
    if (consoleLink) consoleLink.href = lrBuildLink("/console", preset);
    if (briefLink) briefLink.href = lrBuildLink("/brief", preset);
    if (heroConsole) heroConsole.href = lrBuildLink("/console", preset);
    if (heroBrief) heroBrief.href = lrBuildLink("/brief", preset);
    if (stickyConsole) stickyConsole.href = lrBuildLink("/console", preset);
    if (stickyBrief) stickyBrief.href = lrBuildLink("/brief", preset);
    renderAssumptions(preset);
  } else {
    renderAssumptions(null);
  }

  const packBtn = el("downloadSharePack");
  if (packBtn) {
    packBtn.addEventListener("click", async () => {
      packBtn.disabled = true;
      const old = packBtn.textContent;
      packBtn.textContent = "打包中…";
      try {
        await buildSharePackZip({ preset, summaryText: summaryEl?.textContent || "" });
        packBtn.textContent = "完成";
        window.setTimeout(() => (packBtn.textContent = old), 1200);
      } catch (e) {
        console.warn(e);
        packBtn.textContent = "失敗";
        window.setTimeout(() => (packBtn.textContent = old), 1500);
      } finally {
        packBtn.disabled = false;
      }
    });
  }

  try {
    const health = await fetchJson("/health");
    try {
      if (window.lrRenderSourcesCard) {
        window.lrRenderSourcesCard("sourcesCardResults", { title: "資料來源（可追溯）", health });
      }
    } catch (e) {
      console.warn(e);
    }
    const files = health?.files || {};
    const ok =
      Boolean(files.libraries_scored) &&
      Boolean(files.deserts_csv || files.deserts_geojson) &&
      Boolean(files.outreach_recommendations);
    if (!ok) {
      renderResultsFromSummary({
        summary: { metrics: { score_buckets: { low: 0, mid: 0, high: 0 }, deserts_count: 0, outreach_count: 0, avg_accessibility_score: null } },
        note: "尚未偵測到完整 pipeline 輸出。",
      });
      return;
    }

    if (preset) {
      const compare = await (window.lrApi?.postCompare
        ? window.lrApi.postCompare({ scenario: preset.scenario, cities: preset.cities, configPatch: preset.patch || {} })
        : fetchJson("/analysis/compare", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              scenario: preset.scenario,
              cities: preset.cities,
              config_patch: preset.patch || {},
            }),
          }));
      comparePayload = compare;
      renderResultsFromSummary({ summary: compare?.whatif, note: "目前顯示 preset 的 what-if 結果。" });
      const narrative = el("resultsNarrative");
      if (narrative) {
        narrative.innerHTML = lrRenderNarrative(compare, { audience: lrGetAudience(), includeAssumptions: false, includeNextSteps: true });
      }
      return;
    }

    const cfg = await fetchJson("/control/config");
    const base = await (window.lrApi?.getBaselineSummary
      ? window.lrApi.getBaselineSummary({ scenario: cfg?.meta?.scenario || "weekday", cities: cfg?.aoi?.cities || [] })
      : (async () => {
          const qs = new URLSearchParams();
          qs.set("scenario", cfg?.meta?.scenario || "weekday");
          for (const c of cfg?.aoi?.cities || []) qs.append("cities", c);
          return fetchJson(`/analysis/baseline-summary?${qs.toString()}`);
        })());
    renderResultsFromSummary({ summary: base?.summary, note: "目前顯示 pipeline baseline。" });
  } catch (e) {
    console.warn(e);
    renderResultsFromSummary({
      summary: { metrics: { score_buckets: { low: 0, mid: 0, high: 0 }, deserts_count: 0, outreach_count: 0, avg_accessibility_score: null } },
      note: "無法載入成果資料（API 未啟動或路由不可用）。",
    });
  }

  lrInitAudienceTabs("resultsAudience", {
    onChange: () => {
      if (!comparePayload) return;
      const narrative = el("resultsNarrative");
      if (!narrative) return;
      narrative.innerHTML = lrRenderNarrative(comparePayload, { audience: lrGetAudience(), includeAssumptions: false, includeNextSteps: true });
    },
  });
}

main();
