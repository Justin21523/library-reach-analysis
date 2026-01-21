function el(id) {
  return document.getElementById(id);
}

function escapeHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
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

function setText(id, text) {
  const node = el(id);
  if (!node) return;
  node.textContent = text;
}

function slugify(s) {
  return String(s || "")
    .toLowerCase()
    .replaceAll(/[^a-z0-9]+/g, "-")
    .replaceAll(/(^-|-$)/g, "")
    .slice(0, 80);
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

async function buildBriefSharePackZip({ preset, statusText }) {
  if (!window.JSZip) throw new Error("JSZip not loaded");
  const zip = new window.JSZip();
  const meta = {
    generated_at: new Date().toISOString(),
    url: window.location.href,
    preset: preset || null,
  };
  zip.file("meta.json", JSON.stringify(meta, null, 2));
  zip.file("status.txt", String(statusText || "").trim() || "—");

  const assumptions = el("assumptions");
  if (assumptions) zip.file("assumptions.html", assumptions.innerHTML || assumptions.textContent || "");

  const impact = el("impactSummary");
  if (impact) zip.file("impact.html", impact.innerHTML || impact.textContent || "");

  try {
    const health = await (window.lrApi?.getHealth ? window.lrApi.getHealth() : fetchJson("/health"));
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

  const out = await zip.generateAsync({ type: "blob" });
  const stamp = new Date().toISOString().slice(0, 10);
  const sc = preset?.scenario ? slugify(preset.scenario) : "baseline";
  downloadBlob(out, `libraryreach-briefpack-${stamp}-${sc}.zip`);
}

function fmt(n, digits = 0) {
  const v = Number(n);
  if (!Number.isFinite(v)) return "—";
  return v.toFixed(digits);
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

function describeAssumptions(preset) {
  if (!preset) return "未指定 preset（顯示 pipeline baseline）。";
  const scenario = preset.scenario || "weekday";
  const cities = Array.isArray(preset.cities) ? preset.cities : [];
  const patch = preset.patch || {};
  const threshold = patch?.planning?.deserts?.threshold_score;
  const grid = patch?.spatial?.grid?.cell_size_m;
  const radius = patch?.planning?.outreach?.coverage_radius_m;
  const topN = patch?.planning?.outreach?.top_n_per_city;

  const row = (k, v, hint = "") =>
    `<div class="assumption-k">${k}</div><div>${v}${hint ? `<div class="muted">${hint}</div>` : ""}</div>`;

  return `
    <div class="assumption-card">
      <div class="assumption-title">政策假設（可分享）</div>
      <div class="assumption-grid">
        ${row("Scenario", escapeHtml(scenario), "weekday / weekend / after_school")}
        ${row("Cities", cities.length ? escapeHtml(cities.join(", ")) : "—", "選擇分析範圍")}
        ${row(
          "Desert threshold",
          threshold === undefined ? "—" : `${threshold} / 100`,
          "低於門檻的格網會被標示為 deserts（越高越嚴格）"
        )}
        ${row("Grid size", grid === undefined ? "—" : `${grid} m`, "格網越小越細緻，但計算成本較高")}
        ${row("Outreach radius", radius === undefined ? "—" : `${radius} m`, "外展點位的覆蓋半徑")}
        ${row("Top N / city", topN === undefined ? "—" : String(topN), "每個城市推薦點位數量")}
      </div>
    </div>
  `;
}

function computeStats({ libsGeojson, desertsGeojson, outreachRows, cities }) {
  const set = new Set((cities || []).map(String));
  const libs = (libsGeojson?.features || [])
    .map((f) => f?.properties || {})
    .filter((r) => (set.size ? set.has(String(r.city)) : true));
  const scores = libs.map((r) => Number(r.accessibility_score)).filter(Number.isFinite);
  const avg = mean(scores);

  const deserts = (desertsGeojson?.features || []).filter((f) =>
    set.size ? set.has(String(f?.properties?.city)) : true
  );
  const desertCount = deserts.filter((f) => Boolean(f?.properties?.is_desert)).length;

  const outreach = (outreachRows || []).filter((r) => (set.size ? set.has(String(r.city)) : true));
  return { avgScore: avg, deserts: desertCount, libraries: libs.length, outreach: outreach.length, outreachRows: outreach };
}

function deltaText(after, before, { digits = 1, goodWhenNegative = false } = {}) {
  const a = Number(after);
  const b = Number(before);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return "";
  const d = a - b;
  if (d === 0) return "";
  const sign = d > 0 ? "+" : "";
  const good = goodWhenNegative ? d < 0 : d > 0;
  const cls = good ? "pos" : "neg";
  return ` <span class="delta ${cls}">${sign}${fmt(d, digits)}</span>`;
}

function renderTopOutreach(rows) {
  const container = el("topOutreach");
  if (!container) return;
  if (!rows || rows.length === 0) {
    container.textContent = "尚無外展建議（請先跑 pipeline 或在控制台執行 what-if）。";
    return;
  }
  const top = rows.slice(0, 5);
  const items = top
    .map((r, i) => {
      const name = r.name ?? r.id ?? `Site ${i + 1}`;
      const city = r.city ?? "-";
      const district = r.district ?? "-";
      const score = r.outreach_score;
      const why = r.recommendation_explain ?? "";
      return `
        <div class="brief-item">
          <div class="brief-item-title">
            <div>${i + 1}. ${name}</div>
            <div class="pill">${city} · ${district}</div>
          </div>
          <div class="brief-item-body">
            <div><b>OutreachScore</b> ${fmt(score, 1)}</div>
            ${why ? `<div class="muted">${why}</div>` : ""}
          </div>
        </div>
      `;
    })
    .join("");
  container.innerHTML = `<div class="brief-list-grid">${items}</div>`;
}

async function loadBriefData() {
  const status = el("briefStatus");
  try {
    const now = new Date();
    setText("generatedAt", now.toISOString().slice(0, 10));

    const preset = lrReadPresetFromUrl();
    if (preset) {
      const link = el("openConsoleFromBrief");
      if (link) link.href = lrBuildLink("/console", preset);
      const stickyLink = el("stickyConsoleFromBrief");
      if (stickyLink) stickyLink.href = lrBuildLink("/console", preset);
    }
    const assumptions = el("assumptions");
    if (assumptions) {
      if (!preset) assumptions.textContent = describeAssumptions(null);
      else assumptions.innerHTML = describeAssumptions(preset);
    }

    const [health, cfg] = await Promise.all([
      window.lrApi?.getHealth ? window.lrApi.getHealth() : fetchJson("/health"),
      fetchJson("/control/config"),
    ]);
    const scenario = preset?.scenario || cfg?.meta?.scenario || "weekday";
    setText("scenarioName", scenario);

    const files = health?.files || {};
    const ok =
      Boolean(files.libraries_scored) &&
      Boolean(files.deserts_csv || files.deserts_geojson) &&
      Boolean(files.outreach_recommendations);

    if (!ok) {
      setText("bScore", "—");
      setText("bDeserts", "—");
      setText("bLibraries", "—");
      setText("bOutreach", "—");
      if (status) {
        status.textContent = "目前未偵測到完整 pipeline 輸出；你仍可列印這份模板，或先到控制台查看缺少的檔案。";
      }
      renderTopOutreach([]);
      return;
    }

    // Baseline-only or preset compare.
    let comparePayload = null;
    let impactHtml =
      "目前顯示 pipeline baseline。若用控制台 Copy Link 分享，這裡會自動顯示該組假設的 what-if 與影響摘要。";

    if (preset) {
      try {
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
        const m = compare?.whatif?.metrics || {};
        setText("bScore", m.avg_accessibility_score === null || m.avg_accessibility_score === undefined ? "—" : fmt(m.avg_accessibility_score, 1));
        setText("bDeserts", fmt(m.deserts_count, 0));
        setText("bLibraries", fmt(m.libraries_count, 0));
        setText("bOutreach", fmt(m.outreach_count, 0));

        impactHtml = lrRenderNarrative(compare, { audience: lrGetAudience(), includeAssumptions: false, includeNextSteps: true });

        if (status) status.textContent = "已載入 preset 與 what-if 結果。可列印/存成 PDF。";
        renderTopOutreach(compare?.whatif?.outreach_top || []);
      } catch (e) {
        console.warn(e);
        if (status) status.textContent = "已提供 preset，但 compare 計算失敗（請確認輸入檔案齊全）。";
        renderTopOutreach([]);
        impactHtml = "已提供 preset，但 compare 計算失敗（請確認輸入檔案齊全）。";
      }
    } else {
      try {
        const base = await (window.lrApi?.getBaselineSummary
          ? window.lrApi.getBaselineSummary({ scenario, cities: cfg?.aoi?.cities || [] })
          : (async () => {
              const qs = new URLSearchParams();
              qs.set("scenario", scenario);
              for (const c of cfg?.aoi?.cities || []) qs.append("cities", c);
              return fetchJson(`/analysis/baseline-summary?${qs.toString()}`);
            })());
        const m = base?.summary?.metrics || {};
        setText("bScore", m.avg_accessibility_score === null || m.avg_accessibility_score === undefined ? "—" : fmt(m.avg_accessibility_score, 1));
        setText("bDeserts", fmt(m.deserts_count, 0));
        setText("bLibraries", fmt(m.libraries_count, 0));
        setText("bOutreach", fmt(m.outreach_count, 0));
        if (status) status.textContent = "已載入 pipeline baseline。";
        renderTopOutreach(base?.summary?.outreach_top || []);
      } catch (e) {
        console.warn(e);
        if (status) status.textContent = "無法載入 baseline 摘要。";
        renderTopOutreach([]);
      }
    }

    const impact = el("impactSummary");
    if (impact) impact.innerHTML = impactHtml;

    try {
      if (window.lrRenderSourcesCard) {
        window.lrRenderSourcesCard("sourcesCardBrief", { title: "資料來源（本次輸出依據）", health });
      }
    } catch (e) {
      console.warn(e);
    }

    lrInitAudienceTabs("briefAudience", {
      onChange: () => {
        if (!preset || !comparePayload) return;
        const node = el("impactSummary");
        if (!node) return;
        node.innerHTML = lrRenderNarrative(comparePayload, { audience: lrGetAudience(), includeAssumptions: false, includeNextSteps: true });
      },
    });
  } catch (e) {
    if (status) status.textContent = "無法載入資料（API 尚未啟動或輸出檔案不可用）。";
    console.warn(e);
  }
}

function main() {
  initTheme();
  el("printBtn")?.addEventListener("click", () => window.print());
  el("stickyPrintBtn")?.addEventListener("click", () => window.print());
  el("downloadBriefPack")?.addEventListener("click", async () => {
    const btn = el("downloadBriefPack");
    if (!btn) return;
    btn.disabled = true;
    const old = btn.textContent;
    btn.textContent = "打包中…";
    try {
      const preset = lrReadPresetFromUrl();
      await buildBriefSharePackZip({ preset, statusText: el("briefStatus")?.textContent || "" });
      btn.textContent = "完成";
      window.setTimeout(() => (btn.textContent = old), 1200);
    } catch (e) {
      console.warn(e);
      btn.textContent = "失敗";
      window.setTimeout(() => (btn.textContent = old), 1500);
    } finally {
      btn.disabled = false;
    }
  });
  loadBriefData();
}

main();
