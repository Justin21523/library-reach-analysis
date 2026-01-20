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

async function fetchJson(path) {
  if (window.lrApi?.fetchJson) return window.lrApi.fetchJson(path);
  const res = await fetch(path);
  if (!res.ok) throw new Error(`${path} -> ${res.status}`);
  return res.json();
}

function setText(id, text) {
  const node = el(id);
  if (!node) return;
  node.textContent = text;
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

function setActiveAudience(audience) {
  const a = lrSetAudience(audience);

  const map = {
    public: {
      blurb: "這是公共溝通版：用最少術語解釋「到得了」的差異，並提供可以採取的行動方向。",
      ctaPrimary: { text: "先看一頁簡報", href: "/brief" },
      ctaSecondary: { text: "或進控制台探索", href: "/console" },
      mk: { score: "平均可達性", deserts: "服務落差", outreach: "可行外展", cities: "城市覆蓋", youbike: "YouBike" },
      mh: { score: "0–100（越高越好）", deserts: "deserts（越少越好）", outreach: "候選點位清單", cities: "可切換城市", youbike: "站點數（可選圖層）" },
    },
    library: {
      blurb: "這是館務/第一線版：重點在於服務落差的分布、外展候選點位與可快速落地的合作方案。",
      ctaPrimary: { text: "進控制台做情境模擬", href: "/console" },
      ctaSecondary: { text: "需要溝通素材？一頁簡報", href: "/brief" },
      mk: { score: "館點可達性", deserts: "落差熱點", outreach: "外展清單", cities: "適用城市", youbike: "YouBike" },
      mh: { score: "用於找服務不便的館點", deserts: "用於規劃外展優先順序", outreach: "Top N 候選點位", cities: "可分城市檢視", youbike: "站點數（可選圖層）" },
    },
    policy: {
      blurb: "這是決策/跨局處版：重點在於公平性指標、落差的可視化，以及可分享、可追溯的政策假設與影響摘要。",
      ctaPrimary: { text: "成果頁（投影簡報）", href: "/results" },
      ctaSecondary: { text: "做 what-if 並分享連結", href: "/console" },
      mk: { score: "整體平均分數", deserts: "公平性落差", outreach: "補強方案", cities: "涵蓋範圍", youbike: "YouBike" },
      mh: { score: "用於溝通方向與資源配置", deserts: "可用於追蹤改善", outreach: "可啟動合作討論", cities: "可控管範圍", youbike: "站點數（交通補充）" },
    },
  }[a];

  for (const btn of document.querySelectorAll(".audience-tab")) {
    const isActive = btn.getAttribute("data-audience") === a;
    btn.classList.toggle("active", isActive);
    btn.setAttribute("aria-selected", isActive ? "true" : "false");
  }

  const blurbNode = el("audienceBlurb");
  if (blurbNode) blurbNode.textContent = map.blurb;

  const ctaPrimary = el("ctaPrimary");
  const ctaSecondary = el("ctaSecondary");
  if (ctaPrimary) {
    ctaPrimary.textContent = map.ctaPrimary.text;
    ctaPrimary.href = map.ctaPrimary.href;
  }
  if (ctaSecondary) {
    ctaSecondary.textContent = map.ctaSecondary.text;
    ctaSecondary.href = map.ctaSecondary.href;
  }

  setText("mkScore", map.mk.score);
  setText("mkDeserts", map.mk.deserts);
  setText("mkOutreach", map.mk.outreach);
  setText("mkCities", map.mk.cities);
  setText("mkYoubike", map.mk.youbike);
  setText("mhScore", map.mh.score);
  setText("mhDeserts", map.mh.deserts);
  setText("mhOutreach", map.mh.outreach);
  setText("mhCities", map.mh.cities);
  setText("mhYoubike", map.mh.youbike);
}

function initAudience() {
  const stored = localStorage.getItem("lr_audience");
  setActiveAudience(stored || "public");
  for (const btn of document.querySelectorAll(".audience-tab")) {
    btn.addEventListener("click", () => setActiveAudience(btn.getAttribute("data-audience")));
  }
}

function buildCheckboxGrid(containerId, values, selected = null, prefix = "x-") {
  const container = el(containerId);
  if (!container) return;
  container.innerHTML = "";
  const chosen = selected ? new Set(selected.map(String)) : null;
  for (const v of values || []) {
    const id = `${prefix}${v}`;
    const label = document.createElement("label");
    label.className = "check";
    const checked = chosen ? chosen.has(String(v)) : true;
    label.innerHTML = `<input type="checkbox" id="${escapeHtml(id)}" value="${escapeHtml(v)}" ${
      checked ? "checked" : ""
    } />${escapeHtml(v)}`;
    container.appendChild(label);
  }
}

function readCheckboxGroup(containerId) {
  const container = el(containerId);
  if (!container) return [];
  return Array.from(container.querySelectorAll("input[type=checkbox]"))
    .filter((c) => c.checked)
    .map((c) => c.value);
}

function filterByCities(items, cityAccessor, cities) {
  const set = new Set((cities || []).map(String));
  if (set.size === 0) return items || [];
  return (items || []).filter((x) => set.has(String(cityAccessor(x))));
}

function computeBaselineStats({ libsGeojson, desertsGeojson, outreachRows, cities }) {
  const libRows = (libsGeojson?.features || []).map((f) => f?.properties || {});
  const libs = filterByCities(libRows, (r) => r.city, cities);
  const scores = libs.map((r) => Number(r.accessibility_score)).filter(Number.isFinite);
  const avgScore = mean(scores);

  const desertFeatures = filterByCities(desertsGeojson?.features || [], (f) => f?.properties?.city, cities);
  const desertCount = desertFeatures.filter((f) => Boolean(f?.properties?.is_desert)).length;

  const outreach = filterByCities(outreachRows || [], (r) => r.city, cities);
  return { avgScore, desertCount, outreachCount: outreach.length, libraryCount: libs.length };
}

function fmtIso(iso) {
  if (!iso) return null;
  const d = new Date(String(iso));
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toLocaleString("zh-Hant", { hour12: false });
}

async function loadHomeMetrics() {
  const status = el("homeStatus");
  try {
    const [health, cfg] = await Promise.all([
      window.lrApi?.getHealth ? window.lrApi.getHealth() : fetchJson("/health"),
      fetchJson("/control/config"),
    ]);
    const cities = cfg?.aoi?.cities || [];
    setText("mCities", String(cities.length || "—"));

    const files = health?.files || {};
    const ok =
      Boolean(files.libraries_scored) &&
      Boolean(files.deserts_csv || files.deserts_geojson) &&
      Boolean(files.outreach_recommendations);

    if (!ok) {
      setText("mScore", "—");
      setText("mDeserts", "—");
      setText("mOutreach", "—");
      setText("mYoubike", "—");
      if (status) {
        status.textContent =
          "目前尚未偵測到完整 pipeline 輸出（libraries/deserts/outreach）。你仍可閱讀方法，或直接進入控制台查看缺少的檔案。";
      }
      return { cfg, health, baseline: null };
    }

    const qs = new URLSearchParams();
    qs.set("scenario", cfg?.meta?.scenario || "weekday");
    for (const c of cities) qs.append("cities", c);
    const baselineRes = await (window.lrApi?.getBaselineSummary
      ? window.lrApi.getBaselineSummary({ scenario: cfg?.meta?.scenario || "weekday", cities })
      : fetchJson(`/analysis/baseline-summary?${qs.toString()}`));
    const m = baselineRes?.summary?.metrics || {};
    setText("mScore", m.avg_accessibility_score === null || m.avg_accessibility_score === undefined ? "—" : fmt(m.avg_accessibility_score, 1));
    setText("mDeserts", fmt(m.deserts_count, 0));
    setText("mOutreach", fmt(m.outreach_count, 0));
    setText(
      "mYoubike",
      m.youbike_station_count === null || m.youbike_station_count === undefined ? "—" : fmt(m.youbike_station_count, 0)
    );
    if (status) {
      const ing = health?.ingestion_status || {};
      const rl = ing?.rate_limit || {};
      const state = ing?.state;
      const lastOk = fmtIso(ing?.fetch?.last_success_at || ing?.stops_meta?.generated_at);
      const nextRetry = fmtIso(ing?.fetch?.next_retry_at);
      const err = ing?.fetch?.last_error;
      const sm = health?.stops_meta || ing?.stops_meta || {};
      const counts = sm?.counts || {};
      const modes = [];
      if (Number(counts?.bus) > 0) modes.push("bus");
      if (Number(counts?.metro) > 0) modes.push("metro");
      const modeNote = modes.length ? `模式：${modes.join("+")}` : null;
      const http429 = Number(rl?.http_429_count);
      const extra =
        state === "rate_limited"
          ? `（目前被限流，下一次重試：${nextRetry || "—"}；429 次數：${Number.isFinite(http429) ? http429 : "—"}）`
        : state === "error"
            ? `（抓取異常：${String(err || "—")}）`
            : lastOk
              ? `（最近抓取：${lastOk}）`
              : "";
      status.textContent = `系統狀態：已載入 baseline 摘要，可直接進入控制台做情境模擬。${modeNote ? `（${modeNote}）` : ""}${extra}`;
    }

    try {
      if (window.lrRenderSourcesCard) {
        window.lrRenderSourcesCard("sourcesCardHome", { title: "資料來源（本次輸出依據）", health });
      }
    } catch (e) {
      console.warn(e);
    }

    return { cfg, health, baseline: baselineRes };
  } catch (e) {
    if (status) status.textContent = "無法載入系統狀態（API 尚未啟動或路由不可用）。";
    console.warn(e);
    return null;
  }
}

function initTabs() {
  const tabs = Array.from(document.querySelectorAll(".tabs .tab"));
  const hashTabs = tabs.filter((t) => (t.getAttribute("href") || "").startsWith("#"));
  const sections = hashTabs.map((t) => document.querySelector(t.getAttribute("href"))).filter(Boolean);
  const byId = new Map(sections.map((s) => [s.id, s]));

  function setActive(hash) {
    const id = (hash || "#overview").replace("#", "");
    for (const t of hashTabs) t.classList.toggle("active", t.getAttribute("href") === `#${id}`);
  }

  function onHashChange() {
    setActive(window.location.hash);
    const id = (window.location.hash || "#overview").replace("#", "");
    const section = byId.get(id);
    if (section) section.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  window.addEventListener("hashchange", onHashChange);
  setActive(window.location.hash);
}

function setWizardLinks({ scenario, cities, patch }) {
  const preset = lrBuildPreset({ scenario, cities, patch });
  const brief = el("wizBrief");
  const consoleLink = el("wizConsole");
  if (brief) brief.href = lrBuildLink("/brief", preset);
  if (consoleLink) consoleLink.href = lrBuildLink("/console", preset);
}

async function initWizard(boot) {
  const scenarioEl = el("wizScenario");
  const citiesEl = el("wizCities");
  const status = el("wizStatus");

  if (!scenarioEl || !citiesEl) return;

  const urlPreset = lrReadPresetFromUrl();
  const defaultScenario = urlPreset?.scenario || boot?.cfg?.meta?.scenario || "weekday";
  const defaultCities = urlPreset?.cities || boot?.cfg?.aoi?.cities || [];

  scenarioEl.innerHTML = "";
  for (const s of ["weekday", "weekend", "after_school"]) {
    const opt = document.createElement("option");
    opt.value = s;
    opt.textContent = s;
    if (s === defaultScenario) opt.selected = true;
    scenarioEl.appendChild(opt);
  }

  const allCities = boot?.cfg?.aoi?.cities || [];
  buildCheckboxGrid("wizCities", allCities, defaultCities, "wiz-city-");

  async function updateWizard() {
    const scenario = scenarioEl.value;
    const cities = readCheckboxGroup("wizCities");
    const preset = lrBuildPreset({ scenario, cities, patch: {} });
    setWizardLinks(preset);

    const saveBtn = el("wizSave");
    const copyBtn = el("wizCopy");
    if (saveBtn) saveBtn.onclick = () => saveWizardPreset(preset);
    if (copyBtn) copyBtn.onclick = () => copyWizardLink(preset);

    try {
      const qs = new URLSearchParams();
      qs.set("scenario", scenario);
      for (const c of cities) qs.append("cities", c);
      const res = await fetchJson(`/analysis/baseline-summary?${qs.toString()}`);
      const m = res?.summary?.metrics || {};
      setText("wizScore", m.avg_accessibility_score === null || m.avg_accessibility_score === undefined ? "—" : fmt(m.avg_accessibility_score, 1));
      setText("wizDeserts", fmt(m.deserts_count, 0));
      setText("wizOutreach", fmt(m.outreach_count, 0));
      if (status) status.textContent = `已選 ${cities.length} city(s) · ${fmt(m.libraries_count, 0)} libraries`;
    } catch (e) {
      console.warn(e);
      setText("wizScore", "—");
      setText("wizDeserts", "—");
      setText("wizOutreach", "—");
      if (status) status.textContent = "無法載入 baseline 摘要（可能尚未跑 pipeline 或 API 未啟動）。";
    }
  }

  scenarioEl.addEventListener("change", updateWizard);
  citiesEl.addEventListener("change", updateWizard);

  await updateWizard();

  // If the page is opened with a shared preset, show a narrative preview using backend narrative_blocks.
  if (urlPreset) {
    try {
      const compare = await fetchJson("/analysis/compare", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          scenario: urlPreset.scenario,
          cities: urlPreset.cities,
          config_patch: urlPreset.patch || {},
        }),
      });
      const box = el("wizNarrative");
      if (box) {
        box.style.display = "";
        box.innerHTML = lrRenderNarrative(compare, { audience: lrGetAudience(), includeAssumptions: true, includeNextSteps: false });
      }

      window.addEventListener("lr_audience_changed", () => {
        const node = el("wizNarrative");
        if (node) node.innerHTML = lrRenderNarrative(compare, { audience: lrGetAudience(), includeAssumptions: true, includeNextSteps: false });
      });
    } catch (e) {
      console.warn(e);
    }
  }
}

function saveWizardPreset(preset) {
  const status = el("wizStatus");
  try {
    localStorage.setItem("lr_preset_v1", JSON.stringify(preset));
    if (status) status.textContent = "已儲存：這組假設會在控制台自動載入（也可用 Copy Link 分享）。";
  } catch (e) {
    console.warn(e);
    if (status) status.textContent = "儲存失敗：瀏覽器可能禁止 localStorage。";
  }
}

async function copyWizardLink(preset) {
  const status = el("wizStatus");
  try {
    const link = `${window.location.pathname}?lr=${encodeURIComponent(lrEncodePreset(preset))}#start`;
    await navigator.clipboard.writeText(`${window.location.origin}${link}`);
    if (status) status.textContent = "已複製分享連結。";
  } catch (e) {
    console.warn(e);
    if (status) status.textContent = "複製失敗：請從網址列手動複製。";
  }
}

function main() {
  initTheme();
  initTabs();
  initAudience();
  loadHomeMetrics().then((boot) => initWizard(boot));
}

main();
