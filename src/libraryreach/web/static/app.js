const STYLE = {
  version: 8,
  sources: {
    osm: {
      type: "raster",
      tiles: [
        "https://a.tile.openstreetmap.org/{z}/{x}/{y}.png",
        "https://b.tile.openstreetmap.org/{z}/{x}/{y}.png",
        "https://c.tile.openstreetmap.org/{z}/{x}/{y}.png",
      ],
      tileSize: 256,
      attribution: "&copy; OpenStreetMap contributors",
    },
  },
  layers: [{ id: "osm", type: "raster", source: "osm" }],
};

const LAYER_REGISTRY = [
  {
    key: "libraries",
    label: "Libraries",
    checkboxId: "layerLibraries",
    sourceId: "libraries",
    layerIds: ["libraries"],
    eventLayerId: "libraries",
    renderOrder: 30,
    shortcut: "L",
    cursor: "pointer",
    addSource(map) {
      map.addSource("libraries", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    },
    addLayer(map) {
      map.addLayer({
        id: "libraries",
        type: "circle",
        source: "libraries",
        paint: {
          "circle-radius": 7,
          "circle-color": [
            "interpolate",
            ["linear"],
            ["coalesce", ["get", "accessibility_score"], 0],
            0,
            "#ef4444",
            50,
            "#f59e0b",
            100,
            "#22c55e",
          ],
          "circle-opacity": 0.72,
          "circle-stroke-width": 2,
          "circle-stroke-color": "rgba(15,23,42,0.9)",
        },
      });
    },
    tooltip({ props, fmt, escapeHtml }) {
      const name = props.name ?? props.id ?? "Library";
      const city = props.city ?? "-";
      const district = props.district ?? "-";
      const score = props.accessibility_score;
      return `
        <div class="popup-title">${escapeHtml(name)}</div>
        <div class="popup-subtitle">${escapeHtml(city)} · ${escapeHtml(district)}</div>
        <div class="popup-grid">
          <div class="k">Access score</div><div><b>${fmt(score, 1)}</b></div>
        </div>
      `;
    },
  },
  {
    key: "deserts",
    label: "Deserts",
    checkboxId: "layerDeserts",
    sourceId: "deserts",
    layerIds: ["deserts"],
    eventLayerId: "deserts",
    renderOrder: 10,
    shortcut: "D",
    cursor: "help",
    addSource(map) {
      map.addSource("deserts", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    },
    addLayer(map) {
      map.addLayer({
        id: "deserts",
        type: "circle",
        source: "deserts",
        filter: ["==", ["get", "is_desert"], true],
        paint: {
          "circle-radius": 3,
          "circle-color": "rgba(239,68,68,0.65)",
          "circle-opacity": 0.45,
        },
      });
    },
    tooltip({ props, fmt, escapeHtml }) {
      const city = props.city ?? "-";
      const eff = props.effective_score_0_100 ?? props.effective_score ?? null;
      const gap = props.gap_to_threshold ?? null;
      const dist = props.best_library_distance_m ?? null;
      return `
        <div class="popup-title">${escapeHtml(city)} · Desert cell</div>
        <div class="popup-grid">
          <div class="k">Effective score</div><div><b>${fmt(eff, 1)}</b></div>
          <div class="k">Gap</div><div>${fmt(gap, 1)}</div>
          <div class="k">Best distance</div><div>${fmt(dist, 0)} m</div>
        </div>
      `;
    },
  },
  {
    key: "outreach",
    label: "Outreach",
    checkboxId: "layerOutreach",
    sourceId: "outreach",
    layerIds: ["outreach"],
    eventLayerId: "outreach",
    renderOrder: 20,
    shortcut: "O",
    cursor: "pointer",
    addSource(map) {
      map.addSource("outreach", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    },
    addLayer(map) {
      map.addLayer({
        id: "outreach",
        type: "circle",
        source: "outreach",
        paint: {
          "circle-radius": 7,
          "circle-color": "rgba(96,165,250,0.75)",
          "circle-stroke-width": 1.5,
          "circle-stroke-color": "rgba(15,23,42,0.9)",
        },
      });
    },
    tooltip({ props, fmt, escapeHtml }) {
      const name = props.name ?? props.id ?? "Outreach site";
      const city = props.city ?? "-";
      const district = props.district ?? "-";
      const score = props.outreach_score;
      return `
        <div class="popup-title">${escapeHtml(name)}</div>
        <div class="popup-subtitle">${escapeHtml(city)} · ${escapeHtml(district)}</div>
        <div class="popup-grid">
          <div class="k">Outreach score</div><div><b>${fmt(score, 1)}</b></div>
        </div>
      `;
    },
  },
  {
    key: "metro",
    label: "Metro",
    checkboxId: "layerMetro",
    sourceId: "stops_metro",
    layerIds: ["stops-metro", "stops-metro-label"],
    eventLayerId: "stops-metro",
    renderOrder: 40,
    shortcut: "M",
    cursor: "pointer",
    addSource(map) {
      map.addSource("stops_metro", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    },
    addLayer(map) {
      map.addLayer({
        id: "stops-metro",
        type: "circle",
        source: "stops_metro",
        paint: {
          "circle-radius": 6,
          "circle-color": "rgba(167, 139, 250, 0.9)",
          "circle-opacity": 0.78,
          "circle-stroke-width": 2,
          "circle-stroke-color": "rgba(15,23,42,0.9)",
        },
      });
      map.addLayer({
        id: "stops-metro-label",
        type: "symbol",
        source: "stops_metro",
        layout: {
          "text-field": ["concat", "M ", ["slice", ["coalesce", ["get", "name"], ""], 0, 6]],
          "text-size": 11,
          "text-font": ["Open Sans Regular", "Arial Unicode MS Regular"],
          "text-offset": [0, 1.05],
          "text-anchor": "top",
          "text-allow-overlap": false,
          "text-ignore-placement": false,
        },
        paint: {
          "text-color": "rgba(15,23,42,0.92)",
          "text-halo-color": "rgba(255,255,255,0.92)",
          "text-halo-width": 1.2,
        },
      });
    },
    tooltip({ props, escapeHtml }) {
      const name = props.name ?? props.stop_id ?? "Metro station";
      const city = props.city ?? "-";
      const operator = props.source ?? "-";
      return `
        <div class="popup-title">${escapeHtml(name)}</div>
        <div class="popup-subtitle">${escapeHtml(city)}</div>
        <div class="popup-grid">
          <div class="k">Type</div><div>Metro</div>
          <div class="k">Operator</div><div>${escapeHtml(operator)}</div>
        </div>
      `;
    },
  },
  {
    key: "youbike",
    label: "YouBike",
    checkboxId: "layerYoubike",
    sourceId: "youbike",
    layerIds: ["youbike", "youbike-label"],
    eventLayerId: "youbike",
    renderOrder: 50,
    shortcut: "Y",
    cursor: "pointer",
    addSource(map) {
      map.addSource("youbike", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    },
    addLayer(map) {
      map.addLayer({
        id: "youbike",
        type: "circle",
        source: "youbike",
        paint: {
          "circle-radius": 5,
          "circle-color": "rgba(20, 184, 166, 0.9)",
          "circle-opacity": 0.7,
          "circle-stroke-width": 2,
          "circle-stroke-color": "rgba(15,23,42,0.9)",
        },
      });
      map.addLayer({
        id: "youbike-label",
        type: "symbol",
        source: "youbike",
        layout: {
          "text-field": ["concat", "Y ", ["slice", ["coalesce", ["get", "name"], ""], 0, 6]],
          "text-size": 11,
          "text-font": ["Open Sans Regular", "Arial Unicode MS Regular"],
          "text-offset": [0, 1.05],
          "text-anchor": "top",
          "text-allow-overlap": false,
          "text-ignore-placement": false,
        },
        paint: {
          "text-color": "rgba(15,23,42,0.92)",
          "text-halo-color": "rgba(255,255,255,0.92)",
          "text-halo-width": 1.2,
        },
      });
    },
    tooltip({ props, escapeHtml }) {
      const name = props.name ?? props.station_id ?? "YouBike station";
      const city = props.city ?? "-";
      return `
        <div class="popup-title">${escapeHtml(name)}</div>
        <div class="popup-subtitle">${escapeHtml(city)}</div>
        <div class="popup-grid">
          <div class="k">Type</div><div>YouBike</div>
          <div class="k">Status</div><div>Location only</div>
        </div>
      `;
    },
  },
  // Future layers can be added here (youbike, population, etc.).
];

const state = {
  map: null,
  mapReady: null,
  config: null,
  scenario: "weekday",
  lastResult: null,
  baselineStats: null,
  currentStats: null,
  hoverPopup: null,
  keysDown: new Set(),
  cameraLoop: null,
  layers: {
    libraries: true,
    deserts: true,
    outreach: true,
    metro: false,
    youbike: false,
  },
  baselineData: null,
  currentData: null,
  transit: {
    metro_geojson: null,
    youbike_geojson: null,
  },
};

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

  applyMapTheme(t);
}

function applyMapTheme(theme) {
  const map = state.map;
  if (!map) return;
  const dark = theme === "dark";
  const labelColor = dark ? "rgba(229,231,235,0.92)" : "rgba(15,23,42,0.92)";
  const haloColor = dark ? "rgba(15,23,42,0.92)" : "rgba(255,255,255,0.92)";
  for (const id of ["stops-metro-label", "youbike-label"]) {
    if (!map.getLayer(id)) continue;
    map.setPaintProperty(id, "text-color", labelColor);
    map.setPaintProperty(id, "text-halo-color", haloColor);
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
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${path} -> ${res.status} ${text}`);
  }
  return res.json();
}

function setStatus(text) {
  el("status").textContent = text;
}

function showNotice(message, kind = "info", { timeoutMs = 5000 } = {}) {
  const box = el("notice");
  const text = el("noticeText");
  if (!box || !text) return;
  box.classList.remove("hidden", "success", "error", "warning");
  if (kind === "success" || kind === "error" || kind === "warning") box.classList.add(kind);
  text.textContent = message;
  if (timeoutMs && timeoutMs > 0) {
    window.setTimeout(() => {
      if (text.textContent === message) box.classList.add("hidden");
    }, timeoutMs);
  }
}

function pushAction(message, kind = "info", { timeoutMs = 6500 } = {}) {
  const drawer = el("actionDrawer");
  if (!drawer) return;
  const item = document.createElement("div");
  item.className = `action-item ${kind === "success" || kind === "error" || kind === "warning" ? kind : ""}`.trim();
  item.innerHTML = `<span class="action-dot" aria-hidden="true"></span><div class="action-text">${escapeHtml(
    message
  )}</div>`;
  drawer.prepend(item);

  const max = 4;
  const items = Array.from(drawer.querySelectorAll(".action-item"));
  for (const old of items.slice(max)) old.remove();

  if (timeoutMs && timeoutMs > 0) {
    window.setTimeout(() => item.remove(), timeoutMs);
  }
}

function hideNotice() {
  const box = el("notice");
  if (!box) return;
  box.classList.add("hidden");
}

function fmt(n, digits = 1) {
  if (n === null || n === undefined) return "-";
  const v = Number(n);
  if (Number.isNaN(v)) return "-";
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

function computeLibraryStatsFromRows(rows) {
  const scores = (rows || []).map((r) => r?.accessibility_score);
  const avg = mean(scores);
  let low = 0;
  let mid = 0;
  let high = 0;
  for (const s of scores) {
    const v = Number(s);
    if (!Number.isFinite(v)) continue;
    if (v < 40) low += 1;
    else if (v < 70) mid += 1;
    else high += 1;
  }
  return { count: (rows || []).length, avg_score: avg, buckets: { low, mid, high } };
}

function computeLibraryStatsFromGeojson(geojson) {
  const features = geojson?.features || [];
  const rows = features.map((f) => f?.properties || {});
  return computeLibraryStatsFromRows(rows);
}

function computeDesertStatsFromGeojson(geojson) {
  const features = geojson?.features || [];
  const desert = features.filter((f) => Boolean(f?.properties?.is_desert));
  const effectiveScores = desert.map((f) => f?.properties?.effective_score_0_100);
  const gaps = desert.map((f) => f?.properties?.gap_to_threshold);
  return { desert_count: desert.length, avg_effective_score: mean(effectiveScores), avg_gap: mean(gaps) };
}

function computeOutreachStatsFromRows(rows) {
  const scores = (rows || []).map((r) => r?.outreach_score);
  const avg = mean(scores);
  let best = null;
  for (const s of scores) {
    const v = Number(s);
    if (!Number.isFinite(v)) continue;
    best = best === null ? v : Math.max(best, v);
  }
  return { count: (rows || []).length, avg_score: avg, best_score: best };
}

function formatDeltaPill(delta, { digits = 1, goodWhenNegative = false } = {}) {
  const d = Number(delta);
  if (!Number.isFinite(d) || d === 0) return "";
  const sign = d > 0 ? "+" : "";
  const good = goodWhenNegative ? d < 0 : d > 0;
  const cls = good ? "pos" : "neg";
  return `<span class="delta ${cls}">${sign}${fmt(d, digits)}</span>`;
}

function renderStory() {
  const container = el("story");
  if (!container) return;

  const { scenario, cities, patch } = readConfigPatchFromForm();
  const threshold = patch?.planning?.deserts?.threshold_score;
  const grid = patch?.spatial?.grid?.cell_size_m;
  const radius = patch?.planning?.outreach?.coverage_radius_m;
  const topN = patch?.planning?.outreach?.top_n_per_city;

  const baseline = state.baselineStats;
  const current = state.currentStats;

  if (!current) {
    container.innerHTML = `
      <div class="story-title">Setup snapshot</div>
      <div class="popup-subtitle">Scenario <b>${escapeHtml(scenario)}</b> · ${cities.length} city(s)</div>
      <div class="story-grid">
        <div class="story-kv"><div class="k">Desert threshold</div><div>${fmt(threshold, 0)}</div></div>
        <div class="story-kv"><div class="k">Grid size</div><div>${fmt(grid, 0)} m</div></div>
        <div class="story-kv"><div class="k">Outreach radius</div><div>${fmt(radius, 0)} m</div></div>
        <div class="story-kv"><div class="k">Top N / city</div><div>${fmt(topN, 0)}</div></div>
      </div>
    `;
    return;
  }

  const bLib = baseline?.libraries || null;
  const cLib = current?.libraries || null;
  const bDes = baseline?.deserts || null;
  const cDes = current?.deserts || null;
  const cOut = current?.outreach || null;

  const deltaAvgScore =
    bLib?.avg_score !== null && bLib?.avg_score !== undefined && cLib?.avg_score !== null && cLib?.avg_score !== undefined
      ? cLib.avg_score - bLib.avg_score
      : null;
  const deltaDeserts =
    bDes?.desert_count !== null &&
    bDes?.desert_count !== undefined &&
    cDes?.desert_count !== null &&
    cDes?.desert_count !== undefined
      ? cDes.desert_count - bDes.desert_count
      : null;

  container.innerHTML = `
    <div class="story-title">What-if story</div>
    <div class="popup-subtitle">Scenario <b>${escapeHtml(scenario)}</b> · ${cities.length} city(s) · Threshold ${fmt(
      threshold,
      0
    )} · Grid ${fmt(grid, 0)}m</div>
    <div class="story-grid">
      <div class="story-kv">
        <div class="k">Avg access score</div>
        <div><b>${fmt(cLib?.avg_score, 1)}</b>${deltaAvgScore === null ? "" : formatDeltaPill(deltaAvgScore, { digits: 1 })}</div>
      </div>
      <div class="story-kv">
        <div class="k">Access deserts</div>
        <div><b>${fmt(cDes?.desert_count, 0)}</b>${deltaDeserts === null ? "" : formatDeltaPill(deltaDeserts, { digits: 0, goodWhenNegative: true })}</div>
      </div>
      <div class="story-kv">
        <div class="k">Libraries analyzed</div>
        <div><b>${fmt(cLib?.count, 0)}</b></div>
      </div>
      <div class="story-kv">
        <div class="k">Outreach list</div>
        <div><b>${fmt(cOut?.count, 0)}</b> (best ${fmt(cOut?.best_score, 1)})</div>
      </div>
      <div class="story-kv">
        <div class="k">Score buckets</div>
        <div>${fmt(cLib?.buckets?.low, 0)} low · ${fmt(cLib?.buckets?.mid, 0)} mid · ${fmt(cLib?.buckets?.high, 0)} high</div>
      </div>
      <div class="story-kv">
        <div class="k">Baseline</div>
        <div>${baseline ? "Pipeline outputs" : "—"}</div>
      </div>
    </div>
  `;
}

function safeNowMs() {
  return typeof performance !== "undefined" && performance.now ? performance.now() : Date.now();
}

function normalizePair(a, b) {
  const x = Math.max(0, Number(a) || 0);
  const y = Math.max(0, Number(b) || 0);
  const s = x + y;
  if (s <= 0) return [0.5, 0.5];
  return [x / s, y / s];
}

function toUrlSafeBase64(bytes) {
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
}

function fromUrlSafeBase64(s) {
  const base64 = s.replaceAll("-", "+").replaceAll("_", "/");
  const pad = base64.length % 4 === 0 ? "" : "=".repeat(4 - (base64.length % 4));
  const binary = atob(base64 + pad);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}

function encodePresetToQuery(preset) {
  const bytes = new TextEncoder().encode(JSON.stringify(preset));
  return toUrlSafeBase64(bytes);
}

function decodePresetFromQuery(s) {
  const bytes = fromUrlSafeBase64(s);
  const json = new TextDecoder().decode(bytes);
  return JSON.parse(json);
}

function writePresetToUrl(preset) {
  const q = new URLSearchParams(window.location.search);
  q.set("lr", encodePresetToQuery(preset));
  const url = `${window.location.pathname}?${q.toString()}`;
  window.history.replaceState(null, "", url);
}

function readPresetFromUrl() {
  const q = new URLSearchParams(window.location.search);
  const raw = q.get("lr");
  if (!raw) return null;
  try {
    const p = decodePresetFromQuery(raw);
    if (!p || typeof p !== "object") return null;
    return p;
  } catch {
    return null;
  }
}

function readCheckboxGroup(containerId) {
  const container = el(containerId);
  return Array.from(container.querySelectorAll("input[type=checkbox]"))
    .filter((c) => c.checked)
    .map((c) => c.value);
}

function writeCheckboxGroup(containerId, values) {
  const set = new Set(values.map(String));
  const container = el(containerId);
  for (const input of container.querySelectorAll("input[type=checkbox]")) {
    input.checked = set.has(String(input.value));
  }
}

function buildCitiesUI(cities) {
  const container = el("cities");
  container.innerHTML = "";
  for (const city of cities) {
    const id = `city-${city}`;
    const label = document.createElement("label");
    label.className = "check";
    label.innerHTML = `<input type="checkbox" id="${id}" value="${city}" checked />${city}`;
    container.appendChild(label);
  }
}

function buildCandidateTypesUI(types) {
  const container = el("candidateTypes");
  container.innerHTML = "";
  for (const t of types) {
    const id = `type-${t}`;
    const label = document.createElement("label");
    label.className = "check";
    label.innerHTML = `<input type="checkbox" id="${id}" value="${t}" checked />${t}`;
    container.appendChild(label);
  }
}

function applyConfigToForm(config) {
  state.config = config;
  state.scenario = config?.meta?.scenario || "weekday";

  const scenarioEl = el("scenario");
  scenarioEl.innerHTML = "";
  for (const s of ["weekday", "weekend", "after_school"]) {
    const opt = document.createElement("option");
    opt.value = s;
    opt.textContent = s;
    if (s === state.scenario) opt.selected = true;
    scenarioEl.appendChild(opt);
  }

  const cities = config?.aoi?.cities || [];
  buildCitiesUI(cities);

  const allowedTypes = config?.planning?.outreach?.allowed_candidate_types || [];
  buildCandidateTypesUI(allowedTypes);

  const mw = config?.scoring?.mode_weights || { bus: 0.6, metro: 0.4 };
  const [busW, metroW] = normalizePair(mw.bus, mw.metro);
  el("wBus").value = fmt(busW, 2);
  el("wMetro").value = fmt(metroW, 2);

  const rw = config?.scoring?.radius_weights || { "500": 0.6, "1000": 0.4 };
  const [w500, w1000] = normalizePair(rw["500"], rw["1000"]);
  el("w500").value = fmt(w500, 2);
  el("w1000").value = fmt(w1000, 2);

  const targets = config?.scoring?.density_targets_per_km2 || {};
  el("tBus500").value = targets?.bus?.["500"] ?? 20;
  el("tBus1000").value = targets?.bus?.["1000"] ?? 10;
  el("tMetro500").value = targets?.metro?.["500"] ?? 2;
  el("tMetro1000").value = targets?.metro?.["1000"] ?? 1;

  el("desertThreshold").value = config?.planning?.deserts?.threshold_score ?? 30;
  el("gridSize").value = config?.spatial?.grid?.cell_size_m ?? 1000;
  el("desertSearchRadius").value = config?.planning?.deserts?.library_search_radius_m ?? 3000;

  el("outreachRadius").value = config?.planning?.outreach?.coverage_radius_m ?? 1000;
  el("outreachTopN").value = config?.planning?.outreach?.top_n_per_city ?? 10;

  const [wCov, wSite] = normalizePair(
    config?.planning?.outreach?.weight_coverage ?? 0.7,
    config?.planning?.outreach?.weight_site_access ?? 0.3
  );
  el("wCoverage").value = fmt(wCov, 2);
  el("wSite").value = fmt(wSite, 2);

  state.currentStats = null;
  renderStory();
}

function applyPresetToForm(p) {
  if (!p || typeof p !== "object") return false;
  if (p.scenario) el("scenario").value = p.scenario;
  if (Array.isArray(p.cities)) writeCheckboxGroup("cities", p.cities);

  if (p.patch?.scoring?.mode_weights) {
    el("wBus").value = fmt(p.patch.scoring.mode_weights.bus, 2);
    el("wMetro").value = fmt(p.patch.scoring.mode_weights.metro, 2);
  }
  if (p.patch?.scoring?.radius_weights) {
    el("w500").value = fmt(p.patch.scoring.radius_weights["500"], 2);
    el("w1000").value = fmt(p.patch.scoring.radius_weights["1000"], 2);
  }
  if (p.patch?.scoring?.density_targets_per_km2) {
    el("tBus500").value = p.patch.scoring.density_targets_per_km2.bus?.["500"] ?? el("tBus500").value;
    el("tBus1000").value = p.patch.scoring.density_targets_per_km2.bus?.["1000"] ?? el("tBus1000").value;
    el("tMetro500").value = p.patch.scoring.density_targets_per_km2.metro?.["500"] ?? el("tMetro500").value;
    el("tMetro1000").value = p.patch.scoring.density_targets_per_km2.metro?.["1000"] ?? el("tMetro1000").value;
  }
  if (p.patch?.planning?.deserts) {
    el("desertThreshold").value = p.patch.planning.deserts.threshold_score ?? el("desertThreshold").value;
    el("desertSearchRadius").value = p.patch.planning.deserts.library_search_radius_m ?? el("desertSearchRadius").value;
  }
  if (p.patch?.spatial?.grid) {
    el("gridSize").value = p.patch.spatial.grid.cell_size_m ?? el("gridSize").value;
  }
  if (p.patch?.planning?.outreach) {
    el("outreachRadius").value = p.patch.planning.outreach.coverage_radius_m ?? el("outreachRadius").value;
    el("outreachTopN").value = p.patch.planning.outreach.top_n_per_city ?? el("outreachTopN").value;
    el("wCoverage").value = fmt(p.patch.planning.outreach.weight_coverage, 2);
    el("wSite").value = fmt(p.patch.planning.outreach.weight_site_access, 2);
    if (Array.isArray(p.patch.planning.outreach.allowed_candidate_types)) {
      writeCheckboxGroup("candidateTypes", p.patch.planning.outreach.allowed_candidate_types);
    }
  }

  state.currentStats = null;
  renderStory();
  return true;
}

function readConfigPatchFromForm() {
  const scenario = el("scenario").value;
  const cities = readCheckboxGroup("cities");
  const types = readCheckboxGroup("candidateTypes");

  const [busW, metroW] = normalizePair(el("wBus").value, el("wMetro").value);
  const [w500, w1000] = normalizePair(el("w500").value, el("w1000").value);
  const [wCov, wSite] = normalizePair(el("wCoverage").value, el("wSite").value);

  const patch = {
    scoring: {
      mode_weights: { bus: busW, metro: metroW },
      radius_weights: { "500": w500, "1000": w1000 },
      density_targets_per_km2: {
        bus: {
          "500": Number(el("tBus500").value) || 0,
          "1000": Number(el("tBus1000").value) || 0,
        },
        metro: {
          "500": Number(el("tMetro500").value) || 0,
          "1000": Number(el("tMetro1000").value) || 0,
        },
      },
    },
    planning: {
      deserts: {
        threshold_score: Number(el("desertThreshold").value) || 0,
        library_search_radius_m: Number(el("desertSearchRadius").value) || 3000,
        distance_decay: { type: "linear", zero_at_m: Number(el("desertSearchRadius").value) || 3000 },
      },
      outreach: {
        coverage_radius_m: Number(el("outreachRadius").value) || 1000,
        top_n_per_city: Number(el("outreachTopN").value) || 10,
        weight_coverage: wCov,
        weight_site_access: wSite,
        allowed_candidate_types: types,
      },
    },
    spatial: { grid: { cell_size_m: Number(el("gridSize").value) || 1000 } },
  };

  return { scenario, cities, patch };
}

function toggleOverlay(forceOpen = null) {
  const overlay = el("overlay");
  const isHidden = overlay.classList.contains("hidden");
  const open = forceOpen === null ? isHidden : Boolean(forceOpen);
  overlay.classList.toggle("hidden", !open);
}

function shouldIgnoreKeyEvent(e) {
  const t = e.target;
  if (!t) return false;
  const tag = (t.tagName || "").toLowerCase();
  if (tag === "input" || tag === "select" || tag === "textarea") return true;
  if (t.isContentEditable) return true;
  return false;
}

function startCameraLoop(map) {
  if (state.cameraLoop) return;
  state.cameraLoop = window.setInterval(() => {
    if (state.keysDown.size === 0) return;
    const speed = state.keysDown.has("ShiftLeft") || state.keysDown.has("ShiftRight") ? 2.2 : 1.0;
    const slow = state.keysDown.has("AltLeft") || state.keysDown.has("AltRight") ? 0.35 : 1.0;
    const k = speed * slow;

    const panPx = 18 * k;
    const rotDeg = 2.0 * k;
    const pitchDeg = 1.8 * k;

    let dx = 0;
    let dy = 0;
    if (state.keysDown.has("KeyW")) dy -= panPx;
    if (state.keysDown.has("KeyS")) dy += panPx;
    if (state.keysDown.has("KeyA")) dx -= panPx;
    if (state.keysDown.has("KeyD")) dx += panPx;
    if (dx !== 0 || dy !== 0) map.panBy([dx, dy], { duration: 0 });

    if (state.keysDown.has("KeyQ")) map.rotateTo(map.getBearing() - rotDeg, { duration: 0 });
    if (state.keysDown.has("KeyE")) map.rotateTo(map.getBearing() + rotDeg, { duration: 0 });

    if (state.keysDown.has("KeyR")) map.zoomTo(map.getZoom() + 0.06 * k, { duration: 0 });
    if (state.keysDown.has("KeyF")) map.zoomTo(map.getZoom() - 0.06 * k, { duration: 0 });

    if (state.keysDown.has("KeyZ")) map.easeTo({ pitch: Math.min(70, map.getPitch() + pitchDeg), duration: 0 });
    if (state.keysDown.has("KeyX")) map.easeTo({ pitch: Math.max(0, map.getPitch() - pitchDeg), duration: 0 });
  }, 16);
}

function stopCameraLoop() {
  if (!state.cameraLoop) return;
  window.clearInterval(state.cameraLoop);
  state.cameraLoop = null;
}

function initFoldouts() {
  for (const foldout of document.querySelectorAll(".foldout")) {
    const key = foldout.getAttribute("data-foldout") || "";
    const stored = localStorage.getItem(`lr_foldout_${key}`);
    if (stored === "collapsed") foldout.classList.add("collapsed");

    const header = foldout.querySelector(".foldout-header");
    header.addEventListener("click", () => {
      foldout.classList.toggle("collapsed");
      localStorage.setItem(`lr_foldout_${key}`, foldout.classList.contains("collapsed") ? "collapsed" : "open");
    });
  }
}

function setLayerVisibility(layerId, visible) {
  const map = state.map;
  if (!map) return;
  if (!map.getLayer(layerId)) return;
  map.setLayoutProperty(layerId, "visibility", visible ? "visible" : "none");
}

function updateLayerToggles() {
  for (const l of LAYER_REGISTRY) {
    const ids = Array.isArray(l.layerIds) ? l.layerIds : l.layerId ? [l.layerId] : [];
    for (const id of ids) setLayerVisibility(id, Boolean(state.layers?.[l.key]));
  }
  state.hoverPopup?.remove();
}

function fmtIsoLocal(iso) {
  if (!iso) return "—";
  const d = new Date(String(iso));
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toLocaleString("zh-Hant", { hour12: false });
}

function relativeAge(iso) {
  if (!iso) return null;
  const d = new Date(String(iso));
  if (Number.isNaN(d.getTime())) return null;
  const s = Math.max(0, (Date.now() - d.getTime()) / 1000);
  if (s < 90) return `${Math.round(s)}s ago`;
  const m = s / 60;
  if (m < 90) return `${Math.round(m)}m ago`;
  const h = m / 60;
  if (h < 48) return `${Math.round(h)}h ago`;
  const days = h / 24;
  return `${Math.round(days)}d ago`;
}

function renderDataStatus(health) {
  const node = el("dataStatus");
  if (!node) return;
  const ing = health?.ingestion_status || null;
  const run = health?.run_meta || null;
  const files = health?.files || {};
  const state = ing?.state || "unknown";
  const lastFetch = ing?.fetch?.last_success_at || health?.stops_meta?.generated_at || null;
  const nextRetry = ing?.fetch?.next_retry_at || null;
  const lastPipeline = run?.generated_at || null;

  const okOutputs =
    Boolean(files.libraries_scored) && Boolean(files.deserts_csv || files.deserts_geojson) && Boolean(files.outreach_recommendations);

  let kind = "info";
  if (state === "rate_limited") kind = "warning";
  if (state === "error") kind = "error";
  if (!okOutputs) kind = "warning";

  const parts = [];
  parts.push(`Collector: ${state}`);
  if (lastFetch) parts.push(`last fetch ${relativeAge(lastFetch) || fmtIsoLocal(lastFetch)}`);
  if (state === "rate_limited" && nextRetry) parts.push(`retry ${relativeAge(nextRetry) || fmtIsoLocal(nextRetry)}`);
  if (lastPipeline) parts.push(`pipeline ${relativeAge(lastPipeline) || fmtIsoLocal(lastPipeline)}`);
  parts.push(okOutputs ? "outputs OK" : "outputs missing");

  node.textContent = parts.join(" · ");
  node.classList.toggle("muted", kind === "info");
}

async function refreshHealthUi() {
  try {
    const health = await (window.lrApi?.getHealth ? window.lrApi.getHealth() : fetchJson("/health"));
    renderDataStatus(health);
    if (window.lrRenderSourcesCard) window.lrRenderSourcesCard("sourcesCardConsole", { title: "Data & Provenance", health });

    const stopsAt = health?.stops_meta?.generated_at;
    const ybAt = health?.youbike_meta?.generated_at;
    const note = [];
    if (stopsAt) note.push(`Stops ${relativeAge(stopsAt) || fmtIsoLocal(stopsAt)}`);
    if (ybAt) note.push(`YouBike ${relativeAge(ybAt) || fmtIsoLocal(ybAt)}`);
    const elUpdated = el("legendUpdatedAt");
    if (elUpdated) elUpdated.textContent = `Updated: ${note.length ? note.join(" · ") : "—"}`;
  } catch (e) {
    console.warn(e);
  }
}

function renderInspectorLibrary(lib) {
  const score = Number(lib.accessibility_score) || 0;
  const badgeColor =
    score >= 70 ? "rgba(34,197,94,0.35)" : score >= 40 ? "rgba(245,158,11,0.35)" : "rgba(239,68,68,0.35)";
  const explainText = lib.accessibility_explain ?? lib.explain_text ?? "-";
  el("inspector").innerHTML = `
    <div class="kv">
      <div class="k">Name</div><div>${lib.name ?? "-"}</div>
      <div class="k">City</div><div>${lib.city ?? "-"} / ${lib.district ?? "-"}</div>
      <div class="k">Score</div>
      <div><span class="pill" style="background:${badgeColor}">${fmt(score, 1)}</span></div>
      <div class="k">Explain</div><div>${explainText}</div>
    </div>
  `;
}

function renderOutreachList(rows) {
  const container = el("outreachList");
  if (!rows || rows.length === 0) {
    container.textContent = "No recommendations available (run the pipeline or try what-if).";
    return;
  }
  const items = rows
    .map((r) => {
      const score = Number(r.outreach_score) || 0;
      return `
        <div class="item">
          <div class="title">
            <div>${r.name ?? r.id}</div>
            <span class="pill">${r.city ?? "-"}</span>
          </div>
          <div class="body">
            OutreachScore <b>${fmt(score, 1)}</b><br/>
            ${r.recommendation_explain ?? ""}
          </div>
        </div>
      `;
    })
    .join("");
  container.innerHTML = `<div class="list">${items}</div>`;
}

function setGeojson(sourceId, geojson) {
  const map = state.map;
  if (!map) return;
  const src = map.getSource(sourceId);
  if (!src) return;
  src.setData(geojson);
}

function initHighlightLayers(map) {
  map.addSource("highlights", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
  map.addLayer({
    id: "highlights-halo",
    type: "circle",
    source: "highlights",
    paint: {
      "circle-radius": 16,
      "circle-color": [
        "match",
        ["get", "kind"],
        "low_library_score",
        "rgba(239,68,68,0.45)",
        "high_desert_gap",
        "rgba(245,158,11,0.45)",
        "top_outreach",
        "rgba(37,99,235,0.35)",
        "rgba(37,99,235,0.35)",
      ],
      "circle-opacity": 1,
      "circle-stroke-width": 2,
      "circle-stroke-color": "rgba(15,23,42,0.9)",
    },
  });
  map.addLayer({
    id: "highlights-point",
    type: "circle",
    source: "highlights",
    paint: {
      "circle-radius": 7,
      "circle-color": [
        "match",
        ["get", "kind"],
        "low_library_score",
        "rgba(239,68,68,0.85)",
        "high_desert_gap",
        "rgba(245,158,11,0.85)",
        "top_outreach",
        "rgba(37,99,235,0.75)",
        "rgba(37,99,235,0.75)",
      ],
      "circle-opacity": 0.95,
      "circle-stroke-width": 2,
      "circle-stroke-color": "rgba(15,23,42,0.9)",
    },
  });
}

function setHighlightData(features) {
  const map = state.map;
  if (!map) return;
  const src = map.getSource("highlights");
  if (!src) return;
  src.setData({ type: "FeatureCollection", features: features || [] });
}

function clearHighlight() {
  setHighlightData([]);
  pushAction("Highlights cleared.", "info");
}

function applyHighlight() {
  const kind = el("highlightKind")?.value || "low_library_score";
  const topN = Math.max(1, Number(el("highlightTopN")?.value) || 20);
  const data = state.currentData || state.baselineData || null;
  if (!data) {
    pushAction("No baseline loaded yet. Run pipeline or load outputs first.", "warning");
    return;
  }

  const features = [];
  if (kind === "low_library_score") {
    const libs = data.libraries_geojson?.features || [];
    const scoreOf = (f) => {
      const v = Number(f?.properties?.accessibility_score);
      return Number.isFinite(v) ? v : Infinity;
    };
    const sorted = [...libs].sort((a, b) => scoreOf(a) - scoreOf(b));
    for (const f of sorted.slice(0, topN)) {
      features.push({ type: "Feature", geometry: f.geometry, properties: { kind, id: f?.properties?.id, name: f?.properties?.name } });
    }
  } else if (kind === "high_desert_gap") {
    const des = (data.deserts_geojson?.features || []).filter((f) => Boolean(f?.properties?.is_desert));
    const gapOf = (f) => {
      const v = Number(f?.properties?.gap_to_threshold);
      return Number.isFinite(v) ? v : -Infinity;
    };
    const sorted = [...des].sort((a, b) => gapOf(b) - gapOf(a));
    for (const f of sorted.slice(0, topN)) {
      features.push({ type: "Feature", geometry: f.geometry, properties: { kind, city: f?.properties?.city } });
    }
  } else if (kind === "top_outreach") {
    const rows = data.outreach_rows || [];
    const sc = (r) => {
      const v = Number(r?.outreach_score);
      return Number.isFinite(v) ? v : -Infinity;
    };
    const sorted = [...rows].sort((a, b) => sc(b) - sc(a));
    for (const r of sorted.slice(0, topN)) {
      if (r?.lat === undefined || r?.lon === undefined) continue;
      features.push({
        type: "Feature",
        geometry: { type: "Point", coordinates: [Number(r.lon), Number(r.lat)] },
        properties: { kind, id: r?.id, name: r?.name, city: r?.city },
      });
    }
  }

  setHighlightData(features);
  pushAction(`Highlight: ${kind} · Top ${features.length}`, "success");
  if (features.length) fitToGeojson({ type: "FeatureCollection", features });
}

function metersBetween([lon1, lat1], [lon2, lat2]) {
  const rad = Math.PI / 180;
  const lat = (lat1 + lat2) / 2;
  const x = (lon2 - lon1) * rad * Math.cos(lat * rad);
  const y = (lat2 - lat1) * rad;
  return Math.sqrt(x * x + y * y) * 6371000;
}

function setGeojsonSource(sourceId, geojson) {
  const map = state.map;
  if (!map) return;
  const src = map.getSource(sourceId);
  if (!src) return;
  src.setData(geojson || { type: "FeatureCollection", features: [] });
}

function fitToGeojson(geojson) {
  const map = state.map;
  if (!map) return;
  const coords = [];
  for (const f of geojson.features || []) {
    if (f.geometry?.type !== "Point") continue;
    const c = f.geometry.coordinates;
    if (Array.isArray(c) && c.length === 2) coords.push(c);
  }
  if (coords.length === 0) return;
  let minLon = coords[0][0];
  let minLat = coords[0][1];
  let maxLon = coords[0][0];
  let maxLat = coords[0][1];
  for (const [lon, lat] of coords) {
    minLon = Math.min(minLon, lon);
    minLat = Math.min(minLat, lat);
    maxLon = Math.max(maxLon, lon);
    maxLat = Math.max(maxLat, lat);
  }
  map.fitBounds(
    [
      [minLon, minLat],
      [maxLon, maxLat],
    ],
    { padding: 40, duration: 400 }
  );
}

async function loadBaselineOutputs() {
  const baseline = { libraries: null, deserts: null, outreach: null };
  const baselineData = { libraries_geojson: null, deserts_geojson: null, outreach_rows: null };
  try {
    const libs = await fetchJson("/geo/libraries");
    setGeojson("libraries", libs);
    fitToGeojson(libs);
    baseline.libraries = computeLibraryStatsFromGeojson(libs);
    baselineData.libraries_geojson = libs;
  } catch (e) {
    console.warn(e);
  }

  try {
    const deserts = await fetchJson("/geo/deserts");
    setGeojson("deserts", deserts);
    baseline.deserts = computeDesertStatsFromGeojson(deserts);
    baselineData.deserts_geojson = deserts;
  } catch (e) {
    console.warn(e);
  }

  try {
    const recs = await fetchJson("/outreach/recommendations");
    renderOutreachList(recs);
    setGeojson("outreach", {
      type: "FeatureCollection",
      features: recs.map((r) => ({
        type: "Feature",
        geometry: { type: "Point", coordinates: [r.lon, r.lat] },
        properties: r,
      })),
    });
    baseline.outreach = computeOutreachStatsFromRows(recs);
    baselineData.outreach_rows = recs;
  } catch (e) {
    console.warn(e);
  }

  if (baseline.libraries || baseline.deserts || baseline.outreach) {
    state.baselineStats = baseline;
    state.baselineData = baselineData;
    renderStory();
  }
}

async function loadOptionalTransitLayers() {
  try {
    const metro = await fetchJson("/geo/stops?modes=metro&limit=50000");
    setGeojson("stops_metro", metro);
    state.transit.metro_geojson = metro;
  } catch (e) {
    console.warn(e);
  }
  try {
    const yb = await fetchJson("/geo/youbike?limit=50000");
    setGeojson("youbike", yb);
    state.transit.youbike_geojson = yb;
  } catch (e) {
    console.warn(e);
  }
}

async function runWhatIf() {
  const { scenario, cities, patch } = readConfigPatchFromForm();
  const applyBtn = el("apply");
  const oldLabel = applyBtn?.textContent || "Apply";
  if (applyBtn) {
    applyBtn.disabled = true;
    applyBtn.textContent = "Applying…";
  }

  const t0 = safeNowMs();
  setStatus("Running what-if recomputation…");
  pushAction("Running what-if…", "info", { timeoutMs: 2500 });
  try {
    const res = await fetchJson("/analysis/whatif", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ scenario, cities, config_patch: patch }),
    });
    state.lastResult = res;
    setGeojson("libraries", res.libraries_geojson);
    setGeojson("deserts", res.deserts_geojson);
    setGeojson("outreach", res.outreach_geojson);
    renderOutreachList(res.outreach);
    fitToGeojson(res.libraries_geojson);
    const took = Math.max(0, safeNowMs() - t0) / 1000;
    setStatus(`What-if updated: ${res.libraries.length} libraries, ${res.outreach.length} outreach sites`);
    showNotice(
      `Updated ${scenario} for ${cities.length} city(s) in ${fmt(took, 1)}s · ${res.libraries.length} libraries · ${res.outreach.length} outreach`,
      "success",
      { timeoutMs: 6500 }
    );

    state.currentStats = {
      libraries: computeLibraryStatsFromRows(res.libraries || []),
      deserts: computeDesertStatsFromGeojson(res.deserts_geojson),
      outreach: computeOutreachStatsFromRows(res.outreach || []),
    };
    pushAction(`What-if updated (${fmt(took, 1)}s) · deserts ${fmt(state.currentStats?.deserts?.desert_count, 0)}`, "success");
    state.currentData = {
      libraries_geojson: res.libraries_geojson,
      deserts_geojson: res.deserts_geojson,
      outreach_rows: res.outreach || [],
    };
    renderStory();
    writePresetToUrl({ scenario, cities, patch });
  } catch (e) {
    setStatus(`What-if failed: ${e.message}`);
    showNotice(`What-if failed: ${e.message}`, "error", { timeoutMs: 0 });
    pushAction(`What-if failed: ${e.message}`, "error", { timeoutMs: 0 });
    console.error(e);
  } finally {
    if (applyBtn) {
      applyBtn.disabled = false;
      applyBtn.textContent = oldLabel;
    }
  }
}

function resetFormToConfig() {
  if (!state.config) return;
  applyConfigToForm(state.config);
  showNotice("Reset to scenario defaults.", "info", { timeoutMs: 3000 });
  pushAction("Reset to scenario defaults.", "info", { timeoutMs: 3500 });
}

function savePreset() {
  const { scenario, cities, patch } = readConfigPatchFromForm();
  const preset = { scenario, cities, patch };
  localStorage.setItem("lr_preset_v1", JSON.stringify(preset));
  setStatus("Preset saved locally.");
  writePresetToUrl(preset);
  showNotice("Preset saved locally and synced to URL.", "success", { timeoutMs: 3500 });
  pushAction("Preset saved.", "success", { timeoutMs: 3500 });
}

function loadPreset() {
  const raw = localStorage.getItem("lr_preset_v1");
  if (!raw) return false;
  try {
    const p = JSON.parse(raw);
    applyPresetToForm(p);
    setStatus("Preset loaded.");
    pushAction("Preset loaded.", "success", { timeoutMs: 3500 });
    return true;
  } catch (e) {
    console.warn(e);
    return false;
  }
}

async function validateCatalogs() {
  setStatus("Validating catalogs…");
  try {
    const scenario = el("scenario").value;
    const report = await fetchJson(`/control/catalogs/validate?scenario=${encodeURIComponent(scenario)}`, {
      method: "POST",
    });
    if (report.ok) {
      setStatus("Catalogs OK. Report written to reports/catalog_validation.md");
    } else {
      setStatus(`Catalog validation failed (${report.errors.length} errors). See reports/catalog_validation.md`);
    }
  } catch (e) {
    setStatus(`Catalog validation failed: ${e.message}`);
  }
}

function initKeybindings() {
  function toggleLayerByKey(key) {
    state.layers[key] = !state.layers[key];
    const entry = LAYER_REGISTRY.find((x) => x.key === key);
    const checkbox = entry ? el(entry.checkboxId) : null;
    if (checkbox) checkbox.checked = Boolean(state.layers[key]);
    updateLayerToggles();
  }

  window.addEventListener("keydown", (e) => {
    if (shouldIgnoreKeyEvent(e)) return;

    if (e.key === "?" || (e.key === "/" && e.shiftKey)) {
      e.preventDefault();
      toggleOverlay();
      return;
    }
    if (e.key === "Escape") {
      toggleOverlay(false);
      return;
    }

    if (e.ctrlKey && e.key === "Enter") {
      e.preventDefault();
      runWhatIf();
      return;
    }
    if (e.ctrlKey && (e.key === "s" || e.key === "S")) {
      e.preventDefault();
      savePreset();
      return;
    }

    if (e.key === "l" || e.key === "L") return toggleLayerByKey("libraries");
    if (e.key === "d" || e.key === "D") return toggleLayerByKey("deserts");
    if (e.key === "o" || e.key === "O") return toggleLayerByKey("outreach");
    if (e.key === "m" || e.key === "M") return toggleLayerByKey("metro");
    if (e.key === "y" || e.key === "Y") return toggleLayerByKey("youbike");

    if (e.key === "0") {
      state.map?.easeTo({ bearing: 0, pitch: 0, duration: 120 });
      return;
    }

    state.keysDown.add(e.code);
    if (state.map) startCameraLoop(state.map);
  });

  window.addEventListener("keyup", (e) => {
    state.keysDown.delete(e.code);
    if (state.keysDown.size === 0) stopCameraLoop();
  });
}

function initUIHandlers() {
  el("apply").addEventListener("click", runWhatIf);
  el("reset").addEventListener("click", resetFormToConfig);
  el("validate").addEventListener("click", validateCatalogs);
  el("help").addEventListener("click", () => toggleOverlay(true));
  el("closeOverlay").addEventListener("click", () => toggleOverlay(false));
  el("noticeClose").addEventListener("click", hideNotice);
  el("refreshStatus")?.addEventListener("click", () => refreshHealthUi().then(() => pushAction("Status refreshed.", "success", { timeoutMs: 2500 })));
  el("copyFixCmd")?.addEventListener("click", async () => {
    const cmds = [
      "# LibraryReach quick fixes",
      "docker compose ps",
      "docker compose logs -n 200 worker",
      "docker compose logs -n 200 api",
      "docker compose up -d --build worker api",
      "",
      "# If you need to fetch data manually:",
      "python -m libraryreach fetch-stops",
      "python -m libraryreach fetch-youbike",
    ].join("\\n");
    try {
      await navigator.clipboard.writeText(cmds);
      pushAction("Fix commands copied.", "success");
    } catch (e) {
      console.warn(e);
      pushAction("Copy failed. Please copy from the instructions in console.", "warning");
      showNotice("Copy failed. See console logs for suggested commands.", "warning", { timeoutMs: 5000 });
      console.log(cmds);
    }
  });

  const highlightApply = el("highlightApply");
  const highlightClear = el("highlightClear");
  highlightApply?.addEventListener("click", () => applyHighlight());
  highlightClear?.addEventListener("click", () => clearHighlight());

  el("copyLink").addEventListener("click", async () => {
    try {
      const preset = readConfigPatchFromForm();
      writePresetToUrl(preset);
      await navigator.clipboard.writeText(window.location.href);
      showNotice("Link copied.", "success", { timeoutMs: 2000 });
      pushAction("Share link copied.", "success");
    } catch (e) {
      console.warn(e);
      showNotice("Copy failed. Please copy from the address bar.", "warning", { timeoutMs: 5000 });
      pushAction("Copy failed. Please copy from the address bar.", "warning");
    }
  });

  const markDirty = () => {
    state.currentStats = null;
    renderStory();
  };
  for (const id of [
    "wBus",
    "wMetro",
    "w500",
    "w1000",
    "tBus500",
    "tBus1000",
    "tMetro500",
    "tMetro1000",
    "desertThreshold",
    "gridSize",
    "desertSearchRadius",
    "outreachRadius",
    "outreachTopN",
    "wCoverage",
    "wSite",
  ]) {
    const node = el(id);
    node?.addEventListener("input", markDirty);
    node?.addEventListener("change", markDirty);
  }
  el("cities").addEventListener("change", markDirty);
  el("candidateTypes").addEventListener("change", markDirty);

  el("scenario").addEventListener("change", async () => {
    const scenario = el("scenario").value;
    try {
      const cfg = await fetchJson(`/control/config?scenario=${encodeURIComponent(scenario)}`);
      applyConfigToForm(cfg);
      setStatus(`Loaded scenario config: ${scenario}`);
      showNotice(`Loaded scenario: ${scenario}`, "info", { timeoutMs: 2500 });
      pushAction(`Scenario loaded: ${scenario}`, "success", { timeoutMs: 3500 });
    } catch (e) {
      setStatus(`Failed to load scenario config: ${e.message}`);
      showNotice(`Failed to load scenario: ${e.message}`, "error", { timeoutMs: 0 });
      pushAction(`Scenario load failed: ${e.message}`, "error", { timeoutMs: 0 });
    }
  });

  for (const l of LAYER_REGISTRY) {
    const node = el(l.checkboxId);
    node?.addEventListener("change", (e) => {
      state.layers[l.key] = Boolean(e.target.checked);
      updateLayerToggles();
      pushAction(`Layer: ${l.label} ${state.layers[l.key] ? "shown" : "hidden"}`, "info", { timeoutMs: 3500 });
    });
  }
}

function initMap() {
  let resolveReady = null;
  state.mapReady = new Promise((resolve) => {
    resolveReady = resolve;
  });

  const map = new maplibregl.Map({
    container: "map",
    style: STYLE,
    center: [121.0, 23.8],
    zoom: 7,
    attributionControl: true,
  });
  map.addControl(new maplibregl.NavigationControl({ visualizePitch: true }), "top-right");
  state.map = map;

  map.on("load", () => {
    for (const l of LAYER_REGISTRY) l.addSource(map);
    for (const l of [...LAYER_REGISTRY].sort((a, b) => (a.renderOrder ?? 0) - (b.renderOrder ?? 0))) l.addLayer(map);
    initHighlightLayers(map);
    map.addSource("selection", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    map.addSource("youbike_nearby", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    map.addSource("metro_links", { type: "geojson", data: { type: "FeatureCollection", features: [] } });

    map.addLayer({
      id: "metro-links",
      type: "line",
      source: "metro_links",
      paint: {
        "line-color": "rgba(59, 130, 246, 0.65)",
        "line-width": 2,
        "line-opacity": 0.8,
      },
    });

    map.addLayer({
      id: "youbike-nearby",
      type: "circle",
      source: "youbike_nearby",
      paint: {
        "circle-radius": 7,
        "circle-color": [
          "interpolate",
          ["linear"],
          ["coalesce", ["get", "dist_m"], 0],
          0,
          "rgba(34,197,94,0.9)",
          400,
          "rgba(245,158,11,0.9)",
          900,
          "rgba(239,68,68,0.9)",
        ],
        "circle-opacity": 0.9,
        "circle-stroke-width": 2,
        "circle-stroke-color": "rgba(15,23,42,0.9)",
      },
    });

    map.addLayer({
      id: "selection-halo",
      type: "circle",
      source: "selection",
      paint: {
        "circle-radius": 18,
        "circle-color": "rgba(59, 130, 246, 0.18)",
        "circle-stroke-width": 2,
        "circle-stroke-color": "rgba(59, 130, 246, 0.75)",
      },
    });

    map.addLayer({
      id: "selection-point",
      type: "circle",
      source: "selection",
      paint: {
        "circle-radius": 8,
        "circle-color": "rgba(59, 130, 246, 0.75)",
        "circle-stroke-width": 2,
        "circle-stroke-color": "rgba(15,23,42,0.9)",
      },
    });

    const hoverPopup = new maplibregl.Popup({ closeButton: false, closeOnClick: false, offset: 12 });
    state.hoverPopup = hoverPopup;

    function showPopup(lngLat, html) {
      hoverPopup.setLngLat(lngLat).setHTML(html).addTo(map);
    }

    function hidePopup() {
      hoverPopup.remove();
    }

    map.on("click", "libraries", async (e) => {
      const f = e.features?.[0];
      if (!f) return;
      const props = f.properties || {};
      const id = props.id || props.ID || props.library_id;
      if (!id) return;

      if (state.lastResult?.libraries) {
        const lib = state.lastResult.libraries.find((x) => String(x.id) === String(id));
        if (lib) {
          renderInspectorLibrary(lib);
          return;
        }
      }

      try {
        const detail = await fetchJson(`/libraries/${encodeURIComponent(id)}`);
        renderInspectorLibrary(detail);
      } catch (err) {
        console.warn(err);
      }
    });

    map.on("click", "stops-metro", (e) => {
      const f = e.features?.[0];
      const coords = f?.geometry?.coordinates;
      if (!Array.isArray(coords) || coords.length !== 2) return;
      const metroCoord = [Number(coords[0]), Number(coords[1])];
      const props = f?.properties || {};
      setGeojsonSource("selection", {
        type: "FeatureCollection",
        features: [{ type: "Feature", geometry: { type: "Point", coordinates: metroCoord }, properties: { type: "metro" } }],
      });

      const yb = state.transit.youbike_geojson?.features || [];
      if (yb.length === 0) {
        pushAction("No YouBike layer loaded. Enable YouBike ingestion to show nearby stations.", "warning", { timeoutMs: 6000 });
        return;
      }

      const nearby = [];
      for (const yf of yb) {
        const c = yf?.geometry?.coordinates;
        if (!Array.isArray(c) || c.length !== 2) continue;
        const d = metersBetween(metroCoord, [Number(c[0]), Number(c[1])]);
        if (!Number.isFinite(d) || d > 900) continue;
        nearby.push({ f: yf, d });
      }
      nearby.sort((a, b) => a.d - b.d);
      const picked = nearby.slice(0, 24);

      setGeojsonSource("youbike_nearby", {
        type: "FeatureCollection",
        features: picked.map(({ f: yf, d }) => ({
          type: "Feature",
          geometry: yf.geometry,
          properties: { ...(yf.properties || {}), dist_m: Math.round(d) },
        })),
      });

      setGeojsonSource("metro_links", {
        type: "FeatureCollection",
        features: picked.map(({ f: yf, d }) => ({
          type: "Feature",
          geometry: { type: "LineString", coordinates: [metroCoord, yf.geometry.coordinates] },
          properties: { dist_m: Math.round(d) },
        })),
      });

      state.layers.youbike = true;
      const ybToggle = el("layerYoubike");
      if (ybToggle) ybToggle.checked = true;
      updateLayerToggles();

      const name = props.name ?? props.stop_id ?? "Metro station";
      pushAction(`Selected metro: ${name} · nearby YouBike ${picked.length}`, "success", { timeoutMs: 6500 });
    });

    for (const l of LAYER_REGISTRY) {
      const layerId = l.eventLayerId || (Array.isArray(l.layerIds) ? l.layerIds[0] : l.layerId);
      if (!layerId) continue;
      map.on("mousemove", layerId, (e) => {
        const f = e.features?.[0];
        const props = f?.properties || {};
        const html = l.tooltip ? l.tooltip({ props, fmt, escapeHtml }) : "";
        if (!html) return;
        map.getCanvas().style.cursor = l.cursor || "pointer";
        showPopup(e.lngLat, html);
      });

      map.on("mouseleave", layerId, () => {
        hidePopup();
        map.getCanvas().style.cursor = "";
      });
    }

    updateLayerToggles();
    setStatus("Map ready. Loading config…");
    resolveReady?.();
  });
}

async function main() {
  initTheme();
  initFoldouts();
  initUIHandlers();
  initKeybindings();
  initMap();

  await state.mapReady;
  applyMapTheme(document.documentElement.getAttribute("data-theme"));

  refreshHealthUi();
  window.setInterval(refreshHealthUi, 30_000);

  try {
    const cfg = await fetchJson("/control/config");
    applyConfigToForm(cfg);
    setStatus("Config loaded. Loading baseline outputs…");
    pushAction("Config loaded.", "success", { timeoutMs: 3000 });
  } catch (e) {
    setStatus("Config not available.");
    console.warn(e);
    pushAction("Config not available.", "warning", { timeoutMs: 4000 });
  }

  await loadBaselineOutputs();
  await loadOptionalTransitLayers();
  pushAction("Baseline layers loaded.", "success", { timeoutMs: 3500 });

  const urlPreset = readPresetFromUrl();
  if (urlPreset) {
    if (urlPreset.scenario && urlPreset.scenario !== el("scenario").value) {
      try {
        const cfg = await fetchJson(`/control/config?scenario=${encodeURIComponent(urlPreset.scenario)}`);
        applyConfigToForm(cfg);
      } catch (e) {
        console.warn(e);
      }
    }
    if (applyPresetToForm(urlPreset)) {
      setStatus("Preset loaded from URL.");
      showNotice("Loaded preset from URL.", "info", { timeoutMs: 3500 });
      pushAction("Preset loaded from URL.", "success", { timeoutMs: 3500 });
    }
  } else if (loadPreset()) {
    showNotice("Loaded preset from local storage.", "info", { timeoutMs: 3500 });
  }
  setStatus("Ready. Press ? for shortcuts.");
}

main();
