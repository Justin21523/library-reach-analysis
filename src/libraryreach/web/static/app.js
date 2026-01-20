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
    layerId: "libraries",
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
    layerId: "deserts",
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
    layerId: "outreach",
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
    setLayerVisibility(l.layerId, Boolean(state.layers?.[l.key]));
  }
  state.hoverPopup?.remove();
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
  try {
    const libs = await fetchJson("/geo/libraries");
    setGeojson("libraries", libs);
    fitToGeojson(libs);
    baseline.libraries = computeLibraryStatsFromGeojson(libs);
  } catch (e) {
    console.warn(e);
  }

  try {
    const deserts = await fetchJson("/geo/deserts");
    setGeojson("deserts", deserts);
    baseline.deserts = computeDesertStatsFromGeojson(deserts);
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
  } catch (e) {
    console.warn(e);
  }

  if (baseline.libraries || baseline.deserts || baseline.outreach) {
    state.baselineStats = baseline;
    renderStory();
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
    renderStory();
    writePresetToUrl({ scenario, cities, patch });
  } catch (e) {
    setStatus(`What-if failed: ${e.message}`);
    showNotice(`What-if failed: ${e.message}`, "error", { timeoutMs: 0 });
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
}

function savePreset() {
  const { scenario, cities, patch } = readConfigPatchFromForm();
  const preset = { scenario, cities, patch };
  localStorage.setItem("lr_preset_v1", JSON.stringify(preset));
  setStatus("Preset saved locally.");
  writePresetToUrl(preset);
  showNotice("Preset saved locally and synced to URL.", "success", { timeoutMs: 3500 });
}

function loadPreset() {
  const raw = localStorage.getItem("lr_preset_v1");
  if (!raw) return false;
  try {
    const p = JSON.parse(raw);
    applyPresetToForm(p);
    setStatus("Preset loaded.");
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

  el("copyLink").addEventListener("click", async () => {
    try {
      const preset = readConfigPatchFromForm();
      writePresetToUrl(preset);
      await navigator.clipboard.writeText(window.location.href);
      showNotice("Link copied.", "success", { timeoutMs: 2000 });
    } catch (e) {
      console.warn(e);
      showNotice("Copy failed. Please copy from the address bar.", "warning", { timeoutMs: 5000 });
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
    } catch (e) {
      setStatus(`Failed to load scenario config: ${e.message}`);
      showNotice(`Failed to load scenario: ${e.message}`, "error", { timeoutMs: 0 });
    }
  });

  for (const l of LAYER_REGISTRY) {
    const node = el(l.checkboxId);
    node?.addEventListener("change", (e) => {
      state.layers[l.key] = Boolean(e.target.checked);
      updateLayerToggles();
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

    for (const l of LAYER_REGISTRY) {
      map.on("mousemove", l.layerId, (e) => {
        const f = e.features?.[0];
        const props = f?.properties || {};
        const html = l.tooltip ? l.tooltip({ props, fmt, escapeHtml }) : "";
        if (!html) return;
        map.getCanvas().style.cursor = l.cursor || "pointer";
        showPopup(e.lngLat, html);
      });

      map.on("mouseleave", l.layerId, () => {
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

  async function renderSourcesCard() {
    if (!window.lrRenderSourcesCard) return;
    try {
      const health = await (window.lrApi?.getHealth ? window.lrApi.getHealth() : fetchJson("/health"));
      window.lrRenderSourcesCard("sourcesCardConsole", { title: "Data & Provenance", health });
    } catch (e) {
      console.warn(e);
    }
  }

  renderSourcesCard();
  window.setInterval(renderSourcesCard, 30_000);

  try {
    const cfg = await fetchJson("/control/config");
    applyConfigToForm(cfg);
    setStatus("Config loaded. Loading baseline outputs…");
  } catch (e) {
    setStatus("Config not available.");
    console.warn(e);
  }

  await loadBaselineOutputs();

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
    }
  } else if (loadPreset()) {
    showNotice("Loaded preset from local storage.", "info", { timeoutMs: 3500 });
  }
  setStatus("Ready. Press ? for shortcuts.");
}

main();
