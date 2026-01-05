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

const state = {
  map: null,
  mapReady: null,
  config: null,
  scenario: "weekday",
  lastResult: null,
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

async function fetchJson(path, options = {}) {
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

function fmt(n, digits = 1) {
  if (n === null || n === undefined) return "-";
  const v = Number(n);
  if (Number.isNaN(v)) return "-";
  return v.toFixed(digits);
}

function normalizePair(a, b) {
  const x = Math.max(0, Number(a) || 0);
  const y = Math.max(0, Number(b) || 0);
  const s = x + y;
  if (s <= 0) return [0.5, 0.5];
  return [x / s, y / s];
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
  setLayerVisibility("libraries", state.layers.libraries);
  setLayerVisibility("deserts", state.layers.deserts);
  setLayerVisibility("outreach", state.layers.outreach);
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
  try {
    const libs = await fetchJson("/geo/libraries");
    setGeojson("libraries", libs);
    fitToGeojson(libs);
  } catch (e) {
    console.warn(e);
  }

  try {
    const deserts = await fetchJson("/geo/deserts");
    setGeojson("deserts", deserts);
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
  } catch (e) {
    console.warn(e);
  }
}

async function runWhatIf() {
  const { scenario, cities, patch } = readConfigPatchFromForm();
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
    setStatus(`What-if updated: ${res.libraries.length} libraries, ${res.outreach.length} outreach sites`);
  } catch (e) {
    setStatus(`What-if failed: ${e.message}`);
    console.error(e);
  }
}

function resetFormToConfig() {
  if (!state.config) return;
  applyConfigToForm(state.config);
}

function savePreset() {
  const { scenario, cities, patch } = readConfigPatchFromForm();
  const preset = { scenario, cities, patch };
  localStorage.setItem("lr_preset_v1", JSON.stringify(preset));
  setStatus("Preset saved locally.");
}

function loadPreset() {
  const raw = localStorage.getItem("lr_preset_v1");
  if (!raw) return false;
  try {
    const p = JSON.parse(raw);
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
      el("desertSearchRadius").value =
        p.patch.planning.deserts.library_search_radius_m ?? el("desertSearchRadius").value;
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

    if (e.key === "l" || e.key === "L") {
      state.layers.libraries = !state.layers.libraries;
      el("layerLibraries").checked = state.layers.libraries;
      updateLayerToggles();
      return;
    }
    if (e.key === "d" || e.key === "D") {
      state.layers.deserts = !state.layers.deserts;
      el("layerDeserts").checked = state.layers.deserts;
      updateLayerToggles();
      return;
    }
    if (e.key === "o" || e.key === "O") {
      state.layers.outreach = !state.layers.outreach;
      el("layerOutreach").checked = state.layers.outreach;
      updateLayerToggles();
      return;
    }

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

  el("scenario").addEventListener("change", async () => {
    const scenario = el("scenario").value;
    try {
      const cfg = await fetchJson(`/control/config?scenario=${encodeURIComponent(scenario)}`);
      applyConfigToForm(cfg);
      setStatus(`Loaded scenario config: ${scenario}`);
    } catch (e) {
      setStatus(`Failed to load scenario config: ${e.message}`);
    }
  });

  el("layerLibraries").addEventListener("change", (e) => {
    state.layers.libraries = Boolean(e.target.checked);
    updateLayerToggles();
  });
  el("layerDeserts").addEventListener("change", (e) => {
    state.layers.deserts = Boolean(e.target.checked);
    updateLayerToggles();
  });
  el("layerOutreach").addEventListener("change", (e) => {
    state.layers.outreach = Boolean(e.target.checked);
    updateLayerToggles();
  });
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
    map.addSource("libraries", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    map.addSource("deserts", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    map.addSource("outreach", { type: "geojson", data: { type: "FeatureCollection", features: [] } });

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

    map.on("mouseenter", "libraries", () => (map.getCanvas().style.cursor = "pointer"));
    map.on("mouseleave", "libraries", () => (map.getCanvas().style.cursor = ""));
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

    updateLayerToggles();
    setStatus("Map ready. Loading config…");
    resolveReady?.();
  });
}

async function main() {
  initFoldouts();
  initUIHandlers();
  initKeybindings();
  initMap();

  await state.mapReady;

  try {
    const cfg = await fetchJson("/control/config");
    applyConfigToForm(cfg);
    setStatus("Config loaded. Loading baseline outputs…");
  } catch (e) {
    setStatus("Config not available.");
    console.warn(e);
  }

  await loadBaselineOutputs();
  if (!loadPreset()) {
    // no-op
  }
  setStatus("Ready. Press ? for shortcuts.");
}

main();
