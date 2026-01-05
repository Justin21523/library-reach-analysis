async function fetchJson(path) {
  const res = await fetch(path);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${path} -> ${res.status} ${text}`);
  }
  return res.json();
}

function scoreColor(score) {
  const s = Math.max(0, Math.min(100, score));
  const hue = (s / 100) * 120; // 0=red, 120=green
  return `hsl(${hue}, 70%, 45%)`;
}

function fmt(n, digits = 1) {
  if (n === null || n === undefined) return "-";
  const v = Number(n);
  if (Number.isNaN(v)) return "-";
  return v.toFixed(digits);
}

function setStatus(text) {
  document.getElementById("status").textContent = text;
}

function renderLibraryDetails(detail) {
  const el = document.getElementById("library-details");
  const score = detail.accessibility_score ?? 0;
  el.innerHTML = `
    <div class="kv">
      <div class="k">Name</div><div>${detail.name}</div>
      <div class="k">City</div><div>${detail.city} / ${detail.district}</div>
      <div class="k">Score</div>
      <div><span class="pill" style="background:${scoreColor(score)}; color:white;">${fmt(score, 1)}</span></div>
      <div class="k">Explain</div><div>${detail.explain_text ?? "-"}</div>
    </div>
  `;
}

function renderOutreachList(rows) {
  const el = document.getElementById("outreach-list");
  if (!rows.length) {
    el.textContent = "No recommendations yet. Run the pipeline first.";
    return;
  }
  const items = rows
    .map((r) => {
      const score = r.outreach_score ?? 0;
      return `
        <div class="item">
          <div class="title">${r.name} <span class="pill">${r.city}</span></div>
          <div class="body">
            OutreachScore <b>${fmt(score, 1)}</b><br/>
            ${r.recommendation_explain ?? ""}
          </div>
        </div>
      `;
    })
    .join("");
  el.innerHTML = `<div class="list">${items}</div>`;
}

async function main() {
  setStatus("Loading pipeline outputs…");

  const map = L.map("map", { zoomControl: true }).setView([23.8, 121.0], 7);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: "&copy; OpenStreetMap contributors",
  }).addTo(map);

  let libraries = [];
  try {
    libraries = await fetchJson("/libraries");
    setStatus(`Loaded ${libraries.length} libraries.`);
  } catch (e) {
    setStatus("Missing outputs. Run: python -m libraryreach run-all");
    console.error(e);
    return;
  }

  const layer = L.layerGroup().addTo(map);
  const desertsLayer = L.layerGroup().addTo(map);
  const bounds = [];

  for (const lib of libraries) {
    const score = lib.accessibility_score ?? 0;
    const marker = L.circleMarker([lib.lat, lib.lon], {
      radius: 7,
      weight: 2,
      color: scoreColor(score),
      fillColor: scoreColor(score),
      fillOpacity: 0.65,
    });
    marker.bindTooltip(`${lib.name} (${fmt(score, 1)})`);
    marker.on("click", async () => {
      try {
        const detail = await fetchJson(`/libraries/${encodeURIComponent(lib.id)}`);
        renderLibraryDetails(detail);
      } catch (e) {
        console.error(e);
      }
    });
    marker.addTo(layer);
    bounds.push([lib.lat, lib.lon]);
  }

  if (bounds.length) {
    map.fitBounds(bounds, { padding: [20, 20] });
  }

  // Desert cells (grid centroids)
  try {
    const deserts = await fetchJson("/geo/deserts");
    L.geoJSON(deserts, {
      filter: (f) => Boolean(f.properties && f.properties.is_desert),
      pointToLayer: (f, latlng) =>
        L.circleMarker(latlng, {
          radius: 3,
          weight: 0,
          fillColor: "#ef4444",
          fillOpacity: 0.35,
        }),
    }).addTo(desertsLayer);
  } catch (e) {
    console.warn("Deserts layer not available:", e);
  }

  try {
    const recs = await fetchJson("/outreach/recommendations");
    renderOutreachList(recs);
  } catch (e) {
    document.getElementById("outreach-list").textContent =
      "Missing outreach outputs. Run the pipeline first.";
    console.error(e);
  }
}

main();
