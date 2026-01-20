function lrEscapeHtml(s) {
  if (window.lrEscapeHtml) return window.lrEscapeHtml(s);
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function lrShortHash(s) {
  const v = String(s || "");
  return v ? v.slice(0, 8) : "—";
}

function lrFmtIsoLocal(iso) {
  if (!iso) return "—";
  const d = new Date(String(iso));
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toLocaleString("zh-Hant", { hour12: false });
}

function lrSourcesCardHtml({ title, health, meta }) {
  const src = (health?.sources_index || meta?.sources_index) ?? null;
  const ing = (health?.ingestion_status || meta?.ingestion_status) ?? null;
  const stops = (health?.stops_meta || meta?.stops_meta) ?? null;
  const run = (health?.run_meta || meta?.run_meta) ?? null;

  const state = ing?.state || "—";
  const lastFetch = lrFmtIsoLocal(ing?.fetch?.last_success_at || stops?.generated_at);
  const retryAt = lrFmtIsoLocal(ing?.fetch?.next_retry_at);
  const http429 = ing?.rate_limit?.http_429_count;
  const modes = [];
  if (Number(stops?.counts?.bus) > 0) modes.push("bus");
  if (Number(stops?.counts?.metro) > 0) modes.push("metro");

  const runIdShort = run?.run_id ? lrShortHash(run.run_id) : "—";
  const runAt = lrFmtIsoLocal(run?.generated_at);
  const cfgHash = run?.config_hash ? lrShortHash(run.config_hash) : "—";
  const nextFetchWindow = lrFmtIsoLocal(ing?.schedule?.next_fetch_window_start_local);

  const sources = Array.isArray(src?.sources) ? src.sources : [];
  const rowsHtml =
    sources.length === 0
      ? `<div class="muted">尚無來源索引（sources_index.json）。</div>`
      : `
        <div class="sources-table" role="table" aria-label="Data sources">
          ${sources
            .map((r) => {
              const id = lrEscapeHtml(r?.source_id || "—");
              const fetchedAt = lrEscapeHtml(lrFmtIsoLocal(r?.fetched_at));
              const status = lrEscapeHtml(r?.status || "—");
              const hash = lrEscapeHtml(r?.checksum_sha256_short || "—");
              return `
                <div class="sources-row" role="row">
                  <div class="sources-cell id" role="cell">${id}</div>
                  <div class="sources-cell ts" role="cell">${fetchedAt}</div>
                  <div class="sources-cell st" role="cell">${status}</div>
                  <div class="sources-cell hs" role="cell">${hash}</div>
                </div>
              `;
            })
            .join("")}
        </div>
      `;

  return `
    <div class="card sources-card">
      <div class="card-title">${lrEscapeHtml(title || "資料來源")}</div>
      <div class="card-body">
        <div class="sources-kv">
          <div class="sources-k">Analysis run</div><div>${lrEscapeHtml(runIdShort)} <span class="muted">(${lrEscapeHtml(runAt)})</span></div>
          <div class="sources-k">Config hash</div><div>${lrEscapeHtml(cfgHash)}</div>
          <div class="sources-k">Ingestion state</div><div>${lrEscapeHtml(state)}</div>
          <div class="sources-k">Last fetch</div><div>${lrEscapeHtml(lastFetch)}</div>
          <div class="sources-k">Modes</div><div>${lrEscapeHtml(modes.length ? modes.join("+") : "—")}</div>
          <div class="sources-k">429 count</div><div>${lrEscapeHtml(http429 === undefined ? "—" : String(http429))}</div>
          <div class="sources-k">Next retry</div><div>${lrEscapeHtml(retryAt)}</div>
          <div class="sources-k">Next window</div><div>${lrEscapeHtml(nextFetchWindow)}</div>
        </div>
        <div class="sources-subtitle">Sources index</div>
        ${rowsHtml}
        <div class="chart-tools" style="margin-top:10px">
          <button class="btn subtle" type="button" data-sources-action="toggle">展開詳細</button>
        </div>
        <div class="sources-details muted" data-sources-details style="display:none; margin-top:10px">載入中…</div>
      </div>
    </div>
  `;
}

function lrRenderSourcesCard(containerId, { title, health, meta } = {}) {
  const node = document.getElementById(containerId);
  if (!node) return;
  node.innerHTML = lrSourcesCardHtml({ title, health, meta });

  const btn = node.querySelector("[data-sources-action='toggle']");
  const detailNode = node.querySelector("[data-sources-details]");
  if (!btn || !detailNode) return;

  let loaded = false;
  let open = false;
  btn.addEventListener("click", async () => {
    open = !open;
    detailNode.style.display = open ? "" : "none";
    btn.textContent = open ? "收合詳細" : "展開詳細";
    if (!open || loaded) return;
    loaded = true;
    try {
      const full = await window.lrApi.fetchJson("/sources");
      const sources = Array.isArray(full?.sources) ? full.sources : [];
      if (sources.length === 0) {
        detailNode.textContent = "sources_index.json 沒有 sources。";
        return;
      }
      detailNode.innerHTML = sources
        .map((s) => {
          const id = lrEscapeHtml(s?.source_id || "—");
          const ts = lrEscapeHtml(lrFmtIsoLocal(s?.fetched_at));
          const st = lrEscapeHtml(s?.status || "—");
          const out = lrEscapeHtml(s?.output_path || "—");
          const sha = lrEscapeHtml(s?.checksum_sha256 || "—");
          const det = lrEscapeHtml(JSON.stringify(s?.details || {}, null, 2));
          return `
            <details class="sources-detail">
              <summary>${id} · ${st} · ${ts}</summary>
              <div class="muted" style="margin-top:6px">output: ${out}</div>
              <div class="muted">sha256: ${sha}</div>
              <pre class="code">${det}</pre>
            </details>
          `;
        })
        .join("");
    } catch (e) {
      console.warn(e);
      detailNode.textContent = "無法載入 sources 詳細（/sources）。";
    }
  });
}

window.lrRenderSourcesCard = lrRenderSourcesCard;
