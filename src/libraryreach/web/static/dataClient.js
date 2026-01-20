const _lrMemCache = new Map();

function _lrNowMs() {
  return Date.now();
}

function _lrCacheStorageKey(url) {
  return `lr_api_cache_v1::${url}`;
}

function _lrLoadLocal(url) {
  try {
    const raw = localStorage.getItem(_lrCacheStorageKey(url));
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return null;
    return parsed;
  } catch {
    return null;
  }
}

function _lrSaveLocal(url, value) {
  try {
    localStorage.setItem(_lrCacheStorageKey(url), JSON.stringify(value));
  } catch {
    // ignore storage errors
  }
}

async function _lrFetchJsonWithHeaders(url, options) {
  const res = await fetch(url, options);
  if (res.status === 304) return { status: 304, data: null, etag: null };
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`${url} -> ${res.status}${txt ? `: ${txt.slice(0, 200)}` : ""}`);
  }
  const data = await res.json();
  const etag = res.headers.get("ETag");
  return { status: res.status, data, etag };
}

function lrFetchJson(path, options) {
  return _lrFetchJsonWithHeaders(path, options).then((r) => r.data);
}

async function _lrCachedGetJson(url, { ttlMs }) {
  const now = _lrNowMs();
  const mem = _lrMemCache.get(url);
  if (mem && typeof mem.ts === "number" && now - mem.ts < ttlMs) return mem.data;

  const local = _lrLoadLocal(url);
  if (local && typeof local.ts === "number" && now - local.ts < ttlMs) {
    _lrMemCache.set(url, local);
    return local.data;
  }

  const candidate = mem || local;
  const headers = {};
  if (candidate?.etag) headers["If-None-Match"] = String(candidate.etag);

  try {
    const r = await _lrFetchJsonWithHeaders(url, { headers, cache: "no-store" });
    if (r.status === 304 && candidate?.data !== undefined) {
      const out = { ts: now, etag: candidate.etag || null, data: candidate.data };
      _lrMemCache.set(url, out);
      _lrSaveLocal(url, out);
      return out.data;
    }
    const out = { ts: now, etag: r.etag || null, data: r.data };
    _lrMemCache.set(url, out);
    _lrSaveLocal(url, out);
    return out.data;
  } catch (e) {
    if (candidate?.data !== undefined) return candidate.data;
    throw e;
  }
}

function lrGetMeta() {
  return _lrCachedGetJson("/meta", { ttlMs: 30_000 });
}

function lrGetHealth() {
  return _lrCachedGetJson("/health", { ttlMs: 10_000 });
}

function lrGetBaselineSummary({ scenario, cities, topNOutreach } = {}) {
  const qs = new URLSearchParams();
  if (scenario) qs.set("scenario", String(scenario));
  for (const c of cities || []) qs.append("cities", String(c));
  if (topNOutreach !== undefined && topNOutreach !== null) qs.set("top_n_outreach", String(topNOutreach));
  return _lrCachedGetJson(`/analysis/baseline-summary?${qs.toString()}`, { ttlMs: 60_000 });
}

function lrPostCompare({ scenario, cities, configPatch } = {}) {
  return lrFetchJson("/analysis/compare", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      scenario: scenario || "weekday",
      cities: cities || null,
      config_patch: configPatch || {},
    }),
  });
}

window.lrApi = {
  fetchJson: lrFetchJson,
  getMeta: lrGetMeta,
  getHealth: lrGetHealth,
  getBaselineSummary: lrGetBaselineSummary,
  postCompare: lrPostCompare,
};
