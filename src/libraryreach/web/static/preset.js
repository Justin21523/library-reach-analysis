// Shared preset helpers for /console, /brief, /results, / (homepage).
// Uses URL-safe base64(JSON) in the `lr` query param.

function lrToUrlSafeBase64(bytes) {
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
}

function lrFromUrlSafeBase64(s) {
  const base64 = String(s || "").replaceAll("-", "+").replaceAll("_", "/");
  const pad = base64.length % 4 === 0 ? "" : "=".repeat(4 - (base64.length % 4));
  const binary = atob(base64 + pad);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}

function lrEncodePreset(preset) {
  const bytes = new TextEncoder().encode(JSON.stringify(preset));
  return lrToUrlSafeBase64(bytes);
}

function lrDecodePreset(raw) {
  const bytes = lrFromUrlSafeBase64(raw);
  const json = new TextDecoder().decode(bytes);
  return JSON.parse(json);
}

function lrReadPresetFromUrl() {
  const q = new URLSearchParams(window.location.search);
  const raw = q.get("lr");
  if (!raw) return null;
  try {
    const p = lrDecodePreset(raw);
    if (!p || typeof p !== "object") return null;
    return p;
  } catch {
    return null;
  }
}

function lrBuildPreset({ scenario, cities, patch }) {
  const s = String(scenario || "weekday");
  const cs = Array.isArray(cities) ? cities.map(String) : [];
  const p = patch && typeof patch === "object" ? patch : {};
  return { scenario: s, cities: cs, patch: p };
}

function lrBuildLink(path, preset) {
  const q = new URLSearchParams();
  q.set("lr", lrEncodePreset(preset));
  return `${path}?${q.toString()}`;
}

