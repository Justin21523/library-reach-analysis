from __future__ import annotations

import json
import logging
import math
from pathlib import Path
from typing import Any

import pandas as pd

from libraryreach.ingestion.sources_index import load_sources_index


def _log() -> logging.Logger:
    return logging.getLogger("libraryreach")


def _maybe_path(root: Path, raw: str | None) -> Path | None:
    if not raw:
        return None
    p = Path(str(raw))
    if not p.is_absolute():
        p = root / p
    return p


def _equirect_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    # Simple equirectangular approximation in meters (good enough for near-distance dedupe).
    rad = math.pi / 180.0
    x = (lon2 - lon1) * rad * math.cos(((lat1 + lat2) / 2.0) * rad)
    y = (lat2 - lat1) * rad
    return float(math.sqrt(x * x + y * y) * 6371000.0)


def _read_raw(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    if path.suffix.lower() in {".json"}:
        obj = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(obj, list):
            return pd.DataFrame(obj)
        if isinstance(obj, dict) and isinstance(obj.get("data"), list):
            return pd.DataFrame(obj["data"])
        raise ValueError(f"Unsupported JSON shape: {path}")
    return pd.read_csv(path)


def _map_columns(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    out = pd.DataFrame()
    for canonical, raw_col in (mapping or {}).items():
        if not raw_col:
            continue
        if raw_col in df.columns:
            out[canonical] = df[raw_col]
    return out


def _normalize_strings(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            continue
        df[c] = df[c].astype("string").str.strip()
    return df


def _coerce_lat_lon(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["lat", "lon"]:
        if c not in df.columns:
            continue
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _dedupe_exact(df: pd.DataFrame) -> pd.DataFrame:
    keys = [c for c in ["name", "address", "city", "district"] if c in df.columns]
    if not keys:
        return df
    return df.drop_duplicates(subset=keys, keep="first").reset_index(drop=True)


def _dedupe_nearby(df: pd.DataFrame, near_distance_m: float) -> pd.DataFrame:
    if near_distance_m <= 0:
        return df
    need = {"name", "city", "district", "lat", "lon"}
    if not need.issubset(set(df.columns)):
        return df

    df = df.reset_index(drop=True).copy()
    keep = [True] * len(df)

    # Group by name/city/district to reduce comparisons.
    for _, group in df.groupby(["name", "city", "district"], dropna=False, sort=False):
        idxs = list(group.index)
        for i in range(len(idxs)):
            if not keep[idxs[i]]:
                continue
            a = df.loc[idxs[i]]
            if pd.isna(a["lat"]) or pd.isna(a["lon"]):
                continue
            for j in range(i + 1, len(idxs)):
                if not keep[idxs[j]]:
                    continue
                b = df.loc[idxs[j]]
                if pd.isna(b["lat"]) or pd.isna(b["lon"]):
                    continue
                d = _equirect_m(float(a["lat"]), float(a["lon"]), float(b["lat"]), float(b["lon"]))
                if d <= float(near_distance_m):
                    keep[idxs[j]] = False

    return df[pd.Series(keep)].reset_index(drop=True)


def _ensure_id(df: pd.DataFrame) -> pd.DataFrame:
    if "id" in df.columns and df["id"].astype("string").str.strip().replace({"": None}).notna().all():
        df["id"] = df["id"].astype("string").str.strip()
        return df
    # Deterministic fallback ID if missing.
    base = (
        df.get("city", "").astype("string").fillna("").str.strip()
        + "|"
        + df.get("district", "").astype("string").fillna("").str.strip()
        + "|"
        + df.get("name", "").astype("string").fillna("").str.strip()
        + "|"
        + df.get("address", "").astype("string").fillna("").str.strip()
    )
    df["id"] = base.map(lambda s: f"LR-{abs(hash(str(s))) % (10**10):010d}").astype("string")
    return df


def build_libraries_catalog(settings: dict[str, Any]) -> Path:
    """
    Build `data/catalogs/libraries.csv` from an Open Data raw file.

    This keeps catalogs editable and version-controlled, while allowing raw ingestion
    to be refreshed independently and tracked via sources_index.json.
    """
    root = Path(settings["paths"]["root"])
    cfg = (settings.get("catalog_build", {}) or {}).get("libraries", {}) or {}

    raw_path = _maybe_path(root, str(cfg.get("raw_path") or "").strip())
    source_id = str(cfg.get("open_data_source_id") or "").strip()
    if raw_path is None and source_id:
        idx = load_sources_index(settings)
        for row in idx.get("sources", []) or []:
            if isinstance(row, dict) and row.get("source_id") == source_id:
                raw_path = _maybe_path(root, str(row.get("output_path") or "").strip())
                break

    if raw_path is None:
        raise ValueError("No raw library source configured (catalog_build.libraries.raw_path or open_data_source_id)")

    mapping = cfg.get("columns", {}) or {}
    if not isinstance(mapping, dict):
        raise ValueError("catalog_build.libraries.columns must be a mapping")

    out_path = _maybe_path(root, str(cfg.get("output_path") or "data/catalogs/libraries.csv")) or (root / "data/catalogs/libraries.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    raw = _read_raw(raw_path)
    df = _map_columns(raw, {str(k): str(v) for k, v in mapping.items()})

    df = _normalize_strings(df, ["id", "name", "address", "city", "district", "phone", "website", "library_system", "branch_type", "notes"])
    df = _coerce_lat_lon(df)
    df = df.dropna(subset=["name", "city", "district", "lat", "lon"]).copy()
    df = _ensure_id(df)
    df = _dedupe_exact(df)

    dedupe_cfg = cfg.get("dedupe", {}) or {}
    near_m = float(dedupe_cfg.get("near_distance_m", 0) or 0)
    df = _dedupe_nearby(df, near_distance_m=near_m)

    # Canonical column order (keep extras at the end).
    required = ["id", "name", "address", "lat", "lon", "city", "district"]
    optional = ["library_system", "branch_type", "phone", "website", "notes", "source"]
    cols = [c for c in required if c in df.columns] + [c for c in optional if c in df.columns]
    rest = [c for c in df.columns if c not in cols]
    df = df[cols + rest].copy()

    df.to_csv(out_path, index=False)
    _log().info("Built libraries catalog: %s rows -> %s", len(df), out_path)
    return out_path

