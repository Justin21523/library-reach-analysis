from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def _rename_common_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename: dict[str, str] = {}
    if "lat" not in df.columns and "latitude" in df.columns:
        rename["latitude"] = "lat"
    if "lon" not in df.columns and "longitude" in df.columns:
        rename["longitude"] = "lon"
    if "lon" not in df.columns and "lng" in df.columns:
        rename["lng"] = "lon"
    return df.rename(columns=rename) if rename else df


def _strip_string_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for c in columns:
        if c not in df.columns:
            continue
        df[c] = df[c].astype("string").str.strip()
    return df


def _normalize_city(df: pd.DataFrame, *, aliases: dict[str, str] | None) -> pd.DataFrame:
    if "city" not in df.columns:
        return df
    if not aliases:
        return df
    alias_map = {str(k).strip(): str(v).strip() for k, v in aliases.items()}

    def map_city(v: Any) -> Any:
        if v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v):
            return v
        s = str(v).strip()
        return alias_map.get(s, s)

    df["city"] = df["city"].map(map_city)
    return df


def _normalize_candidate_type(df: pd.DataFrame) -> pd.DataFrame:
    if "type" not in df.columns:
        return df
    t = df["type"].astype("string").str.strip().str.lower()
    t = t.str.replace("-", "_", regex=False).str.replace(" ", "_", regex=False)
    df["type"] = t
    return df


def _coerce_lat_lon(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["lat", "lon"]:
        if c not in df.columns:
            continue
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def load_libraries_catalog(settings: dict[str, Any]) -> pd.DataFrame:
    catalogs_dir = Path(settings["paths"]["catalogs_dir"])
    path = catalogs_dir / "libraries.csv"
    df = pd.read_csv(path)
    df = _rename_common_columns(df)
    df = _strip_string_columns(df, ["id", "name", "address", "city", "district"])
    df = _normalize_city(df, aliases=settings.get("aoi", {}).get("city_aliases"))
    df = _coerce_lat_lon(df)
    if "id" in df.columns:
        df["id"] = df["id"].astype("string").str.strip()
    return df


def load_outreach_candidates_catalog(settings: dict[str, Any]) -> pd.DataFrame:
    catalogs_dir = Path(settings["paths"]["catalogs_dir"])
    path = catalogs_dir / "outreach_candidates.csv"
    df = pd.read_csv(path)
    df = _rename_common_columns(df)
    df = _strip_string_columns(df, ["id", "name", "type", "address", "city", "district"])
    df = _normalize_city(df, aliases=settings.get("aoi", {}).get("city_aliases"))
    df = _normalize_candidate_type(df)
    df = _coerce_lat_lon(df)
    if "id" in df.columns:
        df["id"] = df["id"].astype("string").str.strip()
    return df

