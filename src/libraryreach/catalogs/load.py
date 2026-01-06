"""
Catalog loading and normalization (Phase 2: Loading).

We keep our core "catalogs" (libraries and outreach candidates) as simple CSV
files so they can be edited by non-developers, version-controlled, and reused
across multiple cities.

This module is intentionally *not* the place where we enforce strict rules.
Instead, it focuses on:
- loading CSV files into pandas DataFrames,
- normalizing common column names and string fields, and
- applying "city alias" mappings so multi-city analysis uses consistent codes.

Hard validation (required columns, ranges, allowed values) lives in
`libraryreach.catalogs.validators` so "load" and "validate" stay decoupled.
"""

from __future__ import annotations

# `Path` keeps file path joins cross-platform and readable.
from pathlib import Path
# `Any` is used because our YAML-derived settings are not strongly typed yet.
from typing import Any

# `pandas` is our table engine for CSV I/O and column normalization.
import pandas as pd


def _rename_common_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Build a rename mapping only when we detect alternate names.
    rename: dict[str, str] = {}
    # Some sources use `latitude`/`longitude` instead of `lat`/`lon`.
    if "lat" not in df.columns and "latitude" in df.columns:
        # Canonicalize to `lat` so downstream code can assume this column name.
        rename["latitude"] = "lat"
    if "lon" not in df.columns and "longitude" in df.columns:
        # Canonicalize to `lon` to match the rest of the pipeline.
        rename["longitude"] = "lon"
    if "lon" not in df.columns and "lng" in df.columns:
        # `lng` is a common alias in web mapping UIs; we normalize it for analysis code.
        rename["lng"] = "lon"
    # Avoid creating a new DataFrame when no renames are needed (slightly faster and clearer).
    return df.rename(columns=rename) if rename else df


def _strip_string_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    # Normalize "string-ish" columns by trimming whitespace for stable joins and comparisons.
    for c in columns:
        # Skip optional columns that are not present in this particular CSV.
        if c not in df.columns:
            continue
        # Use pandas `string` dtype so missing values stay as `<NA>` (not the literal "nan").
        df[c] = df[c].astype("string").str.strip()
    # Return the same DataFrame to enable fluent `df = _fn(df)` chaining.
    return df


def _normalize_city(df: pd.DataFrame, *, aliases: dict[str, str] | None) -> pd.DataFrame:
    # If the dataset has no `city` column, there is nothing to normalize.
    if "city" not in df.columns:
        return df
    # If no alias mapping is configured, keep the user-provided city values as-is.
    if not aliases:
        return df
    # Pre-normalize alias keys/values so "臺北市" and " 臺北市 " are treated the same.
    alias_map = {str(k).strip(): str(v).strip() for k, v in aliases.items()}

    def map_city(v: Any) -> Any:
        # Preserve missing values so validation can report them instead of silently filling.
        if v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v):
            return v
        # Convert to a trimmed string to make alias lookups deterministic.
        s = str(v).strip()
        # Replace known aliases, otherwise return the cleaned original value.
        return alias_map.get(s, s)

    # Apply the mapping row-by-row; for our small catalogs this is fast and very explicit.
    df["city"] = df["city"].map(map_city)
    # Return the mutated DataFrame for chaining (pandas operations are often "in-place-ish").
    return df


def _normalize_candidate_type(df: pd.DataFrame) -> pd.DataFrame:
    # Outreach candidate catalogs use `type` for planning rules; normalize it for config matching.
    if "type" not in df.columns:
        return df
    # Lowercasing avoids separate config entries for "School" vs "school".
    t = df["type"].astype("string").str.strip().str.lower()
    # Convert human-friendly labels into snake_case so types work well as stable identifiers.
    t = t.str.replace("-", "_", regex=False).str.replace(" ", "_", regex=False)
    # Overwrite the column with normalized values so validators and planners see consistent types.
    df["type"] = t
    return df


def _coerce_lat_lon(df: pd.DataFrame) -> pd.DataFrame:
    # Coordinates must be numeric for spatial math; coercion turns bad inputs into NaN.
    for c in ["lat", "lon"]:
        # If a column is missing we leave it to the validator to report a schema error.
        if c not in df.columns:
            continue
        # `errors="coerce"` is safer than throwing: we want a clean validation report later.
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def load_libraries_catalog(settings: dict[str, Any]) -> pd.DataFrame:
    # Read the catalogs directory from settings so tests can point at temporary folders.
    catalogs_dir = Path(settings["paths"]["catalogs_dir"])
    # Keep the filename stable so the rest of the pipeline and docs can rely on it.
    path = catalogs_dir / "libraries.csv"
    # Load the CSV into a DataFrame; we assume the first row contains headers.
    df = pd.read_csv(path)
    # Normalize common column aliases like `latitude`/`longitude` to `lat`/`lon`.
    df = _rename_common_columns(df)
    # Trim key fields so IDs and city names do not contain invisible whitespace.
    df = _strip_string_columns(df, ["id", "name", "address", "city", "district"])
    # Map Chinese city names to canonical TDX city codes (multi-city consistency).
    df = _normalize_city(df, aliases=settings.get("aoi", {}).get("city_aliases"))
    # Coerce coordinates to numbers so spatial code can rely on `float`-like values.
    df = _coerce_lat_lon(df)
    # Re-normalize IDs defensively (CSV inference can sometimes surprise us).
    if "id" in df.columns:
        # IDs are treated as stable string keys, never as numbers.
        df["id"] = df["id"].astype("string").str.strip()
    # Return normalized data; strict validation happens in `catalogs.validators`.
    return df


def load_outreach_candidates_catalog(settings: dict[str, Any]) -> pd.DataFrame:
    # Read the catalogs directory from settings so tests can point at temporary folders.
    catalogs_dir = Path(settings["paths"]["catalogs_dir"])
    # Keep the filename stable so the rest of the pipeline and docs can rely on it.
    path = catalogs_dir / "outreach_candidates.csv"
    # Load the CSV into a DataFrame; we assume the first row contains headers.
    df = pd.read_csv(path)
    # Normalize common column aliases like `lng` to `lon` for consistent geospatial handling.
    df = _rename_common_columns(df)
    # Trim key fields so IDs and city/district labels are stable for grouping.
    df = _strip_string_columns(df, ["id", "name", "type", "address", "city", "district"])
    # Map Chinese city names to canonical TDX city codes (multi-city consistency).
    df = _normalize_city(df, aliases=settings.get("aoi", {}).get("city_aliases"))
    # Normalize candidate `type` so config filtering (allowed types) is predictable.
    df = _normalize_candidate_type(df)
    # Coerce coordinates to numbers so spatial code can rely on `float`-like values.
    df = _coerce_lat_lon(df)
    # Re-normalize IDs defensively (CSV inference can sometimes surprise us).
    if "id" in df.columns:
        # IDs are treated as stable string keys, never as numbers.
        df["id"] = df["id"].astype("string").str.strip()
    # Return normalized data; strict validation happens in `catalogs.validators`.
    return df
