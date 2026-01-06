"""
Catalog validation rules (Phase 2: Loading).

This module checks that our project-owned catalogs (CSV) are usable for analysis:
- schema checks (required columns exist),
- data quality checks (IDs are unique, coordinates are valid),
- config consistency checks (cities and types match what we configured).

Instead of raising immediately, we return a structured result containing:
- `errors`: must-fix issues that should block analysis runs,
- `warnings`: suspicious but not always fatal issues,
- `stats`: small summaries that are helpful in reports and debugging.
"""

from __future__ import annotations

# `dataclass` gives us a small immutable "result object" without boilerplate.
from dataclasses import dataclass
# Typing helpers keep function signatures readable for beginners.
from typing import Any, Iterable

# NumPy is used for robust NaN checks on numeric values.
import numpy as np
# pandas is our table container; it also provides NA detection utilities.
import pandas as pd


@dataclass(frozen=True)
class CatalogValidationResult:
    # Human-readable error messages that should block the pipeline.
    errors: list[str]
    # Human-readable warning messages that should be reviewed but may be acceptable.
    warnings: list[str]
    # Small machine-readable stats summaries (counts, distinct values, etc.).
    stats: dict[str, Any]

    @property
    def ok(self) -> bool:
        # Treat "no errors" as success; warnings do not fail validation by design.
        return len(self.errors) == 0


def _is_missing(value: Any) -> bool:
    # pandas' NA detection covers many cases (None, NaN, pd.NA) across dtypes.
    if pd.isna(value):
        return True
    # Keep explicit `None` handling for readability and for non-pandas callers.
    if value is None:
        return True
    # NumPy NaN checks only apply to floats; the `isinstance` guard prevents TypeErrors.
    if isinstance(value, float) and np.isnan(value):
        return True
    # Empty strings are "missing" for our catalogs because they break grouping and joins.
    if isinstance(value, str) and value.strip() == "":
        return True
    # Any other value is considered present.
    return False


def _require_columns(df: pd.DataFrame, required: Iterable[str]) -> list[str]:
    # Compute which required columns are absent so we can return a clear schema error list.
    missing = [c for c in required if c not in df.columns]
    # Format errors as user-facing messages (easy to print in CLI reports).
    return [f"Missing required column: {c}" for c in missing]


def _validate_unique_nonempty_id(df: pd.DataFrame, *, id_col: str, label: str) -> tuple[list[str], list[str]]:
    # IDs are the primary keys used for joining results back to catalogs.
    errors: list[str] = []
    warnings: list[str] = []

    # If the catalog has no ID column, we cannot validate uniqueness here.
    if id_col not in df.columns:
        return errors, warnings

    # Normalize IDs as trimmed strings so " L-001 " and "L-001" are treated the same.
    ids = df[id_col].astype("string").str.strip()
    # Missing IDs are fatal because they prevent stable referencing and explainability.
    missing = ids.isna() | (ids == "")
    if missing.any():
        errors.append(f"{label}: '{id_col}' contains empty values")

    # Duplicated IDs are fatal because they make results ambiguous.
    dup = ids[~missing][ids[~missing].duplicated(keep=False)]
    if not dup.empty:
        # Include a few examples so users can locate the problematic rows quickly.
        examples = ", ".join(sorted(set(dup.tolist()))[:5])
        errors.append(f"{label}: '{id_col}' contains duplicates (e.g., {examples})")
    return errors, warnings


def _validate_lat_lon(df: pd.DataFrame, *, lat_col: str, lon_col: str, label: str) -> tuple[list[str], list[str]]:
    # Spatial analysis requires numeric coordinates; we treat missing/invalid coords as errors.
    errors: list[str] = []
    warnings: list[str] = []
    # If either column is missing, schema validation will catch it elsewhere.
    for c in [lat_col, lon_col]:
        if c not in df.columns:
            return errors, warnings

    # Convert to numeric so we can run range checks even if the CSV had strings.
    lat = pd.to_numeric(df[lat_col], errors="coerce")
    lon = pd.to_numeric(df[lon_col], errors="coerce")
    # Any NaNs after coercion indicate missing or non-numeric input.
    if lat.isna().any() or lon.isna().any():
        errors.append(f"{label}: invalid lat/lon (non-numeric or missing)")

    # Global WGS84 bounds are hard errors because they indicate definitely-wrong data.
    if (lat < -90).any() or (lat > 90).any() or (lon < -180).any() or (lon > 180).any():
        errors.append(f"{label}: lat/lon out of valid world bounds")

    # A Taiwan-ish bounding box is only a warning: this repo supports multi-city,
    # but most expected use cases are within Taiwan.
    if not df.empty:
        if ((lat < 18).any() or (lat > 28).any() or (lon < 116).any() or (lon > 124).any()):
            warnings.append(f"{label}: some lat/lon values are outside a Taiwan-ish bounding box (18..28, 116..124)")

    return errors, warnings


def _validate_nonempty_str(df: pd.DataFrame, *, col: str, label: str, required: bool) -> tuple[list[str], list[str]]:
    # String fields are used for UI display and grouping; we want to catch empty values early.
    errors: list[str] = []
    warnings: list[str] = []
    # If the column is missing, schema validation will catch it elsewhere.
    if col not in df.columns:
        return errors, warnings
    # Apply our "missing" predicate across values so we handle `<NA>`, None, and empty strings.
    missing = df[col].apply(_is_missing)
    if missing.any():
        # Use one message per column to keep reports short and readable.
        msg = f"{label}: '{col}' contains empty values"
        # Treat required fields as errors; optional fields become warnings.
        if required:
            errors.append(msg)
        else:
            warnings.append(msg)
    return errors, warnings


def validate_libraries_catalog(
    libraries: pd.DataFrame,
    *,
    allowed_cities: set[str] | None = None,
) -> CatalogValidationResult:
    # The libraries catalog is the "supply" side of accessibility: where our branches are.
    errors: list[str] = []
    warnings: list[str] = []

    # Phase 1 requires a minimal schema that supports geospatial joins and explainability.
    required_cols = ["id", "name", "address", "lat", "lon", "city", "district"]
    errors.extend(_require_columns(libraries, required_cols))
    if errors:
        # Early return avoids confusing downstream KeyErrors when required columns are missing.
        return CatalogValidationResult(errors=errors, warnings=warnings, stats={})

    # IDs must be unique so we can join scores and explanations back to the catalog row.
    e, w = _validate_unique_nonempty_id(libraries, id_col="id", label="libraries")
    errors.extend(e)
    warnings.extend(w)

    # Validate coordinates so spatial computations do not produce nonsense results.
    e, w = _validate_lat_lon(libraries, lat_col="lat", lon_col="lon", label="libraries")
    errors.extend(e)
    warnings.extend(w)

    # Enforce non-empty display and grouping columns.
    for col in ["name", "city", "district"]:
        e, w = _validate_nonempty_str(libraries, col=col, label="libraries", required=True)
        errors.extend(e)
        warnings.extend(w)
    # Address is useful for humans but not strictly required for baseline computations.
    e, w = _validate_nonempty_str(libraries, col="address", label="libraries", required=False)
    errors.extend(e)
    warnings.extend(w)

    # If configured, ensure catalog cities match our configured multi-city AOI list.
    if allowed_cities is not None:
        # Normalize to trimmed strings to avoid "Taipei " being treated as a separate value.
        cities = libraries["city"].astype("string").str.strip().dropna()
        # Unknown cities should fail because later ingestion (TDX calls) depend on correct codes.
        unknown = sorted(set(cities.unique()) - set(map(str, allowed_cities)))
        if unknown:
            errors.append(f"libraries: unknown city values not in config aoi.cities: {unknown}")

    # Stats are included in reports to quickly spot imbalance across cities/districts.
    stats = {
        "rows": int(len(libraries)),
        "cities": libraries["city"].astype(str).value_counts().to_dict(),
        "districts": libraries["district"].astype(str).value_counts().to_dict(),
    }
    return CatalogValidationResult(errors=errors, warnings=warnings, stats=stats)


def validate_outreach_candidates_catalog(
    outreach_candidates: pd.DataFrame,
    *,
    allowed_cities: set[str] | None = None,
    allowed_types: set[str] | None = None,
) -> CatalogValidationResult:
    # Outreach candidates are the "planning" side: where we could deploy mobile/pop-up services.
    errors: list[str] = []
    warnings: list[str] = []

    # Candidates need a minimal schema similar to libraries, plus a `type` for policy filtering.
    required_cols = ["id", "name", "type", "address", "lat", "lon", "city", "district"]
    errors.extend(_require_columns(outreach_candidates, required_cols))
    if errors:
        # Early return avoids confusing downstream KeyErrors when required columns are missing.
        return CatalogValidationResult(errors=errors, warnings=warnings, stats={})

    # IDs must be unique so we can reference candidates in planning outputs.
    e, w = _validate_unique_nonempty_id(outreach_candidates, id_col="id", label="outreach_candidates")
    errors.extend(e)
    warnings.extend(w)

    # Validate coordinates so spatial computations do not produce nonsense results.
    e, w = _validate_lat_lon(outreach_candidates, lat_col="lat", lon_col="lon", label="outreach_candidates")
    errors.extend(e)
    warnings.extend(w)

    # Enforce non-empty display and grouping columns.
    for col in ["name", "type", "city", "district"]:
        e, w = _validate_nonempty_str(outreach_candidates, col=col, label="outreach_candidates", required=True)
        errors.extend(e)
        warnings.extend(w)
    # Address is useful for humans but not strictly required for baseline computations.
    e, w = _validate_nonempty_str(outreach_candidates, col="address", label="outreach_candidates", required=False)
    errors.extend(e)
    warnings.extend(w)

    # Ensure candidate cities match configured AOI city list when provided.
    if allowed_cities is not None:
        cities = outreach_candidates["city"].astype("string").str.strip().dropna()
        unknown = sorted(set(cities.unique()) - set(map(str, allowed_cities)))
        if unknown:
            errors.append(f"outreach_candidates: unknown city values not in config aoi.cities: {unknown}")

    # Ensure candidate types match configured allowed types when provided.
    if allowed_types is not None:
        types = outreach_candidates["type"].astype("string").str.strip().dropna()
        unknown_types = sorted(set(types.unique()) - set(map(str, allowed_types)))
        if unknown_types:
            errors.append(f"outreach_candidates: unknown type values not in config planning.outreach.allowed_candidate_types: {unknown_types}")

    # Stats are included in reports to quickly spot imbalance across cities/types.
    stats = {
        "rows": int(len(outreach_candidates)),
        "cities": outreach_candidates["city"].astype(str).value_counts().to_dict(),
        "types": outreach_candidates["type"].astype(str).value_counts().to_dict(),
    }
    return CatalogValidationResult(errors=errors, warnings=warnings, stats=stats)


def validate_multi_city_consistency(
    *,
    libraries: pd.DataFrame,
    outreach_candidates: pd.DataFrame,
    configured_cities: list[str] | None,
) -> CatalogValidationResult:
    # Multi-city analysis is config-driven; this helper warns when catalogs do not cover configured cities.
    errors: list[str] = []
    warnings: list[str] = []

    # Collect the set of cities seen in each catalog (empty if column is missing).
    libs_cities = (
        set(libraries["city"].astype("string").dropna().unique()) if "city" in libraries.columns else set()
    )
    out_cities = (
        set(outreach_candidates["city"].astype("string").dropna().unique())
        if "city" in outreach_candidates.columns
        else set()
    )
    # The union is useful for reports so we can see which cities appear anywhere.
    union_cities = sorted(libs_cities | out_cities)

    # If the user configured cities explicitly, warn when some cities have no rows.
    if configured_cities:
        # Normalize to strings so config entries like 123 do not break comparisons.
        configured = list(map(str, configured_cities))
        # Missing libraries means we cannot score accessibility for that city.
        missing_libs = sorted(set(configured) - libs_cities)
        if missing_libs:
            warnings.append(f"No libraries found for configured cities: {missing_libs}")
        # Missing outreach candidates means planning suggestions will be incomplete for that city.
        missing_outreach = sorted(set(configured) - out_cities)
        if missing_outreach:
            warnings.append(f"No outreach candidates found for configured cities: {missing_outreach}")

    # Keep a compact stats block for reporting and debugging.
    stats = {
        "catalog_cities_union": union_cities,
        "libraries_city_counts": libraries["city"].astype(str).value_counts().to_dict() if "city" in libraries.columns else {},
        "outreach_city_counts": outreach_candidates["city"].astype(str).value_counts().to_dict()
        if "city" in outreach_candidates.columns
        else {},
    }
    return CatalogValidationResult(errors=errors, warnings=warnings, stats=stats)
