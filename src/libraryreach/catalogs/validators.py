from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class CatalogValidationResult:
    errors: list[str]
    warnings: list[str]
    stats: dict[str, Any]

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and np.isnan(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _require_columns(df: pd.DataFrame, required: Iterable[str]) -> list[str]:
    missing = [c for c in required if c not in df.columns]
    return [f"Missing required column: {c}" for c in missing]


def _validate_unique_nonempty_id(df: pd.DataFrame, *, id_col: str, label: str) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    if id_col not in df.columns:
        return errors, warnings

    ids = df[id_col].astype(str)
    if (ids.str.strip() == "").any():
        errors.append(f"{label}: '{id_col}' contains empty values")

    dup = ids[ids.duplicated(keep=False)]
    if not dup.empty:
        examples = ", ".join(sorted(set(dup.tolist()))[:5])
        errors.append(f"{label}: '{id_col}' contains duplicates (e.g., {examples})")
    return errors, warnings


def _validate_lat_lon(df: pd.DataFrame, *, lat_col: str, lon_col: str, label: str) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    for c in [lat_col, lon_col]:
        if c not in df.columns:
            return errors, warnings

    lat = pd.to_numeric(df[lat_col], errors="coerce")
    lon = pd.to_numeric(df[lon_col], errors="coerce")
    if lat.isna().any() or lon.isna().any():
        errors.append(f"{label}: invalid lat/lon (non-numeric or missing)")

    # Global bounds (errors)
    if (lat < -90).any() or (lat > 90).any() or (lon < -180).any() or (lon > 180).any():
        errors.append(f"{label}: lat/lon out of valid world bounds")

    # Taiwan-ish bounds (warnings)
    if not df.empty:
        if ((lat < 18).any() or (lat > 28).any() or (lon < 116).any() or (lon > 124).any()):
            warnings.append(f"{label}: some lat/lon values are outside a Taiwan-ish bounding box (18..28, 116..124)")

    return errors, warnings


def _validate_nonempty_str(df: pd.DataFrame, *, col: str, label: str, required: bool) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    if col not in df.columns:
        return errors, warnings
    missing = df[col].apply(_is_missing)
    if missing.any():
        msg = f"{label}: '{col}' contains empty values"
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
    errors: list[str] = []
    warnings: list[str] = []

    required_cols = ["id", "name", "address", "lat", "lon", "city", "district"]
    errors.extend(_require_columns(libraries, required_cols))
    if errors:
        return CatalogValidationResult(errors=errors, warnings=warnings, stats={})

    e, w = _validate_unique_nonempty_id(libraries, id_col="id", label="libraries")
    errors.extend(e)
    warnings.extend(w)

    e, w = _validate_lat_lon(libraries, lat_col="lat", lon_col="lon", label="libraries")
    errors.extend(e)
    warnings.extend(w)

    for col in ["name", "city", "district"]:
        e, w = _validate_nonempty_str(libraries, col=col, label="libraries", required=True)
        errors.extend(e)
        warnings.extend(w)
    e, w = _validate_nonempty_str(libraries, col="address", label="libraries", required=False)
    errors.extend(e)
    warnings.extend(w)

    if allowed_cities is not None:
        cities = libraries["city"].astype(str).str.strip()
        unknown = sorted(set(cities.unique()) - set(allowed_cities))
        if unknown:
            errors.append(f"libraries: unknown city values not in config aoi.cities: {unknown}")

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
    errors: list[str] = []
    warnings: list[str] = []

    required_cols = ["id", "name", "type", "address", "lat", "lon", "city", "district"]
    errors.extend(_require_columns(outreach_candidates, required_cols))
    if errors:
        return CatalogValidationResult(errors=errors, warnings=warnings, stats={})

    e, w = _validate_unique_nonempty_id(outreach_candidates, id_col="id", label="outreach_candidates")
    errors.extend(e)
    warnings.extend(w)

    e, w = _validate_lat_lon(outreach_candidates, lat_col="lat", lon_col="lon", label="outreach_candidates")
    errors.extend(e)
    warnings.extend(w)

    for col in ["name", "type", "city", "district"]:
        e, w = _validate_nonempty_str(outreach_candidates, col=col, label="outreach_candidates", required=True)
        errors.extend(e)
        warnings.extend(w)
    e, w = _validate_nonempty_str(outreach_candidates, col="address", label="outreach_candidates", required=False)
    errors.extend(e)
    warnings.extend(w)

    if allowed_cities is not None:
        cities = outreach_candidates["city"].astype(str).str.strip()
        unknown = sorted(set(cities.unique()) - set(allowed_cities))
        if unknown:
            errors.append(f"outreach_candidates: unknown city values not in config aoi.cities: {unknown}")

    if allowed_types is not None:
        types = outreach_candidates["type"].astype(str).str.strip()
        unknown_types = sorted(set(types.unique()) - set(allowed_types))
        if unknown_types:
            errors.append(f"outreach_candidates: unknown type values not in config planning.outreach.allowed_candidate_types: {unknown_types}")

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
    errors: list[str] = []
    warnings: list[str] = []

    libs_cities = set(libraries["city"].astype(str).unique()) if "city" in libraries.columns else set()
    out_cities = (
        set(outreach_candidates["city"].astype(str).unique()) if "city" in outreach_candidates.columns else set()
    )
    union_cities = sorted(libs_cities | out_cities)

    if configured_cities:
        configured = list(map(str, configured_cities))
        missing_libs = sorted(set(configured) - libs_cities)
        if missing_libs:
            warnings.append(f"No libraries found for configured cities: {missing_libs}")
        missing_outreach = sorted(set(configured) - out_cities)
        if missing_outreach:
            warnings.append(f"No outreach candidates found for configured cities: {missing_outreach}")

    stats = {
        "catalog_cities_union": union_cities,
        "libraries_city_counts": libraries["city"].astype(str).value_counts().to_dict() if "city" in libraries.columns else {},
        "outreach_city_counts": outreach_candidates["city"].astype(str).value_counts().to_dict()
        if "city" in outreach_candidates.columns
        else {},
    }
    return CatalogValidationResult(errors=errors, warnings=warnings, stats=stats)

