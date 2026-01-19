from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class SchemaReport:
    ok: bool
    errors: list[str]
    warnings: list[str]
    stats: dict[str, Any]


SCHEMA_VERSION = "phase1-v1"


def _require_cols(df: pd.DataFrame, required: list[str], *, label: str) -> list[str]:
    return [f"{label}: missing required column '{c}'" for c in required if c not in df.columns]


def _numeric_series(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col], errors="coerce") if col in df.columns else pd.Series(dtype=float)


def validate_libraries_scored(df: pd.DataFrame) -> SchemaReport:
    errors: list[str] = []
    warnings: list[str] = []
    required = ["id", "name", "lat", "lon", "city", "district", "accessibility_score"]
    errors.extend(_require_cols(df, required, label="libraries_scored"))
    if errors:
        return SchemaReport(ok=False, errors=errors, warnings=warnings, stats={})

    lat = _numeric_series(df, "lat")
    lon = _numeric_series(df, "lon")
    score = _numeric_series(df, "accessibility_score")
    missing_coords = int(lat.isna().sum() + lon.isna().sum())
    if missing_coords:
        warnings.append(f"libraries_scored: {missing_coords} missing lat/lon values (map may omit these rows)")
    if ((score < 0) | (score > 100)).any():
        warnings.append("libraries_scored: accessibility_score out of 0..100 range detected")

    stats = {
        "rows": int(len(df)),
        "cities": df["city"].astype(str).value_counts().to_dict() if "city" in df.columns else {},
        "score_min": float(score.min()) if not score.empty else None,
        "score_max": float(score.max()) if not score.empty else None,
    }
    return SchemaReport(ok=len(errors) == 0, errors=errors, warnings=warnings, stats=stats)


def validate_deserts(df: pd.DataFrame) -> SchemaReport:
    errors: list[str] = []
    warnings: list[str] = []
    required = [
        "cell_id",
        "city",
        "centroid_lat",
        "centroid_lon",
        "effective_score_0_100",
        "is_desert",
        "gap_to_threshold",
        "best_library_id",
        "best_library_distance_m",
    ]
    errors.extend(_require_cols(df, required, label="deserts"))
    if errors:
        return SchemaReport(ok=False, errors=errors, warnings=warnings, stats={})

    eff = _numeric_series(df, "effective_score_0_100")
    gap = _numeric_series(df, "gap_to_threshold")
    if ((eff < 0) | (eff > 100)).any():
        warnings.append("deserts: effective_score_0_100 out of 0..100 range detected")
    if (gap < 0).any():
        warnings.append("deserts: negative gap_to_threshold detected")

    stats = {
        "rows": int(len(df)),
        "deserts": int((df["is_desert"] == True).sum()) if "is_desert" in df.columns else 0,  # noqa: E712
        "cities": df["city"].astype(str).value_counts().to_dict() if "city" in df.columns else {},
    }
    return SchemaReport(ok=len(errors) == 0, errors=errors, warnings=warnings, stats=stats)


def validate_outreach_recommendations(df: pd.DataFrame) -> SchemaReport:
    errors: list[str] = []
    warnings: list[str] = []
    if df.empty:
        return SchemaReport(ok=True, errors=[], warnings=["outreach: empty recommendations"], stats={"rows": 0})

    required = ["id", "name", "lat", "lon", "city", "district", "outreach_score"]
    errors.extend(_require_cols(df, required, label="outreach"))
    if errors:
        return SchemaReport(ok=False, errors=errors, warnings=warnings, stats={})

    score = _numeric_series(df, "outreach_score")
    if score.isna().any():
        warnings.append("outreach: non-numeric outreach_score detected")
    if (score < 0).any():
        warnings.append("outreach: negative outreach_score detected")

    # Optional explainability fields.
    for col, min_v, max_v in [
        ("coverage_score_0_100", 0, 100),
        ("site_access_score", 0, 100),
        ("contribution_coverage", 0, 100),
        ("contribution_site_access", 0, 100),
    ]:
        if col in df.columns:
            s = _numeric_series(df, col)
            if s.isna().any():
                warnings.append(f"outreach: non-numeric {col} detected")
            if ((s < min_v) | (s > max_v)).any():
                warnings.append(f"outreach: {col} out of {min_v}..{max_v} range detected")

    for col in ["covered_desert_cells", "covered_gap_sum", "weight_coverage", "weight_site_access", "recommendation_explain"]:
        if col not in df.columns:
            warnings.append(f"outreach: missing optional column '{col}' (explainability reduced)")

    stats = {
        "rows": int(len(df)),
        "cities": df["city"].astype(str).value_counts().to_dict() if "city" in df.columns else {},
        "score_min": float(score.min()) if not score.empty else None,
        "score_max": float(score.max()) if not score.empty else None,
    }
    return SchemaReport(ok=len(errors) == 0, errors=errors, warnings=warnings, stats=stats)


def validate_phase1_outputs(
    *,
    libraries_scored: pd.DataFrame,
    deserts: pd.DataFrame,
    outreach_recommendations: pd.DataFrame,
) -> dict[str, Any]:
    lib = validate_libraries_scored(libraries_scored)
    des = validate_deserts(deserts)
    out = validate_outreach_recommendations(outreach_recommendations)
    ok = bool(lib.ok and des.ok and out.ok)
    return {
        "ok": ok,
        "schema_version": SCHEMA_VERSION,
        "libraries_scored": lib.__dict__,
        "deserts": des.__dict__,
        "outreach_recommendations": out.__dict__,
    }
