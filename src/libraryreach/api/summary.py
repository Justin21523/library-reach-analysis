from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class BBox:
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_bbox(raw: str | None) -> BBox | None:
    if not raw:
        return None
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) != 4:
        raise ValueError("bbox must be 'minLon,minLat,maxLon,maxLat'")
    try:
        min_lon, min_lat, max_lon, max_lat = (float(p) for p in parts)
    except ValueError as e:
        raise ValueError("bbox values must be numbers") from e
    if min_lon > max_lon or min_lat > max_lat:
        raise ValueError("bbox min must be <= max")
    return BBox(min_lon=min_lon, min_lat=min_lat, max_lon=max_lon, max_lat=max_lat)


def _city_set(cities: list[str] | None) -> set[str] | None:
    if not cities:
        return None
    return {str(c) for c in cities}


def filter_df_by_cities(df: pd.DataFrame, cities: list[str] | None, *, city_col: str = "city") -> pd.DataFrame:
    city_filter = _city_set(cities)
    if not city_filter or city_col not in df.columns:
        return df
    return df[df[city_col].astype(str).isin(city_filter)].copy()


def score_histogram(scores: pd.Series, *, bins: list[float] | None = None) -> dict[str, Any]:
    bins = bins or [0, 20, 40, 60, 80, 100]
    s = pd.to_numeric(scores, errors="coerce").dropna()
    if s.empty:
        return {"bins": bins, "counts": [0] * (len(bins) - 1)}
    counts = pd.cut(s, bins=bins, include_lowest=True, right=True).value_counts(sort=False).tolist()
    return {"bins": bins, "counts": [int(x) for x in counts]}


def numeric_histogram(values: pd.Series, *, bins: list[float]) -> dict[str, Any]:
    s = pd.to_numeric(values, errors="coerce").dropna()
    if s.empty:
        return {"bins": bins, "counts": [0] * (len(bins) - 1)}
    counts = pd.cut(s, bins=bins, include_lowest=True, right=True).value_counts(sort=False).tolist()
    return {"bins": bins, "counts": [int(x) for x in counts]}


def deserts_distributions(deserts: pd.DataFrame) -> dict[str, Any]:
    if deserts.empty:
        return {
            "effective_score_hist": {"bins": [0, 20, 40, 60, 80, 100], "counts": [0, 0, 0, 0, 0]},
            "gap_hist": {"bins": [0, 5, 10, 20, 30, 50, 100], "counts": [0, 0, 0, 0, 0, 0]},
            "best_distance_hist_m": {"bins": [0, 500, 1000, 2000, 3000, 5000, 10000], "counts": [0, 0, 0, 0, 0, 0]},
        }
    is_desert = deserts["is_desert"] if "is_desert" in deserts.columns else pd.Series([True] * len(deserts))
    d = deserts[is_desert == True].copy()  # noqa: E712
    eff = d["effective_score_0_100"] if "effective_score_0_100" in d.columns else pd.Series(dtype=float)
    gap = d["gap_to_threshold"] if "gap_to_threshold" in d.columns else pd.Series(dtype=float)
    dist = d["best_library_distance_m"] if "best_library_distance_m" in d.columns else pd.Series(dtype=float)
    return {
        "effective_score_hist": numeric_histogram(eff, bins=[0, 20, 40, 60, 80, 100]),
        "gap_hist": numeric_histogram(gap, bins=[0, 5, 10, 20, 30, 50, 100]),
        "best_distance_hist_m": numeric_histogram(dist, bins=[0, 500, 1000, 2000, 3000, 5000, 10000]),
    }


def outreach_distributions(outreach: pd.DataFrame) -> dict[str, Any]:
    if outreach.empty:
        return {
            "outreach_score_hist": {"bins": [0, 20, 40, 60, 80, 100], "counts": [0, 0, 0, 0, 0]},
            "coverage_score_hist": {"bins": [0, 20, 40, 60, 80, 100], "counts": [0, 0, 0, 0, 0]},
            "site_access_score_hist": {"bins": [0, 20, 40, 60, 80, 100], "counts": [0, 0, 0, 0, 0]},
        }
    return {
        "outreach_score_hist": numeric_histogram(outreach.get("outreach_score", pd.Series(dtype=float)), bins=[0, 20, 40, 60, 80, 100]),
        "coverage_score_hist": numeric_histogram(outreach.get("coverage_score_0_100", pd.Series(dtype=float)), bins=[0, 20, 40, 60, 80, 100]),
        "site_access_score_hist": numeric_histogram(outreach.get("site_access_score", pd.Series(dtype=float)), bins=[0, 20, 40, 60, 80, 100]),
    }


def score_buckets(scores: pd.Series) -> dict[str, int]:
    s = pd.to_numeric(scores, errors="coerce").dropna()
    low = int((s < 40).sum())
    mid = int(((s >= 40) & (s < 70)).sum())
    high = int((s >= 70).sum())
    return {"low": low, "mid": mid, "high": high}


def deserts_by_city(deserts: pd.DataFrame) -> list[dict[str, Any]]:
    if deserts.empty or "city" not in deserts.columns:
        return []
    if "is_desert" in deserts.columns:
        d = deserts[deserts["is_desert"] == True]  # noqa: E712
    else:
        d = deserts
    if d.empty:
        return []
    out = (
        d.assign(city=d["city"].astype(str))
        .groupby("city", dropna=False)
        .size()
        .reset_index(name="desert_count")
        .sort_values("desert_count", ascending=False)
    )
    return [{"city": str(r["city"]), "desert_count": int(r["desert_count"])} for _, r in out.iterrows()]


def summarize(
    *,
    libraries: pd.DataFrame,
    deserts: pd.DataFrame,
    outreach: pd.DataFrame,
    cities: list[str] | None,
    top_n_outreach: int = 10,
) -> dict[str, Any]:
    libs = filter_df_by_cities(libraries, cities, city_col="city")
    deserts_f = filter_df_by_cities(deserts, cities, city_col="city")
    outreach_f = filter_df_by_cities(outreach, cities, city_col="city")

    scores = libs["accessibility_score"] if "accessibility_score" in libs.columns else pd.Series(dtype=float)
    avg_score = float(pd.to_numeric(scores, errors="coerce").mean()) if not scores.empty else None

    desert_count = 0
    if not deserts_f.empty and "is_desert" in deserts_f.columns:
        desert_count = int((deserts_f["is_desert"] == True).sum())  # noqa: E712

    outreach_sorted = outreach_f.copy()
    if "outreach_score" in outreach_sorted.columns:
        outreach_sorted["outreach_score"] = pd.to_numeric(outreach_sorted["outreach_score"], errors="coerce")
        outreach_sorted = outreach_sorted.sort_values("outreach_score", ascending=False)
    outreach_top = (
        outreach_sorted.head(int(top_n_outreach)).where(pd.notnull(outreach_sorted), None).to_dict(orient="records")
        if not outreach_sorted.empty
        else []
    )

    return {
        "metrics": {
            "libraries_count": int(len(libs)),
            "avg_accessibility_score": avg_score,
            "score_buckets": score_buckets(scores) if not libs.empty else {"low": 0, "mid": 0, "high": 0},
            "deserts_count": desert_count,
            "outreach_count": int(len(outreach_f)),
        },
        "score_histogram": score_histogram(scores) if not libs.empty else {"bins": [0, 20, 40, 60, 80, 100], "counts": [0, 0, 0, 0, 0]},
        "deserts_distributions": deserts_distributions(deserts_f),
        "deserts_by_city": deserts_by_city(deserts_f),
        "outreach_distributions": outreach_distributions(outreach_f),
        "outreach_top": outreach_top,
    }


def summarize_delta(baseline: dict[str, Any], whatif: dict[str, Any]) -> dict[str, Any]:
    b = baseline.get("metrics", {})
    w = whatif.get("metrics", {})

    def d(key: str) -> float | int | None:
        if key not in b or key not in w:
            return None
        try:
            return (w[key] or 0) - (b[key] or 0)
        except TypeError:
            return None

    return {
        "avg_accessibility_score": d("avg_accessibility_score"),
        "deserts_count": d("deserts_count"),
        "outreach_count": d("outreach_count"),
        "libraries_count": d("libraries_count"),
    }
