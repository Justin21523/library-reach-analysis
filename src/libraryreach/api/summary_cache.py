from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_run_meta(processed_dir: Path) -> dict[str, Any] | None:
    path = processed_dir / "run_meta.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def load_qa_report(processed_dir: Path) -> dict[str, Any] | None:
    path = processed_dir / "qa_report.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def load_summary_by_city(processed_dir: Path) -> dict[str, Any] | None:
    path = processed_dir / "summary_by_city.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def aggregate_summaries(
    *,
    summaries_by_city: dict[str, Any],
    cities: list[str],
    top_n_outreach: int,
) -> dict[str, Any]:
    bins = None
    hist_counts: list[int] | None = None
    deserts_by_city: list[dict[str, Any]] = []
    outreach_top_pool: list[dict[str, Any]] = []

    deserts_dist: dict[str, Any] | None = None
    outreach_dist: dict[str, Any] | None = None

    total_libraries = 0
    total_outreach = 0
    total_deserts = 0
    sum_score = 0.0
    sum_score_weight = 0
    buckets = {"low": 0, "mid": 0, "high": 0}

    for city in cities:
        s = summaries_by_city.get(str(city))
        if not s:
            continue
        m = s.get("metrics", {}) or {}
        libs = int(m.get("libraries_count") or 0)
        total_libraries += libs
        total_outreach += int(m.get("outreach_count") or 0)
        total_deserts += int(m.get("deserts_count") or 0)

        avg = m.get("avg_accessibility_score")
        if avg is not None and libs:
            sum_score += float(avg) * libs
            sum_score_weight += libs

        b = m.get("score_buckets", {}) or {}
        buckets["low"] += int(b.get("low") or 0)
        buckets["mid"] += int(b.get("mid") or 0)
        buckets["high"] += int(b.get("high") or 0)

        h = s.get("score_histogram", {}) or {}
        if bins is None:
            bins = h.get("bins")
            hist_counts = [0] * (len(bins) - 1) if isinstance(bins, list) else None
        if hist_counts is not None and h.get("counts") and bins == h.get("bins"):
            for i, v in enumerate(h.get("counts", [])):
                hist_counts[i] += int(v or 0)

        deserts_by_city.extend(s.get("deserts_by_city", []) or [])
        outreach_top_pool.extend(s.get("outreach_top", []) or [])

        dd = s.get("deserts_distributions") or {}
        if dd and deserts_dist is None:
            deserts_dist = dd
        elif dd and deserts_dist is not None:
            for k in ["effective_score_hist", "gap_hist", "best_distance_hist_m"]:
                if k in dd and k in deserts_dist and dd[k].get("bins") == deserts_dist[k].get("bins"):
                    deserts_dist[k]["counts"] = [
                        int(a or 0) + int(b or 0) for a, b in zip(deserts_dist[k].get("counts", []), dd[k].get("counts", []))
                    ]

        od = s.get("outreach_distributions") or {}
        if od and outreach_dist is None:
            outreach_dist = od
        elif od and outreach_dist is not None:
            for k in ["outreach_score_hist", "coverage_score_hist", "site_access_score_hist"]:
                if k in od and k in outreach_dist and od[k].get("bins") == outreach_dist[k].get("bins"):
                    outreach_dist[k]["counts"] = [
                        int(a or 0) + int(b or 0) for a, b in zip(outreach_dist[k].get("counts", []), od[k].get("counts", []))
                    ]

    avg_score = (sum_score / sum_score_weight) if sum_score_weight else None

    deserts_by_city_sorted = sorted(
        [{"city": d.get("city"), "desert_count": int(d.get("desert_count") or 0)} for d in deserts_by_city],
        key=lambda x: x["desert_count"],
        reverse=True,
    )
    # Deduplicate by city (each per-city summary includes only one city anyway, but keep safe).
    seen: set[str] = set()
    deserts_compact: list[dict[str, Any]] = []
    for row in deserts_by_city_sorted:
        c = str(row.get("city"))
        if c in seen:
            continue
        seen.add(c)
        deserts_compact.append(row)

    outreach_top = sorted(
        outreach_top_pool,
        key=lambda r: float(r.get("outreach_score") or 0),
        reverse=True,
    )[: int(top_n_outreach)]

    return {
        "metrics": {
            "libraries_count": total_libraries,
            "avg_accessibility_score": avg_score,
            "score_buckets": buckets,
            "deserts_count": total_deserts,
            "outreach_count": total_outreach,
        },
        "score_histogram": {"bins": bins or [0, 20, 40, 60, 80, 100], "counts": hist_counts or [0, 0, 0, 0, 0]},
        "deserts_distributions": deserts_dist
        or {
            "effective_score_hist": {"bins": [0, 20, 40, 60, 80, 100], "counts": [0, 0, 0, 0, 0]},
            "gap_hist": {"bins": [0, 5, 10, 20, 30, 50, 100], "counts": [0, 0, 0, 0, 0, 0]},
            "best_distance_hist_m": {"bins": [0, 500, 1000, 2000, 3000, 5000, 10000], "counts": [0, 0, 0, 0, 0, 0]},
        },
        "deserts_by_city": deserts_compact,
        "outreach_distributions": outreach_dist
        or {
            "outreach_score_hist": {"bins": [0, 20, 40, 60, 80, 100], "counts": [0, 0, 0, 0, 0]},
            "coverage_score_hist": {"bins": [0, 20, 40, 60, 80, 100], "counts": [0, 0, 0, 0, 0]},
            "site_access_score_hist": {"bins": [0, 20, 40, 60, 80, 100], "counts": [0, 0, 0, 0, 0]},
        },
        "outreach_top": outreach_top,
    }
