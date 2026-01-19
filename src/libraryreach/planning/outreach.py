from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

from libraryreach.scoring.accessibility import compute_accessibility_scores
from libraryreach.scoring.model import ScoringConfig
from libraryreach.spatial.crs import latlon_to_xy_m
from libraryreach.spatial.joins import compute_point_stop_density


@dataclass(frozen=True)
class OutreachConfig:
    coverage_radius_m: int
    top_n_per_city: int
    weight_coverage: float
    weight_site_access: float


def recommend_outreach_sites(
    *,
    outreach_candidates: pd.DataFrame,
    deserts: pd.DataFrame,
    stops: pd.DataFrame,
    reference_lat_deg: float,
    radii_m: list[int],
    scoring_config: ScoringConfig,
    config: OutreachConfig,
) -> pd.DataFrame:
    required = {"id", "name", "lat", "lon", "city"}
    missing = required - set(outreach_candidates.columns)
    if missing:
        raise ValueError(f"Missing outreach candidate columns: {sorted(missing)}")
    if deserts.empty:
        return pd.DataFrame()

    desert_points = deserts[deserts["is_desert"] == True].copy()  # noqa: E712
    if desert_points.empty:
        return pd.DataFrame()

    d_x, d_y = latlon_to_xy_m(
        desert_points["centroid_lat"].astype(float).to_numpy(),
        desert_points["centroid_lon"].astype(float).to_numpy(),
        reference_lat_deg=reference_lat_deg,
    )
    d_xy = np.column_stack([d_x, d_y])
    d_tree = cKDTree(d_xy)
    gaps = desert_points["gap_to_threshold"].astype(float).to_numpy()

    cand = outreach_candidates.copy()
    cand["id"] = cand["id"].astype(str)

    # Site access score (reuse the library scoring model on candidate stop-density metrics)
    cand_metrics, _ = compute_point_stop_density(
        cand,
        stops,
        radii_m=radii_m,
        point_id_col="id",
        point_lat_col="lat",
        point_lon_col="lon",
        reference_lat_deg=reference_lat_deg,
    )
    cand_with_metrics = cand.merge(cand_metrics, on="id", how="left")
    cand_scored, _ = compute_accessibility_scores(cand_with_metrics, config=scoring_config)
    site_access = cand_scored.set_index("id")["accessibility_score"].astype(float).to_dict()

    c_x, c_y = latlon_to_xy_m(
        cand["lat"].astype(float).to_numpy(),
        cand["lon"].astype(float).to_numpy(),
        reference_lat_deg=reference_lat_deg,
    )
    c_xy = np.column_stack([c_x, c_y])
    neighbors = d_tree.query_ball_point(c_xy, config.coverage_radius_m)

    coverage_counts: list[int] = []
    coverage_gap_sums: list[float] = []
    for idxs in neighbors:
        idxs_arr = np.asarray(idxs, dtype=int)
        coverage_counts.append(int(idxs_arr.size))
        coverage_gap_sums.append(float(np.sum(gaps[idxs_arr])) if idxs_arr.size else 0.0)

    cand["covered_desert_cells"] = coverage_counts
    cand["covered_gap_sum"] = coverage_gap_sums
    cand["site_access_score"] = cand["id"].map(lambda x: float(site_access.get(str(x), 0.0)))

    # City-level normalization for coverage metric
    recommendations: list[pd.DataFrame] = []
    for city, group in cand.groupby(cand["city"].astype(str)):
        g = group.copy()
        max_gap = float(g["covered_gap_sum"].max()) if len(g) else 0.0
        if max_gap <= 0:
            g["coverage_score_0_100"] = 0.0
        else:
            g["coverage_score_0_100"] = (g["covered_gap_sum"] / max_gap) * 100.0

        w_cov = float(config.weight_coverage)
        w_site = float(config.weight_site_access)
        g["weight_coverage"] = w_cov
        g["weight_site_access"] = w_site
        g["contribution_coverage"] = w_cov * g["coverage_score_0_100"]
        g["contribution_site_access"] = w_site * g["site_access_score"]

        g["outreach_score"] = (
            g["contribution_coverage"] + g["contribution_site_access"]
        )
        g = g.sort_values("outreach_score", ascending=False).head(int(config.top_n_per_city))
        g["recommendation_explain"] = g.apply(
            lambda r: (
                f"OutreachScore {float(r['outreach_score']):.1f}. "
                f"Covers {int(r['covered_desert_cells'])} desert cells within {config.coverage_radius_m}m; "
                f"coverage {float(r['coverage_score_0_100']):.1f}/100 (w={w_cov:.2f}) + "
                f"site access {float(r['site_access_score']):.1f}/100 (w={w_site:.2f})."
            ),
            axis=1,
        )
        recommendations.append(g)

    return pd.concat(recommendations, ignore_index=True) if recommendations else pd.DataFrame()
