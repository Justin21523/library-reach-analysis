from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

from libraryreach.spatial.crs import latlon_to_xy_m, xy_to_latlon


@dataclass(frozen=True)
class DesertConfig:
    cell_size_m: int
    library_search_radius_m: int
    threshold_score: float
    decay_type: str
    decay_zero_at_m: int


def _linear_decay(distance_m: float, *, zero_at_m: float) -> float:
    if distance_m <= 0:
        return 1.0
    if distance_m >= zero_at_m:
        return 0.0
    return max(0.0, 1.0 - (distance_m / zero_at_m))


def _build_grid_centroids_xy(
    *,
    min_x: float,
    max_x: float,
    min_y: float,
    max_y: float,
    cell_size_m: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    cs = float(cell_size_m)
    x0 = math.floor(min_x / cs) * cs
    y0 = math.floor(min_y / cs) * cs
    x1 = math.ceil(max_x / cs) * cs
    y1 = math.ceil(max_y / cs) * cs
    xs = np.arange(x0, x1, cs)
    ys = np.arange(y0, y1, cs)
    xx, yy = np.meshgrid(xs, ys)
    cx = (xx + cs / 2.0).ravel()
    cy = (yy + cs / 2.0).ravel()
    return cx, cy, xx.ravel(), yy.ravel()


def compute_access_deserts_grid(
    *,
    cities: Iterable[str],
    libraries: pd.DataFrame,
    outreach_candidates: pd.DataFrame,
    reference_lat_deg: float,
    config: DesertConfig,
) -> pd.DataFrame:
    """
    Grid-based "effective accessibility" for planning:
    effective_score(cell) = max_over_libraries( library_score * distance_decay(distance) )
    """
    required_lib = {"id", "lat", "lon", "accessibility_score"}
    missing = required_lib - set(libraries.columns)
    if missing:
        raise ValueError(f"Missing library columns: {sorted(missing)}")

    libs = libraries.copy()
    libs["id"] = libs["id"].astype(str)

    lib_x, lib_y = latlon_to_xy_m(
        libs["lat"].astype(float).to_numpy(),
        libs["lon"].astype(float).to_numpy(),
        reference_lat_deg=reference_lat_deg,
    )
    lib_xy = np.column_stack([lib_x, lib_y])
    lib_tree = cKDTree(lib_xy)
    lib_scores = libs["accessibility_score"].astype(float).to_numpy()
    lib_ids = libs["id"].astype(str).to_numpy()

    out_rows: list[dict[str, Any]] = []
    padding = float(config.library_search_radius_m)

    for city in cities:
        city_points = []
        for df, label in [(libraries, "library"), (outreach_candidates, "candidate")]:
            if df.empty or "city" not in df.columns:
                continue
            subset = df[df["city"].astype(str) == str(city)]
            if subset.empty:
                continue
            city_points.append(subset[["lat", "lon"]].astype(float))
        if not city_points:
            continue
        aoi = pd.concat(city_points, ignore_index=True)
        aoi_x, aoi_y = latlon_to_xy_m(
            aoi["lat"].to_numpy(),
            aoi["lon"].to_numpy(),
            reference_lat_deg=reference_lat_deg,
        )
        min_x = float(np.min(aoi_x) - padding)
        max_x = float(np.max(aoi_x) + padding)
        min_y = float(np.min(aoi_y) - padding)
        max_y = float(np.max(aoi_y) + padding)

        cx, cy, cell_x0, cell_y0 = _build_grid_centroids_xy(
            min_x=min_x,
            max_x=max_x,
            min_y=min_y,
            max_y=max_y,
            cell_size_m=config.cell_size_m,
        )
        centroids_xy = np.column_stack([cx, cy])
        neighbors = lib_tree.query_ball_point(centroids_xy, config.library_search_radius_m)

        for i, idxs in enumerate(neighbors):
            idxs_arr = np.asarray(idxs, dtype=int)
            if idxs_arr.size == 0:
                best_score = 0.0
                best_id = None
                best_dist = None
                best_base = None
                best_decay = None
            else:
                deltas = lib_xy[idxs_arr] - centroids_xy[i]
                dists = np.sqrt(np.sum(deltas * deltas, axis=1))
                if config.decay_type == "linear":
                    decays = np.vectorize(lambda d: _linear_decay(float(d), zero_at_m=config.decay_zero_at_m))(dists)
                else:
                    decays = np.ones_like(dists, dtype=float)
                effective = lib_scores[idxs_arr] * decays
                j = int(np.argmax(effective))
                best_score = float(effective[j])
                best_id = str(lib_ids[idxs_arr[j]])
                best_dist = float(dists[j])
                best_base = float(lib_scores[idxs_arr[j]])
                best_decay = float(decays[j])

            is_desert = bool(best_score < config.threshold_score)
            gap = float(max(0.0, config.threshold_score - best_score))

            out_rows.append(
                {
                    "city": str(city),
                    "cell_size_m": int(config.cell_size_m),
                    "cell_x0_m": float(cell_x0[i]),
                    "cell_y0_m": float(cell_y0[i]),
                    "centroid_x_m": float(cx[i]),
                    "centroid_y_m": float(cy[i]),
                    "effective_score_0_100": float(best_score),
                    "is_desert": is_desert,
                    "gap_to_threshold": gap,
                    "best_library_id": best_id,
                    "best_library_distance_m": best_dist,
                    "best_library_base_score": best_base,
                    "distance_decay_factor": best_decay,
                }
            )

    df_out = pd.DataFrame(out_rows)
    if df_out.empty:
        return df_out

    lat, lon = xy_to_latlon(
        df_out["centroid_x_m"].to_numpy(),
        df_out["centroid_y_m"].to_numpy(),
        reference_lat_deg=reference_lat_deg,
    )
    df_out["centroid_lat"] = lat
    df_out["centroid_lon"] = lon
    df_out["cell_id"] = (
        df_out["city"].astype(str)
        + "-"
        + (df_out["cell_x0_m"].astype(int).astype(str))
        + "-"
        + (df_out["cell_y0_m"].astype(int).astype(str))
    )
    return df_out


def deserts_points_geojson(deserts: pd.DataFrame) -> dict[str, Any]:
    features: list[dict[str, Any]] = []
    for _, row in deserts.iterrows():
        props: dict[str, Any] = {
            "cell_id": str(row["cell_id"]),
            "city": str(row["city"]),
            "effective_score_0_100": float(row["effective_score_0_100"]),
            "is_desert": bool(row["is_desert"]),
            "gap_to_threshold": float(row["gap_to_threshold"]),
            "best_library_id": row.get("best_library_id"),
            "best_library_distance_m": row.get("best_library_distance_m"),
        }
        if "best_library_base_score" in deserts.columns:
            props["best_library_base_score"] = row.get("best_library_base_score")
        if "distance_decay_factor" in deserts.columns:
            props["distance_decay_factor"] = row.get("distance_decay_factor")
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(row["centroid_lon"]), float(row["centroid_lat"])],
                },
                "properties": props,
            }
        )
    return {"type": "FeatureCollection", "features": features}
