from __future__ import annotations

import math
from typing import Any, Iterable

import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

from libraryreach.spatial.crs import choose_reference_lat_deg, latlon_to_xy_m


def compute_point_stop_density(
    points: pd.DataFrame,
    stops: pd.DataFrame,
    *,
    radii_m: Iterable[int],
    point_id_col: str = "id",
    point_lat_col: str = "lat",
    point_lon_col: str = "lon",
    stop_lat_col: str = "lat",
    stop_lon_col: str = "lon",
    stop_mode_col: str = "mode",
    reference_lat_deg: float | None = None,
    reference_lat_strategy: str = "mean",
) -> tuple[pd.DataFrame, float]:
    required_point = {point_id_col, point_lat_col, point_lon_col}
    required_stop = {stop_lat_col, stop_lon_col, stop_mode_col}
    missing_point = required_point - set(points.columns)
    missing_stop = required_stop - set(stops.columns)
    if missing_point:
        raise ValueError(f"Missing point columns: {sorted(missing_point)}")
    if missing_stop:
        raise ValueError(f"Missing stop columns: {sorted(missing_stop)}")

    points = points.copy()
    stops = stops.copy()
    points = points.dropna(subset=[point_lat_col, point_lon_col])
    stops = stops.dropna(subset=[stop_lat_col, stop_lon_col])

    if reference_lat_deg is None:
        latitudes = np.concatenate(
            [
                points[point_lat_col].astype(float).to_numpy(),
                stops[stop_lat_col].astype(float).to_numpy(),
            ]
        )
        reference_lat_deg = choose_reference_lat_deg(latitudes, strategy=reference_lat_strategy)  # type: ignore[arg-type]

    p_x, p_y = latlon_to_xy_m(
        points[point_lat_col].to_numpy(),
        points[point_lon_col].to_numpy(),
        reference_lat_deg=reference_lat_deg,
    )
    s_x, s_y = latlon_to_xy_m(
        stops[stop_lat_col].to_numpy(),
        stops[stop_lon_col].to_numpy(),
        reference_lat_deg=reference_lat_deg,
    )

    stop_xy = np.column_stack([s_x, s_y])
    tree = cKDTree(stop_xy)
    stop_modes = stops[stop_mode_col].astype(str).to_numpy()

    point_xy = np.column_stack([p_x, p_y])
    out = pd.DataFrame({point_id_col: points[point_id_col].astype(str).to_numpy()})
    out["reference_lat_deg"] = float(reference_lat_deg)

    for r in sorted(int(x) for x in radii_m):
        neighbors = tree.query_ball_point(point_xy, r)
        total_counts: list[int] = []
        bus_counts: list[int] = []
        metro_counts: list[int] = []
        for idxs in neighbors:
            idxs_arr = np.asarray(idxs, dtype=int)
            total = int(idxs_arr.size)
            total_counts.append(total)
            if total == 0:
                bus_counts.append(0)
                metro_counts.append(0)
                continue
            modes = stop_modes[idxs_arr]
            bus_counts.append(int(np.sum(modes == "bus")))
            metro_counts.append(int(np.sum(modes == "metro")))

        area_km2 = math.pi * (r / 1000.0) ** 2
        out[f"stop_count_total_{r}m"] = total_counts
        out[f"stop_count_bus_{r}m"] = bus_counts
        out[f"stop_count_metro_{r}m"] = metro_counts
        out[f"stop_density_total_per_km2_{r}m"] = out[f"stop_count_total_{r}m"] / area_km2
        out[f"stop_density_bus_per_km2_{r}m"] = out[f"stop_count_bus_{r}m"] / area_km2
        out[f"stop_density_metro_per_km2_{r}m"] = out[f"stop_count_metro_{r}m"] / area_km2

    return out, float(reference_lat_deg)

