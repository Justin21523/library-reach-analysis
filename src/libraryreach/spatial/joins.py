"""
Spatial joins for baseline accessibility metrics (Phase 4: Spatial).

Our Phase 1 baseline metric is "transit stop density within X meters of each library".

We compute this without heavyweight GIS libraries by:
1) Projecting points (libraries) and stops (bus/metro) into local x/y meters.
2) Building a KD-tree over stop coordinates for fast radius queries.
3) Counting neighbors within each radius for each point.
4) Converting counts into densities (stops per km^2) for scoring normalization.

This is designed to be:
- Explainable: "count stops within 500m" is easy to communicate.
- Fast: KD-tree radius queries scale well to many points.
- Deterministic: for a given input table, the output is stable across runs.
"""

from __future__ import annotations

# `math` is used for circle area computation (pi * r^2).
import math
# Typing helpers keep signatures readable for beginners.
from typing import Any, Iterable

# NumPy provides fast vector operations and stable array handling.
import numpy as np
# pandas is our tabular container for inputs/outputs.
import pandas as pd
# cKDTree provides fast neighbor searches in Euclidean space.
from scipy.spatial import cKDTree

# CRS helpers convert WGS84 lat/lon to local x/y meters.
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
    # Validate that required columns exist before doing any expensive computation.
    required_point = {point_id_col, point_lat_col, point_lon_col}
    required_stop = {stop_lat_col, stop_lon_col, stop_mode_col}
    missing_point = required_point - set(points.columns)
    missing_stop = required_stop - set(stops.columns)
    if missing_point:
        raise ValueError(f"Missing point columns: {sorted(missing_point)}")
    if missing_stop:
        raise ValueError(f"Missing stop columns: {sorted(missing_stop)}")

    # Work on copies so callers do not see their input DataFrames mutated by dropna/type casts.
    points = points.copy()
    stops = stops.copy()
    # Drop rows with missing coordinates because spatial math requires numeric lat/lon.
    points = points.dropna(subset=[point_lat_col, point_lon_col])
    stops = stops.dropna(subset=[stop_lat_col, stop_lon_col])

    if reference_lat_deg is None:
        # Choose a reference latitude from both datasets so the projection is centered on the AOI.
        latitudes = np.concatenate(
            [
                points[point_lat_col].astype(float).to_numpy(),
                stops[stop_lat_col].astype(float).to_numpy(),
            ]
        )
        # We accept a string strategy here to keep the public API flexible for config-driven usage.
        reference_lat_deg = choose_reference_lat_deg(latitudes, strategy=reference_lat_strategy)  # type: ignore[arg-type]

    # Project point coordinates into x/y meters so we can use a Euclidean KD-tree for radius queries.
    p_x, p_y = latlon_to_xy_m(
        points[point_lat_col].to_numpy(),
        points[point_lon_col].to_numpy(),
        reference_lat_deg=reference_lat_deg,
    )
    # Project stop coordinates into the same x/y meter space (must use the same reference latitude).
    s_x, s_y = latlon_to_xy_m(
        stops[stop_lat_col].to_numpy(),
        stops[stop_lon_col].to_numpy(),
        reference_lat_deg=reference_lat_deg,
    )

    # Stack coordinates into (N, 2) arrays required by cKDTree.
    stop_xy = np.column_stack([s_x, s_y])
    # Build the KD-tree once; neighbor queries for many points/radii become fast.
    tree = cKDTree(stop_xy)
    # Convert stop modes to an array so we can index modes by neighbor indices cheaply.
    stop_modes = stops[stop_mode_col].astype(str).to_numpy()

    # Stack point coordinates into (M, 2) array for vectorized neighbor queries.
    point_xy = np.column_stack([p_x, p_y])
    # Start output with the point IDs so downstream joins back to catalogs are straightforward.
    out = pd.DataFrame({point_id_col: points[point_id_col].astype(str).to_numpy()})
    # Keep reference latitude in output so explain/debug can show which projection anchor was used.
    out["reference_lat_deg"] = float(reference_lat_deg)

    # Normalize radii into a sorted unique list of positive integers.
    radii_sorted = sorted({int(x) for x in radii_m})
    for r in radii_sorted:
        # Radius must be positive; zero would create a zero-area circle (division by zero for densities).
        if r <= 0:
            raise ValueError("All radii_m values must be > 0")
        # Query all points at once: returns a list of neighbor index lists (one list per point).
        neighbors = tree.query_ball_point(point_xy, r)
        # We keep explicit Python lists so we can append counts per point deterministically.
        total_counts: list[int] = []
        bus_counts: list[int] = []
        metro_counts: list[int] = []
        for idxs in neighbors:
            # Convert neighbor indices into a NumPy array for efficient indexing into stop_modes.
            idxs_arr = np.asarray(idxs, dtype=int)
            # Total neighbors is the raw stop count within radius for this point.
            total = int(idxs_arr.size)
            total_counts.append(total)
            if total == 0:
                # Fast path: no neighbors means all mode-specific counts are zero.
                bus_counts.append(0)
                metro_counts.append(0)
                continue
            # Gather the modes for this point's neighbors so we can count bus vs metro.
            modes = stop_modes[idxs_arr]
            # Count modes using vectorized comparisons (simple and fast).
            bus_counts.append(int(np.sum(modes == "bus")))
            metro_counts.append(int(np.sum(modes == "metro")))

        # Area of a circle in km^2; used to convert raw counts into densities for scoring.
        area_km2 = math.pi * (r / 1000.0) ** 2
        # Store counts (raw) and densities (normalized) for each radius.
        out[f"stop_count_total_{r}m"] = total_counts
        out[f"stop_count_bus_{r}m"] = bus_counts
        out[f"stop_count_metro_{r}m"] = metro_counts
        out[f"stop_density_total_per_km2_{r}m"] = out[f"stop_count_total_{r}m"] / area_km2
        out[f"stop_density_bus_per_km2_{r}m"] = out[f"stop_count_bus_{r}m"] / area_km2
        out[f"stop_density_metro_per_km2_{r}m"] = out[f"stop_count_metro_{r}m"] / area_km2

    # Return the metrics table and the reference latitude so callers can reuse it for buffer polygons.
    return out, float(reference_lat_deg)
