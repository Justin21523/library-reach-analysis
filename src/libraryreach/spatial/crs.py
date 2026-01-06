"""
Lightweight coordinate helpers for short-range spatial analysis (Phase 4: Spatial).

We intentionally avoid heavyweight GIS dependencies (pyproj/shapely/geopandas) in Phase 1/2
because our baseline analysis only needs *local* distances within ~1km buffers.

We approximate distances using an equirectangular projection:
- Convert WGS84 lat/lon degrees to planar meters (x/y) using a chosen reference latitude.
- Use x/y meters for Euclidean distance and KD-tree spatial queries.

This is "good enough" for small distances, and the math is easy to explain in reports.
"""

from __future__ import annotations

# `math` provides trigonometric functions for the projection.
import math
# `Literal` constrains allowed strategy strings for reference latitude selection.
from typing import Literal

# NumPy arrays keep vectorized coordinate transforms fast and simple.
import numpy as np

# Earth radius in meters (spherical approximation, sufficient for baseline buffers).
EARTH_RADIUS_M = 6_371_000.0


# Choosing a reference latitude keeps the equirectangular projection accurate around our AOI.
ReferenceLatStrategy = Literal["mean", "median"]


def choose_reference_lat_deg(latitudes_deg: np.ndarray, strategy: ReferenceLatStrategy = "mean") -> float:
    # An empty array cannot produce a meaningful reference latitude, so we fail fast.
    if latitudes_deg.size == 0:
        raise ValueError("Cannot choose reference latitude from empty array")
    # Median is more robust to outliers; mean is smoother when data is well-behaved.
    if strategy == "median":
        return float(np.median(latitudes_deg))
    # Default to mean for the baseline because it is easy to explain and stable across runs.
    return float(np.mean(latitudes_deg))


def latlon_to_xy_m(
    lat_deg: np.ndarray,
    lon_deg: np.ndarray,
    *,
    reference_lat_deg: float,
) -> tuple[np.ndarray, np.ndarray]:
    # Convert degrees to radians so trigonometric functions work correctly.
    lat_rad = np.deg2rad(lat_deg.astype(float))
    lon_rad = np.deg2rad(lon_deg.astype(float))
    # Use a fixed reference latitude so we do not recompute cos(lat) per point.
    ref_lat_rad = math.radians(float(reference_lat_deg))

    # Equirectangular projection:
    # - x scales longitude by cos(reference_lat) to account for meridians converging toward poles.
    # - y scales latitude directly by Earth radius.
    x = EARTH_RADIUS_M * lon_rad * math.cos(ref_lat_rad)
    y = EARTH_RADIUS_M * lat_rad
    # Return x/y in meters so downstream code can use Euclidean distances (KDTree, buffers, etc.).
    return x, y


def xy_to_latlon(
    x_m: np.ndarray,
    y_m: np.ndarray,
    *,
    reference_lat_deg: float,
) -> tuple[np.ndarray, np.ndarray]:
    # Convert the reference latitude to radians for the inverse projection math.
    ref_lat_rad = math.radians(float(reference_lat_deg))
    # Inverse of y = R * lat_rad.
    lat_rad = y_m.astype(float) / EARTH_RADIUS_M
    # Inverse of x = R * lon_rad * cos(ref_lat).
    # Pitfall: near the poles cos(ref_lat) approaches 0; our AOI avoids extreme latitudes.
    lon_rad = x_m.astype(float) / (EARTH_RADIUS_M * math.cos(ref_lat_rad))
    # Convert radians back to degrees to return standard WGS84 lat/lon arrays.
    return np.rad2deg(lat_rad), np.rad2deg(lon_rad)
