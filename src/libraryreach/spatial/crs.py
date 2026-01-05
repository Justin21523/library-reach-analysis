from __future__ import annotations

import math
from typing import Literal

import numpy as np

EARTH_RADIUS_M = 6_371_000.0


ReferenceLatStrategy = Literal["mean", "median"]


def choose_reference_lat_deg(latitudes_deg: np.ndarray, strategy: ReferenceLatStrategy = "mean") -> float:
    if latitudes_deg.size == 0:
        raise ValueError("Cannot choose reference latitude from empty array")
    if strategy == "median":
        return float(np.median(latitudes_deg))
    return float(np.mean(latitudes_deg))


def latlon_to_xy_m(
    lat_deg: np.ndarray,
    lon_deg: np.ndarray,
    *,
    reference_lat_deg: float,
) -> tuple[np.ndarray, np.ndarray]:
    lat_rad = np.deg2rad(lat_deg.astype(float))
    lon_rad = np.deg2rad(lon_deg.astype(float))
    ref_lat_rad = math.radians(float(reference_lat_deg))

    x = EARTH_RADIUS_M * lon_rad * math.cos(ref_lat_rad)
    y = EARTH_RADIUS_M * lat_rad
    return x, y


def xy_to_latlon(
    x_m: np.ndarray,
    y_m: np.ndarray,
    *,
    reference_lat_deg: float,
) -> tuple[np.ndarray, np.ndarray]:
    ref_lat_rad = math.radians(float(reference_lat_deg))
    lat_rad = y_m.astype(float) / EARTH_RADIUS_M
    lon_rad = x_m.astype(float) / (EARTH_RADIUS_M * math.cos(ref_lat_rad))
    return np.rad2deg(lat_rad), np.rad2deg(lon_rad)

