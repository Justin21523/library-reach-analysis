from __future__ import annotations

import math
from typing import Any

import numpy as np

from libraryreach.spatial.crs import latlon_to_xy_m, xy_to_latlon


def circle_polygon_lonlat(
    *,
    center_lat: float,
    center_lon: float,
    radius_m: float,
    reference_lat_deg: float,
    num_points: int = 64,
) -> list[list[float]]:
    """
    Approximate a circle buffer as a lon/lat polygon using a simple local projection.
    Output format matches GeoJSON coordinates: [[lon, lat], ...].
    """
    lat = np.array([center_lat], dtype=float)
    lon = np.array([center_lon], dtype=float)
    x0, y0 = latlon_to_xy_m(lat, lon, reference_lat_deg=reference_lat_deg)
    x0 = float(x0[0])
    y0 = float(y0[0])

    angles = np.linspace(0.0, 2.0 * math.pi, num_points, endpoint=False)
    xs = x0 + radius_m * np.cos(angles)
    ys = y0 + radius_m * np.sin(angles)
    out_lat, out_lon = xy_to_latlon(xs, ys, reference_lat_deg=reference_lat_deg)

    coords = [[float(lon_i), float(lat_i)] for lat_i, lon_i in zip(out_lat, out_lon)]
    coords.append(coords[0])
    return coords


def points_buffers_geojson(
    *,
    points: list[dict[str, Any]],
    radius_m: float,
    reference_lat_deg: float,
    id_key: str = "id",
    lat_key: str = "lat",
    lon_key: str = "lon",
) -> dict[str, Any]:
    features = []
    for p in points:
        coords = circle_polygon_lonlat(
            center_lat=float(p[lat_key]),
            center_lon=float(p[lon_key]),
            radius_m=float(radius_m),
            reference_lat_deg=reference_lat_deg,
        )
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [coords]},
                "properties": {id_key: p[id_key], "radius_m": float(radius_m)},
            }
        )
    return {"type": "FeatureCollection", "features": features}

