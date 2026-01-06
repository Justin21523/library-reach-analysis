"""
Buffer geometry helpers for visualization (Phase 4: Spatial).

Our baseline accessibility metric uses circular buffers (e.g., 500m and 1km)
around each library branch.

We do *not* need full polygon operations for Phase 1. We only need:
- a circle approximation as a polygon for map display (GeoJSON),
- generated consistently using the same local projection as distance calculations.

Important: these polygons are for UI/explainability, not for exact GIS overlay.
The actual counting uses KD-tree radius queries in `libraryreach.spatial.joins`.
"""

from __future__ import annotations

# `math` provides pi/sin/cos for circle point generation.
import math
# `Any` is used for GeoJSON-like dict structures (kept flexible for Phase 1).
from typing import Any

# NumPy provides vectorized circle generation and coordinate transforms.
import numpy as np

# We reuse our lightweight CRS helpers so buffers and joins share the same projection assumptions.
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
    # Guard against invalid inputs early so callers do not generate nonsense polygons.
    if radius_m <= 0:
        raise ValueError("radius_m must be > 0")
    # A polygon needs at least 3 vertices; we use a higher default for smoother circles.
    if num_points < 3:
        raise ValueError("num_points must be >= 3")

    # Represent the center as 1-element arrays so we can reuse vectorized projection helpers.
    lat = np.array([center_lat], dtype=float)
    lon = np.array([center_lon], dtype=float)
    # Project center point into local x/y meters for simple circle math.
    x0, y0 = latlon_to_xy_m(lat, lon, reference_lat_deg=reference_lat_deg)
    # Extract scalar center x/y so we can add radius offsets.
    x0 = float(x0[0])
    y0 = float(y0[0])

    # Create evenly spaced angles around the circle (0..2pi).
    angles = np.linspace(0.0, 2.0 * math.pi, num_points, endpoint=False)
    # Compute circle points in x/y meters around the projected center.
    xs = x0 + radius_m * np.cos(angles)
    ys = y0 + radius_m * np.sin(angles)
    # Invert projection back to WGS84 lat/lon so we can emit GeoJSON coordinates.
    out_lat, out_lon = xy_to_latlon(xs, ys, reference_lat_deg=reference_lat_deg)

    # GeoJSON polygon coordinates are [lon, lat] pairs (note the order).
    coords = [[float(lon_i), float(lat_i)] for lat_i, lon_i in zip(out_lat, out_lon)]
    # Close the ring by repeating the first coordinate (GeoJSON polygon requirement).
    coords.append(coords[0])
    # Return a list of coordinates that can be wrapped into a GeoJSON Polygon geometry.
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
    # A GeoJSON FeatureCollection is a convenient wrapper for many polygons.
    features = []
    for p in points:
        # Compute the circle polygon for this point using configured radius and shared projection reference.
        coords = circle_polygon_lonlat(
            center_lat=float(p[lat_key]),
            center_lon=float(p[lon_key]),
            radius_m=float(radius_m),
            reference_lat_deg=reference_lat_deg,
        )
        # Store geometry + minimal properties so the UI can style by radius and identify the source point.
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [coords]},
                "properties": {id_key: p[id_key], "radius_m": float(radius_m)},
            }
        )
    # Return a standard GeoJSON FeatureCollection that web mapping libraries can render directly.
    return {"type": "FeatureCollection", "features": features}
