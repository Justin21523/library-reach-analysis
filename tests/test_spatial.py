import numpy as np
import pandas as pd

from libraryreach.spatial.crs import latlon_to_xy_m, xy_to_latlon
from libraryreach.spatial.joins import compute_point_stop_density


def test_compute_point_stop_density_counts() -> None:
    reference_lat = 25.0

    libraries = pd.DataFrame(
        [
            {"id": "L1", "lat": 25.0, "lon": 121.0},
            {"id": "L2", "lat": 25.05, "lon": 121.0},
        ]
    )

    # Build stops by placing them at known meter offsets around L1
    l1_x, l1_y = latlon_to_xy_m(
        np.array([25.0]),
        np.array([121.0]),
        reference_lat_deg=reference_lat,
    )
    x0 = float(l1_x[0])
    y0 = float(l1_y[0])

    offsets = [
        (100.0, 0.0, "bus"),
        (0.0, 200.0, "bus"),
        (-250.0, -100.0, "bus"),
        (600.0, 0.0, "metro"),
    ]
    stop_rows = []
    for i, (dx, dy, mode) in enumerate(offsets, start=1):
        lat, lon = xy_to_latlon(
            np.array([x0 + dx]),
            np.array([y0 + dy]),
            reference_lat_deg=reference_lat,
        )
        stop_rows.append(
            {
                "stop_id": f"S{i}",
                "lat": float(lat[0]),
                "lon": float(lon[0]),
                "mode": mode,
            }
        )

    stops = pd.DataFrame(stop_rows)

    metrics, _ = compute_point_stop_density(
        libraries,
        stops,
        radii_m=[500, 1000],
        reference_lat_deg=reference_lat,
    )

    m1 = metrics[metrics["id"] == "L1"].iloc[0]
    assert int(m1["stop_count_total_500m"]) == 3
    assert int(m1["stop_count_bus_500m"]) == 3
    assert int(m1["stop_count_metro_500m"]) == 0

    assert int(m1["stop_count_total_1000m"]) == 4
    assert int(m1["stop_count_bus_1000m"]) == 3
    assert int(m1["stop_count_metro_1000m"]) == 1

    m2 = metrics[metrics["id"] == "L2"].iloc[0]
    assert int(m2["stop_count_total_500m"]) == 0
    assert int(m2["stop_count_total_1000m"]) == 0

