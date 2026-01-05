import pandas as pd

from libraryreach.scoring.accessibility import build_scoring_config, compute_accessibility_scores


def test_accessibility_score_simple_case() -> None:
    settings = {
        "buffers": {"radii_m": [500]},
        "scoring": {
            "mode_weights": {"bus": 0.6, "metro": 0.4},
            "radius_weights": {"500": 1.0},
            "density_targets_per_km2": {"bus": {"500": 20}, "metro": {"500": 2}},
        },
    }
    cfg = build_scoring_config(settings)
    df = pd.DataFrame(
        [
            {
                "id": "L1",
                "stop_density_bus_per_km2_500m": 10.0,  # 0.5 normalized
                "stop_density_metro_per_km2_500m": 2.0,  # 1.0 normalized
            }
        ]
    )
    scored, explain = compute_accessibility_scores(df, config=cfg)
    assert scored.iloc[0]["accessibility_score"] == 70.0
    assert "L1" in explain
    assert "Score 70.0/100" in scored.iloc[0]["accessibility_explain"]

