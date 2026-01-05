from __future__ import annotations

from typing import Any

from libraryreach.scoring.model import ScoringConfig


def build_explain_payload(
    *,
    library_row: dict[str, Any],
    score_0_100: float,
    components: list[dict[str, Any]],
    config: ScoringConfig,
) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for r in config.radii_m:
        for key in [
            f"stop_count_total_{r}m",
            f"stop_count_bus_{r}m",
            f"stop_count_metro_{r}m",
            f"stop_density_total_per_km2_{r}m",
            f"stop_density_bus_per_km2_{r}m",
            f"stop_density_metro_per_km2_{r}m",
        ]:
            if key in library_row:
                metrics[key] = library_row[key]

    return {
        "method": "transit_stop_density_buffer",
        "score_0_100": float(score_0_100),
        "buffers_m": list(config.radii_m),
        "mode_weights": dict(config.mode_weights),
        "radius_weights": {str(k): float(v) for k, v in config.radius_weights.items()},
        "density_targets_per_km2": {
            mode: {str(r): float(v) for r, v in by_r.items()}
            for mode, by_r in config.density_targets_per_km2.items()
        },
        "metrics": metrics,
        "components": components,
    }


def build_explain_text(explain_payload: dict[str, Any]) -> str:
    score = float(explain_payload.get("score_0_100", 0.0))
    parts = [f"Score {score:.1f}/100 (baseline stop-density model)."]

    comps = list(explain_payload.get("components", []))
    comps_sorted = sorted(comps, key=lambda x: float(x.get("contribution_0_1", 0.0)), reverse=True)
    top = comps_sorted[:3]
    if top:
        drivers = []
        for c in top:
            mode = c.get("mode", "?")
            r = c.get("radius_m", "?")
            density = float(c.get("density_per_km2", 0.0))
            target = float(c.get("target_per_km2", 0.0))
            drivers.append(f"{mode}@{r}m {density:.1f}/km² vs {target:.1f}/km²")
        parts.append("Top drivers: " + "; ".join(drivers) + ".")
    return " ".join(parts)
