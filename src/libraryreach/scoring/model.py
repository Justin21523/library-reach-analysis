from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ScoringConfig:
    radii_m: list[int]
    mode_weights: dict[str, float]
    radius_weights: dict[int, float]
    density_targets_per_km2: dict[str, dict[int, float]]

