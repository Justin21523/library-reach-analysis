from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from libraryreach.scoring.model import ScoringConfig
from libraryreach.scoring.explain import build_explain_payload, build_explain_text


def _normalize_weights(raw: dict[Any, Any]) -> dict[Any, float]:
    parsed = {k: float(v) for k, v in raw.items()}
    s = float(sum(parsed.values()))
    if s <= 0:
        raise ValueError("Weights must sum to a positive number")
    return {k: v / s for k, v in parsed.items()}


def _parse_radius_weights(raw: dict[str, Any], radii_m: list[int]) -> dict[int, float]:
    weights: dict[int, float] = {}
    for r in radii_m:
        key = str(r)
        if key not in raw:
            raise ValueError(f"Missing radius weight for {r}m")
        weights[r] = float(raw[key])
    return _normalize_weights(weights)


def _parse_targets(raw: dict[str, Any], radii_m: list[int]) -> dict[str, dict[int, float]]:
    out: dict[str, dict[int, float]] = {}
    for mode, by_r in raw.items():
        if not isinstance(by_r, dict):
            continue
        out[mode] = {r: float(by_r[str(r)]) for r in radii_m if str(r) in by_r}
    return out


def build_scoring_config(settings: dict[str, Any]) -> ScoringConfig:
    radii_m = [int(x) for x in settings["buffers"]["radii_m"]]
    scoring = settings["scoring"]
    mode_weights = _normalize_weights(scoring["mode_weights"])
    radius_weights = _parse_radius_weights(scoring["radius_weights"], radii_m=radii_m)
    targets = _parse_targets(scoring["density_targets_per_km2"], radii_m=radii_m)
    return ScoringConfig(
        radii_m=radii_m,
        mode_weights=mode_weights,
        radius_weights=radius_weights,
        density_targets_per_km2=targets,
    )


def compute_accessibility_scores(
    libraries_with_metrics: pd.DataFrame,
    *,
    config: ScoringConfig,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    df = libraries_with_metrics.copy()
    df["id"] = df["id"].astype(str)

    explain_by_id: dict[str, Any] = {}
    scores: list[float] = []
    explain_texts: list[str] = []

    for _, row in df.iterrows():
        score_01 = 0.0
        components: list[dict[str, Any]] = []

        for r in config.radii_m:
            r_weight = config.radius_weights[r]
            for mode, m_weight in config.mode_weights.items():
                target = config.density_targets_per_km2.get(mode, {}).get(r, 0.0)
                density_col = f"stop_density_{mode}_per_km2_{r}m"
                density = float(row.get(density_col, 0.0) or 0.0)
                if target <= 0:
                    normalized = 0.0
                else:
                    normalized = min(density / target, 1.0)
                contribution = float(m_weight * r_weight * normalized)
                score_01 += contribution
                components.append(
                    {
                        "mode": mode,
                        "radius_m": r,
                        "density_per_km2": density,
                        "target_per_km2": target,
                        "normalized_0_1": normalized,
                        "weight_mode": float(m_weight),
                        "weight_radius": float(r_weight),
                        "contribution_0_1": contribution,
                    }
                )

        score_0_100 = float(np.clip(score_01 * 100.0, 0.0, 100.0))
        scores.append(score_0_100)

        explain_payload = build_explain_payload(
            library_row=row.to_dict(),
            score_0_100=score_0_100,
            components=components,
            config=config,
        )
        explain_by_id[str(row["id"])] = explain_payload
        explain_texts.append(build_explain_text(explain_payload))

    df["accessibility_score"] = scores
    df["accessibility_explain"] = explain_texts
    return df, explain_by_id
