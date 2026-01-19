from __future__ import annotations

from typing import Any


def validate_config_patch(patch: dict[str, Any]) -> list[str]:
    """
    Validate config_patch structure and value ranges.
    Return a list of user-facing error strings (empty means OK).
    """
    errors: list[str] = []

    if not isinstance(patch, dict):
        return ["config_patch must be an object"]

    allowed_top = {"aoi", "buffers", "spatial", "scoring", "planning"}
    unsafe_keys = sorted(set(patch.keys()) - allowed_top)
    if unsafe_keys:
        errors.append(f"Unsupported config_patch keys: {unsafe_keys}")
        return errors

    # scoring.mode_weights + radius_weights in [0,1]
    scoring = patch.get("scoring") or {}
    if isinstance(scoring, dict):
        for section, keys in [("mode_weights", ["bus", "metro"]), ("radius_weights", ["500", "1000"])]:
            w = (scoring.get(section) or {}) if isinstance(scoring.get(section), dict) else None
            if w is None:
                continue
            for k in keys:
                if k not in w:
                    continue
                try:
                    v = float(w[k])
                except (TypeError, ValueError):
                    errors.append(f"scoring.{section}.{k} must be a number")
                    continue
                if v < 0 or v > 1:
                    errors.append(f"scoring.{section}.{k} must be within [0,1]")

        targets = scoring.get("density_targets_per_km2")
        if targets is not None and not isinstance(targets, dict):
            errors.append("scoring.density_targets_per_km2 must be an object")

    # spatial.grid.cell_size_m
    spatial = patch.get("spatial") or {}
    if isinstance(spatial, dict):
        grid = spatial.get("grid") or {}
        if isinstance(grid, dict) and "cell_size_m" in grid:
            try:
                v = int(grid["cell_size_m"])
            except (TypeError, ValueError):
                errors.append("spatial.grid.cell_size_m must be an integer")
            else:
                if v < 100:
                    errors.append("spatial.grid.cell_size_m must be >= 100")

    # planning.deserts threshold & radii; planning.outreach weights & top_n
    planning = patch.get("planning") or {}
    if isinstance(planning, dict):
        deserts = planning.get("deserts") or {}
        if isinstance(deserts, dict):
            if "threshold_score" in deserts:
                try:
                    v = float(deserts["threshold_score"])
                except (TypeError, ValueError):
                    errors.append("planning.deserts.threshold_score must be a number")
                else:
                    if v < 0 or v > 100:
                        errors.append("planning.deserts.threshold_score must be within [0,100]")
            for key in ["library_search_radius_m"]:
                if key in deserts:
                    try:
                        v = int(deserts[key])
                    except (TypeError, ValueError):
                        errors.append(f"planning.deserts.{key} must be an integer")
                    else:
                        if v <= 0:
                            errors.append(f"planning.deserts.{key} must be > 0")
            dd = deserts.get("distance_decay")
            if dd is not None and not isinstance(dd, dict):
                errors.append("planning.deserts.distance_decay must be an object")

        outreach = planning.get("outreach") or {}
        if isinstance(outreach, dict):
            for key in ["coverage_radius_m", "top_n_per_city"]:
                if key in outreach:
                    try:
                        v = int(outreach[key])
                    except (TypeError, ValueError):
                        errors.append(f"planning.outreach.{key} must be an integer")
                    else:
                        if v <= 0:
                            errors.append(f"planning.outreach.{key} must be > 0")
            for key in ["weight_coverage", "weight_site_access"]:
                if key in outreach:
                    try:
                        v = float(outreach[key])
                    except (TypeError, ValueError):
                        errors.append(f"planning.outreach.{key} must be a number")
                    else:
                        if v < 0 or v > 1:
                            errors.append(f"planning.outreach.{key} must be within [0,1]")
            if "allowed_candidate_types" in outreach and not isinstance(outreach["allowed_candidate_types"], list):
                errors.append("planning.outreach.allowed_candidate_types must be a list")

    return errors

