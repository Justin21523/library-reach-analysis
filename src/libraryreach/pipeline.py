from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from libraryreach.catalogs.load import load_libraries_catalog, load_outreach_candidates_catalog
from libraryreach.catalogs.validate import validate_catalogs
from libraryreach.planning.deserts import DesertConfig, compute_access_deserts_grid, deserts_points_geojson
from libraryreach.planning.outreach import OutreachConfig, recommend_outreach_sites
from libraryreach.scoring.accessibility import build_scoring_config, compute_accessibility_scores
from libraryreach.spatial.joins import compute_point_stop_density


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_csv(path)


def _libraries_catalog_path(settings: dict[str, Any]) -> Path:
    return Path(settings["paths"]["catalogs_dir"]) / "libraries.csv"


def _outreach_catalog_path(settings: dict[str, Any]) -> Path:
    return Path(settings["paths"]["catalogs_dir"]) / "outreach_candidates.csv"


def _stops_path(settings: dict[str, Any]) -> Path:
    return Path(settings["paths"]["raw_dir"]) / "tdx" / "stops.csv"


@dataclass(frozen=True)
class Phase1Outputs:
    libraries_with_metrics: pd.DataFrame
    libraries_scored: pd.DataFrame
    explain_by_id: dict[str, Any]
    deserts: pd.DataFrame
    outreach_recommendations: pd.DataFrame
    reference_lat_deg: float


def compute_phase1(settings: dict[str, Any]) -> Phase1Outputs:
    libraries = load_libraries_catalog(settings)
    outreach_candidates = load_outreach_candidates_catalog(settings)

    validate_catalogs(settings, libraries=libraries, outreach_candidates=outreach_candidates, write_report=True)

    stops = _read_csv(_stops_path(settings))
    missing_stops = {"stop_id", "lat", "lon", "mode"} - set(stops.columns)
    if missing_stops:
        raise ValueError(f"stops missing columns: {sorted(missing_stops)}")

    radii_m = [int(x) for x in settings["buffers"]["radii_m"]]
    ref_lat_strategy = settings.get("spatial", {}).get("distance", {}).get("reference_lat_strategy", "mean")

    lib_metrics, reference_lat_deg = compute_point_stop_density(
        libraries,
        stops,
        radii_m=radii_m,
        point_id_col="id",
        point_lat_col="lat",
        point_lon_col="lon",
        reference_lat_strategy=str(ref_lat_strategy),
    )
    libraries_with_metrics = libraries.merge(lib_metrics, on="id", how="left")

    scoring_config = build_scoring_config(settings)
    libraries_scored, explain_by_id = compute_accessibility_scores(
        libraries_with_metrics,
        config=scoring_config,
    )

    cities = list(settings.get("aoi", {}).get("cities", [])) or sorted(libraries_scored["city"].astype(str).unique())

    grid_cell_size_m = int(settings.get("spatial", {}).get("grid", {}).get("cell_size_m", 1000))
    deserts_cfg = settings["planning"]["deserts"]
    desert_config = DesertConfig(
        cell_size_m=grid_cell_size_m,
        library_search_radius_m=int(deserts_cfg["library_search_radius_m"]),
        threshold_score=float(deserts_cfg["threshold_score"]),
        decay_type=str(deserts_cfg.get("distance_decay", {}).get("type", "linear")),
        decay_zero_at_m=int(
            deserts_cfg.get("distance_decay", {}).get("zero_at_m", deserts_cfg["library_search_radius_m"])
        ),
    )
    deserts = compute_access_deserts_grid(
        cities=cities,
        libraries=libraries_scored,
        outreach_candidates=outreach_candidates,
        reference_lat_deg=float(reference_lat_deg),
        config=desert_config,
    )

    outreach_cfg = settings["planning"]["outreach"]
    outreach_config = OutreachConfig(
        coverage_radius_m=int(outreach_cfg["coverage_radius_m"]),
        top_n_per_city=int(outreach_cfg["top_n_per_city"]),
        weight_coverage=float(outreach_cfg["weight_coverage"]),
        weight_site_access=float(outreach_cfg["weight_site_access"]),
    )

    allowed_types = set(map(str, outreach_cfg.get("allowed_candidate_types", [])))
    if allowed_types and "type" in outreach_candidates.columns:
        outreach_for_planning = outreach_candidates[
            outreach_candidates["type"].astype("string").str.strip().isin(allowed_types)
        ].copy()
    else:
        outreach_for_planning = outreach_candidates

    recommendations = recommend_outreach_sites(
        outreach_candidates=outreach_for_planning,
        deserts=deserts,
        stops=stops,
        reference_lat_deg=float(reference_lat_deg),
        radii_m=radii_m,
        scoring_config=scoring_config,
        config=outreach_config,
    )

    return Phase1Outputs(
        libraries_with_metrics=libraries_with_metrics,
        libraries_scored=libraries_scored,
        explain_by_id=explain_by_id,
        deserts=deserts,
        outreach_recommendations=recommendations,
        reference_lat_deg=float(reference_lat_deg),
    )


def run_phase1(settings: dict[str, Any]) -> None:
    processed_dir = Path(settings["paths"]["processed_dir"])
    processed_dir.mkdir(parents=True, exist_ok=True)

    outputs = compute_phase1(settings)

    outputs.libraries_with_metrics.to_csv(processed_dir / "library_metrics.csv", index=False)

    outputs.libraries_scored.to_csv(processed_dir / "libraries_scored.csv", index=False)
    (processed_dir / "libraries_explain.json").write_text(
        json.dumps(outputs.explain_by_id, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    outputs.deserts.to_csv(processed_dir / "deserts.csv", index=False)
    (processed_dir / "deserts.geojson").write_text(
        json.dumps(deserts_points_geojson(outputs.deserts), ensure_ascii=False),
        encoding="utf-8",
    )

    outputs.outreach_recommendations.to_csv(processed_dir / "outreach_recommendations.csv", index=False)
