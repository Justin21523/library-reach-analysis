from __future__ import annotations

import copy
import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from libraryreach.api.schemas import DesertCell, LibraryDetail, LibrarySummary, OutreachRecommendation
from libraryreach.catalogs.load import load_libraries_catalog, load_outreach_candidates_catalog
from libraryreach.catalogs.validate import validate_catalogs
from libraryreach.pipeline import compute_phase1
from libraryreach.planning.deserts import deserts_points_geojson
from libraryreach.settings import load_settings


def _processed_dir() -> Path:
    return Path(os.getenv("LIBRARYREACH_PROCESSED_DIR", "data/processed")).resolve()


def _safe_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return df.where(pd.notnull(df), None).to_dict(orient="records")


app = FastAPI(title="LibraryReach API", version="0.1.0")

STATIC_DIR = Path(__file__).resolve().parents[1] / "web" / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

CONFIG_PATH = Path(os.getenv("LIBRARYREACH_CONFIG", "config/default.yaml")).resolve()
DEFAULT_SCENARIO = os.getenv("LIBRARYREACH_SCENARIO", "weekday")


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


@lru_cache(maxsize=8)
def _settings_for_scenario(scenario: str) -> dict[str, Any]:
    return load_settings(CONFIG_PATH, scenario=scenario)


def _subset_settings(settings: dict[str, Any]) -> dict[str, Any]:
    return {
        "meta": settings.get("_meta", {}),
        "aoi": settings.get("aoi", {}),
        "buffers": settings.get("buffers", {}),
        "spatial": settings.get("spatial", {}),
        "scoring": settings.get("scoring", {}),
        "planning": settings.get("planning", {}),
    }


def _df_to_point_geojson(df: pd.DataFrame, *, lat_col: str = "lat", lon_col: str = "lon") -> dict[str, Any]:
    features: list[dict[str, Any]] = []
    safe = _safe_records(df)
    for row in safe:
        lat = row.get(lat_col)
        lon = row.get(lon_col)
        if lat is None or lon is None:
            continue
        props = dict(row)
        props.pop(lat_col, None)
        props.pop(lon_col, None)
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]},
                "properties": props,
            }
        )
    return {"type": "FeatureCollection", "features": features}


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
def health() -> dict[str, Any]:
    p = _processed_dir()
    return {
        "ok": True,
        "processed_dir": str(p),
        "files": {
            "libraries_scored": (p / "libraries_scored.csv").exists(),
            "libraries_explain": (p / "libraries_explain.json").exists(),
            "deserts_csv": (p / "deserts.csv").exists(),
            "deserts_geojson": (p / "deserts.geojson").exists(),
            "outreach_recommendations": (p / "outreach_recommendations.csv").exists(),
            "tdx_stops": (Path("data/raw/tdx/stops.csv").resolve()).exists(),
        },
    }


@app.get("/control/config")
def control_config(scenario: str | None = None) -> dict[str, Any]:
    s = scenario or DEFAULT_SCENARIO
    settings = _settings_for_scenario(s)
    return _subset_settings(settings)


@app.post("/control/catalogs/validate")
def control_validate_catalogs(scenario: str | None = None) -> dict[str, Any]:
    settings = _settings_for_scenario(scenario or DEFAULT_SCENARIO)
    libraries = load_libraries_catalog(settings)
    outreach = load_outreach_candidates_catalog(settings)
    return validate_catalogs(
        settings,
        libraries=libraries,
        outreach_candidates=outreach,
        write_report=True,
        raise_on_error=False,
    )


@app.post("/analysis/whatif")
def analysis_whatif(payload: dict[str, Any]) -> dict[str, Any]:
    scenario = str(payload.get("scenario") or DEFAULT_SCENARIO)
    config_patch = payload.get("config_patch") or {}
    cities = payload.get("cities")

    if config_patch is not None and not isinstance(config_patch, dict):
        raise HTTPException(status_code=400, detail="config_patch must be an object")
    if cities is not None and not isinstance(cities, list):
        raise HTTPException(status_code=400, detail="cities must be a list")

    base_settings = _settings_for_scenario(scenario)
    settings = copy.deepcopy(base_settings)

    if cities is not None:
        settings.setdefault("aoi", {})["cities"] = [str(c) for c in cities]

    allowed_top = {"aoi", "buffers", "spatial", "scoring", "planning"}
    unsafe_keys = sorted(set(config_patch.keys()) - allowed_top)
    if unsafe_keys:
        raise HTTPException(status_code=400, detail=f"Unsupported config_patch keys: {unsafe_keys}")

    settings = _deep_merge(settings, config_patch)

    try:
        outputs = compute_phase1(settings)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=f"Missing required input file: {e.filename}")
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    libs_cols = [
        "id",
        "name",
        "address",
        "lat",
        "lon",
        "city",
        "district",
        "accessibility_score",
        "accessibility_explain",
    ]
    libs_out = outputs.libraries_scored[[c for c in libs_cols if c in outputs.libraries_scored.columns]].copy()

    deserts_out = outputs.deserts.copy()
    recs_out = outputs.outreach_recommendations.copy()

    return {
        "scenario": scenario,
        "config": _subset_settings(settings),
        "libraries": _safe_records(libs_out),
        "libraries_geojson": _df_to_point_geojson(libs_out, lat_col="lat", lon_col="lon"),
        "deserts_geojson": deserts_points_geojson(deserts_out),
        "outreach": _safe_records(recs_out),
        "outreach_geojson": _df_to_point_geojson(recs_out, lat_col="lat", lon_col="lon") if not recs_out.empty else {"type": "FeatureCollection", "features": []},
    }


def _load_libraries() -> tuple[pd.DataFrame, dict[str, Any]]:
    p = _processed_dir()
    scored_path = p / "libraries_scored.csv"
    explain_path = p / "libraries_explain.json"
    if not scored_path.exists():
        raise FileNotFoundError(scored_path)
    df = pd.read_csv(scored_path)
    explain: dict[str, Any] = {}
    if explain_path.exists():
        explain = json.loads(explain_path.read_text(encoding="utf-8"))
    return df, explain


@app.get("/libraries", response_model=list[LibrarySummary])
def list_libraries() -> list[LibrarySummary]:
    try:
        df, _ = _load_libraries()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=f"Missing pipeline output: {e.filename}")
    cols = ["id", "name", "address", "lat", "lon", "city", "district", "accessibility_score"]
    df = df[[c for c in cols if c in df.columns]].copy()
    return [LibrarySummary(**r) for r in _safe_records(df)]


@app.get("/libraries/{library_id}", response_model=LibraryDetail)
def get_library(library_id: str) -> LibraryDetail:
    try:
        df, explain_by_id = _load_libraries()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=f"Missing pipeline output: {e.filename}")

    df["id"] = df["id"].astype(str)
    match = df[df["id"] == str(library_id)]
    if match.empty:
        raise HTTPException(status_code=404, detail="Library not found")
    row = _safe_records(match)[0]

    core = {k: row.get(k) for k in ["id", "name", "address", "lat", "lon", "city", "district", "accessibility_score"]}
    metrics = {k: v for k, v in row.items() if k.startswith("stop_") or k.endswith("_m")}
    explain = explain_by_id.get(str(library_id), {})
    explain_text = row.get("accessibility_explain")
    return LibraryDetail(**core, metrics=metrics, explain=explain, explain_text=explain_text)


@app.get("/geo/libraries")
def libraries_geojson() -> dict[str, Any]:
    libs = list_libraries()
    features = []
    for lib in libs:
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lib.lon, lib.lat]},
                "properties": lib.model_dump(exclude={"lat", "lon"}),
            }
        )
    return {"type": "FeatureCollection", "features": features}


@app.get("/deserts", response_model=list[DesertCell])
def list_deserts() -> list[DesertCell]:
    p = _processed_dir()
    path = p / "deserts.csv"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Missing pipeline output: deserts.csv")
    df = pd.read_csv(path)
    cols = [
        "cell_id",
        "city",
        "centroid_lat",
        "centroid_lon",
        "effective_score_0_100",
        "is_desert",
        "gap_to_threshold",
        "best_library_id",
        "best_library_distance_m",
    ]
    df = df[[c for c in cols if c in df.columns]].copy()
    return [DesertCell(**r) for r in _safe_records(df)]


@app.get("/geo/deserts")
def deserts_geojson() -> dict[str, Any]:
    p = _processed_dir()
    geo_path = p / "deserts.geojson"
    if geo_path.exists():
        return json.loads(geo_path.read_text(encoding="utf-8"))
    # Fallback: build from deserts.csv
    deserts = pd.read_csv(p / "deserts.csv")
    return deserts_points_geojson(deserts)


@app.get("/outreach/recommendations", response_model=list[OutreachRecommendation])
def outreach_recommendations() -> list[OutreachRecommendation]:
    p = _processed_dir()
    path = p / "outreach_recommendations.csv"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Missing pipeline output: outreach_recommendations.csv")
    df = pd.read_csv(path)
    if df.empty:
        return []
    # Ensure required keys exist for the response model
    required = ["id", "name", "lat", "lon", "city", "district", "outreach_score"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise HTTPException(status_code=500, detail=f"Invalid outreach output schema: missing {missing}")
    return [OutreachRecommendation(**r) for r in _safe_records(df)]
