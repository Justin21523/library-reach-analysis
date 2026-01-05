from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from libraryreach.api.schemas import DesertCell, LibraryDetail, LibrarySummary, OutreachRecommendation
from libraryreach.planning.deserts import deserts_points_geojson


def _processed_dir() -> Path:
    return Path(os.getenv("LIBRARYREACH_PROCESSED_DIR", "data/processed")).resolve()


def _safe_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return df.where(pd.notnull(df), None).to_dict(orient="records")


app = FastAPI(title="LibraryReach API", version="0.1.0")

STATIC_DIR = Path(__file__).resolve().parents[2] / "web" / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


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
        },
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

