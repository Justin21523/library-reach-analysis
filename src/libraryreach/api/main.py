from __future__ import annotations

import copy
import hashlib
import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from libraryreach.api.schemas import DesertCell, LibraryDetail, LibrarySummary, OutreachRecommendation
from libraryreach.api.patch_validation import validate_config_patch
from libraryreach.api.summary import parse_bbox, summarize, summarize_delta, utc_now_iso
from libraryreach.api.summary_cache import aggregate_summaries, load_qa_report, load_run_meta, load_summary_by_city
from libraryreach.catalogs.load import load_libraries_catalog, load_outreach_candidates_catalog
from libraryreach.catalogs.validate import validate_catalogs
from libraryreach.pipeline import compute_phase1
from libraryreach.planning.deserts import deserts_points_geojson
from libraryreach.settings import load_settings


def _processed_dir() -> Path:
    return Path(os.getenv("LIBRARYREACH_PROCESSED_DIR", "data/processed")).resolve()


def _raw_dir(settings: dict[str, Any]) -> Path:
    return Path((settings.get("paths", {}) or {}).get("raw_dir") or "data/raw").resolve()


def _load_optional_json(path: Path) -> dict[str, Any] | None:
    p = Path(path)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _sources_index_summary(settings: dict[str, Any]) -> dict[str, Any] | None:
    raw_dir = _raw_dir(settings)
    idx = _load_optional_json(raw_dir / "sources_index.json")
    if not idx:
        return None
    rows: list[dict[str, Any]] = []
    for s in idx.get("sources", []) or []:
        if not isinstance(s, dict):
            continue
        checksum = s.get("checksum_sha256")
        checksum_short = None
        if isinstance(checksum, str) and checksum:
            checksum_short = checksum[:8]
        rows.append(
            {
                "source_id": s.get("source_id"),
                "fetched_at": s.get("fetched_at"),
                "status": s.get("status"),
                "checksum_sha256_short": checksum_short,
                "output_path": s.get("output_path"),
            }
        )
    return {"generated_at": idx.get("generated_at"), "sources": rows}

def _safe_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return df.where(pd.notnull(df), None).to_dict(orient="records")


def _etag(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _equirect_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    import math

    rad = math.pi / 180.0
    x = (lon2 - lon1) * rad * math.cos(((lat1 + lat2) / 2.0) * rad)
    y = (lat2 - lat1) * rad
    return float(math.sqrt(x * x + y * y) * 6371000.0)


app = FastAPI(title="LibraryReach API", version="0.1.0")
app.add_middleware(GZipMiddleware, minimum_size=1000)

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


def _point_geojson_from_csv(
    path: Path,
    *,
    lat_col: str = "lat",
    lon_col: str = "lon",
    drop_cols: set[str] | None = None,
) -> dict[str, Any]:
    df = pd.read_csv(path)
    if drop_cols:
        for c in drop_cols:
            if c in df.columns:
                df = df.drop(columns=[c])
    return _df_to_point_geojson(df, lat_col=lat_col, lon_col=lon_col)


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/console")
def console() -> FileResponse:
    return FileResponse(STATIC_DIR / "console.html")


@app.get("/brief")
def brief() -> FileResponse:
    return FileResponse(STATIC_DIR / "brief.html")


@app.get("/results")
def results() -> FileResponse:
    return FileResponse(STATIC_DIR / "results.html")


@app.get("/method")
def method() -> FileResponse:
    return FileResponse(STATIC_DIR / "method.html")


@app.get("/health")
def health() -> dict[str, Any]:
    settings = _settings_for_scenario(DEFAULT_SCENARIO)
    p = _processed_dir()
    run_meta = load_run_meta(p)
    qa = load_qa_report(p)
    raw_dir = _raw_dir(settings)
    ingestion_status = _load_optional_json(raw_dir / "tdx" / "ingestion_status.json")
    stops_meta = _load_optional_json(raw_dir / "tdx" / "stops.meta.json")
    youbike_meta = _load_optional_json(raw_dir / "tdx" / "youbike_stations.meta.json")
    return {
        "ok": True,
        "generated_at": utc_now_iso(),
        "processed_dir": str(p),
        "config_path": str(CONFIG_PATH),
        "run_meta": run_meta,
        "qa": qa,
        "ingestion_status": ingestion_status,
        "stops_meta": stops_meta,
        "youbike_meta": youbike_meta,
        "sources_index": _sources_index_summary(settings),
        "files": {
            "libraries_scored": (p / "libraries_scored.csv").exists(),
            "libraries_explain": (p / "libraries_explain.json").exists(),
            "deserts_csv": (p / "deserts.csv").exists(),
            "deserts_geojson": (p / "deserts.geojson").exists(),
            "outreach_recommendations": (p / "outreach_recommendations.csv").exists(),
            "tdx_stops": (raw_dir / "tdx" / "stops.csv").exists(),
            "tdx_youbike_stations": (raw_dir / "tdx" / "youbike_stations.csv").exists(),
        },
        "mtimes": {
            "libraries_scored": (p / "libraries_scored.csv").stat().st_mtime if (p / "libraries_scored.csv").exists() else None,
            "deserts_csv": (p / "deserts.csv").stat().st_mtime if (p / "deserts.csv").exists() else None,
            "deserts_geojson": (p / "deserts.geojson").stat().st_mtime if (p / "deserts.geojson").exists() else None,
            "outreach_recommendations": (p / "outreach_recommendations.csv").stat().st_mtime if (p / "outreach_recommendations.csv").exists() else None,
            "tdx_stops": (raw_dir / "tdx" / "stops.csv").stat().st_mtime if (raw_dir / "tdx" / "stops.csv").exists() else None,
            "tdx_youbike_stations": (raw_dir / "tdx" / "youbike_stations.csv").stat().st_mtime
            if (raw_dir / "tdx" / "youbike_stations.csv").exists()
            else None,
        },
    }


@app.get("/control/config")
def control_config(scenario: str | None = None) -> dict[str, Any]:
    s = scenario or DEFAULT_SCENARIO
    settings = _settings_for_scenario(s)
    return _subset_settings(settings)


@app.get("/meta")
def meta() -> dict[str, Any]:
    settings = _settings_for_scenario(DEFAULT_SCENARIO)
    p = _processed_dir()
    raw_dir = _raw_dir(settings)
    return {
        "generated_at": utc_now_iso(),
        "scenarios": ["weekday", "weekend", "after_school"],
        "default_scenario": DEFAULT_SCENARIO,
        "cities": [str(c) for c in (settings.get("aoi", {}) or {}).get("cities", [])],
        "processed_dir": str(p),
        "config_path": str(CONFIG_PATH),
        "run_meta": load_run_meta(p),
        "qa": load_qa_report(p),
        "ingestion_status": _load_optional_json(raw_dir / "tdx" / "ingestion_status.json"),
        "stops_meta": _load_optional_json(raw_dir / "tdx" / "stops.meta.json"),
        "youbike_meta": _load_optional_json(raw_dir / "tdx" / "youbike_stations.meta.json"),
        "sources_index": _sources_index_summary(settings),
    }


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


@app.get("/reports/latest")
def reports_latest() -> dict[str, Any]:
    settings = _settings_for_scenario(DEFAULT_SCENARIO)
    p = _processed_dir()
    reports: dict[str, Any] = {"generated_at": utc_now_iso(), "processed_dir": str(p), "run_meta": load_run_meta(p)}
    raw_dir = _raw_dir(settings)
    ingestion_status = _load_optional_json(raw_dir / "tdx" / "ingestion_status.json")
    if ingestion_status is not None:
        reports["ingestion_status"] = ingestion_status
    stops_meta = _load_optional_json(raw_dir / "tdx" / "stops.meta.json")
    if stops_meta is not None:
        reports["stops_meta"] = stops_meta
    sources_index = _sources_index_summary(settings)
    if sources_index is not None:
        reports["sources_index"] = sources_index

    qa_json = p / "qa_report.json"
    qa_md = p / "qa_report.md"
    schema_json = p / "outputs_schema_report.json"
    if qa_json.exists():
        reports["qa_report"] = json.loads(qa_json.read_text(encoding="utf-8"))
    if qa_md.exists():
        reports["qa_report_md"] = qa_md.read_text(encoding="utf-8")
    if schema_json.exists():
        reports["outputs_schema_report"] = json.loads(schema_json.read_text(encoding="utf-8"))

    # Catalog validation lives under reports/ (project reports dir) rather than processed outputs.
    reports_dir = Path(_settings_for_scenario(DEFAULT_SCENARIO)["paths"]["reports_dir"])
    cat_json = reports_dir / "catalog_validation.json"
    cat_md = reports_dir / "catalog_validation.md"
    if cat_json.exists():
        reports["catalog_validation"] = json.loads(cat_json.read_text(encoding="utf-8"))
    if cat_md.exists():
        reports["catalog_validation_md"] = cat_md.read_text(encoding="utf-8")

    return reports


@app.get("/sources")
def sources_index(response: Response) -> dict[str, Any]:
    settings = _settings_for_scenario(DEFAULT_SCENARIO)
    raw_dir = _raw_dir(settings)
    path = raw_dir / "sources_index.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Missing sources_index.json (run ingestion first)")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to parse sources_index.json")
    if not isinstance(data, dict):
        raise HTTPException(status_code=500, detail="Invalid sources_index.json format")

    response.headers["ETag"] = _etag(json.dumps({"mtime": path.stat().st_mtime, "size": path.stat().st_size}))
    return data


@app.post("/analysis/whatif")
def analysis_whatif(payload: dict[str, Any]) -> dict[str, Any]:
    scenario = str(payload.get("scenario") or DEFAULT_SCENARIO)
    config_patch = payload.get("config_patch") or {}
    cities = payload.get("cities")

    if config_patch is not None and not isinstance(config_patch, dict):
        raise HTTPException(status_code=400, detail="config_patch must be an object")
    if cities is not None and not isinstance(cities, list):
        raise HTTPException(status_code=400, detail="cities must be a list")

    patch_errors = validate_config_patch(config_patch or {})
    if patch_errors:
        raise HTTPException(status_code=400, detail={"errors": patch_errors})

    base_settings = _settings_for_scenario(scenario)
    settings = copy.deepcopy(base_settings)

    if cities is not None:
        settings.setdefault("aoi", {})["cities"] = [str(c) for c in cities]

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
        "run_meta": load_run_meta(_processed_dir()),
        "libraries": _safe_records(libs_out),
        "libraries_geojson": _df_to_point_geojson(libs_out, lat_col="lat", lon_col="lon"),
        "deserts_geojson": deserts_points_geojson(deserts_out),
        "outreach": _safe_records(recs_out),
        "outreach_geojson": _df_to_point_geojson(recs_out, lat_col="lat", lon_col="lon") if not recs_out.empty else {"type": "FeatureCollection", "features": []},
    }


@app.get("/analysis/baseline-summary")
def analysis_baseline_summary(
    response: Response,
    scenario: str | None = None,
    cities: list[str] | None = Query(default=None),
    top_n_outreach: int = 10,
) -> dict[str, Any]:
    s = scenario or DEFAULT_SCENARIO
    settings = _settings_for_scenario(s)
    default_cities = [str(c) for c in (settings.get("aoi", {}) or {}).get("cities", [])]
    selected_cities = [str(c) for c in cities] if cities else default_cities

    p = _processed_dir()
    run_meta = load_run_meta(p)
    etag_val = _etag(
        json.dumps(
            {
                "run_id": (run_meta or {}).get("run_id"),
                "config_hash": (run_meta or {}).get("config_hash"),
                "scenario": s,
                "cities": selected_cities,
                "top_n_outreach": int(top_n_outreach),
            },
            sort_keys=True,
        )
    )
    response.headers["ETag"] = etag_val

    cache = load_summary_by_city(p)
    summary = None
    if cache and isinstance(cache.get("summaries_by_city"), dict):
        summary = aggregate_summaries(
            summaries_by_city=cache["summaries_by_city"],
            cities=selected_cities,
            top_n_outreach=int(top_n_outreach),
        )
    else:
        libs_path = p / "libraries_scored.csv"
        deserts_path = p / "deserts.csv"
        outreach_path = p / "outreach_recommendations.csv"
        if not libs_path.exists() or not deserts_path.exists() or not outreach_path.exists():
            raise HTTPException(status_code=404, detail="Missing pipeline output(s). Run pipeline first.")
        libs = pd.read_csv(libs_path)
        deserts = pd.read_csv(deserts_path)
        outreach = pd.read_csv(outreach_path)
        summary = summarize(
            libraries=libs,
            deserts=deserts,
            outreach=outreach,
            cities=selected_cities,
            top_n_outreach=int(top_n_outreach),
        )

    # Optional extra layers should be exposed via summary metrics (so UI can stay lightweight).
    summary.setdefault("metrics", {})["youbike_station_count"] = None
    summary.setdefault("metrics", {})["youbike_station_count_by_city"] = None
    try:
        raw_dir = _raw_dir(settings)
        youbike_path = raw_dir / "tdx" / "youbike_stations.csv"
        if youbike_path.exists():
            yb = pd.read_csv(youbike_path)
            if "city" in yb.columns:
                yb_city = yb["city"].astype(str)
                if selected_cities:
                    yb = yb[yb_city.isin([str(c) for c in selected_cities])].copy()
                by_city = yb["city"].astype(str).value_counts().to_dict()
            else:
                by_city = {}
            total = int(len(yb))
            summary["metrics"]["youbike_station_count"] = total
            summary["metrics"]["youbike_station_count_by_city"] = {
                str(k): int(v) for k, v in (by_city or {}).items()
            }
    except Exception:
        # Optional extras must not break baseline summaries.
        pass

    return {
        "generated_at": utc_now_iso(),
        "scenario": s,
        "cities": selected_cities,
        "source": {"type": "processed_outputs", "processed_dir": str(p)},
        "run_meta": run_meta,
        "summary": summary,
        "config": _subset_settings(settings),
    }


@app.post("/analysis/compare")
def analysis_compare(payload: dict[str, Any]) -> dict[str, Any]:
    scenario = str(payload.get("scenario") or DEFAULT_SCENARIO)
    config_patch = payload.get("config_patch") or {}
    cities = payload.get("cities")

    if config_patch is not None and not isinstance(config_patch, dict):
        raise HTTPException(status_code=400, detail="config_patch must be an object")
    if cities is not None and not isinstance(cities, list):
        raise HTTPException(status_code=400, detail="cities must be a list")

    patch_errors = validate_config_patch(config_patch or {})
    if patch_errors:
        raise HTTPException(status_code=400, detail={"errors": patch_errors})

    base_settings = _settings_for_scenario(scenario)
    selected_cities = (
        [str(c) for c in cities]
        if cities
        else [str(c) for c in (base_settings.get("aoi", {}) or {}).get("cities", [])]
    )

    # Baseline from processed outputs
    p = _processed_dir()
    libs_path = p / "libraries_scored.csv"
    deserts_path = p / "deserts.csv"
    outreach_path = p / "outreach_recommendations.csv"
    if not libs_path.exists() or not deserts_path.exists() or not outreach_path.exists():
        raise HTTPException(status_code=404, detail="Missing pipeline output(s). Run pipeline first.")
    libs_base = pd.read_csv(libs_path)
    deserts_base = pd.read_csv(deserts_path)
    outreach_base = pd.read_csv(outreach_path)
    baseline = summarize(
        libraries=libs_base,
        deserts=deserts_base,
        outreach=outreach_base,
        cities=selected_cities,
        top_n_outreach=10,
    )

    # What-if via compute_phase1
    settings = copy.deepcopy(base_settings)
    if cities is not None:
        settings.setdefault("aoi", {})["cities"] = selected_cities
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

    whatif = summarize(
        libraries=libs_out,
        deserts=deserts_out,
        outreach=recs_out,
        cities=selected_cities,
        top_n_outreach=10,
    )
    delta = summarize_delta(baseline, whatif)

    def _fmt_num(v: float | None) -> str:
        return f"{float(v):.1f}" if isinstance(v, (int, float)) and v is not None else "—"

    def _fmt_delta(v: float | None) -> str:
        if not isinstance(v, (int, float)) or v is None:
            return "—"
        return f"{float(v):+.1f}"

    b = baseline["metrics"]
    w = whatif["metrics"]
    narrative = {
        "headline": "情境比較摘要",
        "bullets": [
            f"平均可達性：{_fmt_num(w.get('avg_accessibility_score'))}（Δ {_fmt_delta(delta.get('avg_accessibility_score'))}）",
            f"服務落差 deserts：{int(w.get('deserts_count') or 0)}（Δ {int(delta.get('deserts_count') or 0):+d}；越少越好）",
            f"外展建議數量：{int(w.get('outreach_count') or 0)}（baseline {int(b.get('outreach_count') or 0)}）",
        ],
    }

    # Structured narrative blocks (localizable, reusable in home/brief/results).
    def _block(kind: str, *, title: str, body: str | None = None, items: list[str] | None = None) -> dict[str, Any]:
        out: dict[str, Any] = {"type": kind, "title": title}
        if body:
            out["body"] = body
        if items:
            out["items"] = items
        return out

    assumption_hints: list[str] = []
    try:
        patch = config_patch or {}
        th = patch.get("planning", {}).get("deserts", {}).get("threshold_score")
        gs = patch.get("spatial", {}).get("grid", {}).get("cell_size_m")
        rr = patch.get("planning", {}).get("outreach", {}).get("coverage_radius_m")
        tn = patch.get("planning", {}).get("outreach", {}).get("top_n_per_city")
        if th is not None:
            assumption_hints.append(f"Desert threshold: {float(th):.0f}/100")
        if gs is not None:
            assumption_hints.append(f"Grid size: {int(gs)}m")
        if rr is not None:
            assumption_hints.append(f"Outreach radius: {int(rr)}m")
        if tn is not None:
            assumption_hints.append(f"Top N / city: {int(tn)}")
    except Exception:
        assumption_hints = []

    narrative_blocks = [
        _block(
            "summary",
            title="重點摘要",
            items=narrative["bullets"],
        ),
        _block(
            "assumptions",
            title="本次假設",
            body=" · ".join(
                [f"Scenario: {scenario}", f"Cities: {', '.join(selected_cities) if selected_cities else '—'}"]
                + (assumption_hints if assumption_hints else [])
            ),
        ),
        _block(
            "interpretation",
            title="如何解讀",
            items=[
                "平均可達性上升代表整體更容易抵達館點，但仍需觀察低分區域是否集中。",
                "deserts 變少表示服務落差縮小（越少越好），可搭配 deserts by city 來設定優先順序。",
                "外展建議是候選點位排序；建議搭配現場條件與合作意願做最後篩選。",
            ],
        ),
        _block(
            "next_steps",
            title="下一步建議",
            items=[
                "在成果頁檢視分數分佈與 deserts by city，找出最需要補強的城市/生活圈。",
                "用控制台調整門檻、格網與外展半徑，觀察 delta 是否符合政策目標。",
                "把本次假設分享為 URL，讓跨角色共讀同一組前提與結果。",
            ],
        ),
    ]

    return {
        "generated_at": utc_now_iso(),
        "scenario": scenario,
        "cities": selected_cities,
        "assumptions": {"scenario": scenario, "cities": selected_cities, "config_patch": config_patch},
        "run_meta": load_run_meta(_processed_dir()),
        "baseline": baseline,
        "whatif": whatif,
        "delta": delta,
        "narrative": narrative,
        "narrative_blocks": narrative_blocks,
        "locale": "zh-Hant",
        "config": _subset_settings(settings),
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
def libraries_geojson(
    cities: list[str] | None = Query(default=None),
    bbox: str | None = None,
    limit: int = 50000,
) -> dict[str, Any]:
    try:
        df, _ = _load_libraries()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=f"Missing pipeline output: {e.filename}")

    if cities and "city" in df.columns:
        df = df[df["city"].astype(str).isin([str(c) for c in cities])].copy()

    bb = None
    if bbox:
        try:
            bb = parse_bbox(bbox)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        if "lon" in df.columns and "lat" in df.columns:
            df = df[
                (df["lon"].astype(float) >= bb.min_lon)
                & (df["lon"].astype(float) <= bb.max_lon)
                & (df["lat"].astype(float) >= bb.min_lat)
                & (df["lat"].astype(float) <= bb.max_lat)
            ].copy()

    if limit and len(df) > int(limit):
        df = df.head(int(limit)).copy()

    cols = ["id", "name", "address", "lat", "lon", "city", "district", "accessibility_score"]
    df = df[[c for c in cols if c in df.columns]].copy()
    features = []
    for row in _safe_records(df):
        lat = row.get("lat")
        lon = row.get("lon")
        if lat is None or lon is None:
            continue
        props = dict(row)
        props.pop("lat", None)
        props.pop("lon", None)
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": props,
            }
        )

    out: dict[str, Any] = {"type": "FeatureCollection", "features": features}
    if bb:
        out["bbox"] = [bb.min_lon, bb.min_lat, bb.max_lon, bb.max_lat]
    return out


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
def deserts_geojson(
    cities: list[str] | None = Query(default=None),
    bbox: str | None = None,
    limit: int = 50000,
) -> dict[str, Any]:
    p = _processed_dir()
    geo_path = p / "deserts.geojson"
    if geo_path.exists():
        data = json.loads(geo_path.read_text(encoding="utf-8"))
        features = data.get("features", [])
        if cities:
            city_set = {str(c) for c in cities}
            features = [f for f in features if str((f.get("properties") or {}).get("city")) in city_set]

        bb = None
        if bbox:
            try:
                bb = parse_bbox(bbox)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
            filtered = []
            for f in features:
                coords = (f.get("geometry") or {}).get("coordinates")
                if not (isinstance(coords, list) and len(coords) == 2):
                    continue
                lon, lat = float(coords[0]), float(coords[1])
                if bb.min_lon <= lon <= bb.max_lon and bb.min_lat <= lat <= bb.max_lat:
                    filtered.append(f)
            features = filtered

        if limit and len(features) > int(limit):
            features = features[: int(limit)]

        out: dict[str, Any] = {"type": "FeatureCollection", "features": features}
        if bb:
            out["bbox"] = [bb.min_lon, bb.min_lat, bb.max_lon, bb.max_lat]
        return out
    # Fallback: build from deserts.csv
    deserts = pd.read_csv(p / "deserts.csv")
    if cities and "city" in deserts.columns:
        deserts = deserts[deserts["city"].astype(str).isin([str(c) for c in cities])].copy()

    bb = None
    if bbox:
        try:
            bb = parse_bbox(bbox)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        if "centroid_lon" in deserts.columns and "centroid_lat" in deserts.columns:
            deserts = deserts[
                (deserts["centroid_lon"].astype(float) >= bb.min_lon)
                & (deserts["centroid_lon"].astype(float) <= bb.max_lon)
                & (deserts["centroid_lat"].astype(float) >= bb.min_lat)
                & (deserts["centroid_lat"].astype(float) <= bb.max_lat)
            ].copy()

    if limit and len(deserts) > int(limit):
        deserts = deserts.head(int(limit)).copy()

    out = deserts_points_geojson(deserts)
    if bb:
        out["bbox"] = [bb.min_lon, bb.min_lat, bb.max_lon, bb.max_lat]
    return out


@app.get("/outreach/recommendations", response_model=list[OutreachRecommendation])
def outreach_recommendations(
    scenario: str | None = None,
    cities: list[str] | None = Query(default=None),
    top_n: int = 1000,
) -> list[OutreachRecommendation]:
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
    if cities:
        df = df[df["city"].astype(str).isin([str(c) for c in cities])].copy()
    df["outreach_score"] = pd.to_numeric(df["outreach_score"], errors="coerce")
    df = df.sort_values("outreach_score", ascending=False)
    df = df.head(int(top_n)).copy()
    return [OutreachRecommendation(**r) for r in _safe_records(df)]


@app.get("/geo/stops")
def stops_geojson(
    bbox: str | None = None,
    modes: list[str] = Query(default=["metro"]),
    city: str | None = None,
    limit: int = 50000,
) -> dict[str, Any]:
    settings = _settings_for_scenario(DEFAULT_SCENARIO)
    raw_dir = _raw_dir(settings)
    path = raw_dir / "tdx" / "stops.csv"
    if not path.exists():
        raise HTTPException(status_code=404, detail="stops.csv not found (run ingestion)")

    df = pd.read_csv(path)

    if "mode" in df.columns and modes:
        mode_set = {str(m) for m in modes}
        df = df[df["mode"].astype(str).isin(mode_set)].copy()
    if city and "city" in df.columns:
        df = df[df["city"].astype(str) == str(city)].copy()

    bb = None
    if bbox:
        try:
            bb = parse_bbox(bbox)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        if "lon" in df.columns and "lat" in df.columns:
            df = df[
                (df["lon"].astype(float) >= bb.min_lon)
                & (df["lon"].astype(float) <= bb.max_lon)
                & (df["lat"].astype(float) >= bb.min_lat)
                & (df["lat"].astype(float) <= bb.max_lat)
            ].copy()

    df = df.dropna(subset=["lat", "lon"])
    if limit and len(df) > int(limit):
        df = df.head(int(limit)).copy()

    out = _df_to_point_geojson(df, lat_col="lat", lon_col="lon")
    if bb:
        out["bbox"] = [bb.min_lon, bb.min_lat, bb.max_lon, bb.max_lat]
    return out


@app.get("/geo/youbike")
def youbike_geojson(
    bbox: str | None = None,
    city: str | None = None,
    limit: int = 50000,
) -> dict[str, Any]:
    settings = _settings_for_scenario(DEFAULT_SCENARIO)
    raw_dir = _raw_dir(settings)
    path = raw_dir / "tdx" / "youbike_stations.csv"
    if not path.exists():
        raise HTTPException(status_code=404, detail="youbike_stations.csv not found (enable_youbike + run ingestion)")

    df = pd.read_csv(path)
    if city and "city" in df.columns:
        df = df[df["city"].astype(str) == str(city)].copy()

    bb = None
    if bbox:
        try:
            bb = parse_bbox(bbox)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        if "lon" in df.columns and "lat" in df.columns:
            df = df[
                (df["lon"].astype(float) >= bb.min_lon)
                & (df["lon"].astype(float) <= bb.max_lon)
                & (df["lat"].astype(float) >= bb.min_lat)
                & (df["lat"].astype(float) <= bb.max_lat)
            ].copy()

    df = df.dropna(subset=["lat", "lon"])
    if limit and len(df) > int(limit):
        df = df.head(int(limit)).copy()

    out = _df_to_point_geojson(df, lat_col="lat", lon_col="lon")
    if bb:
        out["bbox"] = [bb.min_lon, bb.min_lat, bb.max_lon, bb.max_lat]
    return out


@app.get("/analysis/nearest-stops")
def nearest_stops(
    lat: float,
    lon: float,
    modes: list[str] = Query(default=["metro"]),
    k: int = 8,
    max_distance_m: float = 3000.0,
) -> dict[str, Any]:
    settings = _settings_for_scenario(DEFAULT_SCENARIO)
    raw_dir = _raw_dir(settings)
    path = raw_dir / "tdx" / "stops.csv"
    if not path.exists():
        raise HTTPException(status_code=404, detail="stops.csv not found (run ingestion)")
    df = pd.read_csv(path)
    if "mode" in df.columns and modes:
        mode_set = {str(m) for m in modes}
        df = df[df["mode"].astype(str).isin(mode_set)].copy()
    df = df.dropna(subset=["lat", "lon"]).copy()
    if df.empty:
        return {"items": []}
    # Keep k small for UI; compute distances in Python loop (fast enough for this k).
    rows = []
    for r in _safe_records(df):
        try:
            d = _equirect_m(float(lat), float(lon), float(r["lat"]), float(r["lon"]))
        except Exception:
            continue
        if d > float(max_distance_m):
            continue
        rows.append({**r, "distance_m": d})
    rows.sort(key=lambda x: float(x.get("distance_m", 1e18)))
    out = []
    for r in rows[: max(1, int(k))]:
        out.append(
            {
                "stop_id": r.get("stop_id"),
                "name": r.get("name"),
                "mode": r.get("mode"),
                "city": r.get("city"),
                "lat": r.get("lat"),
                "lon": r.get("lon"),
                "distance_m": round(float(r.get("distance_m", 0.0)), 1),
            }
        )
    return {"items": out}
