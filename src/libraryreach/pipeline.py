from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from libraryreach.catalogs.load import load_libraries_catalog, load_outreach_candidates_catalog
from libraryreach.catalogs.validate import validate_catalogs
from libraryreach.data.outputs_schema import SCHEMA_VERSION, validate_phase1_outputs
from libraryreach.planning.deserts import DesertConfig, compute_access_deserts_grid, deserts_points_geojson
from libraryreach.planning.outreach import OutreachConfig, recommend_outreach_sites
from libraryreach.run_meta import build_run_meta, file_meta, new_run_id, utc_now_iso, write_json
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

    run_id = new_run_id()
    generated_at = utc_now_iso()

    # Capture previous summary (if any) before overwriting cached artifacts.
    previous_summary_metrics: dict[str, Any] | None = None
    prev_path = processed_dir / "summary_baseline.json"
    if prev_path.exists():
        try:
            previous_summary_metrics = json.loads(prev_path.read_text(encoding="utf-8")).get("summary", {}).get("metrics", {})
        except Exception:
            previous_summary_metrics = None

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

    # Validate output schema and write a structured report for traceability.
    schema_report = validate_phase1_outputs(
        libraries_scored=outputs.libraries_scored,
        deserts=outputs.deserts,
        outreach_recommendations=outputs.outreach_recommendations,
    )
    write_json(processed_dir / "outputs_schema_report.json", schema_report)
    if not schema_report.get("ok", False):
        raise ValueError("Phase 1 outputs failed schema validation. See data/processed/outputs_schema_report.json")

    # Write run metadata for reproducibility and UI traceability.
    meta = settings.get("_meta", {}) or {}
    cities = list(settings.get("aoi", {}).get("cities", [])) or sorted(outputs.libraries_scored["city"].astype(str).unique())
    input_sources = [
        file_meta(Path(settings["paths"]["raw_dir"]) / "tdx" / "stops.csv"),
        file_meta(Path(settings["paths"]["raw_dir"]) / "sources_index.json"),
        file_meta(Path(settings["paths"]["catalogs_dir"]) / "libraries.csv"),
        file_meta(Path(settings["paths"]["catalogs_dir"]) / "outreach_candidates.csv"),
        file_meta(Path(str(meta.get("config_path"))) if meta.get("config_path") else Path("config/default.yaml")),
        file_meta(Path(str(meta.get("scenario_path"))) if meta.get("scenario_path") else Path("config/scenarios/weekday.yaml")),
    ]
    output_files = [
        file_meta(processed_dir / "libraries_scored.csv"),
        file_meta(processed_dir / "libraries_explain.json"),
        file_meta(processed_dir / "deserts.csv"),
        file_meta(processed_dir / "deserts.geojson"),
        file_meta(processed_dir / "outreach_recommendations.csv"),
        file_meta(processed_dir / "outputs_schema_report.json"),
    ]
    run_meta = build_run_meta(
        run_id=run_id,
        generated_at=generated_at,
        settings=settings,
        cities=[str(c) for c in cities],
        input_sources=input_sources,
        outputs=output_files,
        schema_versions={"phase1_outputs": SCHEMA_VERSION},
    )
    write_json(processed_dir / "run_meta.json", run_meta)

    # Write cached summary artifacts for fast API responses.
    from libraryreach.api.summary import summarize

    summary_all = summarize(
        libraries=outputs.libraries_scored,
        deserts=outputs.deserts,
        outreach=outputs.outreach_recommendations,
        cities=[str(c) for c in cities],
        top_n_outreach=50,
    )
    write_json(processed_dir / "summary_baseline.json", {"run_meta": run_meta, "summary": summary_all})

    summaries_by_city: dict[str, Any] = {}
    for city in [str(c) for c in cities]:
        summaries_by_city[city] = summarize(
            libraries=outputs.libraries_scored,
            deserts=outputs.deserts,
            outreach=outputs.outreach_recommendations,
            cities=[city],
            top_n_outreach=50,
        )
    write_json(processed_dir / "summary_by_city.json", {"run_meta": run_meta, "summaries_by_city": summaries_by_city})

    # QA report: lightweight quality checks for traceability and dashboards.
    qa = _build_qa_report(
        outputs=outputs,
        settings=settings,
        run_meta=run_meta,
        previous_summary_metrics=previous_summary_metrics,
    )
    write_json(processed_dir / "qa_report.json", qa)
    _write_qa_markdown(processed_dir / "qa_report.md", qa)


def _build_qa_report(
    *,
    outputs: Phase1Outputs,
    settings: dict[str, Any],
    run_meta: dict[str, Any],
    previous_summary_metrics: dict[str, Any] | None,
) -> dict[str, Any]:
    libs = outputs.libraries_scored.copy()
    deserts = outputs.deserts.copy()
    outreach = outputs.outreach_recommendations.copy()

    def num(df: pd.DataFrame, col: str) -> pd.Series:
        return pd.to_numeric(df[col], errors="coerce") if col in df.columns else pd.Series(dtype=float)

    lib_score = num(libs, "accessibility_score")
    missing_lib_coords = int(num(libs, "lat").isna().sum() + num(libs, "lon").isna().sum())
    deserts_count = int((deserts["is_desert"] == True).sum()) if "is_desert" in deserts.columns else 0  # noqa: E712
    outreach_score = num(outreach, "outreach_score")

    cur = {
        "libraries_count": int(len(libs)),
        "avg_accessibility_score": float(lib_score.mean()) if not lib_score.empty else None,
        "deserts_count": deserts_count,
        "outreach_count": int(len(outreach)),
    }

    deltas = None
    if previous_summary_metrics:
        deltas = {
            "avg_accessibility_score": (cur["avg_accessibility_score"] or 0)
            - float(previous_summary_metrics.get("avg_accessibility_score") or 0),
            "deserts_count": int(cur["deserts_count"] or 0) - int(previous_summary_metrics.get("deserts_count") or 0),
            "outreach_count": int(cur["outreach_count"] or 0)
            - int(previous_summary_metrics.get("outreach_count") or 0),
        }

    return {
        "run_meta": run_meta,
        "generated_at": run_meta.get("generated_at"),
        "scenario": (settings.get("_meta", {}) or {}).get("scenario"),
        "kpis": cur,
        "deltas_vs_previous": deltas,
        "checks": {
            "libraries_missing_coords": missing_lib_coords,
            "libraries_score_min": float(lib_score.min()) if not lib_score.empty else None,
            "libraries_score_max": float(lib_score.max()) if not lib_score.empty else None,
            "deserts_rows": int(len(deserts)),
            "outreach_score_min": float(outreach_score.min()) if not outreach_score.empty else None,
            "outreach_score_max": float(outreach_score.max()) if not outreach_score.empty else None,
        },
        "notes": [
            "This report is designed for dashboards/briefs. For catalog checks see reports/catalog_validation.md.",
        ],
    }


def _write_qa_markdown(path: Path, report: dict[str, Any]) -> None:
    lines: list[str] = []
    lines.append("# QA report (Phase 1)")
    lines.append("")
    rm = report.get("run_meta", {}) or {}
    lines.append(f"- Run ID: {rm.get('run_id', '-')}")
    lines.append(f"- Generated: {rm.get('generated_at', '-')}")
    lines.append(f"- Scenario: {report.get('scenario', '-')}")
    lines.append("")
    lines.append("## KPIs")
    lines.append("```json")
    lines.append(json.dumps(report.get("kpis", {}), ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")
    if report.get("deltas_vs_previous") is not None:
        lines.append("## Deltas vs previous")
        lines.append("```json")
        lines.append(json.dumps(report.get("deltas_vs_previous", {}), ensure_ascii=False, indent=2))
        lines.append("```")
        lines.append("")
    lines.append("## Checks")
    lines.append("```json")
    lines.append(json.dumps(report.get("checks", {}), ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")
    lines.append("## Notes")
    for n in report.get("notes", []):
        lines.append(f"- {n}")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")
