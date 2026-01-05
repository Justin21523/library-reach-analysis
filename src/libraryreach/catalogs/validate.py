from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from libraryreach.catalogs.validators import (
    CatalogValidationResult,
    validate_libraries_catalog,
    validate_multi_city_consistency,
    validate_outreach_candidates_catalog,
)


def validate_catalogs(
    settings: dict[str, Any],
    *,
    libraries: pd.DataFrame,
    outreach_candidates: pd.DataFrame,
    write_report: bool = True,
) -> dict[str, Any]:
    allowed_cities = set(map(str, settings.get("aoi", {}).get("cities", []))) or None
    allowed_types = (
        set(map(str, settings.get("planning", {}).get("outreach", {}).get("allowed_candidate_types", [])))
        or None
    )

    lib_result = validate_libraries_catalog(libraries, allowed_cities=allowed_cities)
    out_result = validate_outreach_candidates_catalog(
        outreach_candidates,
        allowed_cities=allowed_cities,
        allowed_types=allowed_types,
    )
    consistency = validate_multi_city_consistency(
        libraries=libraries,
        outreach_candidates=outreach_candidates,
        configured_cities=list(map(str, settings.get("aoi", {}).get("cities", []))),
    )

    errors = lib_result.errors + out_result.errors + consistency.errors
    warnings = lib_result.warnings + out_result.warnings + consistency.warnings

    report = {
        "ok": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "stats": {
            "libraries": lib_result.stats,
            "outreach_candidates": out_result.stats,
            "consistency": consistency.stats,
        },
    }

    if write_report:
        reports_dir = Path(settings["paths"]["reports_dir"])
        reports_dir.mkdir(parents=True, exist_ok=True)
        (reports_dir / "catalog_validation.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        _write_markdown_report(reports_dir / "catalog_validation.md", report)

    if errors:
        raise ValueError("Catalog validation failed. See reports/catalog_validation.md for details.")

    return report


def _write_markdown_report(path: Path, report: dict[str, Any]) -> None:
    lines: list[str] = []
    ok = bool(report.get("ok"))
    lines.append("# Catalog validation")
    lines.append("")
    lines.append(f"Status: {'OK' if ok else 'FAILED'}")
    lines.append("")

    errors = list(report.get("errors", []))
    warnings = list(report.get("warnings", []))
    if errors:
        lines.append("## Errors")
        lines.extend([f"- {e}" for e in errors])
        lines.append("")
    if warnings:
        lines.append("## Warnings")
        lines.extend([f"- {w}" for w in warnings])
        lines.append("")

    stats = report.get("stats", {}) or {}
    lines.append("## Stats")
    lines.append("```json")
    lines.append(json.dumps(stats, ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")


def format_validation_summary(report: dict[str, Any]) -> str:
    errors = list(report.get("errors", []))
    warnings = list(report.get("warnings", []))
    if errors:
        return f"FAILED: {len(errors)} errors, {len(warnings)} warnings"
    if warnings:
        return f"OK with warnings: {len(warnings)} warnings"
    return "OK"

