"""
Catalog validation orchestration (Phase 2: Loading).

This module is the glue between:
- raw DataFrames produced by `libraryreach.catalogs.load`, and
- rule checks implemented in `libraryreach.catalogs.validators`.

It combines results, optionally writes human-readable reports, and provides a
small summary helper that is convenient for CLI output and API responses.
"""

from __future__ import annotations

# `json` is used to write a machine-readable validation report.
import json
# `Path` is used for safe filesystem writes (reports directory).
from pathlib import Path
# `Any` is used because validation reports contain nested JSON-like structures.
from typing import Any

# pandas DataFrames are the inputs we validate.
import pandas as pd

# Import validators explicitly so readers can see where each check comes from.
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
    raise_on_error: bool = True,
) -> dict[str, Any]:
    # Convert config lists into sets for faster membership checks during validation.
    allowed_cities = set(map(str, settings.get("aoi", {}).get("cities", []))) or None
    allowed_types = (
        set(map(str, settings.get("planning", {}).get("outreach", {}).get("allowed_candidate_types", [])))
        or None
    )

    # Validate each catalog independently so errors are easy to attribute.
    lib_result = validate_libraries_catalog(libraries, allowed_cities=allowed_cities)
    out_result = validate_outreach_candidates_catalog(
        outreach_candidates,
        allowed_cities=allowed_cities,
        allowed_types=allowed_types,
    )
    # Validate cross-catalog consistency for multi-city setups (coverage across configured cities).
    consistency = validate_multi_city_consistency(
        libraries=libraries,
        outreach_candidates=outreach_candidates,
        configured_cities=list(map(str, settings.get("aoi", {}).get("cities", []))),
    )

    # Merge messages so callers can treat this as a single validation step.
    errors = lib_result.errors + out_result.errors + consistency.errors
    warnings = lib_result.warnings + out_result.warnings + consistency.warnings

    # Build a JSON-serializable report so it can be returned by API endpoints as-is.
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
        # Create the reports directory if needed so writing is always safe and idempotent.
        reports_dir = Path(settings["paths"]["reports_dir"])
        reports_dir.mkdir(parents=True, exist_ok=True)
        # Write JSON with UTF-8 so Chinese names remain readable (no ASCII escaping).
        (reports_dir / "catalog_validation.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        # Also write a Markdown report for humans (easier to scan in a browser or PR diff).
        _write_markdown_report(reports_dir / "catalog_validation.md", report)

    # Raise an exception (optional) so CLI commands can fail fast in CI or scripted runs.
    if errors and raise_on_error:
        raise ValueError("Catalog validation failed. See reports/catalog_validation.md for details.")

    # Return the full report so callers (CLI/API) can display details if needed.
    return report


def _write_markdown_report(path: Path, report: dict[str, Any]) -> None:
    # Build lines manually so we can control formatting without extra dependencies.
    lines: list[str] = []
    # `ok` is a boolean flag stored in the report; default to False if missing.
    ok = bool(report.get("ok"))
    # The report starts with a stable title so diffs remain readable.
    lines.append("# Catalog validation")
    lines.append("")
    # A single status line makes it obvious whether the run is acceptable.
    lines.append(f"Status: {'OK' if ok else 'FAILED'}")
    lines.append("")

    # Extract messages as lists so we can safely iterate even if keys are missing.
    errors = list(report.get("errors", []))
    warnings = list(report.get("warnings", []))
    if errors:
        # Errors are shown first because they block the pipeline.
        lines.append("## Errors")
        lines.extend([f"- {e}" for e in errors])
        lines.append("")
    if warnings:
        # Warnings are next so users can decide whether to fix or accept them.
        lines.append("## Warnings")
        lines.extend([f"- {w}" for w in warnings])
        lines.append("")

    # Stats provide context (row counts, distinct values) without adding noise to errors.
    stats = report.get("stats", {}) or {}
    lines.append("## Stats")
    # Embed stats as JSON so it stays structured and copy/paste friendly.
    lines.append("```json")
    lines.append(json.dumps(stats, ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")

    # Write as UTF-8 so bilingual content remains readable.
    path.write_text("\n".join(lines), encoding="utf-8")


def format_validation_summary(report: dict[str, Any]) -> str:
    # A small helper for CLI output: callers usually want a single-line status first.
    errors = list(report.get("errors", []))
    warnings = list(report.get("warnings", []))
    if errors:
        # Include counts so users know whether they are dealing with "one issue" or "many issues".
        return f"FAILED: {len(errors)} errors, {len(warnings)} warnings"
    if warnings:
        # Warnings are non-fatal, but the summary makes it clear that review is needed.
        return f"OK with warnings: {len(warnings)} warnings"
    # A clean run has neither errors nor warnings.
    return "OK"
