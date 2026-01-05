"""
Settings bootstrap for LibraryReach.

This module is intentionally small and dependency-light because it sits at the very
start of our data flow: every CLI command, pipeline run, and API endpoint reads
configuration through `load_settings()` first.
"""

from __future__ import annotations

# Standard library imports are used here to keep bootstrap logic portable.
import os
# `Path` makes path handling cross-platform (Windows/macOS/Linux).
from pathlib import Path
# `Any` is used because YAML is dynamic and we validate gradually across phases.
from typing import Any

# PyYAML provides YAML parsing for config files (human-editable settings).
import yaml

# Logging is configured early so later modules can rely on consistent logs.
from libraryreach.log import configure_logging


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    # Copy the base mapping so we never mutate caller-owned dictionaries (safer and more predictable).
    merged: dict[str, Any] = dict(base)
    # Iterate override keys so overrides always win for conflicts.
    for key, value in override.items():
        # If both sides are dictionaries, merge recursively so scenarios can override only a nested subset.
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            # Recurse to preserve existing nested keys that are not overridden.
            merged[key] = _deep_merge(merged[key], value)
        else:
            # For non-dicts (or when base is not a dict), the override replaces the base value.
            merged[key] = value
    # Return a new dictionary that represents the merged configuration view.
    return merged


def _load_yaml(path: Path) -> dict[str, Any]:
    # Treat missing YAML files as "no overrides" so scenarios are optional.
    if not path.exists():
        return {}
    # Read YAML as UTF-8 so Chinese/Unicode strings are handled correctly.
    with path.open("r", encoding="utf-8") as f:
        # `safe_load` avoids executing arbitrary YAML tags (security best practice).
        data = yaml.safe_load(f) or {}
    # We expect config files to be YAML mappings (key/value), not lists or scalars.
    if not isinstance(data, dict):
        raise ValueError(f"YAML must be a mapping: {path}")
    # Return a plain dict so downstream code can easily access settings["section"]["key"].
    return data


def _load_dotenv_if_present(dotenv_path: Path) -> None:
    # `.env` is optional; if it's missing we simply rely on the existing environment.
    if not dotenv_path.exists():
        return
    # Read the file once and iterate line-by-line so we can support comments and blank lines.
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        # Strip whitespace to avoid accidental spaces becoming part of keys/values.
        line = raw_line.strip()
        # Skip empty lines, comment lines, or malformed lines that are not key/value pairs.
        if not line or line.startswith("#") or "=" not in line:
            continue
        # Split only once so values may legally contain "=" characters.
        key, value = line.split("=", 1)
        # Normalize whitespace around keys to match typical dotenv expectations.
        key = key.strip()
        # Strip whitespace and optional quotes so `.env` can use KEY="value" or KEY='value'.
        value = value.strip().strip('"').strip("'")
        # Do NOT override an already-set environment variable (common pitfall in dev/prod parity).
        os.environ.setdefault(key, value)


def _resolve_project_root(config_path: Path) -> Path:
    # Resolve symlinks / relative paths so downstream path math is deterministic.
    config_dir = config_path.resolve().parent
    # If the user points at config/default.yaml, we treat the repo root as the parent of `config/`.
    if config_dir.name == "config":
        return config_dir.parent
    # Otherwise, we assume the config file lives in the project root already.
    return config_dir


def _ensure_dirs(paths: dict[str, Path]) -> None:
    # Create runtime directories up-front so later pipeline stages can write outputs reliably.
    for p in paths.values():
        # `exist_ok=True` makes this idempotent (safe to call multiple times).
        p.mkdir(parents=True, exist_ok=True)


def load_settings(config_path: Path, scenario: str) -> dict[str, Any]:
    """
    Load base config and merge a scenario override file if present.
    Also initializes runtime directories and logging.
    """
    # Normalize the config path early so we log and store a canonical path.
    config_path = config_path.resolve()
    # Infer the project root so relative paths in config are resolved consistently.
    root = _resolve_project_root(config_path)

    # Load `.env` before reading settings so config and clients can read credentials from env vars.
    _load_dotenv_if_present(root / ".env")

    # Load base settings from YAML (this file is version-controlled).
    base = _load_yaml(config_path)
    # Scenario settings are stored under config/scenarios/<scenario>.yaml (also version-controlled).
    scenario_path = root / "config" / "scenarios" / f"{scenario}.yaml"
    # Missing scenario files are treated as empty overrides (so "weekday" can be minimal).
    override = _load_yaml(scenario_path)
    # Merge base and scenario so the scenario only needs to specify what it changes.
    settings = _deep_merge(base, override)

    # Ensure `project` exists so we can read/write runtime paths in a single place.
    project = settings.setdefault("project", {})
    # Build absolute paths (root + relative) so all modules can write to predictable locations.
    paths = {
        # Store root mostly for debugging and for future "resolve relative path" utilities.
        "root": root,
        # Catalogs are project-owned and are expected to be committed (CSV/JSON).
        "catalogs_dir": root / project.get("catalogs_dir", "data/catalogs"),
        # Raw data stores external pulls (TDX, etc.) for reproducibility and caching.
        "raw_dir": root / project.get("raw_dir", "data/raw"),
        # Processed data stores analysis outputs that are safe to regenerate.
        "processed_dir": root / project.get("processed_dir", "data/processed"),
        # Cache stores runtime caches (tokens, HTTP responses) and is safe to delete.
        "cache_dir": root / project.get("cache_dir", "cache"),
        # Logs store run logs for debugging and auditing.
        "logs_dir": root / project.get("logs_dir", "logs"),
        # Reports store human-facing artifacts like validation reports.
        "reports_dir": root / project.get("reports_dir", "reports"),
    }
    # Create directories so downstream stages don't need to check every write.
    _ensure_dirs(paths)
    # Ensure a dedicated raw subfolder exists for TDX data pulls.
    (paths["raw_dir"] / "tdx").mkdir(parents=True, exist_ok=True)

    # Allow config to control verbosity while keeping a sensible default for local development.
    log_level = project.get("log_level", "INFO")
    # Configure a shared logger early so CLI/pipeline/API logs go to the same places.
    logger = configure_logging(paths["logs_dir"], level=log_level)

    # Store metadata so outputs can be traced back to the exact config inputs used.
    settings["_meta"] = {
        "config_path": str(config_path),
        "scenario": scenario,
        "scenario_path": str(scenario_path),
    }
    # Store resolved paths as strings so settings is JSON-serializable if needed.
    settings["paths"] = {k: str(v) for k, v in paths.items()}
    # Log the bootstrap summary so every run has an obvious starting line in logs.
    logger.info("Loaded settings: config=%s scenario=%s", config_path, scenario)
    # Return settings as a dict to keep Phase 1 simple (we can add stricter typing later).
    return settings
