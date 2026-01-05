from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from libraryreach.log import configure_logging


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(base)
    for key, value in override.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML must be a mapping: {path}")
    return data


def _load_dotenv_if_present(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def _resolve_project_root(config_path: Path) -> Path:
    config_dir = config_path.resolve().parent
    if config_dir.name == "config":
        return config_dir.parent
    return config_dir


def _ensure_dirs(paths: dict[str, Path]) -> None:
    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)


def load_settings(config_path: Path, scenario: str) -> dict[str, Any]:
    """
    Load base config and merge a scenario override file if present.
    Also initializes runtime directories and logging.
    """
    config_path = config_path.resolve()
    root = _resolve_project_root(config_path)

    _load_dotenv_if_present(root / ".env")

    base = _load_yaml(config_path)
    scenario_path = root / "config" / "scenarios" / f"{scenario}.yaml"
    override = _load_yaml(scenario_path)
    settings = _deep_merge(base, override)

    project = settings.setdefault("project", {})
    paths = {
        "root": root,
        "catalogs_dir": root / project.get("catalogs_dir", "data/catalogs"),
        "raw_dir": root / project.get("raw_dir", "data/raw"),
        "processed_dir": root / project.get("processed_dir", "data/processed"),
        "cache_dir": root / project.get("cache_dir", "cache"),
        "logs_dir": root / project.get("logs_dir", "logs"),
        "reports_dir": root / project.get("reports_dir", "reports"),
    }
    _ensure_dirs(paths)
    (paths["raw_dir"] / "tdx").mkdir(parents=True, exist_ok=True)

    log_level = project.get("log_level", "INFO")
    logger = configure_logging(paths["logs_dir"], level=log_level)

    settings["_meta"] = {
        "config_path": str(config_path),
        "scenario": scenario,
        "scenario_path": str(scenario_path),
    }
    settings["paths"] = {k: str(v) for k, v in paths.items()}
    logger.info("Loaded settings: config=%s scenario=%s", config_path, scenario)
    return settings

