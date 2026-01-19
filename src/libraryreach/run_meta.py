from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


@dataclass(frozen=True)
class FileMeta:
    path: str
    exists: bool
    size_bytes: int | None
    mtime: float | None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def file_meta(path: Path) -> FileMeta:
    p = Path(path)
    if not p.exists():
        return FileMeta(path=str(p), exists=False, size_bytes=None, mtime=None)
    st = p.stat()
    return FileMeta(path=str(p), exists=True, size_bytes=int(st.st_size), mtime=float(st.st_mtime))


def _json_hash(data: Any) -> str:
    raw = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def json_hash(data: Any) -> str:
    """
    Public wrapper for hashing JSON-serializable structures deterministically.
    """
    return _json_hash(data)


def config_fingerprint(settings: dict[str, Any]) -> dict[str, Any]:
    """
    Produce a stable, shareable config fingerprint for traceability.
    Keep it limited to analysis-relevant knobs (avoid paths/secrets).
    """
    meta = settings.get("_meta", {}) or {}
    return {
        "scenario": meta.get("scenario"),
        "config_path": meta.get("config_path"),
        "scenario_path": meta.get("scenario_path"),
        "aoi": settings.get("aoi", {}),
        "buffers": settings.get("buffers", {}),
        "spatial": settings.get("spatial", {}),
        "scoring": settings.get("scoring", {}),
        "planning": settings.get("planning", {}),
    }


def new_run_id() -> str:
    return uuid4().hex


def build_run_meta(
    *,
    run_id: str,
    generated_at: str,
    settings: dict[str, Any],
    cities: list[str],
    input_sources: list[FileMeta],
    outputs: list[FileMeta],
    schema_versions: dict[str, Any],
) -> dict[str, Any]:
    fingerprint = config_fingerprint(settings)
    return {
        "run_id": run_id,
        "generated_at": generated_at,
        "scenario": fingerprint.get("scenario"),
        "cities": [str(c) for c in cities],
        "config_hash": _json_hash(fingerprint),
        "config_fingerprint": fingerprint,
        "input_sources": [fm.__dict__ for fm in input_sources],
        "outputs": [fm.__dict__ for fm in outputs],
        "schema_versions": schema_versions,
    }


def write_json(path: Path, data: Any) -> None:
    Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
