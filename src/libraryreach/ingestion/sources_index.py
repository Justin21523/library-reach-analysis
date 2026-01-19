from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json(path: Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_json_atomic(path: Path, data: Any) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


@dataclass(frozen=True)
class SourceRecord:
    source_id: str
    fetched_at: str
    output_path: str
    checksum_sha256: str
    status: str
    details: dict[str, Any]


def sources_index_path(settings: dict[str, Any]) -> Path:
    return Path(settings["paths"]["raw_dir"]) / "sources_index.json"


def load_sources_index(settings: dict[str, Any]) -> dict[str, Any]:
    path = sources_index_path(settings)
    if not path.exists():
        return {"generated_at": utc_now_iso(), "sources": []}
    try:
        data = _read_json(path)
    except Exception:
        return {"generated_at": utc_now_iso(), "sources": []}
    if not isinstance(data, dict):
        return {"generated_at": utc_now_iso(), "sources": []}
    if "sources" not in data or not isinstance(data.get("sources"), list):
        data["sources"] = []
    return data


def upsert_source_record(settings: dict[str, Any], record: SourceRecord) -> Path:
    idx = load_sources_index(settings)
    sources: list[dict[str, Any]] = list(idx.get("sources") or [])

    new_row = {
        "source_id": record.source_id,
        "fetched_at": record.fetched_at,
        "output_path": record.output_path,
        "checksum_sha256": record.checksum_sha256,
        "status": record.status,
        "details": record.details,
    }

    replaced = False
    for i, row in enumerate(sources):
        if isinstance(row, dict) and row.get("source_id") == record.source_id:
            sources[i] = new_row
            replaced = True
            break
    if not replaced:
        sources.append(new_row)

    idx["generated_at"] = utc_now_iso()
    idx["sources"] = sources
    path = sources_index_path(settings)
    _write_json_atomic(path, idx)
    return path

