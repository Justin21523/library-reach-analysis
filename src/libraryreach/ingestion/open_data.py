from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from libraryreach.ingestion.http_download import download_with_cache_headers
from libraryreach.ingestion.sources_index import SourceRecord, sha256_file, upsert_source_record
from libraryreach.run_meta import config_fingerprint, file_meta, json_hash, new_run_id


@dataclass(frozen=True)
class OpenDataSource:
    source_id: str
    enabled: bool
    url: str
    output_path: Path
    format: str
    headers: dict[str, str]
    headers_env: dict[str, str]


def _log() -> logging.Logger:
    return logging.getLogger("libraryreach")


def _read_sources(settings: dict[str, Any]) -> list[OpenDataSource]:
    cfg = settings.get("open_data", {}) or {}
    if not bool(cfg.get("enable", False)):
        return []

    sources = cfg.get("sources", []) or []
    out: list[OpenDataSource] = []
    for s in sources:
        if not isinstance(s, dict):
            continue
        source_id = str(s.get("source_id") or "").strip()
        if not source_id:
            continue
        enabled = bool(s.get("enabled", False))
        url = str(s.get("url") or "").strip()
        output_path = str(s.get("output_path") or "").strip()
        fmt = str(s.get("format") or "unknown").strip()
        headers = s.get("headers", {}) or {}
        headers_env = s.get("headers_env", {}) or {}
        if not isinstance(headers, dict):
            headers = {}
        if not isinstance(headers_env, dict):
            headers_env = {}
        if not output_path:
            continue
        out.append(
            OpenDataSource(
                source_id=source_id,
                enabled=enabled,
                url=url,
                output_path=Path(output_path),
                format=fmt,
                headers={str(k): str(v) for k, v in headers.items()},
                headers_env={str(k): str(v) for k, v in headers_env.items()},
            )
        )
    return out


def _build_headers(settings: dict[str, Any], source: OpenDataSource) -> dict[str, str]:
    cfg = settings.get("open_data", {}) or {}
    headers: dict[str, str] = {}
    ua = cfg.get("user_agent")
    if isinstance(ua, str) and ua.strip():
        headers["User-Agent"] = ua.strip()
    for k, v in (source.headers or {}).items():
        if v is None:
            continue
        headers[str(k)] = str(v)
    for header_name, env_var in (source.headers_env or {}).items():
        if not env_var:
            continue
        value = os.getenv(str(env_var))
        if value:
            headers[str(header_name)] = str(value)
    return headers


def fetch_and_write_open_data(
    settings: dict[str, Any],
    *,
    run_id: str | None = None,
    only_source_ids: set[str] | None = None,
) -> list[Path]:
    """
    Fetch configured Open Data sources and write them under data/raw/open_data/.

    This is a scaffold: it downloads raw files with ETag/Last-Modified caching and writes
    per-source meta + sources_index records so later steps can transform them into catalogs.
    """
    cfg = settings.get("open_data", {}) or {}
    if not bool(cfg.get("enable", False)):
        _log().info("open_data.enable=false; skip Open Data ingestion")
        return []

    timeout_s = int(cfg.get("request_timeout_s", 30) or 30)
    min_interval_s = float(cfg.get("min_request_interval_s", 0.5) or 0.0)

    sources = _read_sources(settings)
    if only_source_ids:
        sources = [s for s in sources if s.source_id in only_source_ids]

    rid = str(run_id or new_run_id())
    fingerprint = config_fingerprint(settings)
    config_hash = json_hash(fingerprint)

    out_paths: list[Path] = []
    last_req_at = 0.0

    for s in sources:
        if not s.enabled:
            continue
        if not s.url:
            _log().warning("Open Data source enabled but missing url: %s", s.source_id)
            continue

        now = time.time()
        sleep_s = max(0.0, min_interval_s - (now - last_req_at))
        if sleep_s > 0:
            time.sleep(sleep_s)

        out_path = Path(settings["paths"]["root"]) / s.output_path
        meta_path = out_path.with_suffix(out_path.suffix + ".meta.json")

        headers = _build_headers(settings, s)
        result = download_with_cache_headers(
            url=s.url,
            output_path=out_path,
            meta_path=meta_path,
            timeout_s=timeout_s,
            headers=headers,
        )
        last_req_at = time.time()

        # Enrich meta with run/config provenance (append-only style; we preserve download meta fields).
        prev = {}
        try:
            prev = json.loads(Path(meta_path).read_text(encoding="utf-8"))
        except Exception:
            prev = {}

        enriched = dict(prev) if isinstance(prev, dict) else {}
        enriched.update(
            {
                "run_id": rid,
                "source_id": s.source_id,
                "format": s.format,
                "config_hash": config_hash,
                "config_fingerprint": fingerprint,
                "input_sources": [
                    file_meta(Path(str((settings.get("_meta", {}) or {}).get("config_path") or "config/default.yaml"))).__dict__,
                    file_meta(
                        Path(str((settings.get("_meta", {}) or {}).get("scenario_path") or "config/scenarios/weekday.yaml"))
                    ).__dict__,
                ],
                "headers_used": {k: ("<redacted>" if k.lower() == "authorization" else v) for k, v in headers.items()},
            }
        )
        tmp = meta_path.with_suffix(meta_path.suffix + ".tmp")
        tmp.write_text(json.dumps(enriched, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(meta_path)

        if not out_path.exists():
            _log().warning("Open Data download did not produce output: %s", out_path)
            continue

        status = "ok" if result.status in ("downloaded", "not_modified") else "error"
        upsert_source_record(
            settings,
            SourceRecord(
                source_id=s.source_id,
                fetched_at=result.fetched_at,
                output_path=str(out_path),
                checksum_sha256=sha256_file(out_path),
                status=status,
                details={
                    "status": result.status,
                    "etag": result.etag,
                    "last_modified": result.last_modified,
                    "url": s.url,
                    "format": s.format,
                },
            ),
        )

        out_paths.append(out_path)
        _log().info("Open Data: %s -> %s (%s)", s.source_id, out_path, result.status)

    return out_paths
