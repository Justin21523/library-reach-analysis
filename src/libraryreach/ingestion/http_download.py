from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _write_json_atomic(path: Path, data: Any) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


@dataclass(frozen=True)
class DownloadResult:
    status: str  # "downloaded" | "not_modified"
    output_path: Path
    fetched_at: str
    etag: str | None
    last_modified: str | None


def download_with_cache_headers(
    *,
    url: str,
    output_path: Path,
    meta_path: Path,
    timeout_s: int = 30,
    headers: dict[str, str] | None = None,
) -> DownloadResult:
    prev: dict[str, Any] = {}
    if Path(meta_path).exists():
        try:
            prev = json.loads(Path(meta_path).read_text(encoding="utf-8"))
        except Exception:
            prev = {}

    req_headers: dict[str, str] = dict(headers or {})
    if isinstance(prev, dict):
        etag = prev.get("etag")
        last_modified = prev.get("last_modified")
        if isinstance(etag, str) and etag:
            req_headers["If-None-Match"] = etag
        if isinstance(last_modified, str) and last_modified:
            req_headers["If-Modified-Since"] = last_modified

    resp = requests.get(url, headers=req_headers, timeout=timeout_s)
    if resp.status_code == 304:
        return DownloadResult(
            status="not_modified",
            output_path=Path(output_path),
            fetched_at=utc_now_iso(),
            etag=prev.get("etag") if isinstance(prev, dict) else None,
            last_modified=prev.get("last_modified") if isinstance(prev, dict) else None,
        )
    resp.raise_for_status()

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(output_path).with_suffix(Path(output_path).suffix + ".tmp")
    tmp.write_bytes(resp.content)
    tmp.replace(Path(output_path))

    etag_out = resp.headers.get("ETag")
    lm_out = resp.headers.get("Last-Modified")
    meta = {
        "url": url,
        "fetched_at": utc_now_iso(),
        "fetched_at_epoch_s": int(time.time()),
        "etag": etag_out,
        "last_modified": lm_out,
        "status": "downloaded",
    }
    _write_json_atomic(Path(meta_path), meta)

    return DownloadResult(
        status="downloaded",
        output_path=Path(output_path),
        fetched_at=meta["fetched_at"],
        etag=etag_out,
        last_modified=lm_out,
    )
