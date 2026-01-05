from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _now_s() -> int:
    return int(time.time())


@dataclass(frozen=True)
class DiskCache:
    base_dir: Path
    default_ttl_s: int = 24 * 60 * 60

    def _path(self, namespace: str, key: str) -> Path:
        safe = _sha256(key)
        return self.base_dir / namespace / f"{safe}.json"

    def get_json(self, namespace: str, key: str, ttl_s: int | None = None) -> Any | None:
        path = self._path(namespace, key)
        if not path.exists():
            return None
        ttl = self.default_ttl_s if ttl_s is None else ttl_s
        age = _now_s() - int(path.stat().st_mtime)
        if ttl >= 0 and age > ttl:
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def set_json(self, namespace: str, key: str, value: Any) -> Path:
        path = self._path(namespace, key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

