from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Any

import requests

from libraryreach.cache import DiskCache


class TDXAuthError(RuntimeError):
    pass


@dataclass
class TDXClient:
    client_id: str
    client_secret: str
    base_url: str
    token_url: str
    cache: DiskCache
    request_timeout_s: int = 30
    logger: logging.Logger | None = None

    def _log(self) -> logging.Logger:
        return self.logger or logging.getLogger("libraryreach")

    @classmethod
    def from_env(cls, *, settings: dict[str, Any], cache: DiskCache) -> "TDXClient":
        client_id = os.getenv("TDX_CLIENT_ID")
        client_secret = os.getenv("TDX_CLIENT_SECRET")
        if not client_id or not client_secret:
            raise TDXAuthError(
                "Missing TDX credentials. Set TDX_CLIENT_ID and TDX_CLIENT_SECRET (see .env.example)."
            )
        tdx = settings["tdx"]
        return cls(
            client_id=client_id,
            client_secret=client_secret,
            base_url=tdx["base_url"].rstrip("/"),
            token_url=tdx["token_url"],
            cache=cache,
            request_timeout_s=int(tdx.get("request_timeout_s", 30)),
            logger=logging.getLogger("libraryreach"),
        )

    def _token_cache_key(self) -> str:
        return f"{self.client_id}@{self.token_url}"

    def get_access_token(self) -> str:
        cached = self.cache.get_json("tdx", self._token_cache_key(), ttl_s=-1)
        now_s = int(time.time())
        if isinstance(cached, dict):
            token = cached.get("access_token")
            expires_at = int(cached.get("expires_at", 0))
            if token and now_s < (expires_at - 60):
                return str(token)

        self._log().info("Requesting new TDX token")
        resp = requests.post(
            self.token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=self.request_timeout_s,
        )
        resp.raise_for_status()
        payload = resp.json()
        token = payload.get("access_token")
        expires_in = int(payload.get("expires_in", 0))
        if not token or expires_in <= 0:
            raise TDXAuthError(f"Unexpected token response: {payload}")

        record = {
            "access_token": token,
            "expires_at": now_s + expires_in,
            "obtained_at": now_s,
        }
        self.cache.set_json("tdx", self._token_cache_key(), record)
        return str(token)

    def _build_url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return f"{self.base_url}{path}"

    def get_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        cache_namespace: str = "http",
        cache_ttl_s: int | None = None,
    ) -> Any:
        url = self._build_url(path)
        params = dict(params or {})
        params.setdefault("$format", "JSON")
        cache_key = f"GET {url} {sorted(params.items())}"
        if cache_ttl_s is not None:
            cached = self.cache.get_json(cache_namespace, cache_key, ttl_s=cache_ttl_s)
            if cached is not None:
                return cached

        token = self.get_access_token()
        resp = requests.get(
            url,
            params=params,
            headers={"authorization": f"Bearer {token}"},
            timeout=self.request_timeout_s,
        )
        if resp.status_code == 401:
            self._log().warning("TDX returned 401, refreshing token")
            self.cache.set_json("tdx", self._token_cache_key(), {"expires_at": 0})
            token = self.get_access_token()
            resp = requests.get(
                url,
                params=params,
                headers={"authorization": f"Bearer {token}"},
                timeout=self.request_timeout_s,
            )
        resp.raise_for_status()
        data = resp.json()
        if cache_ttl_s is not None:
            self.cache.set_json(cache_namespace, cache_key, data)
        return data

    def get_paged_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        page_size: int = 5000,
        cache_ttl_s: int | None = None,
        max_pages: int = 200,
    ) -> list[Any]:
        params = dict(params or {})
        results: list[Any] = []
        for page in range(max_pages):
            page_params = dict(params)
            page_params["$top"] = page_size
            page_params["$skip"] = page * page_size
            chunk = self.get_json(path, params=page_params, cache_ttl_s=cache_ttl_s)
            if not isinstance(chunk, list):
                raise ValueError(f"Expected list response for paged endpoint, got: {type(chunk)}")
            results.extend(chunk)
            if len(chunk) < page_size:
                break
        return results

