"""
TDX API client (Phase 3: Ingestion).

This module is responsible for a single part of our end-to-end data flow:
getting raw transit data from TDX in a reproducible, cache-friendly way.

Why we wrap `requests` instead of calling it directly everywhere:
- Centralize OAuth token handling (TDX uses client-credentials tokens).
- Centralize caching (so we do not re-download the same city data repeatedly).
- Centralize "what-if" safety (consistent timeouts, consistent error messages).

This code is used by ingestion modules like `libraryreach.ingestion.fetch_stops`,
which writes normalized stop tables to `data/raw/tdx/` for later spatial analysis.
"""

from __future__ import annotations

# Logging gives operators visibility into token refreshes and network failures.
import logging
# `os` is used to read TDX credentials from environment variables / .env.
import os
# `time` is used for token expiry checks and for computing "expires_at".
import time
import random
# `dataclass` makes the client a small, explicit container for settings and helpers.
from dataclasses import dataclass
# `Any` keeps settings and JSON payloads flexible without premature strict typing.
from typing import Any, Callable

# `requests` is a lightweight HTTP client; we use it with explicit timeouts.
import requests

# DiskCache is our dependency-light, file-based cache (safe to delete and rebuild).
from libraryreach.cache import DiskCache


class TDXAuthError(RuntimeError):
    # We use a dedicated exception type so callers can distinguish auth issues from other failures.
    pass


def _safe_response_text(resp: requests.Response, *, limit: int = 500) -> str:
    # Response bodies can be large; we truncate so logs stay readable and do not leak too much data.
    try:
        text = resp.text
    except Exception:
        # Some responses may fail to decode; fall back to a minimal placeholder.
        return "<unreadable response body>"
    # Strip whitespace and limit length so error messages remain compact.
    return text.strip()[:limit]


@dataclass
class TDXClient:
    # OAuth client id (TDX application id).
    client_id: str
    # OAuth client secret (must not be committed to git).
    client_secret: str
    # Base URL for API requests (e.g., https://tdx.transportdata.tw).
    base_url: str
    # Token endpoint URL for OAuth client credentials flow.
    token_url: str
    # Cache for tokens and GET responses so ingestion can be fast and reproducible.
    cache: DiskCache
    # Per-request timeout to avoid hanging ingestion jobs.
    request_timeout_s: int = 30
    # Minimum interval between network calls to respect API limits (seconds).
    min_request_interval_s: float = 0.25
    # Optional interval overrides by request type.
    # These let operators throttle token requests and different endpoint families separately.
    min_request_interval_token_s: float | None = None
    min_request_interval_bus_s: float | None = None
    min_request_interval_metro_s: float | None = None
    # Max retries for transient errors (429/5xx) before failing.
    max_retries: int = 5
    # Initial backoff for retries (seconds).
    retry_backoff_initial_s: float = 1.0
    # Max backoff between retries (seconds).
    retry_backoff_max_s: float = 30.0
    # Optional sleep function injection (tests can stub).
    sleep_fn: Callable[[float], None] | None = None
    # Optional callback for retry events (daemon can record rate-limits/backoffs to status files).
    on_retry: Callable[[dict[str, Any]], None] | None = None
    # Monotonic timestamp of last network call (for throttling).
    _last_call_monotonic_s: float = 0.0
    # Optional logger injection for tests or custom logging setups.
    logger: logging.Logger | None = None
    # Optional session injection so tests can stub network calls and prod can reuse connections.
    session: requests.Session | None = None

    def _log(self) -> logging.Logger:
        # Keep a default logger name so logs from CLI/API are consistent across modules.
        return self.logger or logging.getLogger("libraryreach")

    def _http(self) -> requests.Session:
        # Lazily create a session so callers who never use the network do not allocate one.
        if self.session is None:
            # Keeping a single session per client avoids leaking many connection pools across calls.
            self.session = requests.Session()
        # Return the (possibly newly created) session for consistent connection reuse.
        return self.session

    def _sleep(self, seconds: float) -> None:
        (self.sleep_fn or time.sleep)(max(0.0, float(seconds)))

    def _min_interval_for(self, *, url: str, method: str) -> float:
        # Token endpoint can be rate limited separately.
        if method.upper() == "POST" and url == self.token_url and self.min_request_interval_token_s is not None:
            return float(self.min_request_interval_token_s)
        # Heuristic by URL path (stable enough for current TDX endpoints).
        if "/Bus/" in url and self.min_request_interval_bus_s is not None:
            return float(self.min_request_interval_bus_s)
        if "/Rail/Metro/" in url and self.min_request_interval_metro_s is not None:
            return float(self.min_request_interval_metro_s)
        return float(self.min_request_interval_s)

    def _throttle(self, *, min_interval_s: float) -> None:
        # Use monotonic time so sleeps are stable across clock changes.
        now = time.monotonic()
        min_dt = float(min_interval_s)
        if min_dt <= 0:
            return
        dt = now - float(self._last_call_monotonic_s)
        if dt < min_dt:
            self._sleep(min_dt - dt)
        self._last_call_monotonic_s = time.monotonic()

    @classmethod
    def from_env(cls, *, settings: dict[str, Any], cache: DiskCache) -> "TDXClient":
        # Read credentials from env vars so we do not store secrets in config files.
        client_id = os.getenv("TDX_CLIENT_ID")
        client_secret = os.getenv("TDX_CLIENT_SECRET")
        # Missing credentials is an operator error; we fail fast with a clear message.
        if not client_id or not client_secret:
            raise TDXAuthError(
                "Missing TDX credentials. Set TDX_CLIENT_ID and TDX_CLIENT_SECRET (see .env.example)."
            )
        # `settings["tdx"]` contains endpoints and defaults like timeout and base_url.
        tdx = settings["tdx"]
        # Construct a client with normalized base_url so `_build_url` is deterministic.
        min_interval = float(tdx.get("min_request_interval_s", 0.25))
        min_interval_token = tdx.get("min_request_interval_token_s")
        min_interval_bus = tdx.get("min_request_interval_bus_s")
        min_interval_metro = tdx.get("min_request_interval_metro_s")
        max_retries = int(tdx.get("max_retries", 5))
        backoff_initial = float(tdx.get("retry_backoff_initial_s", 1.0))
        backoff_max = float(tdx.get("retry_backoff_max_s", 30.0))
        return cls(
            client_id=client_id,
            client_secret=client_secret,
            base_url=str(tdx["base_url"]).rstrip("/"),
            token_url=str(tdx["token_url"]),
            cache=cache,
            request_timeout_s=int(tdx.get("request_timeout_s", 30)),
            min_request_interval_s=min_interval,
            min_request_interval_token_s=float(min_interval_token) if min_interval_token is not None else None,
            min_request_interval_bus_s=float(min_interval_bus) if min_interval_bus is not None else None,
            min_request_interval_metro_s=float(min_interval_metro) if min_interval_metro is not None else None,
            max_retries=max_retries,
            retry_backoff_initial_s=backoff_initial,
            retry_backoff_max_s=backoff_max,
            logger=logging.getLogger("libraryreach"),
        )

    def _token_cache_key(self) -> str:
        # Token cache is per client id + token endpoint; we never include the secret in cache keys.
        return f"{self.client_id}@{self.token_url}"

    def get_access_token(self) -> str:
        # Tokens are cached with "infinite TTL" because expiry is tracked by `expires_at` inside payload.
        cached = self.cache.get_json("tdx", self._token_cache_key(), ttl_s=-1)
        # Use epoch seconds so values are JSON-serializable and easy to compare.
        now_s = int(time.time())
        if isinstance(cached, dict):
            # Extract token fields carefully so bad cache entries do not crash ingestion.
            token = cached.get("access_token")
            expires_at = int(cached.get("expires_at", 0))
            # Keep a 60s safety buffer to avoid race conditions around expiry during long paged fetches.
            if token and now_s < (expires_at - 60):
                return str(token)

        # If the cache is missing/expired, we request a new token from the OAuth endpoint.
        self._log().info("Requesting new TDX token")
        resp: requests.Response | None = None
        last_exc: Exception | None = None
        for attempt in range(int(self.max_retries) + 1):
            try:
                self._throttle(min_interval_s=self._min_interval_for(url=self.token_url, method="POST"))
                resp = self._http().post(
                    self.token_url,
                    data={
                        # TDX uses standard OAuth2 client credentials flow.
                        "grant_type": "client_credentials",
                        "client_id": self.client_id,
                        "client_secret": self.client_secret,
                    },
                    timeout=self.request_timeout_s,
                )
            except requests.RequestException as e:
                last_exc = e
                break

            if resp.status_code in {429, 502, 503, 504} and attempt < int(self.max_retries):
                retry_after = resp.headers.get("Retry-After") if hasattr(resp, "headers") else None
                sleep_s = None
                if retry_after:
                    try:
                        sleep_s = float(retry_after)
                    except ValueError:
                        sleep_s = None
                if sleep_s is None:
                    base = float(self.retry_backoff_initial_s) * (2 ** attempt)
                    sleep_s = min(float(self.retry_backoff_max_s), base) + random.uniform(0, 0.25)
                if self.on_retry is not None:
                    try:
                        self.on_retry(
                            {
                                "ts_epoch_s": int(time.time()),
                                "url": self.token_url,
                                "method": "POST",
                                "status_code": int(resp.status_code),
                                "attempt": int(attempt + 1),
                                "max_retries": int(self.max_retries),
                                "retry_after": str(retry_after) if retry_after else None,
                                "sleep_s": float(sleep_s),
                            }
                        )
                    except Exception:
                        pass
                self._log().warning(
                    "TDX transient error %s (token), retrying in %.2fs (attempt %s/%s)",
                    resp.status_code,
                    sleep_s,
                    attempt + 1,
                    self.max_retries,
                )
                self._sleep(sleep_s)
                continue
            break

        if resp is None:
            if last_exc is not None:
                raise TDXAuthError(f"Failed to request TDX token: {last_exc}") from last_exc
            raise TDXAuthError("Failed to request TDX token: no response")

        try:
            # `raise_for_status` converts non-2xx into a clear exception with status code.
            resp.raise_for_status()
        except requests.HTTPError as e:
            # Include a small snippet of the body to help debugging invalid credentials or quota issues.
            raise TDXAuthError(
                f"TDX token request failed: status={resp.status_code} body={_safe_response_text(resp)}"
            ) from e

        # Parse JSON payload; token endpoint should always return JSON.
        payload = resp.json()
        token = payload.get("access_token")
        expires_in = int(payload.get("expires_in", 0))
        # Missing token or expiry means the response is not what we expect (misconfig or API change).
        if not token or expires_in <= 0:
            raise TDXAuthError(f"Unexpected token response: {payload}")

        # Store token metadata so we can reuse it safely until shortly before expiry.
        record = {
            "access_token": token,
            "expires_at": now_s + expires_in,
            "obtained_at": now_s,
        }
        # Persist to disk so separate runs can reuse the same token until it expires.
        self.cache.set_json("tdx", self._token_cache_key(), record)
        return str(token)

    def _build_url(self, path: str) -> str:
        # Allow callers to pass full URLs when needed (useful for debugging or alternate endpoints).
        if path.startswith("http://") or path.startswith("https://"):
            return path
        # Ensure there is exactly one "/" between base_url and path to avoid subtle URL bugs.
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}{path}"

    def get_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        cache_namespace: str = "http",
        cache_ttl_s: int | None = None,
    ) -> Any:
        # Resolve the final URL early so cache keys and logs match the actual network call.
        url = self._build_url(path)
        # Copy params so callers can reuse their dict without it being mutated by us.
        params = dict(params or {})
        # TDX OData endpoints support `$format=JSON`; we default it so callers do not forget.
        params.setdefault("$format", "JSON")
        # Cache key includes URL and normalized params; sorting stabilizes key order across runs.
        cache_key = f"GET {url} {sorted(params.items())}"
        if cache_ttl_s is not None:
            # If caching is enabled, we return a cached response when it is still within TTL.
            cached = self.cache.get_json(cache_namespace, cache_key, ttl_s=cache_ttl_s)
            if cached is not None:
                return cached

        # Acquire an access token right before the request so it is fresh enough for the call.
        token = self.get_access_token()
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

        def do_get() -> requests.Response:
            self._throttle(min_interval_s=self._min_interval_for(url=url, method="GET"))
            return self._http().get(url, params=params, headers=headers, timeout=self.request_timeout_s)

        # Retry transient errors with backoff, respecting Retry-After on 429 when present.
        last_error: Exception | None = None
        resp: requests.Response | None = None
        for attempt in range(int(self.max_retries) + 1):
            resp = do_get()

            if resp.status_code == 401:
                # A 401 usually means the token expired or was revoked; refresh once and retry immediately.
                self._log().warning("TDX returned 401, refreshing token")
                self.cache.set_json("tdx", self._token_cache_key(), {"expires_at": 0})
                token = self.get_access_token()
                headers["Authorization"] = f"Bearer {token}"
                resp = do_get()

            if resp.status_code in {429, 502, 503, 504} and attempt < int(self.max_retries):
                retry_after = resp.headers.get("Retry-After") if hasattr(resp, "headers") else None
                sleep_s = None
                if retry_after:
                    try:
                        sleep_s = float(retry_after)
                    except ValueError:
                        sleep_s = None
                if sleep_s is None:
                    base = float(self.retry_backoff_initial_s) * (2 ** attempt)
                    # Add a small jitter to avoid herd effects when multiple containers restart.
                    sleep_s = min(float(self.retry_backoff_max_s), base) + random.uniform(0, 0.25)
                if self.on_retry is not None:
                    try:
                        self.on_retry(
                            {
                                "ts_epoch_s": int(time.time()),
                                "url": url,
                                "method": "GET",
                                "status_code": int(resp.status_code),
                                "attempt": int(attempt + 1),
                                "max_retries": int(self.max_retries),
                                "retry_after": str(retry_after) if retry_after else None,
                                "sleep_s": float(sleep_s),
                            }
                        )
                    except Exception:
                        # Callback failures should not break ingestion.
                        pass
                self._log().warning("TDX transient error %s, retrying in %.2fs (attempt %s/%s)", resp.status_code, sleep_s, attempt + 1, self.max_retries)
                self._sleep(sleep_s)
                continue

            try:
                resp.raise_for_status()
                break
            except requests.HTTPError as e:
                last_error = e
                break

        if resp is None:
            raise RuntimeError("TDX GET failed: no response")
        if last_error is not None:
            raise RuntimeError(
                f"TDX GET failed: url={url} status={resp.status_code} body={_safe_response_text(resp)}"
            ) from last_error

        # Parse JSON; most TDX endpoints return either a list or a dict depending on endpoint.
        data = resp.json()
        if cache_ttl_s is not None:
            # Persist successful responses so repeated runs and pages can be served from disk cache.
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
        # Guardrails prevent infinite loops if the API ignores `$skip` or returns unexpected sizes.
        if page_size <= 0:
            raise ValueError("page_size must be > 0")
        if max_pages <= 0:
            raise ValueError("max_pages must be > 0")

        # Base params apply to every page (we copy per page to avoid mutation bugs).
        params = dict(params or {})
        # Collect all items across pages into a single list for callers (simple and explicit).
        results: list[Any] = []
        for page in range(max_pages):
            # `$top` controls how many rows per page; `$skip` controls the offset.
            page_params = dict(params)
            page_params["$top"] = page_size
            page_params["$skip"] = page * page_size
            # Reuse `get_json` so paging inherits caching, token refresh, and error handling.
            chunk = self.get_json(path, params=page_params, cache_ttl_s=cache_ttl_s)
            # We expect list responses for these endpoints; dict responses indicate a wrong endpoint.
            if not isinstance(chunk, list):
                raise ValueError(f"Expected list response for paged endpoint, got: {type(chunk)}")
            # Append results in-order; order is not guaranteed by API but is stable enough for caching.
            results.extend(chunk)
            # If we got fewer rows than requested, we reached the final page.
            if len(chunk) < page_size:
                break
        return results
