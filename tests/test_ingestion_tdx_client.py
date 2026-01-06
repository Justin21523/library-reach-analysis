"""
Unit tests for the TDX client (Phase 3: Ingestion).

These tests are intentionally offline:
- they never contact the real TDX network,
- they use a temporary DiskCache directory, and
- they use fake HTTP responses to validate token + caching logic.
"""

from __future__ import annotations

# `json` helps us build predictable fake HTTP bodies for error messages.
import json
# `time` is used to build "expires_at" timestamps relative to "now".
import time
# `Path` is used for creating a temporary cache directory in tests.
from pathlib import Path

# `pytest` provides the test runner and fixtures like `tmp_path`.
import pytest
# `requests` provides the HTTPError type that our fake responses emulate.
import requests

# DiskCache is our on-disk cache used by the ingestion client.
from libraryreach.cache import DiskCache
# TDXClient is the unit under test.
from libraryreach.ingestion.tdx_client import TDXClient


class _FakeResponse:
    # A minimal fake `requests.Response` that supports the methods our client uses.
    def __init__(self, *, status_code: int, payload: object | None = None, text: str | None = None) -> None:
        # Store the status code so `raise_for_status` can decide whether to raise.
        self.status_code = int(status_code)
        # Store a JSON-like payload returned by `json()`.
        self._payload = payload
        # Use a caller-provided text body, or serialize the payload for realistic error snippets.
        self._text = text if text is not None else json.dumps(payload, ensure_ascii=False)

    @property
    def text(self) -> str:
        # `TDXClient` reads `.text` only for debugging; return a stable string here.
        return self._text

    def json(self) -> object:
        # Return the payload exactly as configured by the test.
        return self._payload

    def raise_for_status(self) -> None:
        # Match `requests` behavior: raise an HTTPError for 4xx/5xx status codes.
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class _FakeSession:
    # A minimal fake `requests.Session` that returns pre-programmed responses in order.
    def __init__(self, *, post_responses: list[_FakeResponse], get_responses: list[_FakeResponse]) -> None:
        # Keep responses as queues so each call pops one response.
        self._post_responses = list(post_responses)
        self._get_responses = list(get_responses)
        # Record calls so tests can assert cache hits do not perform network work.
        self.post_calls: list[dict[str, object]] = []
        self.get_calls: list[dict[str, object]] = []

    def post(self, url: str, data: dict[str, object], timeout: int) -> _FakeResponse:
        # Record a snapshot of the call for later assertions.
        self.post_calls.append({"url": url, "data": dict(data), "timeout": int(timeout)})
        # Pop the next programmed response (tests ensure the queue is long enough).
        return self._post_responses.pop(0)

    def get(
        self,
        url: str,
        params: dict[str, object],
        headers: dict[str, str],
        timeout: int,
    ) -> _FakeResponse:
        # Record a snapshot of the call; copy dicts so later mutations do not affect the record.
        self.get_calls.append(
            {"url": url, "params": dict(params), "headers": dict(headers), "timeout": int(timeout)}
        )
        # Pop the next programmed response (tests ensure the queue is long enough).
        return self._get_responses.pop(0)


def test_get_access_token_uses_cache_when_valid(tmp_path: Path) -> None:
    # Create an on-disk cache under pytest's temp directory.
    cache = DiskCache(tmp_path / "cache")
    # Construct a client with dummy credentials and no session (we expect no network call).
    client = TDXClient(
        client_id="client-id",
        client_secret="client-secret",
        base_url="https://example.com",
        token_url="https://example.com/token",
        cache=cache,
        session=None,
    )
    # Seed the token cache with a still-valid token so `get_access_token` should not request a new one.
    now_s = int(time.time())
    cache.set_json(
        "tdx",
        client._token_cache_key(),
        {"access_token": "CACHED", "expires_at": now_s + 3600, "obtained_at": now_s},
    )

    # Fetch the token; this should be returned from cache.
    token = client.get_access_token()

    # Assert we got the cached value.
    assert token == "CACHED"
    # Assert we did not allocate a session (a proxy for "no network call happened").
    assert client.session is None


def test_get_access_token_fetches_and_caches_when_expired(tmp_path: Path) -> None:
    # Create an on-disk cache under pytest's temp directory.
    cache = DiskCache(tmp_path / "cache")
    # Build a fake HTTP session that will return a successful token response.
    session = _FakeSession(
        post_responses=[_FakeResponse(status_code=200, payload={"access_token": "NEW", "expires_in": 3600})],
        get_responses=[],
    )
    # Construct a client that uses the fake session so no real network is used.
    client = TDXClient(
        client_id="client-id",
        client_secret="client-secret",
        base_url="https://example.com",
        token_url="https://example.com/token",
        cache=cache,
        session=session,  # type: ignore[arg-type]
    )
    # Seed an expired token so the client is forced to refresh.
    now_s = int(time.time())
    cache.set_json(
        "tdx",
        client._token_cache_key(),
        {"access_token": "OLD", "expires_at": now_s - 1, "obtained_at": now_s - 3600},
    )

    # Fetch the token; this should call the token endpoint via the fake session.
    token = client.get_access_token()

    # Assert the token was refreshed.
    assert token == "NEW"
    # Assert exactly one POST happened (one token request).
    assert len(session.post_calls) == 1
    # Assert the cache now contains the new token (persisted record is part of our ingestion data flow).
    cached = cache.get_json("tdx", client._token_cache_key(), ttl_s=-1)
    assert isinstance(cached, dict)
    assert cached.get("access_token") == "NEW"


def test_get_json_caches_responses(tmp_path: Path) -> None:
    # Create an on-disk cache under pytest's temp directory.
    cache = DiskCache(tmp_path / "cache")
    # Build a fake session that returns a token once, and a GET response once.
    session = _FakeSession(
        post_responses=[_FakeResponse(status_code=200, payload={"access_token": "TOK", "expires_in": 3600})],
        get_responses=[_FakeResponse(status_code=200, payload=[{"ok": True}])],
    )
    # Construct a client that uses the fake session so no real network is used.
    client = TDXClient(
        client_id="client-id",
        client_secret="client-secret",
        base_url="https://api.example.com",
        token_url="https://api.example.com/token",
        cache=cache,
        session=session,  # type: ignore[arg-type]
    )

    # First call should hit the network (fake session) and then write to cache.
    first = client.get_json("/path", params={"foo": "bar"}, cache_ttl_s=3600)
    # Second call should come from cache and avoid a second GET.
    second = client.get_json("/path", params={"foo": "bar"}, cache_ttl_s=3600)

    # Assert payloads match and are stable across cache hits.
    assert first == second
    # Assert only one token request happened.
    assert len(session.post_calls) == 1
    # Assert only one GET happened because the second call should be served from DiskCache.
    assert len(session.get_calls) == 1


def test_get_json_refreshes_token_on_401(tmp_path: Path) -> None:
    # Create an on-disk cache under pytest's temp directory.
    cache = DiskCache(tmp_path / "cache")
    # Build a fake session: first GET returns 401, second GET returns data; one POST returns a refreshed token.
    session = _FakeSession(
        post_responses=[_FakeResponse(status_code=200, payload={"access_token": "NEW", "expires_in": 3600})],
        get_responses=[
            _FakeResponse(status_code=401, payload={"message": "unauthorized"}, text="unauthorized"),
            _FakeResponse(status_code=200, payload=[{"ok": True}]),
        ],
    )
    # Construct a client that uses the fake session so no real network is used.
    client = TDXClient(
        client_id="client-id",
        client_secret="client-secret",
        base_url="https://api.example.com",
        token_url="https://api.example.com/token",
        cache=cache,
        session=session,  # type: ignore[arg-type]
    )
    # Seed a valid token so the first request uses it (the 401 forces a refresh path).
    now_s = int(time.time())
    cache.set_json(
        "tdx",
        client._token_cache_key(),
        {"access_token": "OLD", "expires_at": now_s + 3600, "obtained_at": now_s},
    )

    # Call `get_json`; it should retry once with a refreshed token after the 401.
    data = client.get_json("/path", params={"x": "1"}, cache_ttl_s=None)

    # Assert we got the successful data response.
    assert data == [{"ok": True}]
    # Assert one token refresh happened (POST) after the 401.
    assert len(session.post_calls) == 1
    # Assert two GETs happened: first 401, then retry 200.
    assert len(session.get_calls) == 2
    # Assert the retry used the refreshed token in the Authorization header (data flow correctness).
    assert session.get_calls[1]["headers"]["Authorization"] == "Bearer NEW"

