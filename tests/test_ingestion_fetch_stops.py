"""
Unit tests for stop ingestion (Phase 3: Ingestion).

We test `fetch_and_write_stops` as a "pipeline boundary" function:
- it consumes settings (paths + AOI + endpoints),
- it calls a TDX client to fetch raw JSON,
- it normalizes data into a stable schema, and
- it writes CSV + meta JSON outputs to the raw data folder.

These tests do not use the network; we patch the TDX client constructor to return a fake client.
"""

from __future__ import annotations

# `json` is used to read the metadata file written by the ingestion function.
import json
# `Path` is used for path assertions on generated artifacts.
from pathlib import Path
# `Any` is used for the fake client return payloads (TDX JSON is dynamic).
from typing import Any

# pandas is used to read the generated CSV for assertions.
import pandas as pd

# Import the module under test so we can patch `TDXClient.from_env` within that module namespace.
from libraryreach.ingestion import fetch_stops


class _FakeTDXClient:
    # A fake TDX client that returns deterministic payloads without network access.
    def __init__(self) -> None:
        # Record calls so the test can assert multi-city ingestion behavior.
        self.paged_calls: list[dict[str, Any]] = []
        self.json_calls: list[dict[str, Any]] = []

    def get_paged_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        page_size: int = 5000,
        cache_ttl_s: int | None = None,
        max_pages: int = 200,
    ) -> list[Any]:
        # Record the call to validate that we fetch once per city (data flow correctness).
        self.paged_calls.append(
            {
                "path": path,
                "params": dict(params or {}),
                "page_size": int(page_size),
                "cache_ttl_s": cache_ttl_s,
                "max_pages": int(max_pages),
            }
        )
        # Extract the city from a simple `/bus/<city>` endpoint pattern used in this test.
        city = path.split("/")[-1]
        # Return a minimal bus stop payload matching the fields used by `_normalize_bus_stop`.
        return [
            {
                "StopID": f"{city}-B1",
                "StopName": {"En": f"{city} Bus Stop"},
                "StopPosition": {"PositionLat": 25.0, "PositionLon": 121.0},
                "City": city,
            },
            # Include one invalid record (missing coordinates) to verify we skip unusable rows.
            {"StopID": f"{city}-BAD", "StopName": {"En": "Bad Stop"}, "StopPosition": {}},
        ]

    def get_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        cache_namespace: str = "http",
        cache_ttl_s: int | None = None,
    ) -> Any:
        # Record the call to validate that we fetch metro stations for each operator.
        self.json_calls.append(
            {
                "path": path,
                "params": dict(params or {}),
                "cache_namespace": cache_namespace,
                "cache_ttl_s": cache_ttl_s,
            }
        )
        # Return a minimal metro station payload matching the fields used by `_normalize_metro_station`.
        return [
            {
                "StationID": "M1",
                "StationName": {"En": "Metro Station"},
                "StationPosition": {"PositionLat": 25.1, "PositionLon": 121.1},
                "City": "Taipei",
            }
        ]


def test_fetch_and_write_stops_writes_csv_and_meta(tmp_path: Path, monkeypatch) -> None:
    # Build a minimal settings dict that points all outputs into pytest's temp directory.
    settings = {
        "paths": {
            "cache_dir": str(tmp_path / "cache"),
            "raw_dir": str(tmp_path / "raw"),
        },
        "aoi": {
            # Use two cities so we validate multi-city iteration behavior.
            "cities": ["Taipei", "Taoyuan"],
        },
        "tdx": {
            # Keep cache_ttl_s small for tests; it only affects cache wiring, not logic.
            "cache_ttl_s": 60,
            "page_size": 5000,
            "endpoints": {
                # Use simple endpoint templates so the fake client can parse the city/operator from the path.
                "bus_stops_by_city": "/bus/{city}",
                "metro_stations_by_operator": "/metro/{operator}",
            },
            # Provide one operator so the metro fetch branch is exercised.
            "metro_operator_codes": ["TRTC"],
        },
    }

    # Create a fake client instance that will be returned by the patched `from_env`.
    fake_client = _FakeTDXClient()

    def _fake_from_env(*, settings: dict[str, Any], cache) -> _FakeTDXClient:  # noqa: ANN001
        # Return the shared fake client so we can inspect recorded calls after ingestion.
        return fake_client

    # Patch the constructor in the module under test (not the global class) to avoid real env/network usage.
    monkeypatch.setattr(fetch_stops.TDXClient, "from_env", _fake_from_env)

    # Run the ingestion function; this should write outputs under `tmp_path/raw/tdx/`.
    out_path = fetch_stops.fetch_and_write_stops(settings)

    # Assert the main CSV path exists and is under the expected raw folder structure.
    assert out_path.exists()
    assert out_path.name == "stops.csv"
    assert out_path.parent.name == "tdx"

    # Load the written CSV and validate the normalized schema and row counts.
    df = pd.read_csv(out_path)
    assert set(df.columns) == {"stop_id", "name", "lat", "lon", "city", "mode", "source"}
    # Expect 2 bus rows (one per city) + 1 metro row; invalid bus rows are skipped.
    assert len(df) == 3
    assert set(df["mode"].unique()) == {"bus", "metro"}

    # Assert the meta file was written and includes stable high-level counts.
    meta_path = out_path.parent / "stops.meta.json"
    assert meta_path.exists()
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta["cities"] == ["Taipei", "Taoyuan"]
    assert meta["total"] == 3
    assert meta["counts"]["bus"] == 2
    assert meta["counts"]["metro"] == 1

    # Validate that the fake client was called once per city for bus stop paging.
    assert len(fake_client.paged_calls) == 2
    # Validate that the fake client was called once per operator for metro station fetch.
    assert len(fake_client.json_calls) == 1

