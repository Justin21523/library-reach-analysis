"""
Fetch and normalize transit stop points from TDX (Phase 3: Ingestion).

In Phase 1 we compute "stop density within buffers around libraries".
To do that, we need a unified table of transit stop/station points.

This module:
1) Fetches bus stops for each configured city.
2) Fetches metro stations for each configured operator.
3) Normalizes them into a single DataFrame with a common schema.
4) Writes `data/raw/tdx/stops.csv` and a small `stops.meta.json`.

Design notes:
- We keep this baseline ingestion simple: stops only (not full timetables / routes).
- We use caching (DiskCache) so repeated runs are fast and deterministic.
- We keep output schema stable so spatial/scoring modules do not need to know TDX internals.
"""

from __future__ import annotations

# `json` is used to write a small machine-readable metadata file next to the CSV.
import json
# `logging` provides operator visibility when a TDX endpoint is missing or rate-limited.
import logging
# `time` provides a reproducible "generated_at" timestamp for auditing runs.
import time
# `Path` is used for safe, cross-platform file writes under the project root.
from pathlib import Path
# `Any` is used because TDX JSON payloads are dynamic dictionaries/lists.
from typing import Any, Callable

# pandas is our table engine for writing CSV and for basic cleaning/deduping.
import pandas as pd

# DiskCache stores tokens and HTTP responses on disk (safe to delete and rebuild).
from libraryreach.cache import DiskCache
# TDXClient centralizes token handling, caching, and error handling for HTTP calls.
from libraryreach.ingestion.tdx_client import TDXClient
from libraryreach.ingestion.sources_index import SourceRecord, sha256_file, upsert_source_record
from libraryreach.run_meta import config_fingerprint, file_meta, json_hash, new_run_id, utc_now_iso


def _pick_name(name_obj: Any) -> str | None:
    # TDX often returns bilingual names as a dict; we pick a stable preference order.
    if isinstance(name_obj, dict):
        # Prefer English for consistency across cities, but fall back to Traditional Chinese keys.
        return name_obj.get("En") or name_obj.get("Zh_tw") or name_obj.get("Zh_TW")
    # Some endpoints return names as plain strings; accept them as-is.
    if isinstance(name_obj, str):
        return name_obj
    # Unknown name shapes are treated as missing; validators can later warn if needed.
    return None


def _normalize_bus_stop(item: dict[str, Any], city: str) -> dict[str, Any] | None:
    # Bus stops use a `StopPosition` object with lat/lon under `PositionLat/PositionLon`.
    pos = item.get("StopPosition") or {}
    lat = pos.get("PositionLat")
    lon = pos.get("PositionLon")
    # Missing coordinates means we cannot use this stop for spatial joins; skip it early.
    if lat is None or lon is None:
        return None
    # Return a unified schema that downstream spatial code can consume without special cases.
    return {
        # StopID is the stable identifier provided by TDX for bus stops.
        "stop_id": item.get("StopID"),
        # Store a human-readable name for map popups and explain outputs.
        "name": _pick_name(item.get("StopName")),
        # Convert coordinates to floats so distance math is well-defined.
        "lat": float(lat),
        "lon": float(lon),
        # Prefer the city returned by TDX, but fall back to the city we requested.
        "city": item.get("City") or city,
        # Mode is used by scoring weights (bus vs metro) and by UI layer toggles.
        "mode": "bus",
        # Source is useful for provenance when we merge multiple datasets later.
        "source": "tdx",
    }


def _normalize_metro_station(item: dict[str, Any], operator: str) -> dict[str, Any] | None:
    # Metro endpoints use `StationPosition`; some variants may use `StopPosition`, so we support both.
    pos = item.get("StationPosition") or item.get("StopPosition") or {}
    lat = pos.get("PositionLat")
    lon = pos.get("PositionLon")
    # Missing coordinates means we cannot use this station for spatial joins; skip it early.
    if lat is None or lon is None:
        return None
    # Return the same unified schema as bus stops so downstream code can treat them uniformly.
    return {
        # StationID is preferred, but some endpoints use StopID; we accept either.
        "stop_id": item.get("StationID") or item.get("StopID"),
        # StationName is preferred, but some payloads use StopName; we accept either.
        "name": _pick_name(item.get("StationName") or item.get("StopName")),
        # Convert coordinates to floats so distance math is well-defined.
        "lat": float(lat),
        "lon": float(lon),
        # Some metro station payloads include city; keep empty string when missing (still valid).
        "city": item.get("City") or "",
        # Mode is used by scoring weights (bus vs metro) and by UI layer toggles.
        "mode": "metro",
        # Include operator in the source so provenance is clear (TRTC/TYMC/KRTC, etc.).
        "source": f"tdx:{operator}",
    }


def fetch_and_write_stops(
    settings: dict[str, Any],
    *,
    run_id: str | None = None,
    on_retry: Callable[[dict[str, Any]], None] | None = None,
) -> Path:
    logger = logging.getLogger("libraryreach")
    # Resolve cache directory from settings so tests can redirect it to a temp folder.
    cache_dir = Path(settings["paths"]["cache_dir"])
    # Read TDX config block (URLs, endpoints, defaults) from YAML-derived settings.
    tdx_cfg = settings["tdx"]
    # Use a disk cache so repeated runs are fast and do not overload the API.
    cache = DiskCache(cache_dir, default_ttl_s=int(tdx_cfg.get("cache_ttl_s", 86400)))
    # Create the client from environment variables so credentials are never stored in git.
    client = TDXClient.from_env(settings=settings, cache=cache)
    if on_retry is not None:
        try:
            client.on_retry = on_retry  # type: ignore[assignment]
        except Exception:
            pass

    # Multi-city ingestion is driven by config `aoi.cities`.
    cities: list[str] = list(settings.get("aoi", {}).get("cities", []))
    # Having no cities is a configuration error; fail fast with a clear message.
    if not cities:
        raise ValueError("No cities configured under aoi.cities")

    # Bus stops are fetched per city using a city-formatted endpoint template.
    bus_endpoint = tdx_cfg["endpoints"]["bus_stops_by_city"]
    # Page size trades off between fewer requests and larger responses (TDX supports OData paging).
    page_size = int(tdx_cfg.get("page_size", 5000))

    # Collect normalized bus rows across all configured cities.
    bus_rows: list[dict[str, Any]] = []
    for city in cities:
        # Render the endpoint path using the city code (e.g., Taipei, NewTaipei).
        path = bus_endpoint.format(city=city)
        # Use cached paging so repeated runs within TTL avoid re-downloading unchanged data.
        items = client.get_paged_json(path, page_size=page_size, cache_ttl_s=cache.default_ttl_s)
        for item in items:
            # Defensive: TDX should return dicts, but we skip anything unexpected.
            if not isinstance(item, dict):
                continue
            # Normalize the stop into our unified schema; `None` means "skip".
            row = _normalize_bus_stop(item, city=city)
            if row:
                bus_rows.append(row)

    # Collect normalized metro station rows across all configured operators.
    metro_rows: list[dict[str, Any]] = []
    metro_endpoint = tdx_cfg["endpoints"].get("metro_stations_by_operator")
    # Operator codes control which metro systems we include (e.g., TRTC, TYMC, KRTC).
    operator_codes: list[str] = list(tdx_cfg.get("metro_operator_codes", []))
    metro_enabled = bool(tdx_cfg.get("enable_metro", True))
    skipped_metro_operators: list[str] = []
    if metro_enabled and metro_endpoint and operator_codes:
        for operator in operator_codes:
            # Render the endpoint path using the operator code.
            path = metro_endpoint.format(operator=operator)
            # Fetch JSON with caching; this endpoint is typically smaller than bus stops.
            # Some deployments/accounts may not have access to Rail/Metro endpoints; treat 404 as "skip metro".
            try:
                items = client.get_json(path, cache_ttl_s=cache.default_ttl_s)
            except RuntimeError as e:
                msg = str(e)
                if " status=404 " in msg or msg.endswith(" status=404"):
                    logger.warning("TDX metro endpoint not found for operator=%s; skipping metro ingestion", operator)
                    skipped_metro_operators.append(str(operator))
                    continue
                raise
            # Defensive: if the endpoint returns a non-list, we skip to avoid crashing ingestion.
            if not isinstance(items, list):
                continue
            for item in items:
                # Defensive: only dict items can be normalized into rows.
                if not isinstance(item, dict):
                    continue
                # Normalize the station into our unified schema; `None` means "skip".
                row = _normalize_metro_station(item, operator=operator)
                if row:
                    metro_rows.append(row)

    # Combine bus + metro into one DataFrame so downstream code can do a single spatial join.
    df = pd.DataFrame(bus_rows + metro_rows)
    # If the result is empty, something is wrong (credentials, endpoints, or AOI config).
    if df.empty:
        raise RuntimeError("No stops returned from TDX (check credentials and city/operator config).")

    # Drop any rows missing coordinates; spatial computations require numeric lat/lon.
    df = df.dropna(subset=["lat", "lon"])
    # Drop rows missing stop_id; stop_id is required for stable deduplication and provenance.
    df = df.dropna(subset=["stop_id"])
    # Ensure stop_id is a string so CSV output is stable and not interpreted as numeric.
    df["stop_id"] = df["stop_id"].astype(str)
    # Remove obvious duplicates; keep the first occurrence to preserve a stable row selection.
    df = df.drop_duplicates(subset=["stop_id", "mode"], keep="first")
    # Sort for reproducible CSV diffs across runs (order does not affect spatial computations).
    df = df.sort_values(by=["mode", "city", "stop_id"], kind="mergesort").reset_index(drop=True)

    # Write outputs under the raw TDX directory so later stages can re-run without refetching.
    out_dir = Path(settings["paths"]["raw_dir"]) / "tdx"
    # Create the directory if missing so first-time runs do not fail on file writes.
    out_dir.mkdir(parents=True, exist_ok=True)
    # Keep the filename stable so other modules and docs can reference it.
    out_path = out_dir / "stops.csv"
    # Write as CSV because it is easy to inspect and works well with pandas.
    df.to_csv(out_path, index=False)

    # Store metadata for auditing and debugging (counts by mode and configured cities).
    rid = str(run_id or new_run_id())
    generated_at = utc_now_iso()
    fingerprint = config_fingerprint(settings)
    config_hash = json_hash(fingerprint)

    meta = {
        "run_id": rid,
        "generated_at": generated_at,
        "generated_at_epoch_s": int(time.time()),
        "scenario": (settings.get("_meta", {}) or {}).get("scenario"),
        "cities": cities,
        "counts": df.groupby("mode").size().to_dict(),
        "total": int(len(df)),
        "config_hash": config_hash,
        "config_fingerprint": fingerprint,
        "input_sources": [
            file_meta(Path(str((settings.get("_meta", {}) or {}).get("config_path") or "config/default.yaml"))).__dict__,
            file_meta(
                Path(str((settings.get("_meta", {}) or {}).get("scenario_path") or "config/scenarios/weekday.yaml"))
            ).__dict__,
        ],
        "tdx": {
            "enable_metro": metro_enabled,
            "metro_operator_codes": operator_codes,
            "skipped_metro_operators": skipped_metro_operators,
            "endpoints": dict(tdx_cfg.get("endpoints", {}) or {}),
            "cache_ttl_s": int(tdx_cfg.get("cache_ttl_s", 0) or 0),
            "min_request_interval_s": float(tdx_cfg.get("min_request_interval_s", 0.0) or 0.0),
            "min_request_interval_token_s": tdx_cfg.get("min_request_interval_token_s"),
            "min_request_interval_bus_s": tdx_cfg.get("min_request_interval_bus_s"),
            "min_request_interval_metro_s": tdx_cfg.get("min_request_interval_metro_s"),
            "max_retries": int(tdx_cfg.get("max_retries", 0) or 0),
        },
    }
    # Write JSON with UTF-8 so bilingual content stays readable.
    (out_dir / "stops.meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Update global sources index for cross-source traceability.
    upsert_source_record(
        settings,
        SourceRecord(
            source_id="tdx_stops_v1",
            fetched_at=generated_at,
            output_path=str(out_path),
            checksum_sha256=sha256_file(out_path),
            status="ok",
            details={
                "scenario": meta.get("scenario"),
                "cities": cities,
                "counts": meta.get("counts"),
                "tdx": meta.get("tdx"),
                "inputs": meta.get("input_sources"),
            },
        ),
    )
    # Return the path so callers (CLI/pipeline) can log or reuse it.
    return out_path
