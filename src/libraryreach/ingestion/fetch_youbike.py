from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from libraryreach.cache import DiskCache
from libraryreach.ingestion.sources_index import SourceRecord, sha256_file, upsert_source_record
from libraryreach.ingestion.tdx_client import TDXClient
from libraryreach.run_meta import config_fingerprint, file_meta, json_hash, new_run_id, utc_now_iso


def _pick_name(name_obj: Any) -> str | None:
    if isinstance(name_obj, dict):
        return name_obj.get("En") or name_obj.get("Zh_tw") or name_obj.get("Zh_TW")
    if isinstance(name_obj, str):
        return name_obj
    return None


def fetch_and_write_youbike_stations(
    settings: dict[str, Any],
    *,
    run_id: str | None = None,
    on_retry: Callable[[dict[str, Any]], None] | None = None,
) -> Path | None:
    logger = logging.getLogger("libraryreach")
    tdx_cfg = settings.get("tdx", {}) or {}
    if not bool(tdx_cfg.get("enable_youbike", False)):
        return None

    endpoint_tpl = (tdx_cfg.get("endpoints", {}) or {}).get("bike_stations_by_city")
    if not endpoint_tpl:
        logger.warning("TDX YouBike enabled but endpoint missing: tdx.endpoints.bike_stations_by_city")
        return None

    cache_dir = Path(settings["paths"]["cache_dir"])
    cache = DiskCache(cache_dir, default_ttl_s=int(tdx_cfg.get("cache_ttl_s", 86400)))
    client = TDXClient.from_env(settings=settings, cache=cache)
    if on_retry is not None:
        client.on_retry = on_retry

    cities: list[str] = list((settings.get("aoi", {}) or {}).get("cities", []))
    if not cities:
        raise ValueError("No cities configured under aoi.cities")

    rows: list[dict[str, Any]] = []
    for city in cities:
        path = str(endpoint_tpl).format(city=city)
        try:
            items = client.get_json(path, cache_ttl_s=cache.default_ttl_s)
        except RuntimeError as e:
            msg = str(e)
            if " status=404 " in msg or msg.endswith(" status=404"):
                logger.warning("TDX YouBike endpoint not found for city=%s; skipping", city)
                continue
            raise

        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            pos = item.get("StationPosition") or item.get("StopPosition") or {}
            lat = pos.get("PositionLat")
            lon = pos.get("PositionLon")
            if lat is None or lon is None:
                continue
            rows.append(
                {
                    "station_id": item.get("StationID") or item.get("StopID") or item.get("StationUID"),
                    "name": _pick_name(item.get("StationName") or item.get("StopName")),
                    "lat": float(lat),
                    "lon": float(lon),
                    "city": item.get("City") or city,
                    "source": "tdx",
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        logger.warning("No YouBike stations fetched (maybe endpoint unavailable)")
        return None

    df = df.dropna(subset=["lat", "lon"])
    df = df.dropna(subset=["station_id"])
    df["station_id"] = df["station_id"].astype(str)
    df = df.drop_duplicates(subset=["station_id"], keep="first")
    df = df.sort_values(by=["city", "station_id"], kind="mergesort").reset_index(drop=True)

    out_dir = Path(settings["paths"]["raw_dir"]) / "tdx"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "youbike_stations.csv"
    df.to_csv(out_path, index=False)

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
            "endpoint": str(endpoint_tpl),
            "cache_ttl_s": int(tdx_cfg.get("cache_ttl_s", 0) or 0),
        },
    }
    (out_dir / "youbike_stations.meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    upsert_source_record(
        settings,
        SourceRecord(
            source_id="tdx_youbike_stations_v1",
            fetched_at=generated_at,
            output_path=str(out_path),
            checksum_sha256=sha256_file(out_path),
            status="ok",
            details={"cities": cities, "total": int(len(df)), "endpoint": str(endpoint_tpl)},
        ),
    )
    return out_path

