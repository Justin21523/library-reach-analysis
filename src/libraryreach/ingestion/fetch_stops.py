from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pandas as pd

from libraryreach.cache import DiskCache
from libraryreach.ingestion.tdx_client import TDXClient


def _pick_name(name_obj: Any) -> str | None:
    if isinstance(name_obj, dict):
        return name_obj.get("En") or name_obj.get("Zh_tw") or name_obj.get("Zh_TW")
    if isinstance(name_obj, str):
        return name_obj
    return None


def _normalize_bus_stop(item: dict[str, Any], city: str) -> dict[str, Any] | None:
    pos = item.get("StopPosition") or {}
    lat = pos.get("PositionLat")
    lon = pos.get("PositionLon")
    if lat is None or lon is None:
        return None
    return {
        "stop_id": item.get("StopID"),
        "name": _pick_name(item.get("StopName")),
        "lat": float(lat),
        "lon": float(lon),
        "city": item.get("City") or city,
        "mode": "bus",
        "source": "tdx",
    }


def _normalize_metro_station(item: dict[str, Any], operator: str) -> dict[str, Any] | None:
    pos = item.get("StationPosition") or item.get("StopPosition") or {}
    lat = pos.get("PositionLat")
    lon = pos.get("PositionLon")
    if lat is None or lon is None:
        return None
    return {
        "stop_id": item.get("StationID") or item.get("StopID"),
        "name": _pick_name(item.get("StationName") or item.get("StopName")),
        "lat": float(lat),
        "lon": float(lon),
        "city": item.get("City") or "",
        "mode": "metro",
        "source": f"tdx:{operator}",
    }


def fetch_and_write_stops(settings: dict[str, Any]) -> Path:
    cache_dir = Path(settings["paths"]["cache_dir"])
    tdx_cfg = settings["tdx"]
    cache = DiskCache(cache_dir, default_ttl_s=int(tdx_cfg.get("cache_ttl_s", 86400)))
    client = TDXClient.from_env(settings=settings, cache=cache)

    cities: list[str] = list(settings.get("aoi", {}).get("cities", []))
    if not cities:
        raise ValueError("No cities configured under aoi.cities")

    bus_endpoint = tdx_cfg["endpoints"]["bus_stops_by_city"]
    page_size = int(tdx_cfg.get("page_size", 5000))

    bus_rows: list[dict[str, Any]] = []
    for city in cities:
        path = bus_endpoint.format(city=city)
        items = client.get_paged_json(path, page_size=page_size, cache_ttl_s=cache.default_ttl_s)
        for item in items:
            if not isinstance(item, dict):
                continue
            row = _normalize_bus_stop(item, city=city)
            if row:
                bus_rows.append(row)

    metro_rows: list[dict[str, Any]] = []
    metro_endpoint = tdx_cfg["endpoints"].get("metro_stations_by_operator")
    operator_codes: list[str] = list(tdx_cfg.get("metro_operator_codes", []))
    if metro_endpoint and operator_codes:
        for operator in operator_codes:
            path = metro_endpoint.format(operator=operator)
            items = client.get_json(path, cache_ttl_s=cache.default_ttl_s)
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                row = _normalize_metro_station(item, operator=operator)
                if row:
                    metro_rows.append(row)

    df = pd.DataFrame(bus_rows + metro_rows)
    if df.empty:
        raise RuntimeError("No stops returned from TDX (check credentials and city/operator config).")

    df = df.dropna(subset=["lat", "lon"])
    df["stop_id"] = df["stop_id"].astype(str)
    df = df.drop_duplicates(subset=["stop_id", "mode"], keep="first")

    out_dir = Path(settings["paths"]["raw_dir"]) / "tdx"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "stops.csv"
    df.to_csv(out_path, index=False)

    meta = {
        "generated_at_epoch_s": int(time.time()),
        "cities": cities,
        "counts": df.groupby("mode").size().to_dict(),
        "total": int(len(df)),
    }
    (out_dir / "stops.meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return out_path

