from __future__ import annotations

import json
import logging
import os
import random
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

from zoneinfo import ZoneInfo


def _log() -> logging.Logger:
    return logging.getLogger("libraryreach")


def _read_json(path: Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _parse_iso_to_epoch_s(value: str) -> int | None:
    try:
        dt = datetime.fromisoformat(value)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _status_path(settings: dict[str, Any]) -> Path:
    return Path(settings["paths"]["raw_dir"]) / "tdx" / "ingestion_status.json"


def _load_status(settings: dict[str, Any]) -> dict[str, Any]:
    p = _status_path(settings)
    if not p.exists():
        return {}
    try:
        data = _read_json(p)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _write_status(settings: dict[str, Any], status: dict[str, Any]) -> None:
    p = _status_path(settings)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _daemon_tz(settings: dict[str, Any]) -> ZoneInfo:
    tz_name = (
        (settings.get("daemon", {}) or {}).get("timezone")
        or (settings.get("project", {}) or {}).get("timezone")
        or "UTC"
    )
    try:
        return ZoneInfo(str(tz_name))
    except Exception:
        return ZoneInfo("UTC")


def _parse_hhmm(value: str) -> tuple[int, int]:
    parts = str(value).strip().split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid time (HH:MM): {value}")
    h = int(parts[0])
    m = int(parts[1])
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError(f"Invalid time (HH:MM): {value}")
    return h, m


def _window_bounds_for_day(day: date, *, tz: ZoneInfo, start_hhmm: str, end_hhmm: str) -> tuple[datetime, datetime]:
    sh, sm = _parse_hhmm(start_hhmm)
    eh, em = _parse_hhmm(end_hhmm)
    start = datetime(day.year, day.month, day.day, sh, sm, tzinfo=tz)
    end = datetime(day.year, day.month, day.day, eh, em, tzinfo=tz)
    if end <= start:
        end = end + timedelta(days=1)
    return start, end


def _in_window(now: datetime, *, start: datetime, end: datetime) -> bool:
    return start <= now <= end


def _epoch_date_local(epoch_s: int, *, tz: ZoneInfo) -> date:
    return datetime.fromtimestamp(int(epoch_s), tz=timezone.utc).astimezone(tz).date()


def _next_window_start(now: datetime, *, tz: ZoneInfo, start_hhmm: str, end_hhmm: str) -> datetime:
    today = now.date()
    start, end = _window_bounds_for_day(today, tz=tz, start_hhmm=start_hhmm, end_hhmm=end_hhmm)
    if now < start:
        return start
    if now <= end:
        return now
    # Next day
    start2, _ = _window_bounds_for_day(today + timedelta(days=1), tz=tz, start_hhmm=start_hhmm, end_hhmm=end_hhmm)
    return start2


def _ensure_daily_target(
    status: dict[str, Any],
    *,
    key: str,
    day: date,
    window_start: datetime,
    window_end: datetime,
    jitter_max_s: float,
) -> datetime:
    schedule = status.setdefault("schedule", {})
    cache = schedule.setdefault("daily_targets", {})
    day_key = day.isoformat()

    cached = cache.get(key)
    if isinstance(cached, dict) and cached.get("day") == day_key and isinstance(cached.get("target_local"), str):
        try:
            return datetime.fromisoformat(cached["target_local"])
        except Exception:
            pass

    window_len_s = max(1.0, float((window_end - window_start).total_seconds()))
    cap = float(jitter_max_s) if float(jitter_max_s) > 0 else window_len_s
    offset_s = random.uniform(0.0, min(window_len_s, cap))
    target = window_start + timedelta(seconds=offset_s)

    cache[key] = {"day": day_key, "target_local": target.isoformat()}
    return target


def _stops_generated_at_epoch_s(settings: dict[str, Any]) -> int | None:
    raw_dir = Path(settings["paths"]["raw_dir"])
    meta_path = raw_dir / "tdx" / "stops.meta.json"
    if not meta_path.exists():
        return None
    meta = _read_json(meta_path)
    value = meta.get("generated_at_epoch_s")
    try:
        return int(value)
    except Exception:
        return None


def _run_meta_generated_at_epoch_s(settings: dict[str, Any]) -> int | None:
    processed_dir = Path(settings["paths"]["processed_dir"])
    meta_path = processed_dir / "run_meta.json"
    if not meta_path.exists():
        return None
    meta = _read_json(meta_path)
    generated_at = meta.get("generated_at")
    if isinstance(generated_at, str):
        return _parse_iso_to_epoch_s(generated_at)
    return None


def _is_due(last_epoch_s: int | None, *, interval_s: float) -> bool:
    if interval_s <= 0:
        return True
    if last_epoch_s is None:
        return True
    return (time.time() - float(last_epoch_s)) >= float(interval_s)


def _seconds_until_due(last_epoch_s: int | None, *, interval_s: float) -> float:
    if interval_s <= 0:
        return 0.0
    if last_epoch_s is None:
        return 0.0
    return max(0.0, float(interval_s) - (time.time() - float(last_epoch_s)))


@contextmanager
def _process_lock(lock_path: Path) -> Iterator[None]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    f = lock_path.open("w", encoding="utf-8")
    try:
        try:
            import fcntl  # type: ignore

            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except ModuleNotFoundError:
            pass
        except BlockingIOError as e:
            raise RuntimeError(f"Daemon already running (lock busy): {lock_path}") from e
        f.write(str(int(time.time())))
        f.flush()
        yield
    finally:
        try:
            f.close()
        except Exception:
            pass


def run_daemon(
    settings: dict[str, Any],
    *,
    fetch_interval_s: float | None = None,
    pipeline_interval_s: float | None = None,
    jitter_s: float = 3.0,
    poll_max_s: float = 300.0,
    failure_backoff_s: float = 300.0,
    once: bool = False,
    skip_fetch: bool = False,
    skip_pipeline: bool = False,
    lock_file: str | None = None,
) -> None:
    tdx = settings.get("tdx", {}) or {}
    daemon_cfg = settings.get("daemon", {}) or {}
    tz = _daemon_tz(settings)

    # If the caller did not force explicit intervals, default to daily window scheduling.
    if fetch_interval_s is None and "fetch_window" not in daemon_cfg:
        fetch_interval_s = float(tdx.get("cache_ttl_s", 86400))
    if pipeline_interval_s is None and "pipeline_window" not in daemon_cfg:
        pipeline_interval_s = float(tdx.get("cache_ttl_s", 86400))

    poll_max_s = float(daemon_cfg.get("poll_max_s", poll_max_s))
    failure_backoff_s = float(daemon_cfg.get("failure_backoff_s", failure_backoff_s))
    window_jitter_max_s = float(daemon_cfg.get("window_jitter_max_s", 0.0))
    fetch_window = daemon_cfg.get("fetch_window", {}) or {}
    pipeline_window = daemon_cfg.get("pipeline_window", {}) or {}

    lock_path = (
        Path(lock_file)
        if lock_file
        else (Path(settings["paths"]["cache_dir"]) / "daemon.lock")
    )

    from libraryreach.ingestion.fetch_stops import fetch_and_write_stops
    from libraryreach.ingestion.fetch_youbike import fetch_and_write_youbike_stations
    from libraryreach.ingestion.sources_index import SourceRecord, sha256_file, sources_index_path, upsert_source_record
    from libraryreach.ingestion.tdx_client import TDXAuthError
    from libraryreach.pipeline import run_phase1

    with _process_lock(lock_path):
        status = _load_status(settings)
        status.setdefault("generated_at", _utc_now_iso())
        status.setdefault("tz", str(tz.key))
        status.setdefault("state", "idle")
        status.setdefault("fetch", {})
        status.setdefault("pipeline", {})
        status.setdefault("rate_limit", {"http_429_count": 0, "retry_events": 0})
        _write_status(settings, status)

        # If we already have raw artifacts from a previous run, ensure the global sources index exists.
        try:
            idx_path = sources_index_path(settings)
            raw_dir = Path(settings["paths"]["raw_dir"]) / "tdx"
            stops_csv = raw_dir / "stops.csv"
            stops_meta = raw_dir / "stops.meta.json"
            if stops_csv.exists() and not idx_path.exists():
                details: dict[str, Any] = {}
                if stops_meta.exists():
                    try:
                        details["stops_meta"] = _read_json(stops_meta)
                    except Exception:
                        pass
                upsert_source_record(
                    settings,
                    SourceRecord(
                        source_id="tdx_stops_v1",
                        fetched_at=str((details.get("stops_meta") or {}).get("generated_at") or _utc_now_iso()),
                        output_path=str(stops_csv),
                        checksum_sha256=sha256_file(stops_csv),
                        status="ok",
                        details=details,
                    ),
                )
        except Exception:
            _log().exception("Failed to backfill sources_index.json from existing raw files")

        _log().info(
            "Daemon started: fetch_interval_s=%s pipeline_interval_s=%s lock=%s",
            fetch_interval_s,
            pipeline_interval_s,
            lock_path,
        )

        def record_retry(event: dict[str, Any]) -> None:
            nonlocal status
            rl = status.setdefault("rate_limit", {"http_429_count": 0, "retry_events": 0})
            rl["retry_events"] = int(rl.get("retry_events", 0)) + 1
            if int(event.get("status_code") or 0) == 429:
                rl["http_429_count"] = int(rl.get("http_429_count", 0)) + 1
                status["state"] = "rate_limited"
                status["fetch"]["last_429_at"] = _utc_now_iso()
                status["fetch"]["last_retry_after_s"] = event.get("sleep_s")
                try:
                    status["fetch"]["next_retry_at"] = datetime.fromtimestamp(
                        time.time() + float(event.get("sleep_s") or 0),
                        tz=timezone.utc,
                    ).replace(microsecond=0).isoformat()
                except Exception:
                    pass
            status["generated_at"] = _utc_now_iso()
            _write_status(settings, status)

        while True:
            did_work = False
            try:
                now_local = datetime.now(tz)
                status["generated_at"] = _utc_now_iso()

                status.setdefault("schedule", {})
                status["schedule"]["now_local"] = now_local.replace(microsecond=0).isoformat()
                status["schedule"]["tz"] = str(tz.key)
                if fetch_window.get("start") and fetch_window.get("end"):
                    status["schedule"]["fetch_window"] = {"start": fetch_window["start"], "end": fetch_window["end"]}
                    status["schedule"]["next_fetch_window_start_local"] = _next_window_start(
                        now_local,
                        tz=tz,
                        start_hhmm=str(fetch_window["start"]),
                        end_hhmm=str(fetch_window["end"]),
                    ).replace(microsecond=0).isoformat()
                if pipeline_window.get("start") and pipeline_window.get("end"):
                    status["schedule"]["pipeline_window"] = {
                        "start": pipeline_window["start"],
                        "end": pipeline_window["end"],
                    }
                    status["schedule"]["next_pipeline_window_start_local"] = _next_window_start(
                        now_local,
                        tz=tz,
                        start_hhmm=str(pipeline_window["start"]),
                        end_hhmm=str(pipeline_window["end"]),
                    ).replace(microsecond=0).isoformat()

                if not skip_fetch:
                    last_stops = _stops_generated_at_epoch_s(settings)
                    can_run_fetch = False

                    if fetch_interval_s is not None:
                        can_run_fetch = _is_due(last_stops, interval_s=float(fetch_interval_s))
                    elif fetch_window.get("start") and fetch_window.get("end"):
                        start, end = _window_bounds_for_day(
                            now_local.date(),
                            tz=tz,
                            start_hhmm=str(fetch_window["start"]),
                            end_hhmm=str(fetch_window["end"]),
                        )
                        ran_today = last_stops is not None and _epoch_date_local(last_stops, tz=tz) == now_local.date()
                        if _in_window(now_local, start=start, end=end) and not ran_today:
                            target = _ensure_daily_target(
                                status,
                                key="fetch",
                                day=now_local.date(),
                                window_start=start,
                                window_end=end,
                                jitter_max_s=window_jitter_max_s,
                            )
                            if now_local < target:
                                wait_s = (target - now_local).total_seconds()
                                status["state"] = "waiting_window"
                                status["fetch"]["next_scheduled_local"] = target.replace(microsecond=0).isoformat()
                                _write_status(settings, status)
                                _log().info("Waiting %.0fs for scheduled fetch (%s)", wait_s, target.isoformat())
                                time.sleep(max(1.0, wait_s))
                                now_local = datetime.now(tz)
                            can_run_fetch = True

                    if can_run_fetch:
                        if not (os.getenv("TDX_CLIENT_ID") and os.getenv("TDX_CLIENT_SECRET")):
                            _log().error(
                                "TDX credentials missing (TDX_CLIENT_ID/TDX_CLIENT_SECRET). "
                                "Set them in .env (or container env) and restart the worker."
                            )
                        else:
                            _log().info("Fetching stops from TDX")
                            try:
                                status["state"] = "fetching"
                                status["fetch"]["last_attempt_at"] = _utc_now_iso()
                                _write_status(settings, status)

                                fetch_and_write_stops(settings, run_id=None, on_retry=record_retry)
                                did_work = True
                                status["state"] = "idle"
                                status["fetch"]["last_success_at"] = _utc_now_iso()
                                # Snapshot stops meta (so /health can show freshness without reading CSV).
                                meta_path = Path(settings["paths"]["raw_dir"]) / "tdx" / "stops.meta.json"
                                if meta_path.exists():
                                    try:
                                        status["stops_meta"] = _read_json(meta_path)
                                    except Exception:
                                        pass

                                # Optional additional layers.
                                try:
                                    fetch_and_write_youbike_stations(settings, on_retry=record_retry)
                                except Exception:
                                    _log().exception("YouBike ingestion failed (optional layer)")
                                _write_status(settings, status)
                            except TDXAuthError:
                                _log().exception(
                                    "TDX auth failed; check credentials and restart the worker if you updated .env"
                                )
                                status["state"] = "error"
                                status["fetch"]["last_error_at"] = _utc_now_iso()
                                status["fetch"]["last_error"] = "TDXAuthError"
                                _write_status(settings, status)
                            except Exception as e:
                                status["state"] = "error"
                                status["fetch"]["last_error_at"] = _utc_now_iso()
                                status["fetch"]["last_error"] = repr(e)
                                _write_status(settings, status)
                                raise
                    else:
                        if fetch_interval_s is not None:
                            _log().info(
                                "Stops fresh; skip fetch (next in ~%.0fs)",
                                _seconds_until_due(last_stops, interval_s=float(fetch_interval_s)),
                            )
                        elif fetch_window.get("start") and fetch_window.get("end"):
                            _log().info("Outside fetch window or already ran today; skip fetch")

                if not skip_pipeline:
                    last_run = _run_meta_generated_at_epoch_s(settings)
                    stops_path = Path(settings["paths"]["raw_dir"]) / "tdx" / "stops.csv"
                    if not stops_path.exists():
                        _log().warning("No stops.csv yet; skip pipeline until ingestion succeeds (%s)", stops_path)
                    else:
                        can_run_pipeline = False
                        if did_work:
                            can_run_pipeline = True
                        elif pipeline_interval_s is not None:
                            can_run_pipeline = _is_due(last_run, interval_s=float(pipeline_interval_s))
                        elif pipeline_window.get("start") and pipeline_window.get("end"):
                            start, end = _window_bounds_for_day(
                                now_local.date(),
                                tz=tz,
                                start_hhmm=str(pipeline_window["start"]),
                                end_hhmm=str(pipeline_window["end"]),
                            )
                            ran_today = last_run is not None and _epoch_date_local(last_run, tz=tz) == now_local.date()
                            if _in_window(now_local, start=start, end=end) and not ran_today:
                                target = _ensure_daily_target(
                                    status,
                                    key="pipeline",
                                    day=now_local.date(),
                                    window_start=start,
                                    window_end=end,
                                    jitter_max_s=window_jitter_max_s,
                                )
                                if now_local < target:
                                    wait_s = (target - now_local).total_seconds()
                                    _log().info("Waiting %.0fs for scheduled pipeline (%s)", wait_s, target.isoformat())
                                    time.sleep(max(1.0, wait_s))
                                    now_local = datetime.now(tz)
                                can_run_pipeline = True

                        if can_run_pipeline:
                            _log().info("Running pipeline (phase1)")
                            status["state"] = "running_pipeline"
                            status["pipeline"]["last_attempt_at"] = _utc_now_iso()
                            _write_status(settings, status)
                            run_phase1(settings)
                            did_work = True
                            status["state"] = "idle"
                            status["pipeline"]["last_success_at"] = _utc_now_iso()
                            _write_status(settings, status)
                        else:
                            if pipeline_interval_s is not None:
                                _log().info(
                                    "Pipeline fresh; skip run (next in ~%.0fs)",
                                    _seconds_until_due(last_run, interval_s=float(pipeline_interval_s)),
                                )
                            else:
                                _log().info("Outside pipeline window or already ran today; skip run")

                if once:
                    _log().info("Daemon exiting (--once)")
                    return

                if did_work:
                    sleep_s = min(float(poll_max_s), 10.0)
                else:
                    candidates: list[float] = [float(poll_max_s)]
                    if not skip_fetch:
                        if fetch_interval_s is not None:
                            candidates.append(
                                _seconds_until_due(_stops_generated_at_epoch_s(settings), interval_s=float(fetch_interval_s))
                            )
                        elif fetch_window.get("start") and fetch_window.get("end"):
                            nxt = _next_window_start(
                                now_local,
                                tz=tz,
                                start_hhmm=str(fetch_window["start"]),
                                end_hhmm=str(fetch_window["end"]),
                            )
                            candidates.append(max(0.0, (nxt - now_local).total_seconds()))
                    if not skip_pipeline:
                        if pipeline_interval_s is not None:
                            candidates.append(
                                _seconds_until_due(_run_meta_generated_at_epoch_s(settings), interval_s=float(pipeline_interval_s))
                            )
                        elif pipeline_window.get("start") and pipeline_window.get("end"):
                            nxt = _next_window_start(
                                now_local,
                                tz=tz,
                                start_hhmm=str(pipeline_window["start"]),
                                end_hhmm=str(pipeline_window["end"]),
                            )
                            candidates.append(max(0.0, (nxt - now_local).total_seconds()))

                    sleep_s = min(float(poll_max_s), max(5.0, min(candidates)))

                sleep_s = max(0.0, float(sleep_s) + random.uniform(0.0, float(jitter_s)))
                status["generated_at"] = _utc_now_iso()
                _write_status(settings, status)
                _log().info("Sleeping %.1fs", sleep_s)
                time.sleep(sleep_s)
            except Exception:
                _log().exception("Daemon cycle failed; backing off")
                if once:
                    raise
                sleep_s = float(failure_backoff_s) + random.uniform(0.0, float(jitter_s))
                status["generated_at"] = _utc_now_iso()
                status["state"] = "error"
                status.setdefault("daemon", {})["last_error_at"] = _utc_now_iso()
                _write_status(settings, status)
                time.sleep(max(1.0, sleep_s))
