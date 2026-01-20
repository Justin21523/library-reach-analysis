import argparse
from pathlib import Path

from libraryreach.settings import load_settings


def _build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", default="config/default.yaml", help="Path to config YAML")
    common.add_argument("--scenario", default="weekday", help="Scenario name (config/scenarios/<name>.yaml)")

    parser = argparse.ArgumentParser(prog="libraryreach", description="LibraryReach CLI", parents=[common])

    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("fetch-stops", parents=[common], help="Fetch transit stops (bus + metro) from TDX")
    sub.add_parser("fetch-youbike", parents=[common], help="Fetch YouBike stations from TDX (optional)")
    fetch_open = sub.add_parser("fetch-open-data", parents=[common], help="Fetch non-TDX Open Data sources (optional)")
    fetch_open.add_argument(
        "--only",
        action="append",
        default=None,
        help="Limit to a specific source_id (repeatable). If omitted, fetches all enabled sources.",
    )
    run_all = sub.add_parser("run-all", parents=[common], help="Run the full Phase 1 pipeline")
    run_all.add_argument("--skip-fetch", action="store_true", help="Skip TDX fetch (use existing stops.csv)")
    daemon = sub.add_parser("daemon", parents=[common], help="Run continuous ingestion + pipeline loop")
    daemon.add_argument("--once", action="store_true", help="Run a single cycle then exit")
    daemon.add_argument("--skip-fetch", action="store_true", help="Do not fetch stops from TDX")
    daemon.add_argument("--skip-pipeline", action="store_true", help="Do not run the analysis pipeline")
    daemon.add_argument("--fetch-interval-s", type=float, default=None, help="Minimum seconds between fetch runs")
    daemon.add_argument("--pipeline-interval-s", type=float, default=None, help="Minimum seconds between pipeline runs")
    daemon.add_argument("--jitter-s", type=float, default=3.0, help="Random jitter added to sleep time")
    daemon.add_argument("--poll-max-s", type=float, default=300.0, help="Maximum sleep between checks")
    daemon.add_argument("--failure-backoff-s", type=float, default=300.0, help="Sleep after a failed cycle")
    daemon.add_argument("--lock-file", default=None, help="Lock file path to avoid duplicate daemons")
    sub.add_parser("validate-catalogs", parents=[common], help="Validate catalog CSVs and write a report")
    sub.add_parser("api-info", parents=[common], help="Print API run instructions")
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    settings = load_settings(Path(args.config), scenario=args.scenario)

    if args.command == "api-info":
        host = settings["api"]["host"]
        port = settings["api"]["port"]
        print(f"Run: uvicorn libraryreach.api.main:app --reload --host {host} --port {port}")
        return

    if args.command == "fetch-stops":
        from libraryreach.ingestion.fetch_stops import fetch_and_write_stops

        fetch_and_write_stops(settings)
        return

    if args.command == "fetch-youbike":
        from libraryreach.ingestion.fetch_youbike import fetch_and_write_youbike_stations

        fetch_and_write_youbike_stations(settings)
        return

    if args.command == "fetch-open-data":
        from libraryreach.ingestion.open_data import fetch_and_write_open_data

        only = getattr(args, "only", None)
        only_set = {str(x) for x in (only or [])} if only else None
        fetch_and_write_open_data(settings, only_source_ids=only_set)
        return

    if args.command == "validate-catalogs":
        from libraryreach.catalogs.validate import format_validation_summary, validate_catalogs

        from libraryreach.catalogs.load import load_libraries_catalog, load_outreach_candidates_catalog

        libraries = load_libraries_catalog(settings)
        outreach = load_outreach_candidates_catalog(settings)
        report = validate_catalogs(settings, libraries=libraries, outreach_candidates=outreach, write_report=True)
        print(format_validation_summary(report))
        return

    if args.command == "run-all":
        from libraryreach.pipeline import run_phase1

        if not getattr(args, "skip_fetch", False):
            from libraryreach.ingestion.fetch_stops import fetch_and_write_stops

            fetch_and_write_stops(settings)
        run_phase1(settings)
        return

    if args.command == "daemon":
        from libraryreach.daemon import run_daemon

        run_daemon(
            settings,
            fetch_interval_s=getattr(args, "fetch_interval_s", None),
            pipeline_interval_s=getattr(args, "pipeline_interval_s", None),
            jitter_s=float(getattr(args, "jitter_s", 3.0)),
            poll_max_s=float(getattr(args, "poll_max_s", 300.0)),
            failure_backoff_s=float(getattr(args, "failure_backoff_s", 300.0)),
            once=bool(getattr(args, "once", False)),
            skip_fetch=bool(getattr(args, "skip_fetch", False)),
            skip_pipeline=bool(getattr(args, "skip_pipeline", False)),
            lock_file=getattr(args, "lock_file", None),
        )
        return

    raise SystemExit(f"Unknown command: {args.command}")
