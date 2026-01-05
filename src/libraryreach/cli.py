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
    run_all = sub.add_parser("run-all", parents=[common], help="Run the full Phase 1 pipeline")
    run_all.add_argument("--skip-fetch", action="store_true", help="Skip TDX fetch (use existing stops.csv)")
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

    raise SystemExit(f"Unknown command: {args.command}")
