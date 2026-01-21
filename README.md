# LibraryReach (Phase 1 MVP)

LibraryReach is a library accessibility and outreach planning platform that applies transit analytics to public service planning.

For a screenshot-first showcase and demo narrative, see `READMD.md`.

Phase 1 focuses on an explainable baseline: **transit stop density within 500m/1km buffers** around each library branch, plus grid-based "access desert" detection and outreach site recommendations.

## Quickstart

1) Create your environment and install dependencies:
- `pip install -e ".[dev]"`

2) Configure credentials:
- Copy `.env.example` to `.env`
- Set `TDX_CLIENT_ID` and `TDX_CLIENT_SECRET`

3) Run the pipeline (multi-city by default via `config/default.yaml`):
- `libraryreach run-all --scenario weekday`
  - Use `--skip-fetch` if you already have `data/raw/tdx/stops.csv`.

4) Start the API + minimal web UI:
- `uvicorn libraryreach.api.main:app --reload`
- Open `http://127.0.0.1:8000/`
  - The web UI includes a control console for what-if parameter tuning and keyboard shortcuts (press `?`).
  - The map uses MapLibre + OSM raster tiles (internet required in the browser).

## Docker (Always-On Ingestion)

- Create `.env` with TDX credentials: `cp .env.example .env`
- Start API + background worker (auto-restart): `docker compose up -d --build`
- If you edit `.env`, restart containers to apply env changes: `docker compose restart`
- Default host port is `8001` (set `LR_HOST_PORT=8000 docker compose up -d --build` if you want `:8000`).
- Tail logs:
  - `docker compose logs -f worker`
  - `docker compose logs -f api`
- Tune rate limiting / retries in `config/default.yaml` under `tdx.*` (for example `min_request_interval_s`).
- Ops notes (health/backup/autostart): `docs/10_ops.md`

## Configuration

- `config/default.yaml` defines cities, buffer radii, scoring weights, thresholds, and planning parameters.
- `config/scenarios/*.yaml` can override weights and thresholds for different time-window scenarios (weekday/weekend/after-school).

## Data

- `data/catalogs/libraries.csv` is a project-owned library branch catalog (replace the sample rows with your real catalog).
- `data/catalogs/outreach_candidates.csv` lists candidate outreach sites (community centers, schools, etc.).
- Catalog schema reference: `data/catalogs/README.md`
  - If your catalogs use Chinese city names, map them under `aoi.city_aliases` in `config/default.yaml`.

## Validation

- `libraryreach validate-catalogs --scenario weekday` writes `reports/catalog_validation.md` and fails on schema or consistency errors.
