# LibraryReach (Phase 1 MVP)

LibraryReach is a library accessibility and outreach planning platform that applies transit analytics to public service planning.

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

## Configuration

- `config/default.yaml` defines cities, buffer radii, scoring weights, thresholds, and planning parameters.
- `config/scenarios/*.yaml` can override weights and thresholds for different time-window scenarios (weekday/weekend/after-school).

## Data

- `data/catalogs/libraries.csv` is a project-owned library branch catalog (replace the sample rows with your real catalog).
- `data/catalogs/outreach_candidates.csv` lists candidate outreach sites (community centers, schools, etc.).
