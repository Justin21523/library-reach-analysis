# Catalogs (Multi-city, Phase 1)

This project uses two project-owned catalogs:

- `libraries.csv`: library branch locations
- `outreach_candidates.csv`: candidate outreach sites (schools, community centers, etc.)

Both catalogs are designed to support **multi-city** analysis.

## Common conventions

- `id` must be unique within each catalog and stable over time.
- `city` should match TDX city codes used in `config/default.yaml` (e.g., `Taipei`, `NewTaipei`, `Taoyuan`).
- `lat`/`lon` are WGS84 decimal degrees.
- Prefer English identifiers; free-text fields can be bilingual if needed.

## `libraries.csv` schema

Required columns:
- `id`: string (unique)
- `name`: string
- `address`: string
- `lat`: float
- `lon`: float
- `city`: string
- `district`: string

Optional columns (recommended):
- `library_system`: string (e.g., city library system name)
- `branch_type`: string (e.g., central/branch/mobile)
- `phone`: string
- `website`: string
- `notes`: string
- `source`: string

## `outreach_candidates.csv` schema

Required columns:
- `id`: string (unique)
- `name`: string
- `type`: string (see allowed values below)
- `address`: string
- `lat`: float
- `lon`: float
- `city`: string
- `district`: string

Optional columns (recommended):
- `owner`: string
- `notes`: string
- `source`: string

Allowed `type` values (default):
- `community_center`
- `school`
- `park`
- `market`
- `transit_hub`
- `government_office`
- `other`

