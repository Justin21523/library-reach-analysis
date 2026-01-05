# LibraryReach: Library Accessibility & Outreach Planning (LIS + Mobility Analytics)

## 1. Project Overview

LibraryReach applies transportation analytics to Library and Information Science (LIS) service planning.

The platform evaluates:
- how accessible public libraries (branches) are via public transport
- which neighborhoods are underserved (service deserts)
- where outreach programs (mobile libraries, pop-up services) should be deployed

It combines:
- public transport reachability indicators from TDX
- spatial boundaries and demographic proxies (optional)
- time-window scenarios (after-school, weekend, rainy day)

Outputs:
- accessibility maps and reachability scores
- fairness insights across districts
- recommended outreach locations with rationale

---

## 2. Core Questions

1. Which library branches have poor public-transport accessibility?
2. How does accessibility change by time-of-day (e.g., after-school vs weekend)?
3. Which areas are likely "library access deserts" under transit constraints?
4. Where should outreach services be placed to maximize service coverage?

---

## 3. Data Sources

### 3.1 TDX Mobility Data (Primary)
- Bus stops/routes and schedules (as available)
- Rail/metro station metadata
- Optional: bike-sharing stations for last-mile accessibility
- Optional: parking availability for car-based access

### 3.2 Library & Context Data
- Library branches dataset (name, address, coordinates, opening hours)
- Administrative boundaries (GIS)
- Optional: population / schools distribution proxies (open data)

---

## 4. Methodology

### 4.1 Reachability Modeling (Baseline First)
- Catchment areas:
  - simple radius buffers (500m / 1km)
  - time-based isochrones (Phase 2+)
- Accessibility score:
  - transit stop density near branch
  - multi-modal connectivity (bus + metro)
  - last-mile support (bike-sharing)

### 4.2 Equity & Planning
- Identify underserved zones by combining:
  - low reachability
  - high demand proxies (schools density, residential density)
- Outreach location selection:
  - candidate points (schools/community centers)
  - maximize coverage under constraints

---

## 5. Web App Features

- Map: library branches + catchment layer
- Branch detail page:
  - accessibility score breakdown
  - nearby transit connectivity summary
- Outreach planner:
  - select scenario/time window
  - see suggested outreach spots + explanation
- Export: report PDF/CSV for planning meetings

---

## 6. Architecture

libraryreach/
├─ data/
│ ├─ catalogs/
│ │ ├─ libraries.csv
│ │ └─ outreach_candidates.csv
│ └─ external/
├─ src/
│ ├─ ingestion/
│ │ ├─ tdx_transit_client.py
│ │ └─ geocoding.py
│ ├─ spatial/
│ │ ├─ buffers.py
│ │ └─ joins.py
│ ├─ scoring/
│ │ ├─ accessibility.py
│ │ └─ explain.py
│ ├─ planning/
│ │ ├─ deserts.py
│ │ └─ outreach_optimizer.py
│ ├─ api/
│ └─ utils/
├─ web/
├─ outputs/
└─ PROJECT_DESCRIPTION.md


---

## 7. Deliverables

- Branch-level accessibility dataset
- Interactive accessibility map + dashboard
- Outreach suggestions engine + exportable reports

---

## 8. Roadmap

Phase 1 (MVP):
- Library catalog + transit stop density scoring
- Map UI with branch scoring
- Simple outreach suggestions based on underserved zones

Phase 2:
- Time-based reachability (isochrones) using transit schedules
- Better demand proxies and optimization

Phase 3:
- User feedback loop and scenario planner improvements
