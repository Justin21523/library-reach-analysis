from __future__ import annotations

import os
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "README.md"
SHOT_DIR = ROOT / "docs" / "screenshots"


def img(path: str, alt: str) -> str:
    return f"![{alt}]({path})"


def main() -> int:
    shots = {
        "home": "docs/screenshots/home.png",
        "results": "docs/screenshots/results.png",
        "brief": "docs/screenshots/brief.png",
        "method": "docs/screenshots/method.png",
        "console": "docs/screenshots/console.png",
        "console_mobile": "docs/screenshots/console-mobile.png",
    }

    missing = [k for k, v in shots.items() if not (ROOT / v).exists()]
    note = ""
    if missing:
        note = (
            "\n> Note: some screenshots are missing. Generate them with `npm run e2e:screenshots`.\n"
            f"> Missing: {', '.join(missing)}\n"
        )

    md = f"""# LibraryReach

LibraryReach is a narrative-first, public-policy-style dashboard for **library accessibility** and **outreach planning**.
It turns transit accessibility signals into explainable KPIs, maps, and a shareable brief.
{note}

## Demo links

- Home: `/`
- Results: `/results`
- Brief: `/brief`
- Console: `/console`
- Method: `/method`

## Gallery (deterministic demo data)

{img(shots["home"], "Home (narrative hero)")}\n
{img(shots["results"], "Results (projection-ready)")}\n
{img(shots["brief"], "Brief (one-page)")}\n
{img(shots["console"], "Console (map + controls)")}\n
{img(shots["console_mobile"], "Console (mobile bottom sheet)")}\n
{img(shots["method"], "Method (explainable model)")}\n

## What’s inside (recent highlights)

- Narrative-first UX: unified hero + typography + story blocks (Problem → Insight → Action).
- Projection-ready results: 3 key charts, consistent map palette, copy/download tools.
- Control console upgrades: spotlight mode, clearer legend, action drawer feedback, library labels.
- Mobile experience: console controls become a draggable bottom sheet + sticky CTAs on content pages.
- Data provenance: run/source cards for traceability.
- Deterministic fixture mode for demos/tests: `LIBRARYREACH_E2E_FIXTURES=1`.

## Run locally

### Option A: Docker (recommended)

```bash
docker compose up -d --build api
open http://127.0.0.1:${{LR_HOST_PORT:-8001}}/
```

### Option B: Run API directly

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
uvicorn libraryreach.api.main:app --reload --host 127.0.0.1 --port 8001
```

## Tests (unit + Playwright + screenshots)

### 1) Install Playwright

```bash
npm install
npx playwright install --with-deps
```

### 2) Run everything (pytest + Playwright + README update)

```bash
npm run test:all
```

### One-liner (bash)

```bash
./scripts/test_all.sh
```

### 3) Generate screenshots only

```bash
npm run e2e:screenshots
python scripts/update_readme.py
```

### Reports

- Playwright HTML report: `reports/playwright-report/index.html`
- Playwright artifacts: `reports/playwright-results/`
"""

    OUT.write_text(md.strip() + "\n", encoding="utf-8")
    os.makedirs(SHOT_DIR, exist_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
