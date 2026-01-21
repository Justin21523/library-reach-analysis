#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

echo "[1/4] Python unit tests"
pytest -q

echo "[2/4] Install Node deps (if needed)"
if [ ! -d node_modules ]; then
  npm install
fi

echo "[3/4] Playwright tests (fixture mode) + screenshots"
npx playwright install chromium
npx playwright test

echo "[4/4] Update README.md"
python scripts/update_readme.py

echo "Done."
