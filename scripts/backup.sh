#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

BACKUP_DIR="${BACKUP_DIR:-$ROOT_DIR/backups}"
KEEP_N="${KEEP_N:-30}"

mkdir -p "$BACKUP_DIR"

ts="$(date -u +'%Y%m%d-%H%M%S')"
out="$BACKUP_DIR/libraryreach-backup-$ts.tar.gz"

tar -czf "$out" \
  config \
  data/catalogs \
  data/processed/run_meta.json \
  data/processed/summary_baseline.json \
  data/processed/summary_by_city.json \
  data/processed/qa_report.json \
  data/processed/outputs_schema_report.json \
  data/raw/tdx/stops.meta.json \
  data/raw/tdx/ingestion_status.json \
  2>/dev/null || true

echo "Wrote: $out"

if [[ "$KEEP_N" =~ ^[0-9]+$ ]] && [[ "$KEEP_N" -gt 0 ]]; then
  ls -1t "$BACKUP_DIR"/libraryreach-backup-*.tar.gz 2>/dev/null | tail -n +"$((KEEP_N + 1))" | xargs -r rm -f
fi
