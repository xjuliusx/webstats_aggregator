#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CRON_FILE="$ROOT_DIR/scripts/schedule/weekly_ingest.cron"

if [[ ! -f "$CRON_FILE" ]]; then
  echo "Missing cron template: $CRON_FILE" >&2
  exit 1
fi

ENTRY="$(cat "$CRON_FILE")"
TMP="$(mktemp)"

crontab -l 2>/dev/null | grep -v "weekly_goatcounter_to_duckdb.py" > "$TMP" || true
echo "$ENTRY" >> "$TMP"
crontab "$TMP"
rm -f "$TMP"

echo "Installed cron entry:"
crontab -l | grep "weekly_goatcounter_to_duckdb.py"
