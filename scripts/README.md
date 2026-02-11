# Scripts

## Structure

- `scripts/collection/`: data pull and ingest jobs.
- `scripts/queries/`: query runners and SQL files.
- `scripts/exports/`: export jobs (xlsx, etc.).
- `scripts/schedule/`: cron templates and install helpers.

## Common Commands

- Weekly ingest to DuckDB:
  - `python3 scripts/collection/weekly_goatcounter_to_duckdb.py`
- Legacy raw pull to parquet:
  - `python3 scripts/collection/goatcounter_hits_to_parquet.py`
- Query page views:
  - `python3 scripts/queries/query_hits.py`
- Query click events:
  - `python3 scripts/queries/query_click_events.py`
- Export workbook:
  - `python3 scripts/exports/export_duckdb_schema_to_xlsx.py`

## Schedule

- Install cron entry from template:
  - `scripts/schedule/install_weekly_cron.sh`
