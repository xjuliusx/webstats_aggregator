#!/usr/bin/env python3
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "output" / "webstats.duckdb"
SQL_PATH = PROJECT_ROOT / "scripts" / "queries" / "sql" / "query_hits.sql"


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"Database not found: {DB_PATH}")
    if not SQL_PATH.exists():
        raise SystemExit(f"SQL file not found: {SQL_PATH}")

    con = duckdb.connect(str(DB_PATH), read_only=True)
    rows = con.execute(SQL_PATH.read_text()).fetchall()
    con.close()

    if not rows:
        print("No rows returned.")
        return

    print("path\tviews")
    for path, views in rows:
        print(f"{path}\t{views}")


if __name__ == "__main__":
    main()
