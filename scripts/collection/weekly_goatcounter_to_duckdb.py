#!/usr/bin/env python3
import argparse
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd
import requests


PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "output"
DB_PATH = OUTPUT_DIR / "webstats.duckdb"

# Prefer env vars; fall back to current local defaults.
SITE = os.getenv("GOATCOUNTER_SITE", "xjuliusx.goatcounter.com")
TOKEN = os.getenv("GOATCOUNTER_TOKEN", "1m38v65r60fmogmmvydita4sprsaq2j21slo1dbej9f795l0e")

BOOTSTRAP_DAYS = 30
OVERLAP_DAYS = 7
URL = f"https://{SITE}/api/v0/stats/hits"
JOB_NAME = "weekly_goatcounter_to_duckdb"


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS goatcounter_daily_hits (
          date DATE NOT NULL,
          url VARCHAR NOT NULL,
          hits BIGINT NOT NULL,
          event BOOLEAN,
          path_id BIGINT,
          title VARCHAR,
          loaded_at TIMESTAMP NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_run_log (
          job_name VARCHAR NOT NULL,
          week_start DATE NOT NULL,
          status VARCHAR NOT NULL,
          run_at TIMESTAMP NOT NULL,
          message VARCHAR
        )
        """
    )


def week_start_utc(d: date) -> date:
    # Monday as start of week.
    return d - timedelta(days=d.weekday())


def has_success_for_week(con: duckdb.DuckDBPyConnection, week_start: date) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM ingest_run_log
        WHERE job_name = ?
          AND week_start = ?
          AND status = 'success'
        """,
        [JOB_NAME, week_start],
    ).fetchone()
    return bool(row and row[0] > 0)


def log_run(
    con: duckdb.DuckDBPyConnection, week_start: date, status: str, message: str
) -> None:
    con.execute(
        """
        INSERT INTO ingest_run_log (job_name, week_start, status, run_at, message)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?)
        """,
        [JOB_NAME, week_start, status, message],
    )


def compute_start_date(con: duckdb.DuckDBPyConnection) -> date:
    max_date = con.execute("SELECT MAX(date) FROM goatcounter_daily_hits").fetchone()[0]
    today = datetime.now(timezone.utc).date()
    bootstrap_start = today - timedelta(days=BOOTSTRAP_DAYS)
    if max_date is None:
        return bootstrap_start
    return max(bootstrap_start, max_date - timedelta(days=OVERLAP_DAYS))


def fetch_hits(start_date: date) -> list[dict]:
    headers = {"Authorization": f"Bearer {TOKEN}"}
    params = {"start": start_date.isoformat(), "limit": 10000}
    response = requests.get(URL, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response.json().get("hits", [])


def flatten_hits(rows: list[dict]) -> pd.DataFrame:
    flat_rows: list[dict] = []
    for row in rows:
        stats = row.get("stats") or []
        for stat in stats:
            day = stat.get("day")
            daily = stat.get("daily")
            if day is None or daily is None:
                continue
            hits = int(daily)
            if hits <= 0:
                continue
            flat_rows.append(
                {
                    "date": day,
                    "url": row.get("path"),
                    "hits": hits,
                    "event": row.get("event"),
                    "path_id": row.get("path_id"),
                    "title": row.get("title"),
                }
            )

    if not flat_rows:
        return pd.DataFrame(columns=["date", "url", "hits", "event", "path_id", "title"])

    df = pd.DataFrame(flat_rows)
    # Collapse any accidental duplicates from payload into one row per date/url/event.
    df = (
        df.groupby(["date", "url", "event"], dropna=False, as_index=False)
        .agg({"hits": "sum", "path_id": "max", "title": "last"})
    )
    return df


def upsert_daily_hits(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    con.register("staging_df", df)
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE goatcounter_daily_hits_stage AS
        SELECT
          CAST(date AS DATE) AS date,
          CAST(url AS VARCHAR) AS url,
          CAST(hits AS BIGINT) AS hits,
          CAST(event AS BOOLEAN) AS event,
          CAST(path_id AS BIGINT) AS path_id,
          CAST(title AS VARCHAR) AS title
        FROM staging_df
        """
    )
    con.execute(
        """
        DELETE FROM goatcounter_daily_hits t
        USING goatcounter_daily_hits_stage s
        WHERE t.date = s.date
          AND t.url = s.url
          AND COALESCE(t.event, FALSE) = COALESCE(s.event, FALSE)
        """
    )
    con.execute(
        """
        INSERT INTO goatcounter_daily_hits
          (date, url, hits, event, path_id, title, loaded_at)
        SELECT
          date, url, hits, event, path_id, title, CURRENT_TIMESTAMP
        FROM goatcounter_daily_hits_stage
        """
    )
    return len(df)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--once-per-week",
        action="store_true",
        help="Skip if a successful run already happened this week (UTC Monday-Sunday).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run regardless of weekly success marker.",
    )
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    ensure_schema(con)
    this_week_start = week_start_utc(datetime.now(timezone.utc).date())

    if args.once_per_week and not args.force and has_success_for_week(con, this_week_start):
        print(
            f"Skip: successful run already logged for week starting {this_week_start.isoformat()}."
        )
        con.close()
        return

    start_date = compute_start_date(con)
    rows = fetch_hits(start_date)
    df = flatten_hits(rows)
    upserted = upsert_daily_hits(con, df)

    total_rows = con.execute("SELECT COUNT(*) FROM goatcounter_daily_hits").fetchone()[0]
    max_date = con.execute("SELECT MAX(date) FROM goatcounter_daily_hits").fetchone()[0]
    log_run(
        con,
        this_week_start,
        "success",
        f"payload_rows={len(rows)}, upserted_rows={upserted}, latest_date={max_date}",
    )
    con.close()

    print(f"Start date: {start_date.isoformat()}")
    print(f"Rows from GoatCounter payload: {len(rows)}")
    print(f"Rows upserted into goatcounter_daily_hits: {upserted}")
    print(f"Total rows in goatcounter_daily_hits: {total_rows}")
    print(f"Latest date in table: {max_date}")


if __name__ == "__main__":
    main()
