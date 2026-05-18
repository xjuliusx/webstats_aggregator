#!/usr/bin/env python3
import argparse
import io
import os
import smtplib
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
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

NOTIFY_EMAIL = os.getenv("NOTIFY_EMAIL", "juliuskim@gmail.com")
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")


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


SITE_LAUNCH_DATE = date(2026, 4, 15)


def build_trend_chart(con: duckdb.DuckDBPyConnection) -> bytes:
    df = con.execute(
        """
        SELECT date, SUM(hits) AS hits
        FROM goatcounter_daily_hits
        WHERE event = FALSE AND date >= ?
        GROUP BY date ORDER BY date
        """,
        [SITE_LAUNCH_DATE],
    ).df()
    df["date"] = pd.to_datetime(df["date"])

    fig, ax = plt.subplots(figsize=(10, 3))
    ax.fill_between(df["date"], df["hits"], alpha=0.2, color="#4A90D9")
    ax.plot(df["date"], df["hits"], color="#4A90D9", linewidth=1.5)
    ax.set_title("Total Daily Page Views", fontsize=13, pad=10)
    ax.set_ylabel("Hits")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate()
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150)
    plt.close(fig)
    return buf.getvalue()


def send_notification(subject: str, body: str, chart_png: bytes = None) -> None:
    if not SMTP_USER or not SMTP_PASSWORD:
        return
    if chart_png:
        msg = MIMEMultipart("related")
        msg.attach(MIMEText(body, "html"))
        img = MIMEImage(chart_png, "png")
        img.add_header("Content-ID", "<trend_chart>")
        img.add_header("Content-Disposition", "inline", filename="trend.png")
        msg.attach(img)
    else:
        msg = MIMEText(body, "html")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = NOTIFY_EMAIL
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(SMTP_USER, SMTP_PASSWORD)
        smtp.sendmail(SMTP_USER, NOTIFY_EMAIL, msg.as_string())


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

    try:
        start_date = compute_start_date(con)
        rows = fetch_hits(start_date)
        df = flatten_hits(rows)
        upserted = upsert_daily_hits(con, df)

        total_rows = con.execute("SELECT COUNT(*) FROM goatcounter_daily_hits").fetchone()[0]
        max_date = con.execute("SELECT MAX(date) FROM goatcounter_daily_hits").fetchone()[0]

        def query_metrics(con, date_filter, params):
            summary = con.execute(
                f"""
                SELECT SUM(hits), COUNT(DISTINCT url)
                FROM goatcounter_daily_hits
                WHERE {date_filter} AND event = FALSE
                """,
                params,
            ).fetchone()
            pages = con.execute(
                f"""
                SELECT url, SUM(hits) AS hits
                FROM goatcounter_daily_hits
                WHERE {date_filter} AND event = FALSE
                GROUP BY url ORDER BY hits DESC
                """,
                params,
            ).fetchall()
            clicks = con.execute(
                f"""
                SELECT
                  CASE WHEN url LIKE 'click-%' THEN SUBSTR(url, 7) ELSE url END AS label,
                  SUM(hits) AS hits
                FROM goatcounter_daily_hits
                WHERE {date_filter} AND event = TRUE
                GROUP BY label ORDER BY hits DESC
                """,
                params,
            ).fetchall()
            return summary, pages, clicks

        # Previous full week (Mon–Sun)
        today = datetime.now(timezone.utc).date()
        this_week_mon = today - timedelta(days=today.weekday())
        last_week_mon = this_week_mon - timedelta(days=7)
        last_week_sun = this_week_mon - timedelta(days=1)

        last_week, last_week_pages, last_week_clicks = query_metrics(
            con, "date >= ? AND date <= ?", [last_week_mon, last_week_sun]
        )

        # Current month
        month_start = today.replace(day=1)
        month, month_pages, month_clicks = query_metrics(
            con, "date >= ?", [month_start]
        )

        # All time (from site launch)
        alltime_row = con.execute(
            "SELECT SUM(hits), COUNT(DISTINCT url), MIN(date), MAX(date) FROM goatcounter_daily_hits WHERE event = FALSE AND date >= ?",
            [SITE_LAUNCH_DATE],
        ).fetchone()
        _, alltime_pages, alltime_clicks = query_metrics(con, "date >= ?", [SITE_LAUNCH_DATE])

        chart_b64 = build_trend_chart(con)

        log_run(
            con,
            this_week_start,
            "success",
            f"payload_rows={len(rows)}, upserted_rows={upserted}, latest_date={max_date}",
        )
        con.close()

        def html_table(page_rows):
            if not page_rows:
                return "<p><em>(none)</em></p>"
            rows_html = "".join(
                f"<tr><td>{url}</td><td style='text-align:right'>{hits:,}</td></tr>"
                for url, hits in page_rows
            )
            return (
                "<table style='border-collapse:collapse;font-family:monospace;font-size:13px'>"
                "<thead><tr>"
                "<th style='text-align:left;padding:4px 16px 4px 0;border-bottom:2px solid #ccc'>Page</th>"
                "<th style='text-align:right;padding:4px 0;border-bottom:2px solid #ccc'>Hits</th>"
                "</tr></thead>"
                f"<tbody>{rows_html}</tbody>"
                "</table>"
            )

        def section(title, summary, page_rows, click_rows):
            return (
                f"<h3 style='margin-bottom:4px'>{title}</h3>"
                f"<p style='margin:0 0 8px'>Page views: <strong>{summary[0] or 0:,}</strong> &nbsp;|&nbsp; "
                f"Unique pages: <strong>{summary[1] or 0:,}</strong></p>"
                f"<p style='margin:4px 0 4px'><strong>Page Views</strong></p>"
                f"{html_table(page_rows)}"
                f"<p style='margin:12px 0 4px'><strong>Click Navigations</strong></p>"
                f"{html_table(click_rows)}"
            )

        subject = f"webstats ingest: {start_date.isoformat()} to {max_date}"
        body = f"""
        <html><body style='font-family:sans-serif;font-size:14px;color:#222'>
        <p><strong>Records imported: {upserted:,}</strong></p>
        {section(f"Last Week ({last_week_mon.strftime('%b %d')} – {last_week_sun.strftime('%b %d')})", last_week, last_week_pages, last_week_clicks)}
        <br>
        {section(f"This Month ({month_start.strftime('%B %Y')})", month, month_pages, month_clicks)}
        <br>
        {section(f"All Time (since {SITE_LAUNCH_DATE.strftime('%b %d, %Y')})", alltime_row, alltime_pages, alltime_clicks)}
        <br>
        <h3 style='margin-bottom:8px'>Usage Trend</h3>
        <img src="cid:trend_chart" style="max-width:100%;border:1px solid #eee;border-radius:4px">
        </body></html>
        """

        print(f"Start date: {start_date.isoformat()}")
        print(f"Rows from GoatCounter payload: {len(rows)}")
        print(f"Rows upserted into goatcounter_daily_hits: {upserted}")
        print(f"Total rows in goatcounter_daily_hits: {total_rows}")
        print(f"Latest date in table: {max_date}")
        send_notification(subject=subject, body=body, chart_png=chart_b64)
    except Exception as exc:
        log_run(con, this_week_start, "failure", str(exc))
        con.close()
        send_notification(subject=f"webstats ingest: FAILED {datetime.now(timezone.utc).date()}", body=f"<pre>{exc}</pre>")
        raise


if __name__ == "__main__":
    main()
