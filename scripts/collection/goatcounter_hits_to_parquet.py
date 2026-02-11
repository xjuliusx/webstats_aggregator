import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "output"

# ---- config ----
SITE = "xjuliusx.goatcounter.com"              # your GoatCounter site host
TOKEN = "1m38v65r60fmogmmvydita4sprsaq2j21slo1dbej9f795l0e"     # API token
OUT = OUTPUT_DIR / "goatcounter_hits.parquet"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# pull last N days only (idempotent-friendly)
DAYS = 2

# GoatCounter API endpoint
URL = f"https://{SITE}/api/v0/stats/hits"

since = (datetime.now(timezone.utc) - timedelta(days=DAYS)).strftime("%Y-%m-%d")

headers = {
    "Authorization": f"Bearer {TOKEN}"
}

params = {
    "start": since,
    "limit": 10000
}

rows = []
r = requests.get(URL, headers=headers, params=params, timeout=30)
r.raise_for_status()
data = r.json()
rows = data.get("hits", [])

if not rows:
    print("No new rows")
    exit(0)

df_new = pd.DataFrame(rows)

# Normalize timestamp
if "created_at" in df_new.columns:
    df_new["created_at"] = pd.to_datetime(df_new["created_at"], utc=True)

# Append to local store
if OUT.exists():
    df_old = pd.read_parquet(OUT)
    df_all = pd.concat([df_old, df_new], ignore_index=True)

    # GoatCounter /stats/hits rows are grouped by path and do not include hit IDs.
    if "path_id" in df_all.columns:
        df_all = df_all.drop_duplicates(subset=["path_id"], keep="last")
    elif "path" in df_all.columns:
        dedupe_cols = ["path"]
        if "event" in df_all.columns:
            dedupe_cols.append("event")
        df_all = df_all.drop_duplicates(subset=dedupe_cols, keep="last")
else:
    df_all = df_new

df_all.to_parquet(OUT, index=False)

print(f"Saved {len(df_new)} new rows. Total={len(df_all)}")
