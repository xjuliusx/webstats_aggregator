import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# ---- config ----
SITE = "yoursite"              # your GoatCounter site code
TOKEN = "gc_xxxxxxxxxxxxx"     # API token
OUT = Path("goatcounter_hits.parquet")

# pull last N days only (idempotent-friendly)
DAYS = 2

# GoatCounter API endpoint
URL = "https://goatcounter.com/api/v0/stats/hits"

since = (datetime.utcnow() - timedelta(days=DAYS)).strftime("%Y-%m-%d")

headers = {
    "Authorization": f"Bearer {TOKEN}"
}

params = {
    "site": SITE,
    "start": since,
    "limit": 10000
}

rows = []
page = 1

while True:
    params["page"] = page
    r = requests.get(URL, headers=headers, params=params, timeout=30)
    r.raise_for_status()

    data = r.json()
    batch = data.get("hits", [])

    if not batch:
        break

    rows.extend(batch)

    if len(batch) < params["limit"]:
        break

    page += 1

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

    # de-dup on hit id (important)
    if "id" in df_all.columns:
        df_all = df_all.drop_duplicates(subset=["id"])
else:
    df_all = df_new

df_all.to_parquet(OUT, index=False)

print(f"Saved {len(df_new)} new rows. Total={len(df_all)}")
