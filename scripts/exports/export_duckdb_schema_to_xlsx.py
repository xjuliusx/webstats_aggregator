#!/usr/bin/env python3
from pathlib import Path

import duckdb
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "output" / "webstats.duckdb"
OUT_PATH = PROJECT_ROOT / "output" / "duckdb_tables_columns.xlsx"


def safe_sheet_name(name: str) -> str:
    cleaned = "".join(ch for ch in name if ch not in r'[]:*?/\\')
    return (cleaned or "sheet")[:31]


def flatten_stats_by_day(df: pd.DataFrame) -> pd.DataFrame:
    if "stats" not in df.columns:
        return df

    out = df.copy().explode("stats", ignore_index=True)
    out["date"] = out["stats"].map(
        lambda value: value.get("day") if isinstance(value, dict) else None
    )
    out["hits"] = out["stats"].map(
        lambda value: value.get("daily") if isinstance(value, dict) else None
    )
    if "path" in out.columns:
        out = out.rename(columns={"path": "url"})

    out = out[["date", "url", "hits"]]
    out = out.dropna(subset=["date", "url", "hits"])
    out["hits"] = pd.to_numeric(out["hits"], errors="coerce").fillna(0).astype(int)
    out = out.groupby(["date", "url"], as_index=False)["hits"].sum()
    out = out[out["hits"] > 0]
    out = out.sort_values(["date", "url"], kind="stable").reset_index(drop=True)
    return out


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"Database not found: {DB_PATH}")

    con = duckdb.connect(str(DB_PATH), read_only=True)
    tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]

    if not tables:
        con.close()
        raise SystemExit("No tables found in database.")

    with pd.ExcelWriter(OUT_PATH, engine="openpyxl") as writer:
        used_names: set[str] = set()

        for table in tables:
            # Export full table data for each table tab.
            df = con.execute(f'SELECT * FROM "{table}"').df()
            df = flatten_stats_by_day(df)

            base = safe_sheet_name(table)
            sheet = base
            i = 1
            while sheet in used_names:
                suffix = f"_{i}"
                sheet = f"{base[:31-len(suffix)]}{suffix}"
                i += 1
            used_names.add(sheet)

            df.to_excel(writer, index=False, sheet_name=sheet)

    con.close()
    print(f"Wrote workbook: {OUT_PATH}")
    print(f"Tables exported: {len(tables)}")


if __name__ == "__main__":
    main()
