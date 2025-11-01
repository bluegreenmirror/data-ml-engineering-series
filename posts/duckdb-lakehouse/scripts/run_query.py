
#!/usr/bin/env python
import argparse, duckdb
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2025-10-01")
    ap.add_argument("--end",   default="2025-10-07")
    ap.add_argument("--out",   default=str(BASE/"out"/"daily_fact.parquet"))
    args = ap.parse_args()

    con = duckdb.connect()
    con.execute((BASE/"sql"/"00_bootstrap.sql").read_text())
    q = (BASE/"sql"/"10_daily_fact.sql").read_text().replace(":start_date", args.start).replace(":end_date", args.end)

    print("---- EXPLAIN ----")
    print(con.execute("EXPLAIN " + q).fetchdf().iloc[0,0][:800], "...")
    rel = con.sql(q)
    out = Path(args.out); out.parent.mkdir(parents=True, exist_ok=True)
    rel.write_parquet(str(out), compression="zstd")
    print(f"✅ Wrote {out} with {rel.count()} rows")

if __name__ == "__main__":
    main()
