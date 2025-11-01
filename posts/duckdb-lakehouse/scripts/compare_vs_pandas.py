
#!/usr/bin/env python
import argparse, pandas as pd
from pathlib import Path
from glob import glob
import time

BASE = Path(__file__).resolve().parents[1]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2025-10-01")
    ap.add_argument("--end",   default="2025-10-07")
    ap.add_argument("--out",   default=str(BASE/"out"/"daily_fact_pandas.parquet"))
    args = ap.parse_args()

    t0 = time.time()
    parts = sorted(glob(str(BASE / "data" / "events" / "date=*/part-*.parquet")))
    parts = [p for p in parts if (Path(p).parent.name >= f"date={args.start}" and Path(p).parent.name <= f"date={args.end}")]
    df = pd.read_parquet(parts, engine="pyarrow", columns=["user_id","event_ts","country","revenue","cost"])
    df = df[df["country"] == "US"]
    df["profit"] = df["revenue"] - df["cost"]
    df["hour"] = df["event_ts"].dt.floor("H")
    users = pd.read_parquet(BASE / "data" / "dim" / "users.parquet", engine="pyarrow", columns=["user_id","tier","region"])
    out = (df.merge(users, on="user_id", how="left")
             .groupby(["hour","tier"], as_index=False)
             .agg(events=("user_id","size"), profit_sum=("profit","sum"), profit_avg=("profit","mean"))
             .sort_values(["hour","tier"]))
    out.to_parquet(args.out, index=False)
    print(f"✅ pandas baseline wrote {args.out} with {len(out)} rows in {time.time()-t0:.2f}s")

if __name__ == "__main__":
    main()
