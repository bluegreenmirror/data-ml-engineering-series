
#!/usr/bin/env python
# Generate synthetic Parquet data: partitioned 'events' per day and a tiny 'users' dimension.
# Requires: pandas, pyarrow.
import numpy as np, pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import pyarrow as pa, pyarrow.parquet as pq

BASE = Path(__file__).resolve().parents[1]
EV_DIR = BASE / "data" / "events"
DIM_DIR = BASE / "data" / "dim"

def write_partition(df: pd.DataFrame, date_str: str, file_idx: int):
    part_dir = EV_DIR / f"date={date_str}"
    part_dir.mkdir(parents=True, exist_ok=True)
    path = part_dir / f"part-{file_idx:04d}.parquet"
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)

def main(start="2025-09-20", days=20, files_per_day=3, rows_per_file=5000, seed=7):
    np.random.seed(seed)
    EV_DIR.mkdir(parents=True, exist_ok=True)
    DIM_DIR.mkdir(parents=True, exist_ok=True)

    # Users dimension
    n_users = 20000
    user_ids = np.arange(1, n_users+1)
    tiers = np.random.choice(["free","pro","enterprise"], size=n_users, p=[0.7,0.25,0.05])
    regions = np.random.choice(["NA","EU","APAC","LATAM"], size=n_users, p=[0.5,0.2,0.25,0.05])
    users = pd.DataFrame({"user_id": user_ids, "tier": tiers, "region": regions})
    pq.write_table(pa.Table.from_pandas(users, preserve_index=False), DIM_DIR/"users.parquet")

    # Events fact
    start_dt = datetime.fromisoformat(start)
    countries = ["US","CA","GB","IN"]
    for d in range(days):
        day = (start_dt + timedelta(days=d)).date()
        date_str = str(day)
        for i in range(files_per_day):
            n = rows_per_file
            df = pd.DataFrame({
                "user_id": np.random.choice(user_ids, size=n, replace=True),
                "event_ts": pd.to_datetime(np.random.randint(
                    int(pd.Timestamp(day).value),
                    int(pd.Timestamp(day + timedelta(days=1)).value),
                    size=n)).tz_localize("UTC"),
                "country": np.random.choice(countries, size=n, p=[0.55,0.15,0.15,0.15]),
                "revenue": np.random.gamma(shape=2.0, scale=10.0, size=n).round(2),
                "cost":    np.random.gamma(shape=2.0, scale=5.0,  size=n).round(2),
            })
            write_partition(df, date_str, i)

    print(f"✅ Wrote users dim and {days*files_per_day} event files under {EV_DIR}")

if __name__ == "__main__":
    main()
