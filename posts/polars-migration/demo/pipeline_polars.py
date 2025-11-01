#!/usr/bin/env python
"""Tiny Polars pipeline used by CI as a smoke test."""
import polars as pl
from pathlib import Path

def main():
    Path("out").mkdir(exist_ok=True)
    df = pl.read_csv("data/events.csv")
    out = (df.with_columns((pl.col("num")/pl.col("den")).alias("rate"))
             .filter(pl.col("country")=="US")
             .group_by("user_id")
             .agg(pl.col("rate").mean().alias("avg_rate"))
             .sort("user_id"))
    out.write_parquet("out/by_user.parquet")
    print("✅ wrote out/by_user.parquet", out.height)

if __name__ == "__main__":
    main()
