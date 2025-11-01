
# DuckDB over Parquet for pandas users: SQL speed without a cluster

Self-contained folder to demo DuckDB querying partitioned Parquet with pushdown,
plus a pandas baseline and Polars handoff. Generate synthetic data locally, then run.

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate    # or conda env create
pip install -r requirements.txt

# 0) Generate partitioned data (Parquet) + users dim
python scripts/generate_data.py

# 1) Run DuckDB SQL pipeline → out/daily_fact.parquet
python scripts/run_query.py --start 2025-10-01 --end 2025-10-07

# 2) Compare with pandas baseline
python scripts/compare_vs_pandas.py --start 2025-10-01 --end 2025-10-07

# 3) Open the guided notebook
jupyter lab notebooks/01_duckdb_migration.ipynb
```
