# Data/ML Engineering Mini-Series

A single repo hosting runnable demos and articles for a practical data/ML engineering series:

- **Polars migration** → `posts/polars-migration/`
- **sklearn pipelines to production-ish** → `posts/sklearn-pipelines/`
- **DuckDB over Parquet** → `posts/duckdb-over-parquet/`

Each folder contains:

- A Blogger-ready HTML article
- A small demo with a smoke-testable CLI (used by CI)

## Quickstart

```bash
git clone <your-repo-url>
cd data-ml-engineering-series
python -m venv .venv && source .venv/bin/activate
pip install -r posts/polars-migration/demo/requirements.txt
```

See each post's README for details.
