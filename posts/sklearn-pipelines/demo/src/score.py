#!/usr/bin/env python
from pathlib import Path
import argparse, joblib, pandas as pd
REQUIRED = {"age":"number","balance":"number","segment":"string","region":"string"}

def validate(df: pd.DataFrame):
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing: raise ValueError(f"Missing columns: {missing}")
    import pandas.api.types as ptypes
    for c,k in REQUIRED.items():
        if k=="number" and not ptypes.is_numeric_dtype(df[c]): raise TypeError(f"{c} must be numeric")
        if k=="string" and not (ptypes.is_string_dtype(df[c]) or df[c].dtype=='object'): raise TypeError(f"{c} must be string-like")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="artifacts/model.joblib")
    ap.add_argument("--input", help="CSV or Parquet to score")
    ap.add_argument("--output", default="out/preds.csv")
    ap.add_argument("--threshold", type=float, default=0.5)
    args = ap.parse_args()

    pipe = joblib.load(args.model)
    if args.input:
        df = pd.read_parquet(args.input) if args.input.endswith(".parquet") else pd.read_csv(args.input)
    else:
        df = pd.DataFrame([{"age":45,"balance":8000,"segment":"A","region":"NA"},
                           {"age":29,"balance":3000,"segment":"C","region":"EU"}])
    validate(df)
    proba = pipe.predict_proba(df)[:,1]
    pred = (proba >= args.threshold).astype(int)
    Path("out").mkdir(exist_ok=True)
    pd.DataFrame({"proba": proba, "pred": pred}).to_csv(args.output, index=False)
    print(f"✅ wrote {args.output} with {len(proba)} rows")
if __name__ == "__main__":
    main()
