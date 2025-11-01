#!/usr/bin/env python
from pathlib import Path
import numpy as np, pandas as pd, joblib
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split

RANDOM_STATE = 42
def make_synth(n=4000, seed=RANDOM_STATE):
    rng = np.random.default_rng(seed)
    df = pd.DataFrame({
        "age": rng.integers(18, 70, n),
        "balance": rng.normal(5000, 2000, n).round(2),
        "segment": rng.choice(["A","B","C"], n, p=[0.5,0.3,0.2]),
        "region": rng.choice(["NA","EU","APAC"], n, p=[0.5,0.3,0.2]),
    })
    p = 1/(1 + np.exp(-(-3 + 0.001*df["balance"] + 0.05*(df["age"]>40).astype(int))))
    y = (rng.random(n) < p).astype(int)
    return df, y

def build_pipeline(num_cols, cat_cols):
    pre = ColumnTransformer([
        ("num", Pipeline([("impute", SimpleImputer(strategy="median")), ("scale", StandardScaler())]), num_cols),
        ("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols),
    ])
    return Pipeline([("pre", pre), ("clf", LogisticRegression(max_iter=300, random_state=RANDOM_STATE))])

def main():
    Path("artifacts").mkdir(exist_ok=True)
    df, y = make_synth()
    num_cols, cat_cols = ["age","balance"], ["segment","region"]
    pipe = build_pipeline(num_cols, cat_cols)
    Xtr, Xte, ytr, yte = train_test_split(df, y, test_size=0.25, stratify=y, random_state=RANDOM_STATE)
    pipe.fit(Xtr, ytr)
    from sklearn.metrics import roc_auc_score
    auc = roc_auc_score(yte, pipe.predict_proba(Xte)[:,1])
    print(f"Holdout ROC AUC: {auc:.3f}")
    joblib.dump(pipe, "artifacts/model.joblib")
    print("✅ wrote artifacts/model.joblib")
if __name__ == "__main__":
    main()
