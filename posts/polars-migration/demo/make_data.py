#!/usr/bin/env python
import pandas as pd
import numpy as np
from pathlib import Path
Path("data").mkdir(exist_ok=True)
n=1000
df = pd.DataFrame({
    "user_id": np.random.randint(1, 100, n),
    "num": np.random.randint(1, 100, n),
    "den": np.random.randint(1, 100, n),
    "country": np.random.choice(["US","CA","IN"], n, p=[0.6,0.2,0.2])
})
df.to_csv("data/events.csv", index=False)
print("✅ wrote data/events.csv", len(df))
