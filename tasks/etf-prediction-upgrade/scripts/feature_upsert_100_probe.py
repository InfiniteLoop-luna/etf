from __future__ import annotations

import json
import time

import pandas as pd

from src.ml_stock_dataset import (
    build_feature_frame,
    get_engine,
    load_feature_source_df,
    load_feature_target_df,
    upsert_feature_rows,
)

engine = get_engine()
start_date = "2026-05-08"
end_date = "2026-05-08"
history_buffer_days = 70

result = {}

t0 = time.time()
target_df = load_feature_target_df(engine, start_date=start_date, end_date=end_date)
source_df = load_feature_source_df(engine, start_date=start_date, end_date=end_date, history_buffer_days=history_buffer_days)
feature_df = build_feature_frame(source_df)
target_keys = target_df.copy()
target_keys["trade_date"] = pd.to_datetime(target_keys["trade_date"], errors="coerce")
filtered = feature_df.merge(target_keys, on=["trade_date", "ts_code"], how="inner")
filtered = filtered.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)
rows = filtered.head(100).to_dict(orient="records")
result["prep_seconds"] = round(time.time() - t0, 3)
result["sample_rows"] = len(rows)

t1 = time.time()
written = upsert_feature_rows(engine, rows)
result["upsert_seconds"] = round(time.time() - t1, 3)
result["written"] = int(written)

print(json.dumps(result, ensure_ascii=False, indent=2))
