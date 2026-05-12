from __future__ import annotations

import json
import sys
import time

import pandas as pd

from src.ml_stock_dataset import (
    build_feature_frame,
    get_engine,
    load_feature_source_df,
    load_feature_target_df,
    upsert_feature_rows,
)


def stamp(name: str, **extra):
    payload = {"stage": name, **extra}
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def main() -> int:
    engine = get_engine()
    start_date = "2026-05-08"
    end_date = "2026-05-08"
    history_buffer_days = 70

    t0 = time.time()
    stamp("start", start_date=start_date, end_date=end_date, history_buffer_days=history_buffer_days)

    target_df = load_feature_target_df(engine, start_date=start_date, end_date=end_date)
    stamp("target_loaded", seconds=round(time.time() - t0, 3), rows=int(len(target_df)))

    t1 = time.time()
    source_df = load_feature_source_df(
        engine,
        start_date=start_date,
        end_date=end_date,
        history_buffer_days=history_buffer_days,
    )
    stamp("source_loaded", seconds=round(time.time() - t1, 3), rows=int(len(source_df)))

    t2 = time.time()
    feature_df = build_feature_frame(source_df)
    stamp("feature_built", seconds=round(time.time() - t2, 3), rows=int(len(feature_df)))

    target_keys = target_df.copy()
    target_keys["trade_date"] = pd.to_datetime(target_keys["trade_date"], errors="coerce")
    filtered = feature_df.merge(target_keys, on=["trade_date", "ts_code"], how="inner")
    filtered = filtered.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)
    stamp("feature_filtered", rows=int(len(filtered)))

    rows = filtered.to_dict(orient="records")
    stamp("upsert_begin", rows=int(len(rows)))
    t3 = time.time()
    written = upsert_feature_rows(engine, rows)
    stamp("upsert_done", seconds=round(time.time() - t3, 3), rows_written=int(written))
    stamp("done", total_seconds=round(time.time() - t0, 3))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
