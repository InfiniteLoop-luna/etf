from __future__ import annotations

import json

from src.ml_stock_dataset import get_engine, load_sample_dataset
from src.ml_stock_train_v1 import prepare_training_data, split_by_date

engine = get_engine()
sample_df = load_sample_dataset(
    engine,
    start_date="2026-02-27",
    end_date="2026-05-08",
    only_eligible=True,
)
prepared = prepare_training_data(
    sample_df,
    task_type="classification",
    target_column="y_up_5d",
    feature_columns=None,
    fill_method="none",
)
train_split, test_split = split_by_date(prepared, cutoff_date="2026-05-01")

summary = {
    "rows_loaded": int(len(sample_df)),
    "sample_min_trade_date": str(sample_df["trade_date"].min()) if not sample_df.empty else None,
    "sample_max_trade_date": str(sample_df["trade_date"].max()) if not sample_df.empty else None,
    "prepared_row_count": int(len(prepared.rows)),
    "prepared_min_trade_date": str(prepared.rows["trade_date"].min()) if not prepared.rows.empty else None,
    "prepared_max_trade_date": str(prepared.rows["trade_date"].max()) if not prepared.rows.empty else None,
    "target_column": prepared.target_column,
    "train_rows": int(len(train_split.rows)),
    "test_rows": int(len(test_split.rows)),
    "train_min_trade_date": str(train_split.rows["trade_date"].min()) if not train_split.rows.empty else None,
    "train_max_trade_date": str(train_split.rows["trade_date"].max()) if not train_split.rows.empty else None,
    "test_min_trade_date": str(test_split.rows["trade_date"].min()) if not test_split.rows.empty else None,
    "test_max_trade_date": str(test_split.rows["trade_date"].max()) if not test_split.rows.empty else None,
}
print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
