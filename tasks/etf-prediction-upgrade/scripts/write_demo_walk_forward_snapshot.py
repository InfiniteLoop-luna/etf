#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    )
)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.ml_stock_train_v1 import (
    DEFAULT_REGRESSION_TARGET,
    prepare_training_data,
    run_walk_forward_evaluation,
)

OUTPUT_PATH = os.path.join(
    PROJECT_ROOT,
    "tasks",
    "etf-prediction-upgrade",
    "outputs",
    "ml_prediction_upgrade_walk_forward_snapshot.json",
)


def build_demo_sample_df() -> pd.DataFrame:
    rows = []
    for day_idx, trade_date in enumerate(pd.date_range("2026-01-05", periods=8, freq="D")):
        for stock_idx in range(4):
            signal = day_idx * 0.55 + stock_idx * 0.35
            forward_return = (
                -0.8
                + day_idx * 0.7
                + stock_idx * 0.4
                - (1.8 if (day_idx + stock_idx) % 4 == 0 else 0.0)
            ) / 100.0
            rows.append(
                {
                    "trade_date": trade_date,
                    "ts_code": f"{600000 + day_idx * 10 + stock_idx}.SH",
                    "listing_days": 120 + day_idx * 5 + stock_idx,
                    "is_current_st": 0,
                    "has_ever_st": int(stock_idx == 3 and day_idx % 3 == 0),
                    "close": 10 + signal * 2.5,
                    "ret_1d": -0.3 + signal * 0.12,
                    "ret_3d": -0.1 + signal * 0.18,
                    "ret_5d": signal * 0.22,
                    "ret_10d": signal * 0.27,
                    "ret_20d": signal * 0.33,
                    "close_over_ma5": 0.97 + signal * 0.03,
                    "close_over_ma20": 0.95 + signal * 0.035,
                    "ma5_over_ma20": 0.93 + signal * 0.04,
                    "w_ema5_over_30": 0.94 + signal * 0.035,
                    "feature_complete_ratio": 0.99 - stock_idx * 0.01,
                    "y_up_5d": int(forward_return > 0.003),
                    "ret_fwd_5d": forward_return,
                }
            )
    return pd.DataFrame(rows)


def build_sample_overview(sample_df: pd.DataFrame) -> dict:
    trade_dates = pd.to_datetime(sample_df["trade_date"], errors="coerce")
    return {
        "row_count": int(len(sample_df)),
        "day_count": int(trade_dates.nunique()),
        "symbol_count": int(sample_df["ts_code"].nunique()),
        "date_start": trade_dates.min().strftime("%Y-%m-%d"),
        "date_end": trade_dates.max().strftime("%Y-%m-%d"),
    }


def build_snapshot_section(*, sample_df: pd.DataFrame, task_type: str, model_kind: str, fill_method: str, classifier: str | None = None, regressor: str | None = None, target_column: str | None = None) -> dict:
    prepared = prepare_training_data(
        sample_df,
        task_type=task_type,
        target_column=target_column,
        fill_method=fill_method,
    )
    result = run_walk_forward_evaluation(
        prepared,
        model_kind=model_kind,
        fill_method=fill_method,
        classifier=classifier or "logistic",
        regressor=regressor or "ridge",
        min_train_rows=8,
        min_test_rows=8,
        max_windows=3,
    )
    summary = result.to_summary()
    return {
        "task_type": summary.get("task_type") or task_type,
        "model_kind": summary.get("model_kind") or model_kind,
        "target_column": prepared.target_column,
        "fill_method": fill_method,
        "classifier": classifier if task_type == "classification" else None,
        "regressor": regressor if task_type == "regression" else None,
        "rows_loaded": int(len(sample_df)),
        "sample_overview": build_sample_overview(sample_df),
        "prepared": prepared.to_summary(),
        "aggregate": summary.get("aggregate") or {},
        "window_results": summary.get("window_results") or [],
        "skipped_windows": summary.get("skipped_windows") or [],
    }


def main() -> None:
    sample_df = build_demo_sample_df()
    classification = build_snapshot_section(
        sample_df=sample_df,
        task_type="classification",
        model_kind="baseline",
        fill_method="median",
        classifier="logistic",
    )
    regression = build_snapshot_section(
        sample_df=sample_df,
        task_type="regression",
        model_kind="sklearn",
        fill_method="median",
        regressor="linear",
        target_column=DEFAULT_REGRESSION_TARGET,
    )

    payload = {
        "snapshot_type": "ml_stock_walk_forward",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "data_source": "demo_snapshot",
        "sample_overview": build_sample_overview(sample_df),
        "classification": classification,
        "regression": regression,
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
        f.write("\n")

    print(OUTPUT_PATH)


if __name__ == "__main__":
    main()
