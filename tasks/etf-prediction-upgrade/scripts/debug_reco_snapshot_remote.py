from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.ml_reco_candidate_scores import (
    _load_candidate_frame,
    _load_recent_training_frame,
    _resolve_effective_trade_date,
    collect_payload_candidate_codes,
)
from src.ml_stock_dataset import SAMPLE_VIEW, get_engine as get_ml_engine
from src.ml_stock_train_v1 import (
    DEFAULT_CLASSIFICATION_TARGET,
    DEFAULT_REGRESSION_TARGET,
    prepare_training_data,
)
from src.trend_reco_store import fetch_trend_reco_payload, get_engine as get_trend_engine

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LATEST_TREND_RECO_PATH = PROJECT_ROOT / "data" / "recommendations" / "latest_trend_recommendations.json"


def load_latest_file_payload() -> dict:
    if not LATEST_TREND_RECO_PATH.exists():
        return {}
    try:
        return json.loads(LATEST_TREND_RECO_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def main() -> None:
    trend_engine = get_trend_engine()
    payload = fetch_trend_reco_payload(trend_engine, trade_date=None) or load_latest_file_payload()
    trade_date = str(payload.get("trade_date") or "").strip()
    candidate_codes = collect_payload_candidate_codes(payload)

    print("payload_trade_date", trade_date)
    print("candidate_code_count", len(candidate_codes))
    print("candidate_codes_head", list(candidate_codes[:8]))

    ml_engine = get_ml_engine()
    with ml_engine.connect() as conn:
        sample_max_date = pd.read_sql(text(f"SELECT MAX(trade_date) AS trade_date FROM {SAMPLE_VIEW}"), conn)
        print("sample_max_trade_date", sample_max_date.iloc[0]["trade_date"])

    effective_trade_date = _resolve_effective_trade_date(ml_engine, trade_date, candidate_codes)
    print("effective_trade_date", effective_trade_date)

    for day in [trade_date, effective_trade_date, "2026-05-11", "2026-05-12"]:
        if not day:
            continue
        try:
            candidate_df = _load_candidate_frame(ml_engine, day, candidate_codes, max_candidates=30)
            print("candidate_rows", day, len(candidate_df))
        except Exception as exc:
            print("candidate_rows_error", day, repr(exc))

    if not effective_trade_date:
        return

    history_df = _load_recent_training_frame(
        ml_engine,
        effective_trade_date,
        lookback_days=60,
        min_train_rows=2000,
        recent_train_rows=6000,
    )
    print("history_rows", len(history_df))
    if history_df.empty:
        return

    print("history_date_min", history_df["trade_date"].min())
    print("history_date_max", history_df["trade_date"].max())
    print("history_symbol_count", history_df["ts_code"].astype(str).nunique())

    feature_columns = [
        column
        for column in V1_FEATURE_COLUMNS
        if column in history_df.columns
        and pd.to_numeric(history_df.get(column), errors='coerce').notna().any()
    ]
    print("feature_column_count", len(feature_columns))
    print("feature_columns_head", feature_columns[:12])

    cls_prepared = prepare_training_data(
        history_df,
        task_type="classification",
        target_column=DEFAULT_CLASSIFICATION_TARGET,
        feature_columns=feature_columns,
        fill_method="median",
    )
    reg_prepared = prepare_training_data(
        history_df,
        task_type="regression",
        target_column=DEFAULT_REGRESSION_TARGET,
        feature_columns=feature_columns,
        fill_method="median",
    )
    print("cls_rows_before", cls_prepared.row_count_before_filter)
    print("cls_rows_after", cls_prepared.row_count_after_filter)
    print("cls_positive", int(pd.to_numeric(cls_prepared.target, errors="coerce").fillna(0).sum()))
    print("reg_rows_before", reg_prepared.row_count_before_filter)
    print("reg_rows_after", reg_prepared.row_count_after_filter)


if __name__ == "__main__":
    main()
