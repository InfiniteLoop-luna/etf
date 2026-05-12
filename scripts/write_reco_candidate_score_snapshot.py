#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.ml_reco_candidate_scores import (
    DEFAULT_RUNTIME_SNAPSHOT_PATH,
    build_candidate_score_snapshot,
    collect_payload_candidate_codes,
    write_candidate_score_snapshot_bundle,
)
from src.trend_reco_store import fetch_trend_reco_payload, get_engine

DOTENV_PATH = os.path.join(PROJECT_ROOT, ".env")
LATEST_TREND_RECO_PATH = os.path.join(PROJECT_ROOT, "data", "recommendations", "latest_trend_recommendations.json")


def _load_dotenv_if_present(dotenv_path: str = DOTENV_PATH) -> bool:
    if not os.path.exists(dotenv_path):
        return False

    loaded_any = False
    with open(dotenv_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if not key or key in os.environ:
                continue
            os.environ[key] = value
            loaded_any = True
    return loaded_any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate offline ML candidate-score snapshot for trend recommendation pages.")
    parser.add_argument("--trade-date", default="", help="Optional trade date, defaults to latest trend_reco payload in DB")
    parser.add_argument("--output", default=DEFAULT_RUNTIME_SNAPSHOT_PATH, help="Snapshot JSON output path")
    parser.add_argument("--lookback-days", type=int, default=120)
    parser.add_argument("--min-train-rows", type=int, default=5000)
    parser.add_argument("--max-candidates", type=int, default=200)
    parser.add_argument("--recent-train-rows", type=int, default=12000)
    parser.add_argument("--classification-model-kind", default="sklearn")
    parser.add_argument("--regression-model-kind", default="sklearn")
    parser.add_argument("--classifier", default="logistic")
    parser.add_argument("--regressor", default="ridge")
    return parser.parse_args()


def _load_latest_payload_from_file(file_path: str = LATEST_TREND_RECO_PATH) -> dict:
    path = Path(file_path)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def main() -> None:
    args = parse_args()
    _load_dotenv_if_present()

    engine = get_engine()
    payload = fetch_trend_reco_payload(engine, trade_date=args.trade_date or None) or {}
    payload_source = "trend_reco_db"
    if not payload:
        payload = _load_latest_payload_from_file()
        payload_source = "trend_reco_latest_file"
    if not payload:
        raise SystemExit("error: no trend recommendation payload found")

    trade_date = str(payload.get("trade_date") or "").strip()
    if not trade_date:
        raise SystemExit("error: trend recommendation payload missing trade_date")

    candidate_codes = collect_payload_candidate_codes(payload)
    snapshot = build_candidate_score_snapshot(
        trade_date,
        candidate_codes=candidate_codes,
        source=payload_source,
        lookback_days=args.lookback_days,
        min_train_rows=args.min_train_rows,
        max_candidates=args.max_candidates,
        recent_train_rows=args.recent_train_rows,
        classification_model_kind=args.classification_model_kind,
        regression_model_kind=args.regression_model_kind,
        classifier=args.classifier,
        regressor=args.regressor,
    )
    output_info = write_candidate_score_snapshot_bundle(snapshot, latest_output_path=args.output)

    print("[OK] ml reco candidate score snapshot generated")
    print("trade_date=", snapshot.get("trade_date"))
    print("candidate_codes=", len(snapshot.get("candidate_codes") or []))
    print("row_count=", snapshot.get("row_count"))
    print("latest_output=", output_info.get("latest_path"))
    print("archive_output=", output_info.get("archive_path"))
    print("mirror_latest_output=", output_info.get("mirror_latest_path"))


if __name__ == "__main__":
    main()
