#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    )
)
DOTENV_PATH = os.path.join(PROJECT_ROOT, ".env")

RUNTIME_OUTPUT_PATH = os.path.join(
    PROJECT_ROOT,
    "tasks",
    "etf-prediction-upgrade",
    "outputs",
    "runtime",
    "ml_prediction_upgrade_walk_forward_snapshot.json",
)

SUPPORTED_MODEL_KINDS = ("baseline", "sklearn")
SUPPORTED_CLASSIFIERS = ("logistic",)
SUPPORTED_REGRESSORS = ("ridge", "linear")
DEFAULT_REGRESSION_TARGET = "ret_fwd_5d"


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


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _write_json(path: str, payload: dict) -> None:
    output_path = os.path.abspath(path)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
        f.write("\n")


def _read_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _extract_snapshot_section(payload: dict, task_type: str) -> dict:
    return {
        "task_type": task_type,
        "model_kind": payload.get("model_kind"),
        "target_column": payload.get("target_column"),
        "fill_method": payload.get("fill_method"),
        "classifier": payload.get("classifier"),
        "regressor": payload.get("regressor"),
        "rows_loaded": payload.get("rows_loaded"),
        "sample_overview": payload.get("sample_overview") or {},
        "prepared": payload.get("prepared") or {},
        "aggregate": payload.get("aggregate") or {},
        "window_results": payload.get("window_results") or [],
        "skipped_windows": payload.get("skipped_windows") or [],
    }


def _run_train_snapshot(
    *,
    args: argparse.Namespace,
    task_type: str,
    model_kind: str,
    classifier: str | None,
    regressor: str | None,
    target_column: str | None,
    snapshot_out: str,
) -> dict:
    command = [
        sys.executable,
        os.path.join(PROJECT_ROOT, "scripts", "train_ml_stock_v1.py"),
        "--eval-mode",
        "walk-forward",
        "--task-type",
        task_type,
        "--model-kind",
        model_kind,
        "--fill-method",
        args.fill_method,
        "--min-train-rows",
        str(args.min_train_rows),
        "--min-test-rows",
        str(args.min_test_rows),
        "--max-windows",
        str(args.max_windows),
        "--snapshot-out",
        os.path.abspath(snapshot_out),
    ]

    if classifier:
        command.extend(["--classifier", classifier])
    if regressor:
        command.extend(["--regressor", regressor])
    if target_column:
        command.extend(["--target-column", target_column])
    if args.start_date:
        command.extend(["--start-date", args.start_date])
    if args.end_date:
        command.extend(["--end-date", args.end_date])
    if args.limit is not None:
        command.extend(["--limit", str(args.limit)])
    if args.include_ineligible:
        command.append("--include-ineligible")

    print(f"[snapshot] task={task_type} model_kind={model_kind} output={os.path.abspath(snapshot_out)}")
    result = subprocess.run(command, cwd=PROJECT_ROOT, check=False, env=os.environ.copy())
    if result.returncode != 0:
        raise SystemExit(result.returncode)
    return _read_json(snapshot_out)


def _build_payload(
    *,
    classification_payload: dict | None,
    regression_payload: dict | None,
) -> dict:
    sample_overview = {}
    for payload in (classification_payload, regression_payload):
        if payload and (payload.get("sample_overview") or {}):
            sample_overview = payload.get("sample_overview") or {}
            break

    merged = {
        "snapshot_type": "ml_stock_walk_forward",
        "generated_at": _utcnow_iso(),
        "data_source": "runtime_real_snapshot",
        "sample_overview": sample_overview,
    }
    if classification_payload:
        merged["classification"] = _extract_snapshot_section(classification_payload, "classification")
    if regression_payload:
        merged["regression"] = _extract_snapshot_section(regression_payload, "regression")
    return merged


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate real-data walk-forward snapshots for the ML prediction upgrade page."
    )
    parser.add_argument("--task-type", choices=("classification", "regression", "both"), default="both")
    parser.add_argument("--classification-model-kind", choices=SUPPORTED_MODEL_KINDS, default="sklearn")
    parser.add_argument("--regression-model-kind", choices=SUPPORTED_MODEL_KINDS, default="sklearn")
    parser.add_argument("--classifier", choices=SUPPORTED_CLASSIFIERS, default="logistic")
    parser.add_argument("--regressor", choices=SUPPORTED_REGRESSORS, default="ridge")
    parser.add_argument("--classification-target-column")
    parser.add_argument("--regression-target-column", default=DEFAULT_REGRESSION_TARGET)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--max-windows", type=int, default=5)
    parser.add_argument("--min-train-rows", type=int, default=200)
    parser.add_argument("--min-test-rows", type=int, default=50)
    parser.add_argument("--fill-method", choices=("none", "drop", "median"), default="median")
    parser.add_argument("--include-ineligible", action="store_true")
    parser.add_argument("--snapshot-out", default=RUNTIME_OUTPUT_PATH)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    _load_dotenv_if_present()

    if not (os.environ.get("ETF_PG_PASSWORD") or os.environ.get("PGPASSWORD")):
        raise SystemExit(
            f"error: missing PostgreSQL password. Set ETF_PG_PASSWORD or PGPASSWORD, or add it to {DOTENV_PATH}"
        )

    with tempfile.TemporaryDirectory(prefix="ml-prediction-runtime-") as temp_dir:
        classification_payload = None
        regression_payload = None

        if args.task_type in {"classification", "both"}:
            classification_payload = _run_train_snapshot(
                args=args,
                task_type="classification",
                model_kind=args.classification_model_kind,
                classifier=args.classifier,
                regressor=None,
                target_column=args.classification_target_column,
                snapshot_out=os.path.join(temp_dir, "classification_snapshot.json"),
            )

        if args.task_type in {"regression", "both"}:
            regression_payload = _run_train_snapshot(
                args=args,
                task_type="regression",
                model_kind=args.regression_model_kind,
                classifier=None,
                regressor=args.regressor,
                target_column=args.regression_target_column,
                snapshot_out=os.path.join(temp_dir, "regression_snapshot.json"),
            )

        merged_payload = _build_payload(
            classification_payload=classification_payload,
            regression_payload=regression_payload,
        )
        _write_json(args.snapshot_out, merged_payload)

    print(f"snapshot_merged_written {os.path.abspath(args.snapshot_out)}")


if __name__ == "__main__":
    main()
