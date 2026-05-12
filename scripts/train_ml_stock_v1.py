#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import sys

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.ml_stock_dataset import get_engine, load_sample_dataset
from src.ml_stock_train_v1 import (
    apply_feature_fill,
    compute_fill_values,
    evaluate_classification_predictions,
    evaluate_regression_predictions,
    fit_classification_model,
    fit_regression_model,
    prepare_training_data,
    run_walk_forward_evaluation,
    split_by_date,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the conservative training/evaluation scaffold for ml_stock_sample_daily."
    )
    parser.add_argument(
        "--eval-mode",
        choices=("single", "walk-forward"),
        default="single",
        help="Evaluation mode. Default: single",
    )
    parser.add_argument(
        "--model-kind",
        choices=("baseline", "sklearn"),
        default="baseline",
        help="Training backend. Default: baseline",
    )
    parser.add_argument(
        "--task-type",
        choices=("classification", "regression"),
        default="classification",
        help="Training task type. Default: classification",
    )
    parser.add_argument(
        "--target-column",
        help="Optional target column override, e.g. y_up_5d or ret_fwd_5d",
    )
    parser.add_argument(
        "--cutoff-date",
        help="Chronological split boundary for single mode. Train rows use trade_date < cutoff; eval rows use trade_date >= cutoff.",
    )
    parser.add_argument(
        "--cutoff-dates",
        help="Optional comma-separated explicit cutoff dates for walk-forward mode",
    )
    parser.add_argument("--start-date", help="Optional inclusive start date for sample loading")
    parser.add_argument("--end-date", help="Optional inclusive end date for sample loading")
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional LIMIT applied when reading ml_stock_sample_daily",
    )
    parser.add_argument(
        "--feature-columns",
        help="Optional comma-separated feature column override",
    )
    parser.add_argument(
        "--fill-method",
        choices=("none", "drop", "median"),
        default="median",
        help="Missing numeric feature handling. Default: median",
    )
    parser.add_argument(
        "--classifier",
        choices=("logistic",),
        default="logistic",
        help="Sklearn classifier choice when --model-kind=sklearn and --task-type=classification. Default: logistic",
    )
    parser.add_argument(
        "--regressor",
        choices=("ridge", "linear"),
        default="ridge",
        help="Sklearn regressor choice when --model-kind=sklearn and --task-type=regression. Default: ridge",
    )
    parser.add_argument(
        "--max-windows",
        type=int,
        help="Optional maximum number of walk-forward windows to evaluate",
    )
    parser.add_argument(
        "--min-train-rows",
        type=int,
        default=1,
        help="Minimum training rows required per walk-forward window. Default: 1",
    )
    parser.add_argument(
        "--min-test-rows",
        type=int,
        default=1,
        help="Minimum test rows required per walk-forward window. Default: 1",
    )
    parser.set_defaults(only_eligible=True)
    parser.add_argument(
        "--only-eligible",
        dest="only_eligible",
        action="store_true",
        help="Only load sample_eligible rows (default)",
    )
    parser.add_argument(
        "--include-ineligible",
        dest="only_eligible",
        action="store_false",
        help="Load both eligible and ineligible sample rows",
    )
    parser.add_argument(
        "--snapshot-out",
        help="Optional JSON output path for a walk-forward snapshot file",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON summary")
    return parser.parse_args()


def _parse_feature_columns(feature_columns_arg: str | None) -> list[str] | None:
    if not feature_columns_arg:
        return None
    columns = [column.strip() for column in feature_columns_arg.split(",")]
    return [column for column in columns if column]


def _parse_cutoff_dates(cutoff_dates_arg: str | None) -> list[str] | None:
    if not cutoff_dates_arg:
        return None
    values = [value.strip() for value in cutoff_dates_arg.split(",")]
    parsed = [value for value in values if value]
    return parsed or None


def _build_sample_overview(sample_df) -> dict:
    trade_dates = pd.to_datetime(sample_df.get("trade_date"), errors="coerce")
    has_trade_dates = getattr(trade_dates, "notna", lambda: pd.Series([], dtype=bool))().any()
    return {
        "row_count": int(len(sample_df)),
        "day_count": int(trade_dates.nunique()) if has_trade_dates else 0,
        "symbol_count": int(sample_df["ts_code"].nunique()) if "ts_code" in sample_df.columns else 0,
        "date_start": trade_dates.min().strftime("%Y-%m-%d") if has_trade_dates else None,
        "date_end": trade_dates.max().strftime("%Y-%m-%d") if has_trade_dates else None,
    }


def _build_walk_forward_snapshot(
    *,
    args: argparse.Namespace,
    sample_df,
    prepared,
    walk_forward_result,
) -> dict:
    walk_forward_summary = walk_forward_result.to_summary()
    return {
        "snapshot_type": "ml_stock_walk_forward",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "eval_mode": args.eval_mode,
        "task_type": walk_forward_summary.get("task_type") or args.task_type,
        "model_kind": walk_forward_summary.get("model_kind") or args.model_kind,
        "target_column": prepared.target_column,
        "fill_method": args.fill_method,
        "classifier": args.classifier if args.task_type == "classification" else None,
        "regressor": args.regressor if args.task_type == "regression" else None,
        "rows_loaded": int(len(sample_df)),
        "sample_overview": _build_sample_overview(sample_df),
        "prepared": prepared.to_summary(),
        "aggregate": walk_forward_summary.get("aggregate") or {},
        "window_results": walk_forward_summary.get("window_results") or [],
        "skipped_windows": walk_forward_summary.get("skipped_windows") or [],
    }



def _write_json_file(output_path: str, payload: dict) -> None:
    output_abspath = os.path.abspath(output_path)
    output_dir = os.path.dirname(output_abspath)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(output_abspath, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
        f.write("\n")


def _build_summary(
    *,
    args: argparse.Namespace,
    sample_df,
    prepared,
    train_split,
    test_split,
    train_ready,
    test_ready,
    model,
    train_metrics,
    test_metrics,
) -> dict:
    return {
        "eval_mode": args.eval_mode,
        "model_kind": args.model_kind,
        "task_type": args.task_type,
        "target_column": prepared.target_column,
        "cutoff_date": str(args.cutoff_date),
        "fill_method": args.fill_method,
        "classifier": args.classifier if args.task_type == "classification" else None,
        "regressor": args.regressor if args.task_type == "regression" else None,
        "rows_loaded": int(len(sample_df)),
        "prepared": prepared.to_summary(),
        "train_rows_before_fill": int(len(train_split.rows)),
        "test_rows_before_fill": int(len(test_split.rows)),
        "train_rows_after_fill": int(len(train_ready.rows)),
        "test_rows_after_fill": int(len(test_ready.rows)),
        "model": model.to_summary(),
        "train_metrics": train_metrics,
        "test_metrics": test_metrics,
    }


def _build_walk_forward_summary(
    *,
    args: argparse.Namespace,
    sample_df,
    prepared,
    walk_forward_result,
) -> dict:
    return {
        "eval_mode": args.eval_mode,
        "model_kind": args.model_kind,
        "task_type": args.task_type,
        "target_column": prepared.target_column,
        "fill_method": args.fill_method,
        "classifier": args.classifier if args.task_type == "classification" else None,
        "regressor": args.regressor if args.task_type == "regression" else None,
        "rows_loaded": int(len(sample_df)),
        "prepared": prepared.to_summary(),
        "walk_forward": walk_forward_result.to_summary(),
    }


def _print_text_summary(summary: dict) -> None:
    model = summary["model"]
    print(
        "eval_mode={eval_mode} task={task} model_kind={model_kind} target={target} cutoff={cutoff} features={feature_count} fill={fill}".format(
            eval_mode=summary["eval_mode"],
            task=summary["task_type"],
            model_kind=summary["model_kind"],
            target=summary["target_column"],
            cutoff=summary["cutoff_date"],
            feature_count=summary["prepared"]["feature_count"],
            fill=summary["fill_method"],
        )
    )
    print(
        "rows loaded={loaded} prepared={prepared} train={train_after}/{train_before} test={test_after}/{test_before}".format(
            loaded=summary["rows_loaded"],
            prepared=summary["prepared"]["row_count_after_filter"],
            train_after=summary["train_rows_after_fill"],
            train_before=summary["train_rows_before_fill"],
            test_after=summary["test_rows_after_fill"],
            test_before=summary["test_rows_before_fill"],
        )
    )
    print(
        "model selection={selection} estimator={estimator} strategy={strategy} score_feature={score_feature} threshold={threshold}".format(
            selection=model.get("classifier") or model.get("regressor") or model.get("model_kind"),
            estimator=model.get("estimator_name"),
            strategy=model.get("strategy"),
            score_feature=model.get("score_feature"),
            threshold=model.get("threshold"),
        )
    )
    print(f"train_metrics {json.dumps(summary['train_metrics'], ensure_ascii=False, sort_keys=True)}")
    print(f"test_metrics {json.dumps(summary['test_metrics'], ensure_ascii=False, sort_keys=True)}")


def _print_walk_forward_text_summary(summary: dict) -> None:
    walk_forward = summary["walk_forward"]
    aggregate = walk_forward["aggregate"]
    print(
        "eval_mode={eval_mode} task={task} model_kind={model_kind} target={target} windows={windows} rows_loaded={rows_loaded}".format(
            eval_mode=summary["eval_mode"],
            task=summary["task_type"],
            model_kind=summary["model_kind"],
            target=summary["target_column"],
            windows=aggregate.get("window_count"),
            rows_loaded=summary["rows_loaded"],
        )
    )
    print(f"aggregate {json.dumps(aggregate, ensure_ascii=False, sort_keys=True)}")
    for window in walk_forward["window_results"]:
        print(f"window {json.dumps(window, ensure_ascii=False, sort_keys=True)}")
    for skipped in walk_forward.get("skipped_windows", []):
        print(f"skipped {json.dumps(skipped, ensure_ascii=False, sort_keys=True)}")


def main() -> None:
    args = parse_args()
    feature_columns = _parse_feature_columns(args.feature_columns)
    cutoff_dates = _parse_cutoff_dates(args.cutoff_dates)

    try:
        engine = get_engine()
        sample_df = load_sample_dataset(
            engine,
            start_date=args.start_date,
            end_date=args.end_date,
            only_eligible=args.only_eligible,
            limit=args.limit,
        )

        prepared = prepare_training_data(
            sample_df,
            task_type=args.task_type,
            target_column=args.target_column,
            feature_columns=feature_columns,
            fill_method="none",
        )

        if args.eval_mode == "walk-forward":
            result = run_walk_forward_evaluation(
                prepared,
                model_kind=args.model_kind,
                fill_method=args.fill_method,
                classifier=args.classifier,
                regressor=args.regressor,
                min_train_rows=args.min_train_rows,
                min_test_rows=args.min_test_rows,
                max_windows=args.max_windows,
                cutoff_dates=cutoff_dates,
            )
            summary = _build_walk_forward_summary(
                args=args,
                sample_df=sample_df,
                prepared=prepared,
                walk_forward_result=result,
            )
            if args.snapshot_out:
                snapshot_payload = _build_walk_forward_snapshot(
                    args=args,
                    sample_df=sample_df,
                    prepared=prepared,
                    walk_forward_result=result,
                )
                _write_json_file(args.snapshot_out, snapshot_payload)
            if args.json:
                print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
                return
            _print_walk_forward_text_summary(summary)
            if args.snapshot_out:
                print(f"snapshot_written {os.path.abspath(args.snapshot_out)}")
            return

        if not args.cutoff_date:
            raise ValueError("--cutoff-date is required when --eval-mode=single")

        train_split, test_split = split_by_date(prepared, cutoff_date=args.cutoff_date)

        prepared_min_trade_date = (
            str(prepared.rows["trade_date"].min().date())
            if not prepared.rows.empty and "trade_date" in prepared.rows
            else None
        )
        prepared_max_trade_date = (
            str(prepared.rows["trade_date"].max().date())
            if not prepared.rows.empty and "trade_date" in prepared.rows
            else None
        )

        if train_split.rows.empty:
            raise ValueError(
                f"No training rows remain before the cutoff date; prepared date range is {prepared_min_trade_date} to {prepared_max_trade_date}"
            )
        if test_split.rows.empty:
            raise ValueError(
                f"No evaluation rows remain on or after the cutoff date; prepared date range is {prepared_min_trade_date} to {prepared_max_trade_date}"
            )

        fill_values = compute_fill_values(train_split.features, fill_method=args.fill_method)
        train_ready = apply_feature_fill(
            train_split,
            fill_method=args.fill_method,
            fill_values=fill_values,
        )
        test_ready = apply_feature_fill(
            test_split,
            fill_method=args.fill_method,
            fill_values=fill_values,
        )

        if train_ready.rows.empty:
            raise ValueError("No training rows remain after feature fill/filtering")
        if test_ready.rows.empty:
            raise ValueError("No evaluation rows remain after feature fill/filtering")

        if args.task_type == "classification":
            run = fit_classification_model(
                train_ready.features,
                train_ready.target,
                test_ready.features,
                model_kind=args.model_kind,
                classifier=args.classifier,
            )
            train_metrics = evaluate_classification_predictions(
                train_ready.target,
                run.train_predictions,
                y_score=run.train_scores,
            )
            test_metrics = evaluate_classification_predictions(
                test_ready.target,
                run.test_predictions,
                y_score=run.test_scores,
            )
        else:
            run = fit_regression_model(
                train_ready.features,
                train_ready.target,
                test_ready.features,
                model_kind=args.model_kind,
                regressor=args.regressor,
            )
            train_metrics = evaluate_regression_predictions(train_ready.target, run.train_predictions)
            test_metrics = evaluate_regression_predictions(test_ready.target, run.test_predictions)

        summary = _build_summary(
            args=args,
            sample_df=sample_df,
            prepared=prepared,
            train_split=train_split,
            test_split=test_split,
            train_ready=train_ready,
            test_ready=test_ready,
            model=run.model,
            train_metrics=train_metrics,
            test_metrics=test_metrics,
        )

        if args.json:
            print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
            return

        _print_text_summary(summary)
    except Exception as exc:
        raise SystemExit(f"error: {exc}") from exc


if __name__ == "__main__":
    main()
