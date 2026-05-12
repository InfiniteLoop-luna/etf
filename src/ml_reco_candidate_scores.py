from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.ml_stock_dataset import (
    SAMPLE_VIEW,
    ensure_sample_view,
    ensure_storage_objects,
    get_engine as get_ml_stock_engine,
)
from src.ml_stock_train_v1 import (
    V1_FEATURE_COLUMNS,
    DEFAULT_CLASSIFICATION_TARGET,
    DEFAULT_REGRESSION_TARGET,
    apply_feature_fill,
    fit_classification_model,
    fit_regression_model,
    prepare_training_data,
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SNAPSHOT_TYPE = "ml_stock_reco_candidate_scores"
DEFAULT_LOOKBACK_DAYS = 120
DEFAULT_MIN_TRAIN_ROWS = 5000
DEFAULT_MAX_CANDIDATES = 200
DEFAULT_RECENT_TRAIN_ROWS = 12000
DEFAULT_RUNTIME_SNAPSHOT_PATH = os.path.join(
    PROJECT_ROOT,
    "tasks",
    "etf-prediction-upgrade",
    "outputs",
    "runtime",
    "ml_prediction_reco_candidate_scores_latest.json",
)
DEFAULT_SNAPSHOT_PATH = os.path.join(
    PROJECT_ROOT,
    "tasks",
    "etf-prediction-upgrade",
    "outputs",
    "ml_prediction_reco_candidate_scores_latest.json",
)
DEFAULT_ARCHIVE_DIR = os.path.join(
    PROJECT_ROOT,
    "tasks",
    "etf-prediction-upgrade",
    "outputs",
    "reco-candidate-score-snapshots",
)


CANDIDATE_SCORE_COLUMNS = [
    "trade_date",
    "ts_code",
    "name",
    "industry",
    "close",
    "ml_new_prob_up_5d",
    "ml_new_pred_ret_5d",
    "ml_new_classifier",
    "ml_new_regressor",
]


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _dedupe_columns(values) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        name = str(value or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


def normalize_candidate_codes(candidate_codes) -> tuple[str, ...]:
    values: list[str] = []
    seen: set[str] = set()
    for code in candidate_codes or ():
        normalized = str(code or "").strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        values.append(normalized)
    return tuple(values)


def collect_payload_candidate_codes(payload: dict | None) -> tuple[str, ...]:
    payload = payload or {}
    values: list[str] = []
    for key in ("top_uptrend", "top_avoid"):
        for item in payload.get(key) or []:
            if not isinstance(item, dict):
                continue
            code = str(item.get("ts_code") or "").strip().upper()
            if code:
                values.append(code)
    return normalize_candidate_codes(values)


def _ensure_sample_objects(engine: Engine) -> None:
    ensure_storage_objects(engine)
    ensure_sample_view(engine)


def _build_code_filter(candidate_codes: tuple[str, ...], prefix: str = "code") -> tuple[str, dict[str, object]]:
    normalized_codes = normalize_candidate_codes(candidate_codes)
    if not normalized_codes:
        return "", {}

    placeholders: list[str] = []
    params: dict[str, object] = {}
    for idx, code in enumerate(normalized_codes):
        key = f"{prefix}_{idx}"
        placeholders.append(f":{key}")
        params[key] = code
    return f" AND UPPER(ts_code) IN ({', '.join(placeholders)})", params


def _read_sql_df(engine: Engine, sql: str, params: dict[str, object]) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def _resolve_effective_trade_date(
    engine: Engine,
    requested_trade_date: str,
    candidate_codes: tuple[str, ...],
) -> str:
    requested_ts = pd.to_datetime(requested_trade_date, errors="coerce")
    if pd.isna(requested_ts):
        return ""

    _ensure_sample_objects(engine)
    code_filter_sql, code_params = _build_code_filter(candidate_codes)
    sql = f"""
    SELECT MAX(trade_date) AS trade_date
    FROM {SAMPLE_VIEW}
    WHERE trade_date <= :trade_date
      AND sample_eligible = TRUE
      {code_filter_sql}
    """
    params = {"trade_date": requested_ts.strftime("%Y-%m-%d")}
    params.update(code_params)

    df = _read_sql_df(engine, sql, params)
    if df.empty or "trade_date" not in df.columns:
        return ""

    value = pd.to_datetime(df.iloc[0]["trade_date"], errors="coerce")
    if pd.isna(value):
        return ""
    return value.strftime("%Y-%m-%d")


def _load_candidate_frame(
    engine: Engine,
    trade_date: str,
    candidate_codes: tuple[str, ...],
    max_candidates: int,
) -> pd.DataFrame:
    columns = _dedupe_columns([
        "trade_date",
        "ts_code",
        "name",
        "industry",
        "close",
        *V1_FEATURE_COLUMNS,
    ])
    selected_columns_sql = ", ".join(columns)
    code_filter_sql, code_params = _build_code_filter(candidate_codes)

    sql = f"""
    SELECT {selected_columns_sql}
    FROM {SAMPLE_VIEW}
    WHERE trade_date = :trade_date
      AND sample_eligible = TRUE
      {code_filter_sql}
    ORDER BY ts_code
    """
    params: dict[str, object] = {"trade_date": trade_date}
    params.update(code_params)
    if not candidate_codes:
        sql += " LIMIT :limit"
        params["limit"] = int(max_candidates)

    df = _read_sql_df(engine, sql, params)
    if df.empty:
        return pd.DataFrame(columns=columns)
    df["trade_date"] = pd.to_datetime(df.get("trade_date"), errors="coerce")
    return df.dropna(subset=["trade_date", "ts_code"]).reset_index(drop=True)


def _load_recent_training_frame(
    engine: Engine,
    effective_trade_date: str,
    *,
    lookback_days: int,
    min_train_rows: int,
    recent_train_rows: int,
) -> pd.DataFrame:
    cutoff_ts = pd.to_datetime(effective_trade_date, errors="coerce")
    if pd.isna(cutoff_ts):
        return pd.DataFrame()

    start_ts = cutoff_ts - pd.Timedelta(days=int(lookback_days))
    row_limit = max(int(recent_train_rows), int(min_train_rows), int(min_train_rows) * 2)
    columns = _dedupe_columns([
        "trade_date",
        "ts_code",
        *V1_FEATURE_COLUMNS,
        DEFAULT_CLASSIFICATION_TARGET,
        DEFAULT_REGRESSION_TARGET,
    ])
    selected_columns_sql = ", ".join(columns)
    sql = f"""
    SELECT {selected_columns_sql}
    FROM {SAMPLE_VIEW}
    WHERE trade_date >= :start_date
      AND trade_date < :trade_date
      AND sample_eligible = TRUE
      AND {DEFAULT_CLASSIFICATION_TARGET} IS NOT NULL
      AND {DEFAULT_REGRESSION_TARGET} IS NOT NULL
    ORDER BY trade_date DESC, ts_code
    LIMIT :limit
    """
    params = {
        "start_date": start_ts.strftime("%Y-%m-%d"),
        "trade_date": cutoff_ts.strftime("%Y-%m-%d"),
        "limit": row_limit,
    }
    df = _read_sql_df(engine, sql, params)
    if df.empty:
        return pd.DataFrame(columns=columns)

    df["trade_date"] = pd.to_datetime(df.get("trade_date"), errors="coerce")
    df = df.dropna(subset=["trade_date", "ts_code"]).sort_values(["trade_date", "ts_code"]).reset_index(drop=True)
    return df


def compute_candidate_scores(
    trade_date: str = "",
    *,
    candidate_codes: tuple[str, ...] = (),
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    min_train_rows: int = DEFAULT_MIN_TRAIN_ROWS,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
    recent_train_rows: int = DEFAULT_RECENT_TRAIN_ROWS,
    classification_model_kind: str = "sklearn",
    regression_model_kind: str = "sklearn",
    classifier: str = "logistic",
    regressor: str = "ridge",
) -> pd.DataFrame:
    trade_date_text = str(trade_date or "").strip()
    normalized_candidate_codes = normalize_candidate_codes(candidate_codes)
    if not trade_date_text:
        return pd.DataFrame(columns=CANDIDATE_SCORE_COLUMNS)

    try:
        engine = get_ml_stock_engine()
        effective_trade_date = _resolve_effective_trade_date(engine, trade_date_text, normalized_candidate_codes)
        if not effective_trade_date:
            return pd.DataFrame(columns=CANDIDATE_SCORE_COLUMNS)

        candidate_df = _load_candidate_frame(
            engine,
            effective_trade_date,
            normalized_candidate_codes,
            max_candidates=int(max_candidates),
        )
        if candidate_df.empty:
            return pd.DataFrame(columns=CANDIDATE_SCORE_COLUMNS)

        history_df = _load_recent_training_frame(
            engine,
            effective_trade_date,
            lookback_days=int(lookback_days),
            min_train_rows=int(min_train_rows),
            recent_train_rows=int(recent_train_rows),
        )
        if history_df.empty or len(history_df) < int(min_train_rows):
            logger.info(
                "compute_candidate_scores skipped: insufficient training rows trade_date=%s rows=%s min_train_rows=%s",
                effective_trade_date,
                len(history_df),
                min_train_rows,
            )
            return pd.DataFrame(columns=CANDIDATE_SCORE_COLUMNS)

        cls_prepared = prepare_training_data(
            history_df,
            task_type="classification",
            target_column=DEFAULT_CLASSIFICATION_TARGET,
            fill_method="median",
        )
        reg_prepared = prepare_training_data(
            history_df,
            task_type="regression",
            target_column=DEFAULT_REGRESSION_TARGET,
            fill_method="median",
        )
        if len(cls_prepared.rows) < int(min_train_rows) or len(reg_prepared.rows) < int(min_train_rows):
            return pd.DataFrame(columns=CANDIDATE_SCORE_COLUMNS)

        candidate_cls = candidate_df.copy()
        candidate_cls[DEFAULT_CLASSIFICATION_TARGET] = 0
        candidate_reg = candidate_df.copy()
        candidate_reg[DEFAULT_REGRESSION_TARGET] = 0.0

        candidate_cls_prepared = prepare_training_data(
            candidate_cls,
            task_type="classification",
            target_column=DEFAULT_CLASSIFICATION_TARGET,
            feature_columns=cls_prepared.feature_columns,
            fill_method="none",
        )
        candidate_cls_prepared = apply_feature_fill(
            candidate_cls_prepared,
            fill_method="median",
            fill_values=cls_prepared.fill_values,
        )
        candidate_reg_prepared = prepare_training_data(
            candidate_reg,
            task_type="regression",
            target_column=DEFAULT_REGRESSION_TARGET,
            feature_columns=reg_prepared.feature_columns,
            fill_method="none",
        )
        candidate_reg_prepared = apply_feature_fill(
            candidate_reg_prepared,
            fill_method="median",
            fill_values=reg_prepared.fill_values,
        )
        if candidate_cls_prepared.rows.empty or candidate_reg_prepared.rows.empty:
            return pd.DataFrame(columns=CANDIDATE_SCORE_COLUMNS)

        cls_run = fit_classification_model(
            cls_prepared.features,
            cls_prepared.target,
            candidate_cls_prepared.features,
            model_kind=classification_model_kind,
            classifier=classifier,
        )
        reg_run = fit_regression_model(
            reg_prepared.features,
            reg_prepared.target,
            candidate_reg_prepared.features,
            model_kind=regression_model_kind,
            regressor=regressor,
        )

        out = candidate_df[[col for col in ["trade_date", "ts_code", "name", "industry", "close"] if col in candidate_df.columns]].copy()
        out["trade_date"] = pd.to_datetime(out.get("trade_date"), errors="coerce").dt.strftime("%Y-%m-%d")
        out["ml_new_prob_up_5d"] = pd.to_numeric(cls_run.test_scores, errors="coerce") if cls_run.test_scores is not None else pd.to_numeric(cls_run.test_predictions, errors="coerce")
        out["ml_new_pred_ret_5d"] = pd.to_numeric(reg_run.test_predictions, errors="coerce")
        out["ml_new_classifier"] = cls_run.model_summary.get("classifier")
        out["ml_new_regressor"] = reg_run.model_summary.get("regressor")
        out = out.drop_duplicates(subset=["ts_code"], keep="first").reset_index(drop=True)
        for column in CANDIDATE_SCORE_COLUMNS:
            if column not in out.columns:
                out[column] = None
        return out[CANDIDATE_SCORE_COLUMNS]
    except Exception as exc:
        logger.warning("compute_candidate_scores failed for %s: %s", trade_date_text, exc)
        return pd.DataFrame(columns=CANDIDATE_SCORE_COLUMNS)


def build_candidate_score_snapshot(
    trade_date: str,
    *,
    candidate_codes: tuple[str, ...] = (),
    source: str = "trend_reco_latest",
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    min_train_rows: int = DEFAULT_MIN_TRAIN_ROWS,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
    recent_train_rows: int = DEFAULT_RECENT_TRAIN_ROWS,
    classification_model_kind: str = "sklearn",
    regression_model_kind: str = "sklearn",
    classifier: str = "logistic",
    regressor: str = "ridge",
) -> dict:
    score_df = compute_candidate_scores(
        trade_date,
        candidate_codes=candidate_codes,
        lookback_days=lookback_days,
        min_train_rows=min_train_rows,
        max_candidates=max_candidates,
        recent_train_rows=recent_train_rows,
        classification_model_kind=classification_model_kind,
        regression_model_kind=regression_model_kind,
        classifier=classifier,
        regressor=regressor,
    )
    snapshot_trade_date = str(trade_date or "").strip()
    if not score_df.empty and "trade_date" in score_df.columns:
        non_empty_dates = [str(v).strip() for v in score_df["trade_date"].tolist() if str(v).strip()]
        if non_empty_dates:
            snapshot_trade_date = non_empty_dates[0]

    serializable_df = score_df.astype(object).where(pd.notna(score_df), None)
    return {
        "snapshot_type": SNAPSHOT_TYPE,
        "generated_at": _utcnow_iso(),
        "source": str(source or "trend_reco_latest"),
        "trade_date": snapshot_trade_date,
        "requested_trade_date": str(trade_date or "").strip(),
        "lookback_days": int(lookback_days),
        "min_train_rows": int(min_train_rows),
        "max_candidates": int(max_candidates),
        "recent_train_rows": int(recent_train_rows),
        "classification_model_kind": classification_model_kind,
        "regression_model_kind": regression_model_kind,
        "classifier": classifier,
        "regressor": regressor,
        "candidate_codes": list(normalize_candidate_codes(candidate_codes)),
        "row_count": int(len(serializable_df)),
        "rows": serializable_df.to_dict(orient="records"),
    }


def build_trade_date_snapshot_path(trade_date: str, archive_dir: str = DEFAULT_ARCHIVE_DIR) -> str:
    trade_date_text = str(trade_date or "").strip()
    if not trade_date_text:
        raise ValueError("trade_date is required for archive snapshot path")
    return os.path.join(os.path.abspath(archive_dir), f"{trade_date_text}_ml_prediction_reco_candidate_scores.json")


def _write_json(path: str, payload: dict) -> str:
    normalized_path = os.path.abspath(path)
    os.makedirs(os.path.dirname(normalized_path), exist_ok=True)
    with open(normalized_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
        f.write("\n")
    return normalized_path


def write_candidate_score_snapshot(snapshot: dict, output_path: str = DEFAULT_RUNTIME_SNAPSHOT_PATH) -> str:
    return _write_json(output_path, snapshot)


def write_candidate_score_snapshot_bundle(
    snapshot: dict,
    *,
    latest_output_path: str = DEFAULT_RUNTIME_SNAPSHOT_PATH,
    write_latest_copy: bool = True,
    archive_dir: str = DEFAULT_ARCHIVE_DIR,
) -> dict:
    latest_path = write_candidate_score_snapshot(snapshot, latest_output_path)
    archive_path = ""

    trade_date = str(snapshot.get("trade_date") or "").strip()
    if trade_date:
        archive_path = _write_json(build_trade_date_snapshot_path(trade_date, archive_dir=archive_dir), snapshot)

    mirror_latest_path = ""
    if write_latest_copy and os.path.abspath(DEFAULT_SNAPSHOT_PATH) != os.path.abspath(latest_output_path):
        mirror_latest_path = _write_json(DEFAULT_SNAPSHOT_PATH, snapshot)

    return {
        "latest_path": latest_path,
        "archive_path": archive_path,
        "mirror_latest_path": mirror_latest_path,
    }


def resolve_snapshot_paths(trade_date: str = "", snapshot_path: str | None = None) -> list[str]:
    candidate_paths: list[str] = []
    if snapshot_path:
        candidate_paths.append(snapshot_path)

    trade_date_text = str(trade_date or "").strip()
    if trade_date_text:
        try:
            candidate_paths.append(build_trade_date_snapshot_path(trade_date_text))
        except Exception:
            pass

    candidate_paths.extend([
        DEFAULT_RUNTIME_SNAPSHOT_PATH,
        DEFAULT_SNAPSHOT_PATH,
    ])

    seen: set[str] = set()
    normalized_paths: list[str] = []
    for path in candidate_paths:
        normalized = os.path.abspath(path)
        if normalized in seen:
            continue
        seen.add(normalized)
        normalized_paths.append(normalized)
    return normalized_paths


def load_candidate_score_snapshot(trade_date: str = "", snapshot_path: str | None = None) -> dict:
    for normalized_path in resolve_snapshot_paths(trade_date=trade_date, snapshot_path=snapshot_path):
        try:
            if not os.path.exists(normalized_path):
                continue
            with open(normalized_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if str(payload.get("snapshot_type") or "").strip() != SNAPSHOT_TYPE:
                continue
            payload["snapshot_path"] = normalized_path
            return payload
        except Exception as exc:
            logger.warning("load_candidate_score_snapshot failed for %s: %s", normalized_path, exc)
    return {}


def load_candidate_scores_from_snapshot(
    trade_date: str = "",
    *,
    candidate_codes: tuple[str, ...] = (),
    snapshot_path: str | None = None,
) -> pd.DataFrame:
    payload = load_candidate_score_snapshot(trade_date=trade_date, snapshot_path=snapshot_path)
    if not payload:
        return pd.DataFrame(columns=CANDIDATE_SCORE_COLUMNS)

    requested_trade_date = str(trade_date or "").strip()
    snapshot_trade_date = str(payload.get("trade_date") or "").strip()
    if requested_trade_date and snapshot_trade_date and requested_trade_date != snapshot_trade_date:
        return pd.DataFrame(columns=CANDIDATE_SCORE_COLUMNS)

    rows = payload.get("rows") or []
    if not rows:
        return pd.DataFrame(columns=CANDIDATE_SCORE_COLUMNS)

    df = pd.DataFrame(rows).copy()
    for column in CANDIDATE_SCORE_COLUMNS:
        if column not in df.columns:
            df[column] = None
    df["ts_code"] = df["ts_code"].astype(str)

    normalized_candidate_codes = normalize_candidate_codes(candidate_codes)
    if normalized_candidate_codes:
        df = df.loc[df["ts_code"].str.upper().isin(normalized_candidate_codes)].copy()

    if df.empty:
        return pd.DataFrame(columns=CANDIDATE_SCORE_COLUMNS)

    return df[CANDIDATE_SCORE_COLUMNS].drop_duplicates(subset=["ts_code"], keep="first").reset_index(drop=True)
