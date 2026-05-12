from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Sequence

import numpy as np
import pandas as pd

DEFAULT_CLASSIFICATION_TARGET = "y_up_5d"
DEFAULT_REGRESSION_TARGET = "ret_fwd_5d"
SUPPORTED_MODEL_KINDS = ("baseline", "sklearn")
SUPPORTED_CLASSIFIERS = ("logistic",)
SUPPORTED_REGRESSORS = ("ridge", "linear")

SUPPORTED_TARGETS = {
    "classification": (
        "y_up_1d",
        "y_up_3d",
        "y_up_5d",
        "y_up_10d",
        "y_up_20d",
    ),
    "regression": (
        "ret_fwd_1d",
        "ret_fwd_3d",
        "ret_fwd_5d",
        "ret_fwd_10d",
        "ret_fwd_20d",
        "max_dd_fwd_3d",
        "max_dd_fwd_5d",
        "max_dd_fwd_10d",
        "max_dd_fwd_20d",
        "max_upside_fwd_5d",
        "max_upside_fwd_20d",
    ),
}

V1_FEATURE_COLUMNS = (
    "listing_days",
    "is_current_st",
    "has_ever_st",
    "close",
    "ret_1d",
    "ret_3d",
    "ret_5d",
    "ret_10d",
    "ret_20d",
    "ret_60d",
    "close_over_ma5",
    "close_over_ma20",
    "close_over_ma60",
    "ma5_over_ma20",
    "ma20_over_ma60",
    "volatility_5d",
    "volatility_20d",
    "distance_to_20d_high",
    "distance_to_20d_low",
    "distance_to_60d_high",
    "distance_to_60d_low",
    "turnover_rate",
    "turnover_rate_f",
    "volume_ratio",
    "vol_ma5_ratio",
    "amount_ma5_ratio",
    "pb",
    "pe_ttm",
    "ps_ttm",
    "dv_ttm",
    "log_total_mv",
    "log_circ_mv",
    "w_ema5_over_30",
    "m_ema5_over_30",
    "is_weekly_ema_bearish",
    "is_monthly_ema_bearish",
    "feature_complete_ratio",
)

CLASSIFICATION_SCORE_FEATURES = (
    "ret_20d",
    "ret_5d",
    "ma5_over_ma20",
    "w_ema5_over_30",
    "m_ema5_over_30",
    "feature_complete_ratio",
)

STRATEGY_TOPN_LEVELS = (1, 3, 5)


@dataclass
class PreparedTrainingData:
    rows: pd.DataFrame
    features: pd.DataFrame
    target: pd.Series
    feature_columns: list[str]
    target_column: str
    task_type: str
    fill_method: str
    fill_values: dict[str, float]
    row_count_before_filter: int
    row_count_after_filter: int

    def to_summary(self) -> dict[str, Any]:
        return {
            "task_type": self.task_type,
            "target_column": self.target_column,
            "feature_columns": list(self.feature_columns),
            "feature_count": len(self.feature_columns),
            "fill_method": self.fill_method,
            "fill_values": dict(self.fill_values),
            "row_count_before_filter": self.row_count_before_filter,
            "row_count_after_filter": self.row_count_after_filter,
        }


@dataclass
class ClassificationBaselineModel:
    strategy: str
    prior_positive_rate: float
    default_prediction: int
    score_feature: str | None = None
    threshold: float | None = None
    positive_when_above_threshold: bool | None = None
    model_kind: str = "baseline"

    def predict(self, features: pd.DataFrame) -> pd.Series:
        if self.strategy == "prior_rate":
            values = np.full(len(features), self.default_prediction, dtype=int)
            return pd.Series(values, index=features.index, name="prediction")

        if self.score_feature is None or self.threshold is None:
            raise ValueError("single_feature_threshold baseline requires score_feature and threshold")

        score_series = pd.to_numeric(features[self.score_feature], errors="coerce")
        is_above = score_series >= float(self.threshold)
        positive_prediction = 1 if self.positive_when_above_threshold else 0
        negative_prediction = 0 if self.positive_when_above_threshold else 1
        values = np.where(is_above, positive_prediction, negative_prediction).astype(int)
        values = np.where(score_series.isna(), self.default_prediction, values)
        return pd.Series(values, index=features.index, name="prediction")

    def to_summary(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RegressionBaselineModel:
    strategy: str
    constant_value: float
    model_kind: str = "baseline"

    def predict(self, features: pd.DataFrame) -> pd.Series:
        values = np.full(len(features), self.constant_value, dtype=float)
        return pd.Series(values, index=features.index, name="prediction")

    def to_summary(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SklearnModel:
    task_type: str
    selection_key: str
    selection_name: str
    estimator_name: str
    params: dict[str, Any]
    estimator: Any = field(repr=False)
    model_kind: str = "sklearn"

    def predict(self, features: pd.DataFrame) -> pd.Series:
        feature_frame = _prepare_model_features(features)
        values = self.estimator.predict(feature_frame)
        prediction = pd.Series(values, index=feature_frame.index, name="prediction")
        if self.task_type == "classification":
            return pd.to_numeric(prediction, errors="coerce").fillna(0).astype(int)
        return pd.to_numeric(prediction, errors="coerce").astype(float)

    def predict_scores(self, features: pd.DataFrame) -> pd.Series | None:
        if self.task_type != "classification":
            return None

        feature_frame = _prepare_model_features(features)
        if hasattr(self.estimator, "predict_proba"):
            probabilities = self.estimator.predict_proba(feature_frame)
            class_values = list(getattr(self.estimator, "classes_", []))
            positive_class_index = (
                class_values.index(1)
                if 1 in class_values
                else len(class_values) - 1
            )
            return pd.Series(
                probabilities[:, positive_class_index],
                index=feature_frame.index,
                name="score",
                dtype=float,
            )

        if hasattr(self.estimator, "decision_function"):
            decision_values = self.estimator.decision_function(feature_frame)
            return pd.Series(decision_values, index=feature_frame.index, name="score", dtype=float)
        return None

    def to_summary(self) -> dict[str, Any]:
        return {
            "model_kind": self.model_kind,
            "task_type": self.task_type,
            self.selection_key: self.selection_name,
            "estimator_name": self.estimator_name,
            "params": dict(self.params),
        }


@dataclass
class TrainingRunResult:
    model: Any
    model_summary: dict[str, Any]
    train_predictions: pd.Series
    test_predictions: pd.Series
    train_scores: pd.Series | None = None
    test_scores: pd.Series | None = None


@dataclass
class WalkForwardWindowResult:
    cutoff_date: pd.Timestamp
    train_rows_before_fill: int
    test_rows_before_fill: int
    train_rows_after_fill: int
    test_rows_after_fill: int
    model_summary: dict[str, Any]
    train_metrics: dict[str, Any]
    test_metrics: dict[str, Any]
    strategy_metrics: dict[str, Any] | None = None

    def to_summary(self) -> dict[str, Any]:
        return {
            "cutoff_date": self.cutoff_date.strftime("%Y-%m-%d"),
            "train_rows": self.train_rows_after_fill,
            "test_rows": self.test_rows_after_fill,
            "train_rows_before_fill": self.train_rows_before_fill,
            "test_rows_before_fill": self.test_rows_before_fill,
            "train_rows_after_fill": self.train_rows_after_fill,
            "test_rows_after_fill": self.test_rows_after_fill,
            "model_summary": dict(self.model_summary),
            "train_metrics": dict(self.train_metrics),
            "test_metrics": dict(self.test_metrics),
            "strategy_metrics": dict(self.strategy_metrics or {}),
        }


@dataclass
class WalkForwardEvaluationResult:
    task_type: str
    model_kind: str
    window_results: list[WalkForwardWindowResult]
    aggregate: dict[str, Any]
    skipped_windows: list[dict[str, Any]]

    def to_summary(self) -> dict[str, Any]:
        return {
            "task_type": self.task_type,
            "model_kind": self.model_kind,
            "aggregate": dict(self.aggregate),
            "window_results": [window.to_summary() for window in self.window_results],
            "skipped_windows": [dict(item) for item in self.skipped_windows],
        }


def _safe_date_string(value) -> str:
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return str(value)
    return pd.Timestamp(timestamp).strftime("%Y-%m-%d")


def _finite_mean(values: Sequence[Any]) -> float:
    numeric_values: list[float] = []
    for value in values:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if np.isfinite(numeric):
            numeric_values.append(numeric)
    if not numeric_values:
        return float("nan")
    return float(np.mean(numeric_values))


def _normalize_task_type(task_type: str) -> str:
    normalized = str(task_type or "").strip().lower()
    if normalized not in SUPPORTED_TARGETS:
        raise ValueError(
            f"task_type must be one of {sorted(SUPPORTED_TARGETS)}, got: {task_type!r}"
        )
    return normalized


def _normalize_model_kind(model_kind: str) -> str:
    normalized = str(model_kind or "").strip().lower()
    if normalized not in SUPPORTED_MODEL_KINDS:
        raise ValueError(
            f"model_kind must be one of {list(SUPPORTED_MODEL_KINDS)}, got: {model_kind!r}"
        )
    return normalized


def _normalize_classifier(classifier: str) -> str:
    normalized = str(classifier or "").strip().lower()
    if normalized not in SUPPORTED_CLASSIFIERS:
        raise ValueError(
            f"classifier must be one of {list(SUPPORTED_CLASSIFIERS)}, got: {classifier!r}"
        )
    return normalized


def _normalize_regressor(regressor: str) -> str:
    normalized = str(regressor or "").strip().lower()
    if normalized not in SUPPORTED_REGRESSORS:
        raise ValueError(
            f"regressor must be one of {list(SUPPORTED_REGRESSORS)}, got: {regressor!r}"
        )
    return normalized


def _normalize_optional_positive_int(value: Any, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive integer, got: {value!r}")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive integer, got: {value!r}") from exc
    if normalized <= 0:
        raise ValueError(f"{name} must be a positive integer, got: {value!r}")
    return normalized


def _coerce_binary_value(value) -> float | None:
    if pd.isna(value):
        return None
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "t", "yes", "y"}:
            return 1.0
        if normalized in {"0", "false", "f", "no", "n"}:
            return 0.0
        return None
    if isinstance(value, (bool, np.bool_)):
        return float(bool(value))
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(numeric):
        return None
    return 1.0 if numeric > 0 else 0.0


def resolve_target_column(task_type: str, target_column: str | None = None) -> str:
    normalized_task = _normalize_task_type(task_type)
    if target_column is None:
        return (
            DEFAULT_CLASSIFICATION_TARGET
            if normalized_task == "classification"
            else DEFAULT_REGRESSION_TARGET
        )

    normalized_target = str(target_column).strip()
    if normalized_target not in SUPPORTED_TARGETS[normalized_task]:
        raise ValueError(
            f"target_column {normalized_target!r} is not supported for task_type "
            f"{normalized_task!r}. Supported targets: {list(SUPPORTED_TARGETS[normalized_task])}"
        )
    return normalized_target


def select_v1_feature_columns(
    sample_df_or_columns: pd.DataFrame | Sequence[str],
    feature_columns: Sequence[str] | None = None,
) -> list[str]:
    if isinstance(sample_df_or_columns, pd.DataFrame):
        available_columns = set(sample_df_or_columns.columns)
    else:
        available_columns = {str(column) for column in sample_df_or_columns}

    requested = list(feature_columns) if feature_columns is not None else list(V1_FEATURE_COLUMNS)
    selected = [column for column in requested if column in available_columns]

    if feature_columns is not None:
        missing_columns = [column for column in requested if column not in available_columns]
        if missing_columns:
            raise ValueError(f"Requested feature columns are missing from the sample data: {missing_columns}")

    if not selected:
        raise ValueError("No V1 feature columns are available in the sample dataframe")
    return selected


def compute_fill_values(features: pd.DataFrame, fill_method: str = "median") -> dict[str, float]:
    normalized_method = str(fill_method or "none").strip().lower()
    if normalized_method in {"", "none", "drop"}:
        return {}
    if normalized_method != "median":
        raise ValueError(f"Unsupported fill_method for fill value fitting: {fill_method!r}")

    medians = features.median(numeric_only=True)
    return {
        column: float(value)
        for column, value in medians.items()
        if pd.notna(value) and np.isfinite(value)
    }


def _subset_prepared_data(
    prepared: PreparedTrainingData,
    row_mask: pd.Series,
    fill_method: str | None = None,
    fill_values: dict[str, float] | None = None,
    preserve_input_counts: bool = False,
) -> PreparedTrainingData:
    normalized_mask = pd.Series(row_mask, index=prepared.rows.index).fillna(False).astype(bool)
    rows = prepared.rows.loc[normalized_mask].reset_index(drop=True)
    features = prepared.features.loc[normalized_mask].reset_index(drop=True)
    target = prepared.target.loc[normalized_mask].reset_index(drop=True)
    return PreparedTrainingData(
        rows=rows,
        features=features,
        target=target,
        feature_columns=list(prepared.feature_columns),
        target_column=prepared.target_column,
        task_type=prepared.task_type,
        fill_method=prepared.fill_method if fill_method is None else fill_method,
        fill_values=dict(prepared.fill_values if fill_values is None else fill_values),
        row_count_before_filter=(
            prepared.row_count_before_filter if preserve_input_counts else int(len(rows))
        ),
        row_count_after_filter=int(len(rows)),
    )


def apply_feature_fill(
    prepared: PreparedTrainingData,
    fill_method: str = "median",
    fill_values: dict[str, float] | None = None,
) -> PreparedTrainingData:
    normalized_method = str(fill_method or "none").strip().lower()
    if normalized_method not in {"none", "drop", "median"}:
        raise ValueError(f"Unsupported fill_method: {fill_method!r}")

    if prepared.features.empty:
        return PreparedTrainingData(
            rows=prepared.rows.copy(),
            features=prepared.features.copy(),
            target=prepared.target.copy(),
            feature_columns=list(prepared.feature_columns),
            target_column=prepared.target_column,
            task_type=prepared.task_type,
            fill_method=normalized_method,
            fill_values=dict(fill_values or {}),
            row_count_before_filter=prepared.row_count_before_filter,
            row_count_after_filter=prepared.row_count_after_filter,
        )

    features = prepared.features.copy()
    computed_fill_values = dict(fill_values or {})

    if normalized_method == "median":
        if not computed_fill_values:
            computed_fill_values = compute_fill_values(features, fill_method="median")
        features = features.fillna(computed_fill_values)

    if normalized_method == "none":
        return PreparedTrainingData(
            rows=prepared.rows.copy(),
            features=features.reset_index(drop=True),
            target=prepared.target.reset_index(drop=True),
            feature_columns=list(prepared.feature_columns),
            target_column=prepared.target_column,
            task_type=prepared.task_type,
            fill_method=normalized_method,
            fill_values=computed_fill_values,
            row_count_before_filter=prepared.row_count_before_filter,
            row_count_after_filter=prepared.row_count_after_filter,
        )

    complete_mask = features.notna().all(axis=1)
    filtered = _subset_prepared_data(
        prepared,
        complete_mask,
        fill_method=normalized_method,
        fill_values=computed_fill_values,
        preserve_input_counts=True,
    )
    filtered.features = features.loc[complete_mask].reset_index(drop=True)
    filtered.target = filtered.target.reset_index(drop=True)
    filtered.row_count_after_filter = int(len(filtered.rows))
    return filtered


def prepare_training_data(
    sample_df: pd.DataFrame,
    task_type: str,
    target_column: str | None = None,
    feature_columns: Sequence[str] | None = None,
    fill_method: str = "none",
    date_column: str = "trade_date",
) -> PreparedTrainingData:
    if sample_df is None:
        raise ValueError("sample_df is required")
    if date_column not in sample_df.columns:
        raise ValueError(f"sample_df must include date column {date_column!r}")

    normalized_task = _normalize_task_type(task_type)
    resolved_target = resolve_target_column(normalized_task, target_column)
    if resolved_target not in sample_df.columns:
        raise ValueError(f"sample_df is missing target column {resolved_target!r}")

    selected_features = select_v1_feature_columns(sample_df, feature_columns=feature_columns)
    prepared_rows = sample_df.copy()
    prepared_rows[date_column] = pd.to_datetime(prepared_rows[date_column], errors="coerce")
    prepared_rows = prepared_rows.dropna(subset=[date_column]).copy()
    sort_columns = [date_column]
    if "ts_code" in prepared_rows.columns:
        sort_columns.append("ts_code")
    prepared_rows = prepared_rows.sort_values(sort_columns).reset_index(drop=True)

    raw_features = prepared_rows.loc[:, selected_features].apply(pd.to_numeric, errors="coerce")
    raw_features = raw_features.astype(float)

    if normalized_task == "classification":
        raw_target = prepared_rows[resolved_target].map(_coerce_binary_value)
    else:
        raw_target = pd.to_numeric(prepared_rows[resolved_target], errors="coerce")

    valid_target_mask = raw_target.notna()
    filtered = PreparedTrainingData(
        rows=prepared_rows.loc[valid_target_mask].reset_index(drop=True),
        features=raw_features.loc[valid_target_mask].reset_index(drop=True),
        target=raw_target.loc[valid_target_mask].reset_index(drop=True),
        feature_columns=list(selected_features),
        target_column=resolved_target,
        task_type=normalized_task,
        fill_method="none",
        fill_values={},
        row_count_before_filter=int(len(prepared_rows)),
        row_count_after_filter=int(valid_target_mask.sum()),
    )

    if normalized_task == "classification":
        filtered.target = filtered.target.astype(int)
    else:
        filtered.target = filtered.target.astype(float)

    return apply_feature_fill(filtered, fill_method=fill_method)


def split_by_date(
    prepared: PreparedTrainingData,
    cutoff_date,
    date_column: str = "trade_date",
) -> tuple[PreparedTrainingData, PreparedTrainingData]:
    if date_column not in prepared.rows.columns:
        raise ValueError(f"Prepared rows are missing date column {date_column!r}")

    cutoff_ts = pd.to_datetime(cutoff_date, errors="coerce")
    if pd.isna(cutoff_ts):
        raise ValueError(f"Invalid cutoff_date: {cutoff_date!r}")

    trade_dates = pd.to_datetime(prepared.rows[date_column], errors="coerce")
    train_mask = trade_dates < cutoff_ts
    test_mask = trade_dates >= cutoff_ts
    return _subset_prepared_data(prepared, train_mask), _subset_prepared_data(prepared, test_mask)


def generate_walk_forward_cutoff_dates(
    prepared: PreparedTrainingData,
    min_train_rows: int = 1,
    min_test_rows: int = 1,
    max_windows: int | None = None,
    cutoff_dates: Sequence[Any] | None = None,
    date_column: str = "trade_date",
) -> list[pd.Timestamp]:
    if date_column not in prepared.rows.columns:
        raise ValueError(f"Prepared rows are missing date column {date_column!r}")

    normalized_min_train_rows = _normalize_optional_positive_int(min_train_rows, "min_train_rows")
    normalized_min_test_rows = _normalize_optional_positive_int(min_test_rows, "min_test_rows")
    normalized_max_windows = _normalize_optional_positive_int(max_windows, "max_windows")
    if normalized_min_train_rows is None:
        raise ValueError("min_train_rows must be provided")
    if normalized_min_test_rows is None:
        raise ValueError("min_test_rows must be provided")

    trade_dates = pd.to_datetime(prepared.rows[date_column], errors="coerce")
    valid_trade_dates = trade_dates.dropna()
    if valid_trade_dates.empty:
        return []

    if cutoff_dates is None:
        candidate_dates = sorted(pd.Timestamp(value) for value in pd.unique(valid_trade_dates))
    else:
        candidate_dates = []
        for value in cutoff_dates:
            timestamp = pd.to_datetime(value, errors="coerce")
            if pd.isna(timestamp):
                raise ValueError(f"Invalid walk-forward cutoff_date: {value!r}")
            candidate_dates.append(pd.Timestamp(timestamp))
        candidate_dates = sorted(set(candidate_dates))

    valid_cutoff_dates: list[pd.Timestamp] = []
    for cutoff_ts in candidate_dates:
        train_count = int((valid_trade_dates < cutoff_ts).sum())
        test_count = int((valid_trade_dates >= cutoff_ts).sum())
        if train_count >= normalized_min_train_rows and test_count >= normalized_min_test_rows:
            valid_cutoff_dates.append(cutoff_ts)

    if normalized_max_windows is not None:
        valid_cutoff_dates = valid_cutoff_dates[-normalized_max_windows:]
    return valid_cutoff_dates


def train_classification_baseline(
    train_features: pd.DataFrame,
    train_target: pd.Series,
) -> ClassificationBaselineModel:
    if train_target.empty:
        raise ValueError("train_target must not be empty")

    positive_rate = float(train_target.mean())
    default_prediction = int(positive_rate >= 0.5)

    for column in CLASSIFICATION_SCORE_FEATURES:
        if column not in train_features.columns:
            continue
        score_series = pd.to_numeric(train_features[column], errors="coerce")
        valid_mask = score_series.notna()
        if valid_mask.sum() < 2:
            continue
        score_series = score_series.loc[valid_mask]
        target_series = train_target.loc[valid_mask]
        if score_series.nunique(dropna=True) < 2:
            continue

        threshold = float(score_series.median())
        above_mask = score_series >= threshold
        below_mask = ~above_mask
        if int(above_mask.sum()) == 0 or int(below_mask.sum()) == 0:
            continue

        above_rate = float(target_series.loc[above_mask].mean())
        below_rate = float(target_series.loc[below_mask].mean())
        return ClassificationBaselineModel(
            strategy="single_feature_threshold",
            prior_positive_rate=positive_rate,
            default_prediction=default_prediction,
            score_feature=column,
            threshold=threshold,
            positive_when_above_threshold=above_rate >= below_rate,
        )

    return ClassificationBaselineModel(
        strategy="prior_rate",
        prior_positive_rate=positive_rate,
        default_prediction=default_prediction,
    )


def train_regression_baseline(train_target: pd.Series) -> RegressionBaselineModel:
    if train_target.empty:
        raise ValueError("train_target must not be empty")
    return RegressionBaselineModel(
        strategy="mean_target",
        constant_value=float(pd.to_numeric(train_target, errors="coerce").mean()),
    )


def _prepare_model_features(features: pd.DataFrame) -> pd.DataFrame:
    if features is None:
        raise ValueError("features must not be None")
    feature_frame = features.copy() if isinstance(features, pd.DataFrame) else pd.DataFrame(features)
    if feature_frame.empty:
        raise ValueError("features must not be empty")
    feature_frame = feature_frame.apply(pd.to_numeric, errors="coerce").astype(float)
    if feature_frame.isna().any().any():
        raise ValueError("features contain missing or non-numeric values; apply feature fill first")
    return feature_frame


def _prepare_model_target(target: pd.Series, task_type: str) -> pd.Series:
    if target is None:
        raise ValueError("target must not be None")
    target_series = target.copy() if isinstance(target, pd.Series) else pd.Series(target)
    if target_series.empty:
        raise ValueError("target must not be empty")
    target_series = pd.to_numeric(target_series, errors="coerce")
    if target_series.isna().any():
        raise ValueError("target contains missing or non-numeric values")
    if task_type == "classification":
        return target_series.astype(int)
    return target_series.astype(float)


def _serialize_summary_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(key): _serialize_summary_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize_summary_value(item) for item in value]
    return str(value)


def _extract_estimator_params(estimator_name: str, estimator: Any) -> dict[str, Any]:
    if estimator_name == "LogisticRegression":
        param_names = ("C", "class_weight", "fit_intercept", "max_iter", "penalty", "solver")
    elif estimator_name == "Ridge":
        param_names = ("alpha", "fit_intercept", "solver", "tol")
    elif estimator_name == "LinearRegression":
        param_names = ("fit_intercept", "positive", "tol")
    else:
        param_names = tuple(estimator.get_params(deep=False).keys())
    params = estimator.get_params(deep=False)
    return {
        name: _serialize_summary_value(params.get(name))
        for name in param_names
        if name in params
    }


def train_classification_sklearn(
    train_features: pd.DataFrame,
    train_target: pd.Series,
    classifier: str = "logistic",
) -> SklearnModel:
    normalized_classifier = _normalize_classifier(classifier)
    feature_frame = _prepare_model_features(train_features)
    target_series = _prepare_model_target(train_target, task_type="classification")
    if target_series.nunique(dropna=True) < 2:
        raise ValueError("classification training requires at least two target classes")

    from sklearn.linear_model import LogisticRegression

    if normalized_classifier != "logistic":
        raise ValueError(f"Unsupported sklearn classifier: {classifier!r}")

    estimator = LogisticRegression(max_iter=1000, solver="lbfgs")
    estimator.fit(feature_frame, target_series)
    return SklearnModel(
        task_type="classification",
        selection_key="classifier",
        selection_name=normalized_classifier,
        estimator_name="LogisticRegression",
        params=_extract_estimator_params("LogisticRegression", estimator),
        estimator=estimator,
    )


def train_regression_sklearn(
    train_features: pd.DataFrame,
    train_target: pd.Series,
    regressor: str = "ridge",
) -> SklearnModel:
    normalized_regressor = _normalize_regressor(regressor)
    feature_frame = _prepare_model_features(train_features)
    target_series = _prepare_model_target(train_target, task_type="regression")

    from sklearn.linear_model import LinearRegression, Ridge

    if normalized_regressor == "ridge":
        estimator = Ridge(alpha=1.0)
        estimator_name = "Ridge"
    elif normalized_regressor == "linear":
        estimator = LinearRegression()
        estimator_name = "LinearRegression"
    else:
        raise ValueError(f"Unsupported sklearn regressor: {regressor!r}")

    estimator.fit(feature_frame, target_series)
    return SklearnModel(
        task_type="regression",
        selection_key="regressor",
        selection_name=normalized_regressor,
        estimator_name=estimator_name,
        params=_extract_estimator_params(estimator_name, estimator),
        estimator=estimator,
    )


def fit_classification_model(
    train_features: pd.DataFrame,
    train_target: pd.Series,
    test_features: pd.DataFrame,
    model_kind: str = "baseline",
    classifier: str = "logistic",
) -> TrainingRunResult:
    normalized_model_kind = _normalize_model_kind(model_kind)
    train_frame = _prepare_model_features(train_features)
    test_frame = _prepare_model_features(test_features)
    train_series = _prepare_model_target(train_target, task_type="classification")

    if normalized_model_kind == "baseline":
        model = train_classification_baseline(train_frame, train_series)
        train_scores = None
        test_scores = None
    else:
        model = train_classification_sklearn(
            train_frame,
            train_series,
            classifier=classifier,
        )
        train_scores = model.predict_scores(train_frame)
        test_scores = model.predict_scores(test_frame)

    return TrainingRunResult(
        model=model,
        model_summary=model.to_summary(),
        train_predictions=model.predict(train_frame),
        test_predictions=model.predict(test_frame),
        train_scores=train_scores,
        test_scores=test_scores,
    )


def fit_regression_model(
    train_features: pd.DataFrame,
    train_target: pd.Series,
    test_features: pd.DataFrame,
    model_kind: str = "baseline",
    regressor: str = "ridge",
) -> TrainingRunResult:
    normalized_model_kind = _normalize_model_kind(model_kind)
    train_frame = _prepare_model_features(train_features)
    test_frame = _prepare_model_features(test_features)
    train_series = _prepare_model_target(train_target, task_type="regression")

    if normalized_model_kind == "baseline":
        model = train_regression_baseline(train_series)
    else:
        model = train_regression_sklearn(
            train_frame,
            train_series,
            regressor=regressor,
        )

    return TrainingRunResult(
        model=model,
        model_summary=model.to_summary(),
        train_predictions=model.predict(train_frame),
        test_predictions=model.predict(test_frame),
    )


def evaluate_classification_predictions(
    y_true: pd.Series,
    y_pred: pd.Series,
    y_score: pd.Series | None = None,
) -> dict[str, float | int]:
    truth = pd.to_numeric(y_true, errors="coerce")
    pred = pd.to_numeric(y_pred, errors="coerce")
    valid_mask = truth.notna() & pred.notna()
    truth = truth.loc[valid_mask].astype(int)
    pred = pred.loc[valid_mask].astype(int)

    if truth.empty:
        return {
            "sample_count": 0,
            "accuracy": float("nan"),
            "positive_rate_pred": float("nan"),
            "positive_rate_actual": float("nan"),
            "average_label": float("nan"),
            "roc_auc": float("nan"),
        }

    metrics = {
        "sample_count": int(len(truth)),
        "accuracy": float((truth == pred).mean()),
        "positive_rate_pred": float(pred.mean()),
        "positive_rate_actual": float(truth.mean()),
        "average_label": float(truth.mean()),
    }
    if y_score is None:
        metrics["roc_auc"] = float("nan")
        return metrics

    score = pd.to_numeric(y_score, errors="coerce").loc[valid_mask]
    score_valid_mask = score.notna()
    metrics["roc_auc"] = float("nan")
    if int(score_valid_mask.sum()) > 0 and truth.loc[score_valid_mask].nunique(dropna=True) >= 2:
        from sklearn.metrics import roc_auc_score

        metrics["roc_auc"] = float(
            roc_auc_score(truth.loc[score_valid_mask], score.loc[score_valid_mask])
        )
    return metrics


def evaluate_regression_predictions(
    y_true: pd.Series,
    y_pred: pd.Series,
) -> dict[str, float | int]:
    truth = pd.to_numeric(y_true, errors="coerce")
    pred = pd.to_numeric(y_pred, errors="coerce")
    valid_mask = truth.notna() & pred.notna()
    truth = truth.loc[valid_mask].astype(float)
    pred = pred.loc[valid_mask].astype(float)

    if truth.empty:
        return {
            "sample_count": 0,
            "mae": float("nan"),
            "rmse": float("nan"),
            "target_mean": float("nan"),
            "prediction_mean": float("nan"),
            "r2": float("nan"),
        }

    errors = pred - truth
    r2 = float("nan")
    if len(truth) >= 2:
        centered_truth = truth - truth.mean()
        total_sum_squares = float(np.square(centered_truth).sum())
        if total_sum_squares > 0.0:
            residual_sum_squares = float(np.square(errors).sum())
            r2 = float(1.0 - residual_sum_squares / total_sum_squares)

    return {
        "sample_count": int(len(truth)),
        "mae": float(np.abs(errors).mean()),
        "rmse": float(np.sqrt(np.square(errors).mean())),
        "target_mean": float(truth.mean()),
        "prediction_mean": float(pred.mean()),
        "r2": r2,
    }


def _resolve_strategy_return_column(rows: pd.DataFrame, target_column: str) -> str | None:
    if "ret_fwd_5d" in rows.columns:
        return "ret_fwd_5d"
    if str(target_column).startswith("ret_fwd_") and target_column in rows.columns:
        return target_column
    return None


def _resolve_strategy_label_column(rows: pd.DataFrame, target_column: str) -> str | None:
    if str(target_column).startswith("y_up_") and target_column in rows.columns:
        return target_column
    if "y_up_5d" in rows.columns:
        return "y_up_5d"
    return None


def _build_strategy_ranking_score(
    prepared: PreparedTrainingData,
    run: TrainingRunResult,
    ready: PreparedTrainingData,
) -> tuple[pd.Series, str]:
    if prepared.task_type == "classification":
        if run.test_scores is not None:
            return pd.to_numeric(run.test_scores, errors="coerce").rename("score"), "model_score"

        model = run.model
        if (
            isinstance(model, ClassificationBaselineModel)
            and model.score_feature
            and model.score_feature in ready.features.columns
        ):
            score = pd.to_numeric(ready.features[model.score_feature], errors="coerce")
            if model.positive_when_above_threshold is False:
                score = -score
            return score.rename("score"), f"feature:{model.score_feature}"

    return pd.to_numeric(run.test_predictions, errors="coerce").rename("score"), "prediction"


def compute_daily_topn_strategy_metrics(
    rows: pd.DataFrame,
    ranking_score: pd.Series,
    *,
    return_column: str | None = None,
    label_column: str | None = None,
    score_source: str = "prediction",
    topn_levels: Sequence[int] = STRATEGY_TOPN_LEVELS,
    date_column: str = "trade_date",
) -> dict[str, Any]:
    frame = rows.copy().reset_index(drop=True)
    ranking_series = pd.to_numeric(pd.Series(ranking_score), errors="coerce").reset_index(drop=True)
    if len(ranking_series) != len(frame):
        raise ValueError("ranking_score length must match rows length")

    normalized_topn_levels: list[int] = []
    for top_n in topn_levels:
        normalized = _normalize_optional_positive_int(top_n, "top_n")
        if normalized is not None and normalized not in normalized_topn_levels:
            normalized_topn_levels.append(normalized)

    if date_column not in frame.columns:
        result = {
            "score_source": score_source,
            "return_column": return_column,
            "label_column": label_column,
            "row_count": int(len(frame)),
            "day_count": 0,
        }
        for top_n in normalized_topn_levels:
            result[f"top{top_n}"] = {
                "days_ranked": 0,
                "pick_count_total": 0,
                "return_day_count": 0,
                "hit_day_count": 0,
                "avg_return": float("nan"),
                "hit_rate": float("nan"),
            }
        return result

    frame[date_column] = pd.to_datetime(frame[date_column], errors="coerce")
    frame["_ranking_score"] = ranking_series

    if return_column and return_column in frame.columns:
        frame["_strategy_return"] = pd.to_numeric(frame[return_column], errors="coerce")
        frame["_strategy_hit"] = np.where(
            frame["_strategy_return"].notna(),
            (frame["_strategy_return"] > 0).astype(float),
            np.nan,
        )
    elif label_column and label_column in frame.columns:
        label_series = frame[label_column].map(_coerce_binary_value)
        frame["_strategy_hit"] = pd.to_numeric(label_series, errors="coerce")

    grouped_frames = [
        day_frame.copy()
        for _, day_frame in frame.loc[
            frame[date_column].notna() & frame["_ranking_score"].notna()
        ].groupby(date_column, sort=True)
    ]

    result = {
        "score_source": score_source,
        "return_column": return_column if return_column in frame.columns else None,
        "label_column": label_column if label_column in frame.columns else None,
        "row_count": int(len(frame)),
        "day_count": int(frame[date_column].dropna().nunique()),
    }

    for top_n in normalized_topn_levels:
        per_day_returns: list[float] = []
        per_day_hits: list[float] = []
        pick_count_total = 0
        for day_frame in grouped_frames:
            if "ts_code" in day_frame.columns:
                ranked = day_frame.sort_values(
                    ["_ranking_score", "ts_code"],
                    ascending=[False, True],
                    kind="mergesort",
                )
            else:
                ranked = day_frame.sort_values(
                    ["_ranking_score"],
                    ascending=[False],
                    kind="mergesort",
                )
            selected = ranked.head(min(top_n, len(ranked)))
            pick_count_total += int(len(selected))

            if "_strategy_return" in selected.columns:
                returns = pd.to_numeric(selected["_strategy_return"], errors="coerce")
                if returns.notna().any():
                    per_day_returns.append(float(returns.mean()))

            if "_strategy_hit" in selected.columns:
                hits = pd.to_numeric(selected["_strategy_hit"], errors="coerce")
                if hits.notna().any():
                    per_day_hits.append(float(hits.mean()))

        result[f"top{top_n}"] = {
            "days_ranked": int(len(grouped_frames)),
            "pick_count_total": int(pick_count_total),
            "return_day_count": int(len(per_day_returns)),
            "hit_day_count": int(len(per_day_hits)),
            "avg_return": _finite_mean(per_day_returns),
            "hit_rate": _finite_mean(per_day_hits),
        }

    return result


def _build_walk_forward_strategy_aggregate(
    window_results: Sequence[WalkForwardWindowResult],
) -> dict[str, Any]:
    windows_with_metrics = [
        window for window in window_results if isinstance(window.strategy_metrics, dict)
    ]
    aggregate = {
        "topn_levels": list(STRATEGY_TOPN_LEVELS),
        "window_count_with_strategy_metrics": int(len(windows_with_metrics)),
        "score_sources": sorted(
            {
                str(window.strategy_metrics.get("score_source"))
                for window in windows_with_metrics
                if window.strategy_metrics.get("score_source")
            }
        ),
    }

    return_columns = {
        str(window.strategy_metrics.get("return_column"))
        for window in windows_with_metrics
        if window.strategy_metrics.get("return_column")
    }
    aggregate["return_column"] = next(iter(return_columns)) if len(return_columns) == 1 else None

    for top_n in STRATEGY_TOPN_LEVELS:
        top_key = f"top{top_n}"
        aggregate[f"average_daily_{top_key}_return"] = _finite_mean(
            (window.strategy_metrics or {}).get(top_key, {}).get("avg_return")
            for window in windows_with_metrics
        )
        aggregate[f"average_daily_{top_key}_hit_rate"] = _finite_mean(
            (window.strategy_metrics or {}).get(top_key, {}).get("hit_rate")
            for window in windows_with_metrics
        )

    return aggregate


def _build_walk_forward_aggregate(
    task_type: str,
    window_results: Sequence[WalkForwardWindowResult],
    skipped_windows: Sequence[dict[str, Any]],
    candidate_cutoff_count: int,
    selected_cutoff_count: int,
) -> dict[str, Any]:
    aggregate_test_metrics: dict[str, float] = {}
    if task_type == "classification":
        aggregate_test_metrics = {
            "accuracy": _finite_mean(window.test_metrics.get("accuracy") for window in window_results),
            "roc_auc": _finite_mean(window.test_metrics.get("roc_auc") for window in window_results),
        }
    else:
        aggregate_test_metrics = {
            "mae": _finite_mean(window.test_metrics.get("mae") for window in window_results),
            "rmse": _finite_mean(window.test_metrics.get("rmse") for window in window_results),
            "r2": _finite_mean(window.test_metrics.get("r2") for window in window_results),
        }

    aggregate = {
        "candidate_cutoff_count": int(candidate_cutoff_count),
        "selected_cutoff_count": int(selected_cutoff_count),
        "window_count": int(len(window_results)),
        "rows_evaluated_total": int(sum(window.test_rows_after_fill for window in window_results)),
        "cutoff_dates": [window.cutoff_date.strftime("%Y-%m-%d") for window in window_results],
        "skipped_window_count": int(len(skipped_windows)),
        "aggregate_test_metrics": aggregate_test_metrics,
        "strategy_metrics": _build_walk_forward_strategy_aggregate(window_results),
    }
    if task_type == "classification":
        aggregate["average_test_accuracy"] = aggregate_test_metrics["accuracy"]
        aggregate["average_test_roc_auc"] = aggregate_test_metrics["roc_auc"]
    else:
        aggregate["average_test_mae"] = aggregate_test_metrics["mae"]
        aggregate["average_test_rmse"] = aggregate_test_metrics["rmse"]
        aggregate["average_test_r2"] = aggregate_test_metrics["r2"]
    return aggregate


def run_walk_forward_evaluation(
    prepared: PreparedTrainingData,
    model_kind: str = "baseline",
    fill_method: str = "median",
    classifier: str = "logistic",
    regressor: str = "ridge",
    min_train_rows: int = 1,
    min_test_rows: int = 1,
    max_windows: int | None = None,
    cutoff_dates: Sequence[Any] | None = None,
    date_column: str = "trade_date",
) -> WalkForwardEvaluationResult:
    normalized_model_kind = _normalize_model_kind(model_kind)
    candidate_cutoffs = generate_walk_forward_cutoff_dates(
        prepared,
        min_train_rows=min_train_rows,
        min_test_rows=min_test_rows,
        max_windows=None,
        cutoff_dates=cutoff_dates,
        date_column=date_column,
    )
    cutoff_schedule = generate_walk_forward_cutoff_dates(
        prepared,
        min_train_rows=min_train_rows,
        min_test_rows=min_test_rows,
        max_windows=max_windows,
        cutoff_dates=cutoff_dates,
        date_column=date_column,
    )

    window_results: list[WalkForwardWindowResult] = []
    skipped_windows: list[dict[str, Any]] = []
    for cutoff_ts in cutoff_schedule:
        train_split, test_split = split_by_date(prepared, cutoff_ts, date_column=date_column)
        try:
            fill_values = compute_fill_values(train_split.features, fill_method=fill_method)
            train_ready = apply_feature_fill(
                train_split,
                fill_method=fill_method,
                fill_values=fill_values,
            )
            test_ready = apply_feature_fill(
                test_split,
                fill_method=fill_method,
                fill_values=fill_values,
            )

            if len(train_ready.rows) < int(min_train_rows):
                raise ValueError("training rows after fill/filtering are below min_train_rows")
            if len(test_ready.rows) < int(min_test_rows):
                raise ValueError("evaluation rows after fill/filtering are below min_test_rows")

            if prepared.task_type == "classification":
                run = fit_classification_model(
                    train_ready.features,
                    train_ready.target,
                    test_ready.features,
                    model_kind=normalized_model_kind,
                    classifier=classifier,
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
                    model_kind=normalized_model_kind,
                    regressor=regressor,
                )
                train_metrics = evaluate_regression_predictions(
                    train_ready.target,
                    run.train_predictions,
                )
                test_metrics = evaluate_regression_predictions(
                    test_ready.target,
                    run.test_predictions,
                )

            strategy_score, strategy_score_source = _build_strategy_ranking_score(
                prepared,
                run,
                test_ready,
            )
            strategy_metrics = compute_daily_topn_strategy_metrics(
                test_ready.rows,
                strategy_score,
                return_column=_resolve_strategy_return_column(test_ready.rows, prepared.target_column),
                label_column=_resolve_strategy_label_column(test_ready.rows, prepared.target_column),
                score_source=strategy_score_source,
            )

            window_results.append(
                WalkForwardWindowResult(
                    cutoff_date=pd.Timestamp(cutoff_ts),
                    train_rows_before_fill=int(len(train_split.rows)),
                    test_rows_before_fill=int(len(test_split.rows)),
                    train_rows_after_fill=int(len(train_ready.rows)),
                    test_rows_after_fill=int(len(test_ready.rows)),
                    model_summary=dict(run.model_summary),
                    train_metrics=dict(train_metrics),
                    test_metrics=dict(test_metrics),
                    strategy_metrics=strategy_metrics,
                )
            )
        except ValueError as exc:
            skipped_windows.append(
                {
                    "cutoff_date": _safe_date_string(cutoff_ts),
                    "reason": str(exc),
                }
            )

    return WalkForwardEvaluationResult(
        task_type=prepared.task_type,
        model_kind=normalized_model_kind,
        window_results=window_results,
        aggregate=_build_walk_forward_aggregate(
            prepared.task_type,
            window_results,
            skipped_windows,
            candidate_cutoff_count=len(candidate_cutoffs),
            selected_cutoff_count=len(cutoff_schedule),
        ),
        skipped_windows=skipped_windows,
    )
