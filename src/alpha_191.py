from __future__ import annotations

import numpy as np
import pandas as pd


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = pd.to_numeric(denominator, errors="coerce").replace(0, np.nan)
    return pd.to_numeric(numerator, errors="coerce") / denominator


def add_alpha095(
    df: pd.DataFrame,
    window: int = 20,
    ddof: int = 1,
    code_col: str = "code",
    date_col: str = "date",
    amount_col: str = "amount",
) -> pd.DataFrame:
    return add_alpha095_family(
        df=df,
        window=window,
        ddof=ddof,
        code_col=code_col,
        date_col=date_col,
        amount_col=amount_col,
    )


def add_alpha095_family(
    df: pd.DataFrame,
    window: int = 20,
    ddof: int = 1,
    code_col: str = "code",
    date_col: str = "date",
    amount_col: str = "amount",
) -> pd.DataFrame:
    required = {code_col, date_col, amount_col}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"df is missing required columns: {sorted(missing)}")

    if window <= 0:
        raise ValueError("window must be a positive integer")

    result = df.copy()
    result["_orig_order"] = np.arange(len(result))
    result[date_col] = pd.to_datetime(result[date_col], errors="coerce")
    result[amount_col] = pd.to_numeric(result[amount_col], errors="coerce")

    ordered = result.sort_values([code_col, date_col, "_orig_order"], kind="mergesort").copy()
    grouped_amount = ordered.groupby(code_col, sort=False)[amount_col]

    ordered["alpha095"] = grouped_amount.transform(
        lambda s: s.rolling(window=window, min_periods=window).std(ddof=ddof)
    )
    rolling_mean = grouped_amount.transform(
        lambda s: s.rolling(window=window, min_periods=window).mean()
    )
    ordered["alpha095_cv"] = _safe_ratio(ordered["alpha095"], rolling_mean)

    nonnegative_amount = ordered[amount_col].where(ordered[amount_col] >= 0)
    log_amount = np.log1p(nonnegative_amount)
    ordered["alpha095_logstd"] = log_amount.groupby(ordered[code_col], sort=False).transform(
        lambda s: s.rolling(window=window, min_periods=window).std(ddof=ddof)
    )

    amount_pct = grouped_amount.pct_change(fill_method=None)
    ordered["alpha095_pctstd"] = amount_pct.groupby(ordered[code_col], sort=False).transform(
        lambda s: s.rolling(window=window, min_periods=window).std(ddof=ddof)
    )

    return ordered.sort_values("_orig_order", kind="mergesort").drop(columns="_orig_order")
