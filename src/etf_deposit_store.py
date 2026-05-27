from __future__ import annotations

from datetime import date, datetime
import os
import re

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL


TABLE_NAME = "macro_fx_rmb_deposits_monthly"
BALANCE_LABELS = {
    "rmb_deposit_balance": "人民币存款余额",
    "fx_deposit_balance": "外币存款余额",
    "total_deposit_balance": "本外币存款余额",
}
CHANGE_LABELS = {
    "mom_delta": "环比变动额",
    "yoy_delta": "同比变动额",
}
DISPLAY_COLUMN_LABELS = {
    "month": "月份",
    "rmb_deposit_balance": "人民币存款余额",
    "fx_deposit_balance": "外币存款余额",
    "total_deposit_balance": "本外币存款余额",
    "household_deposit_increase": "住户存款增加额",
    "corp_deposit_increase": "非金融企业存款增加额",
    "fiscal_deposit_increase": "财政性存款增加额",
    "nonbank_deposit_increase": "非银行业金融机构存款增加额",
    "total_deposit_increase": "存款合计增加额",
    "household_long_loan_increase": "居民长期贷款增加额",
    "source_type": "数据来源",
    "source_file": "来源文件",
    "created_at": "创建时间",
    "updated_at": "更新时间",
}
NUMERIC_FIELDS = [
    "rmb_deposit_balance",
    "fx_deposit_balance",
    "total_deposit_balance",
    "household_deposit_increase",
    "corp_deposit_increase",
    "fiscal_deposit_increase",
    "nonbank_deposit_increase",
    "total_deposit_increase",
    "household_long_loan_increase",
]


def build_db_url():
    from src.sync_tushare_security_data import build_db_url as _sync_build_db_url

    return _sync_build_db_url()


def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)


def normalize_month(value) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.replace(day=1)
    if isinstance(value, datetime):
        return value.date().replace(day=1)

    raw = str(value).strip()
    if not raw:
        raise ValueError("month 不能为空")

    compact = re.sub(r"\s+", "", raw)
    normalized = (
        compact.replace("年", "-")
        .replace("月", "-")
        .replace("日", "")
        .replace("/", "-")
        .replace(".", "-")
    )
    normalized = re.sub(r"-+", "-", normalized).strip("-")

    candidates = [raw, compact, normalized]

    for candidate in candidates:
        try:
            if len(candidate) == 10 and candidate.count("-") == 2:
                return datetime.strptime(candidate, "%Y-%m-%d").date().replace(day=1)
            if len(candidate) == 8 and candidate.isdigit():
                return datetime.strptime(candidate, "%Y%m%d").date().replace(day=1)
            if len(candidate) == 6 and candidate.isdigit():
                return datetime.strptime(candidate, "%Y%m").date().replace(day=1)
        except ValueError:
            continue

    month_match = re.fullmatch(r"(\d{4})-(\d{1,2})", normalized)
    if month_match:
        year = int(month_match.group(1))
        month = int(month_match.group(2))
        return date(year, month, 1)

    day_match = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", normalized)
    if day_match:
        year = int(day_match.group(1))
        month = int(day_match.group(2))
        day = int(day_match.group(3))
        return date(year, month, day).replace(day=1)

    raise ValueError(f"无法识别月份格式: {value}，请使用 YYYY-MM，例如 2026-05")


def build_deposit_summary(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {
            "latest_month": None,
            "latest_value": None,
            "mom_delta": None,
            "yoy_delta": None,
        }

    data = df.copy()
    data["month"] = pd.to_datetime(data["month"])
    data = data.sort_values("month")
    latest = data.iloc[-1]
    prev_month = data.iloc[-2] if len(data) >= 2 else None
    yoy_month = latest["month"] - pd.DateOffset(years=1)
    yoy_candidates = data.loc[data["month"] == yoy_month]
    yoy_row = yoy_candidates.iloc[-1] if not yoy_candidates.empty else None

    latest_value = float(latest["total_deposit_balance"])
    mom_delta = (
        latest_value - float(prev_month["total_deposit_balance"])
        if prev_month is not None
        else None
    )
    yoy_delta = (
        latest_value - float(yoy_row["total_deposit_balance"])
        if yoy_row is not None
        else None
    )

    return {
        "latest_month": latest["month"].strftime("%Y-%m"),
        "latest_value": latest_value,
        "mom_delta": mom_delta,
        "yoy_delta": yoy_delta,
    }


def build_balance_trend_df(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    data["month"] = pd.to_datetime(data["month"])
    melted = data.melt(
        id_vars=["month"],
        value_vars=list(BALANCE_LABELS.keys()),
        var_name="metric_key",
        value_name="value",
    )
    melted["metric"] = melted["metric_key"].map(BALANCE_LABELS)
    melted["metric_order"] = melted["metric_key"].map(
        {key: idx for idx, key in enumerate(BALANCE_LABELS.keys())}
    )
    return melted.sort_values(["month", "metric_order"]).reset_index(drop=True)


def build_change_trend_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["month", "metric", "value"])

    data = df.copy()
    data["month"] = pd.to_datetime(data["month"])
    data = data.sort_values("month").reset_index(drop=True)
    data["total_deposit_balance"] = data["total_deposit_balance"].astype(float)
    data["mom_delta"] = data["total_deposit_balance"].diff()
    yoy_lookup = data[["month", "total_deposit_balance"]].rename(
        columns={"month": "yoy_month", "total_deposit_balance": "yoy_value"}
    )
    data["yoy_month"] = data["month"] - pd.DateOffset(years=1)
    data = data.merge(yoy_lookup, on="yoy_month", how="left")
    data["yoy_delta"] = data["total_deposit_balance"] - data["yoy_value"]

    melted = data.melt(
        id_vars=["month"],
        value_vars=list(CHANGE_LABELS.keys()),
        var_name="metric_key",
        value_name="value",
    )
    melted["metric"] = melted["metric_key"].map(CHANGE_LABELS)
    melted["metric_order"] = melted["metric_key"].map(
        {key: idx for idx, key in enumerate(CHANGE_LABELS.keys())}
    )
    return melted.sort_values(["month", "metric_order"]).reset_index(drop=True)


def ensure_deposit_table(engine: Engine) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        month DATE PRIMARY KEY,
        rmb_deposit_balance NUMERIC(18, 4) NOT NULL,
        fx_deposit_balance NUMERIC(18, 4) NOT NULL,
        total_deposit_balance NUMERIC(18, 4) NOT NULL,
        household_deposit_increase NUMERIC(18, 4) NOT NULL,
        corp_deposit_increase NUMERIC(18, 4) NOT NULL,
        fiscal_deposit_increase NUMERIC(18, 4) NOT NULL,
        nonbank_deposit_increase NUMERIC(18, 4) NOT NULL,
        total_deposit_increase NUMERIC(18, 4) NOT NULL,
        household_long_loan_increase NUMERIC(18, 4) NOT NULL,
        source_type VARCHAR(16) NOT NULL,
        source_file TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_macro_fx_rmb_deposits_monthly_month_desc
        ON {TABLE_NAME}(month DESC);
    """
    with engine.begin() as conn:
        for statement in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(statement))


def build_upsert_rows(rows, source_type: str, source_file: str | None) -> list[dict]:
    payload_rows = []
    for row in rows:
        payload = {
            "month": normalize_month(row["month"]),
            "source_type": source_type,
            "source_file": source_file,
        }
        for field in NUMERIC_FIELDS:
            payload[field] = float(row[field])
        payload_rows.append(payload)
    return payload_rows


def classify_import_rows(incoming_df: pd.DataFrame, existing_df: pd.DataFrame) -> dict:
    incoming = incoming_df.copy()
    incoming["month"] = pd.to_datetime(incoming["month"]).dt.strftime("%Y-%m-%d")

    if existing_df is None or existing_df.empty:
        return {
            "to_insert": incoming.reset_index(drop=True),
            "to_overwrite": incoming.iloc[0:0].copy(),
        }

    existing_months = set(pd.to_datetime(existing_df["month"]).dt.strftime("%Y-%m-%d"))
    to_insert = incoming[~incoming["month"].isin(existing_months)].reset_index(drop=True)
    to_overwrite = incoming[incoming["month"].isin(existing_months)].reset_index(drop=True)
    return {"to_insert": to_insert, "to_overwrite": to_overwrite}


def to_deposit_display_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()

    display_df = df.copy()
    if display_df.empty:
        return display_df.rename(columns=DISPLAY_COLUMN_LABELS)

    if "month" in display_df.columns:
        display_df["month"] = pd.to_datetime(display_df["month"]).dt.strftime("%Y-%m-%d")

    for column in ("created_at", "updated_at"):
        if column in display_df.columns:
            display_df[column] = pd.to_datetime(display_df[column]).dt.strftime("%Y-%m-%d %H:%M:%S")

    ordered_columns = [column for column in DISPLAY_COLUMN_LABELS if column in display_df.columns]
    if ordered_columns:
        display_df = display_df[ordered_columns]

    return display_df.rename(columns=DISPLAY_COLUMN_LABELS)


def upsert_deposit_rows(engine: Engine, rows: list[dict]) -> int:
    ensure_deposit_table(engine)
    insert_sql = text(
        f"""
        INSERT INTO {TABLE_NAME} (
            month, rmb_deposit_balance, fx_deposit_balance, total_deposit_balance,
            household_deposit_increase, corp_deposit_increase, fiscal_deposit_increase,
            nonbank_deposit_increase, total_deposit_increase, household_long_loan_increase,
            source_type, source_file
        ) VALUES (
            :month, :rmb_deposit_balance, :fx_deposit_balance, :total_deposit_balance,
            :household_deposit_increase, :corp_deposit_increase, :fiscal_deposit_increase,
            :nonbank_deposit_increase, :total_deposit_increase, :household_long_loan_increase,
            :source_type, :source_file
        )
        ON CONFLICT (month) DO UPDATE SET
            rmb_deposit_balance = EXCLUDED.rmb_deposit_balance,
            fx_deposit_balance = EXCLUDED.fx_deposit_balance,
            total_deposit_balance = EXCLUDED.total_deposit_balance,
            household_deposit_increase = EXCLUDED.household_deposit_increase,
            corp_deposit_increase = EXCLUDED.corp_deposit_increase,
            fiscal_deposit_increase = EXCLUDED.fiscal_deposit_increase,
            nonbank_deposit_increase = EXCLUDED.nonbank_deposit_increase,
            total_deposit_increase = EXCLUDED.total_deposit_increase,
            household_long_loan_increase = EXCLUDED.household_long_loan_increase,
            source_type = EXCLUDED.source_type,
            source_file = EXCLUDED.source_file,
            updated_at = NOW();
        """
    )
    with engine.begin() as conn:
        conn.execute(insert_sql, rows)
    return len(rows)


def delete_deposit_months(engine: Engine, months: list) -> int:
    ensure_deposit_table(engine)
    normalized_months = sorted({normalize_month(month) for month in months if str(month).strip()})
    if not normalized_months:
        return 0

    delete_sql = text(f"DELETE FROM {TABLE_NAME} WHERE month = :month")
    deleted_count = 0
    with engine.begin() as conn:
        for month in normalized_months:
            result = conn.execute(delete_sql, {"month": month})
            deleted_count += int(result.rowcount or 0)
    return deleted_count


def load_deposit_monthly_df(engine: Engine | None = None) -> pd.DataFrame:
    actual_engine = engine or get_engine()
    ensure_deposit_table(actual_engine)
    sql = text(f"SELECT * FROM {TABLE_NAME} ORDER BY month ASC")
    with actual_engine.begin() as conn:
        return pd.read_sql(sql, conn)
