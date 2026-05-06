from __future__ import annotations

import os

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL


TABLE_NAME = "macro_index_monitor_monthly"
DISPLAY_COLUMN_LABELS = {
    "month": "月份",
    "index_name": "指数名称",
    "monthly_change_pct": "当月涨幅",
    "open_price": "开盘价格",
    "close_price": "收盘价格",
    "low_price": "最低点",
    "high_price": "最高点",
    "static_pe": "期末静态市盈率",
    "dynamic_pe": "期末动态市盈率",
    "mom_change_pct": "环比涨幅变化",
    "yoy_change_pct": "同比涨幅变化",
    "mom_static_pe_change_rate": "静态市盈率变化率",
    "mom_dynamic_pe_change_rate": "动态市盈率变化率",
    "source_type": "数据来源",
    "updated_at": "更新时间",
}
PRICE_FIELDS = ["open_price", "close_price", "low_price", "high_price"]
VALUATION_FIELDS = ["static_pe", "dynamic_pe"]
NUMERIC_FIELDS = [
    "monthly_change_pct",
    "open_price",
    "close_price",
    "low_price",
    "high_price",
    "static_pe",
    "dynamic_pe",
    "mom_change_pct",
    "mom_open_price",
    "mom_close_price",
    "mom_low_price",
    "mom_high_price",
    "mom_static_pe",
    "mom_dynamic_pe",
    "mom_static_pe_change_rate",
    "mom_dynamic_pe_change_rate",
    "yoy_change_pct",
    "yoy_open_price",
    "yoy_close_price",
    "yoy_low_price",
    "yoy_high_price",
    "yoy_static_pe",
    "yoy_dynamic_pe",
]


def build_db_url():
    try:
        from src.sync_tushare_security_data import build_db_url as _sync_build_db_url

        return _sync_build_db_url()
    except Exception:
        pass

    direct_url = os.getenv("ETF_PG_URL") or os.getenv("DATABASE_URL")
    if direct_url:
        return direct_url

    password = os.getenv("ETF_PG_PASSWORD") or os.getenv("PGPASSWORD")
    if not password:
        raise RuntimeError("未配置数据库密码，请设置 ETF_PG_PASSWORD 或 PGPASSWORD")

    return URL.create(
        "postgresql+psycopg2",
        username=os.getenv("ETF_PG_USER", "postgres"),
        password=password,
        host=os.getenv("ETF_PG_HOST", "67.216.207.73"),
        port=int(os.getenv("ETF_PG_PORT", "5432")),
        database=os.getenv("ETF_PG_DATABASE", "postgres"),
        query={"sslmode": os.getenv("ETF_PG_SSLMODE", "disable")},
    )


def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)


def ensure_index_monitor_table(engine: Engine) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        month DATE NOT NULL,
        index_name TEXT NOT NULL,
        monthly_change_pct NUMERIC(18, 4),
        open_price NUMERIC(18, 4),
        close_price NUMERIC(18, 4),
        low_price NUMERIC(18, 4),
        high_price NUMERIC(18, 4),
        static_pe NUMERIC(18, 4),
        dynamic_pe NUMERIC(18, 4),
        mom_change_pct NUMERIC(18, 4),
        mom_open_price NUMERIC(18, 4),
        mom_close_price NUMERIC(18, 4),
        mom_low_price NUMERIC(18, 4),
        mom_high_price NUMERIC(18, 4),
        mom_static_pe NUMERIC(18, 4),
        mom_dynamic_pe NUMERIC(18, 4),
        mom_static_pe_change_rate NUMERIC(18, 4),
        mom_dynamic_pe_change_rate NUMERIC(18, 4),
        yoy_change_pct NUMERIC(18, 4),
        yoy_open_price NUMERIC(18, 4),
        yoy_close_price NUMERIC(18, 4),
        yoy_low_price NUMERIC(18, 4),
        yoy_high_price NUMERIC(18, 4),
        yoy_static_pe NUMERIC(18, 4),
        yoy_dynamic_pe NUMERIC(18, 4),
        source_type VARCHAR(16) NOT NULL,
        source_file TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (month, index_name)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def build_index_monitor_summary(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {
            "latest_month": None,
            "avg_change_pct": None,
            "strongest_index": None,
            "weakest_index": None,
            "avg_static_pe": None,
            "avg_dynamic_pe": None,
        }

    data = df.copy()
    data["month"] = pd.to_datetime(data["month"])
    latest_month = data["month"].max()
    latest_df = data[data["month"] == latest_month].copy()
    latest_df["monthly_change_pct"] = pd.to_numeric(latest_df["monthly_change_pct"], errors="coerce")
    latest_df["static_pe"] = pd.to_numeric(latest_df["static_pe"], errors="coerce")
    latest_df["dynamic_pe"] = pd.to_numeric(latest_df["dynamic_pe"], errors="coerce")
    strongest = (
        latest_df.sort_values("monthly_change_pct", ascending=False).iloc[0]["index_name"]
        if latest_df["monthly_change_pct"].notna().any()
        else None
    )
    weakest = (
        latest_df.sort_values("monthly_change_pct", ascending=True).iloc[0]["index_name"]
        if latest_df["monthly_change_pct"].notna().any()
        else None
    )
    return {
        "latest_month": latest_month.strftime("%Y-%m"),
        "avg_change_pct": latest_df["monthly_change_pct"].mean(),
        "strongest_index": strongest,
        "weakest_index": weakest,
        "avg_static_pe": latest_df["static_pe"].mean(),
        "avg_dynamic_pe": latest_df["dynamic_pe"].mean(),
    }


def build_price_trend_df(df: pd.DataFrame, value_field: str = "close_price") -> pd.DataFrame:
    data = df.copy()
    data["month"] = pd.to_datetime(data["month"])
    data["value"] = pd.to_numeric(data[value_field], errors="coerce")
    return data[["month", "index_name", "value"]].sort_values(["index_name", "month"]).reset_index(drop=True)


def build_valuation_trend_df(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    data["month"] = pd.to_datetime(data["month"])
    melted = data.melt(
        id_vars=["month", "index_name"],
        value_vars=VALUATION_FIELDS,
        var_name="metric_key",
        value_name="value",
    )
    melted["metric"] = melted["metric_key"].map(
        {"static_pe": "期末静态市盈率", "dynamic_pe": "期末动态市盈率"}
    )
    melted["metric_order"] = melted["metric_key"].map(
        {"static_pe": 0, "dynamic_pe": 1}
    )
    return melted.sort_values(["index_name", "month", "metric_order"]).reset_index(drop=True)


def classify_index_import_rows(incoming_df: pd.DataFrame, existing_df: pd.DataFrame) -> dict:
    incoming = incoming_df.copy()
    incoming["business_key"] = incoming["month"].astype(str) + "|" + incoming["index_name"].astype(str)
    if existing_df is None or existing_df.empty:
        return {
            "to_insert": incoming.drop(columns=["business_key"]).reset_index(drop=True),
            "to_overwrite": incoming.iloc[0:0].drop(columns=["business_key"]).copy(),
        }
    existing = existing_df.copy()
    existing["business_key"] = existing["month"].astype(str) + "|" + existing["index_name"].astype(str)
    existing_keys = set(existing["business_key"].tolist())
    to_insert = incoming[~incoming["business_key"].isin(existing_keys)].drop(columns=["business_key"]).reset_index(drop=True)
    to_overwrite = incoming[incoming["business_key"].isin(existing_keys)].drop(columns=["business_key"]).reset_index(drop=True)
    return {"to_insert": to_insert, "to_overwrite": to_overwrite}


def build_index_upsert_rows(rows, source_type: str, source_file: str | None) -> list[dict]:
    payload_rows = []
    for row in rows:
        payload = {
            "month": pd.to_datetime(row["month"]).date(),
            "index_name": str(row["index_name"]).strip(),
            "source_type": source_type,
            "source_file": source_file,
        }
        for field in NUMERIC_FIELDS:
            payload[field] = float(row[field]) if row.get(field) is not None else None
        payload_rows.append(payload)
    return payload_rows


def upsert_index_monitor_rows(engine: Engine, rows: list[dict]) -> int:
    ensure_index_monitor_table(engine)
    columns = [
        "month",
        "index_name",
        *NUMERIC_FIELDS,
        "source_type",
        "source_file",
    ]
    update_columns = [field for field in NUMERIC_FIELDS] + ["source_type", "source_file"]
    insert_sql = text(
        f"""
        INSERT INTO {TABLE_NAME} ({", ".join(columns)})
        VALUES ({", ".join(f":{col}" for col in columns)})
        ON CONFLICT (month, index_name) DO UPDATE SET
            {", ".join(f"{col} = EXCLUDED.{col}" for col in update_columns)},
            updated_at = NOW();
        """
    )
    with engine.begin() as conn:
        conn.execute(insert_sql, rows)
    return len(rows)


def load_index_monitor_df(engine: Engine | None = None) -> pd.DataFrame:
    actual_engine = engine or get_engine()
    ensure_index_monitor_table(actual_engine)
    with actual_engine.begin() as conn:
        return pd.read_sql(
            text(f"SELECT * FROM {TABLE_NAME} ORDER BY month ASC, index_name ASC"),
            conn,
        )


def to_index_monitor_display_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    display_df = df.copy()
    if display_df.empty:
        return display_df.rename(columns=DISPLAY_COLUMN_LABELS)
    display_df["month"] = pd.to_datetime(display_df["month"]).dt.strftime("%Y-%m-%d")
    if "updated_at" in display_df.columns:
        display_df["updated_at"] = pd.to_datetime(display_df["updated_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    ordered = [col for col in DISPLAY_COLUMN_LABELS if col in display_df.columns]
    return display_df[ordered].rename(columns=DISPLAY_COLUMN_LABELS)
