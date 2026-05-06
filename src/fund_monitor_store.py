from __future__ import annotations

import os

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL


TABLE_NAME = "macro_fund_monitor_monthly"
DISPLAY_COLUMN_LABELS = {
    "month": "月份",
    "category_name": "分类名称",
    "fund_count": "基金数量（只）",
    "share_amount": "份额（亿份）",
    "nav_amount": "净值（亿元）",
    "unit_nav": "单位净值（元）",
    "mom_fund_count": "环比基金数量变动",
    "mom_share_amount": "环比份额变动",
    "mom_nav_amount": "环比净值变动",
    "yoy_fund_count": "同比基金数量变动",
    "yoy_share_amount": "同比份额变动",
    "yoy_nav_amount": "同比净值变动",
    "source_type": "数据来源",
    "updated_at": "更新时间",
}
NUMERIC_FIELDS = [
    "fund_count",
    "share_amount",
    "nav_amount",
    "unit_nav",
    "mom_fund_count",
    "mom_share_amount",
    "mom_nav_amount",
    "mom_unit_nav",
    "yoy_fund_count",
    "yoy_share_amount",
    "yoy_nav_amount",
    "yoy_unit_nav",
]
CHANGE_TREND_FIELDS = {
    "fund_count": {"label": "基金数量（只）", "mom": "mom_fund_count", "yoy": "yoy_fund_count"},
    "share_amount": {"label": "份额（亿份）", "mom": "mom_share_amount", "yoy": "yoy_share_amount"},
    "nav_amount": {"label": "净值（亿元）", "mom": "mom_nav_amount", "yoy": "yoy_nav_amount"},
    "unit_nav": {"label": "单位净值（元）", "mom": "mom_unit_nav", "yoy": "yoy_unit_nav"},
}
CHANGE_TREND_FIELD_LABELS = {key: value["label"] for key, value in CHANGE_TREND_FIELDS.items()}


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


def ensure_fund_monitor_table(engine: Engine) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        month DATE NOT NULL,
        category_name TEXT NOT NULL,
        category_group VARCHAR(32),
        category_level VARCHAR(16),
        sort_order INTEGER,
        fund_count NUMERIC(18, 4),
        share_amount NUMERIC(18, 4),
        nav_amount NUMERIC(18, 4),
        unit_nav NUMERIC(18, 8),
        mom_fund_count NUMERIC(18, 4),
        mom_share_amount NUMERIC(18, 4),
        mom_nav_amount NUMERIC(18, 4),
        mom_unit_nav NUMERIC(18, 8),
        yoy_fund_count NUMERIC(18, 4),
        yoy_share_amount NUMERIC(18, 4),
        yoy_nav_amount NUMERIC(18, 4),
        yoy_unit_nav NUMERIC(18, 8),
        source_type VARCHAR(16) NOT NULL,
        source_file TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (month, category_name)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def build_fund_monitor_summary(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {
            "latest_month": None,
            "public_total_nav": None,
            "equity_fund_nav": None,
            "hybrid_fund_nav": None,
            "private_nav": None,
            "wealth_nav": None,
        }

    data = df.copy()
    data["month"] = pd.to_datetime(data["month"])
    latest_month = data["month"].max()
    latest_df = data[data["month"] == latest_month].copy()
    latest_df["nav_amount"] = pd.to_numeric(latest_df["nav_amount"], errors="coerce")

    def nav_of(name: str):
        rows = latest_df.loc[latest_df["category_name"] == name, "nav_amount"]
        return rows.iloc[0] if not rows.empty else None

    return {
        "latest_month": latest_month.strftime("%Y-%m"),
        "public_total_nav": nav_of("合计"),
        "equity_fund_nav": nav_of("其中：股票基金"),
        "hybrid_fund_nav": nav_of("其中：混合基金"),
        "private_nav": nav_of("私募证券投资基金"),
        "wealth_nav": nav_of("权益类理财产品"),
    }


def build_fund_monitor_trend_df(df: pd.DataFrame, value_field: str = "nav_amount") -> pd.DataFrame:
    data = df.copy()
    data["month"] = pd.to_datetime(data["month"])
    data["value"] = pd.to_numeric(data[value_field], errors="coerce")
    return data[["month", "category_name", "value"]].sort_values(["category_name", "month"]).reset_index(drop=True)


def build_fund_monitor_change_trend_df(df: pd.DataFrame, metric_key: str = "nav_amount") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["month", "category_name", "change_type", "metric", "value"])

    config = CHANGE_TREND_FIELDS[metric_key]
    data = df.copy()
    data["month"] = pd.to_datetime(data["month"])
    frames = []
    for change_type, field_name in (("环比", config["mom"]), ("同比", config["yoy"])):
        trend_df = data[["month", "category_name", field_name]].copy()
        trend_df["value"] = pd.to_numeric(trend_df[field_name], errors="coerce")
        trend_df["change_type"] = change_type
        trend_df["metric"] = config["label"]
        frames.append(trend_df.drop(columns=[field_name]))
    result = pd.concat(frames, ignore_index=True)
    result["change_order"] = result["change_type"].map({"环比": 0, "同比": 1})
    return result.sort_values(["category_name", "month", "change_order"]).drop(columns=["change_order"]).reset_index(drop=True)


def classify_fund_monitor_import_rows(incoming_df: pd.DataFrame, existing_df: pd.DataFrame) -> dict:
    incoming = incoming_df.copy()
    incoming["business_key"] = incoming["month"].astype(str) + "|" + incoming["category_name"].astype(str)
    if existing_df is None or existing_df.empty:
        return {
            "to_insert": incoming.drop(columns=["business_key"]).reset_index(drop=True),
            "to_overwrite": incoming.iloc[0:0].drop(columns=["business_key"]).copy(),
        }
    existing = existing_df.copy()
    existing["business_key"] = existing["month"].astype(str) + "|" + existing["category_name"].astype(str)
    existing_keys = set(existing["business_key"].tolist())
    to_insert = incoming[~incoming["business_key"].isin(existing_keys)].drop(columns=["business_key"]).reset_index(drop=True)
    to_overwrite = incoming[incoming["business_key"].isin(existing_keys)].drop(columns=["business_key"]).reset_index(drop=True)
    return {"to_insert": to_insert, "to_overwrite": to_overwrite}


def build_fund_monitor_upsert_rows(rows, source_type: str, source_file: str | None) -> list[dict]:
    payload_rows = []
    for row in rows:
        payload = {
            "month": pd.to_datetime(row["month"]).date(),
            "category_name": str(row["category_name"]).strip(),
            "category_group": row.get("category_group"),
            "category_level": row.get("category_level"),
            "sort_order": row.get("sort_order"),
            "source_type": source_type,
            "source_file": source_file,
        }
        for field in NUMERIC_FIELDS:
            payload[field] = float(row[field]) if row.get(field) is not None else None
        payload_rows.append(payload)
    return payload_rows


def upsert_fund_monitor_rows(engine: Engine, rows: list[dict]) -> int:
    ensure_fund_monitor_table(engine)
    columns = [
        "month",
        "category_name",
        "category_group",
        "category_level",
        "sort_order",
        *NUMERIC_FIELDS,
        "source_type",
        "source_file",
    ]
    update_columns = ["category_group", "category_level", "sort_order", *NUMERIC_FIELDS, "source_type", "source_file"]
    insert_sql = text(
        f"""
        INSERT INTO {TABLE_NAME} ({", ".join(columns)})
        VALUES ({", ".join(f":{col}" for col in columns)})
        ON CONFLICT (month, category_name) DO UPDATE SET
            {", ".join(f"{col} = EXCLUDED.{col}" for col in update_columns)},
            updated_at = NOW();
        """
    )
    with engine.begin() as conn:
        conn.execute(insert_sql, rows)
    return len(rows)


def load_fund_monitor_df(engine: Engine | None = None) -> pd.DataFrame:
    actual_engine = engine or get_engine()
    ensure_fund_monitor_table(actual_engine)
    with actual_engine.begin() as conn:
        return pd.read_sql(
            text(f"SELECT * FROM {TABLE_NAME} ORDER BY month ASC, sort_order ASC, category_name ASC"),
            conn,
        )


def to_fund_monitor_display_df(df: pd.DataFrame) -> pd.DataFrame:
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
