from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import sys
import time
import uuid
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.volume_fetcher import _init_tushare

DEFAULT_DB_HOST = "67.216.207.73"
DEFAULT_DB_PORT = 5432
DEFAULT_DB_NAME = "postgres"
DEFAULT_DB_USER = "postgres"
DEFAULT_DB_SSLMODE = "disable"
DEFAULT_DAILY_START = "20250101"
DEFAULT_FINANCIAL_START = "20240101"
DEFAULT_API_SLEEP = float(os.getenv("TUSHARE_SYNC_API_SLEEP", "0.2"))
DEFAULT_SCHEDULE_INTERVAL_MINUTES = int(os.getenv("TUSHARE_SYNC_INTERVAL_MINUTES", "60"))
DEFAULT_DAILY_LOOKBACK_DAYS = int(os.getenv("TUSHARE_SYNC_DAILY_LOOKBACK_DAYS", "1"))
DEFAULT_FINANCIAL_LOOKBACK_DAYS = int(os.getenv("TUSHARE_SYNC_FINANCIAL_LOOKBACK_DAYS", "30"))
DEFAULT_DAILY_MIN_COVERAGE_RATIO = float(os.getenv("TUSHARE_SYNC_DAILY_MIN_COVERAGE_RATIO", "0.9"))
DEFAULT_DAILY_PUBLISH_CUTOFF_HOUR = int(os.getenv("TUSHARE_SYNC_DAILY_PUBLISH_CUTOFF_HOUR", "20"))
DEFAULT_INDEX_PUBLISH_CUTOFF_HOUR = int(os.getenv("TUSHARE_SYNC_INDEX_PUBLISH_CUTOFF_HOUR", "20"))
DEFAULT_INDEX_CODES = [
    "000001.SH",
    "399001.SZ",
    "000016.SH",
    "000905.SH",
    "399005.SZ",
    "399006.SZ",
]
STOCK_CODES_CACHE: list[str] | None = None
STOCK_BASIC_CACHE: pd.DataFrame | None = None

DATASET_TABLES = {
    "stock_basic": "ts_stock_basic",
    "stock_company": "ts_stock_company",
    "income": "ts_stock_income",
    "balancesheet": "ts_stock_balancesheet",
    "cashflow": "ts_stock_cashflow",
    "fina_indicator": "ts_stock_fina_indicator",
    "daily": "ts_stock_daily",
    "daily_basic": "ts_stock_daily_basic",
    "index_dailybasic": "ts_index_dailybasic",
    "namechange": "ts_stock_namechange",
    "stk_week_month_adj": "ts_stk_week_month_adj",
    "cn_gdp": "ts_macro_cn_gdp",
    "cn_cpi": "ts_macro_cn_cpi",
    "cn_ppi": "ts_macro_cn_ppi",
    "cn_m": "ts_macro_cn_m",
    "shibor": "ts_macro_shibor",
    "shibor_lpr": "ts_macro_shibor_lpr",
}
STOCK_BASIC_FIELDS = ",".join(
    [
        "ts_code",
        "symbol",
        "name",
        "area",
        "industry",
        "fullname",
        "enname",
        "cnspell",
        "market",
        "exchange",
        "curr_type",
        "list_status",
        "list_date",
        "delist_date",
        "is_hs",
        "act_name",
        "act_ent_type",
    ]
)
NORMALIZED_VIEW_SPECS = {
    "stock_basic": {
        "view_name": "vw_ts_stock_basic",
        "columns": [
            ("text", "symbol"),
            ("text", "name"),
            ("text", "area"),
            ("text", "industry"),
            ("text", "fullname"),
            ("text", "enname"),
            ("text", "cnspell"),
            ("text", "market"),
            ("text", "exchange"),
            ("text", "curr_type"),
            ("text", "list_status"),
            ("date", "list_date"),
            ("date", "delist_date"),
            ("text", "is_hs"),
            ("text", "act_name"),
            ("text", "act_ent_type"),
        ],
    },
    "stock_company": {
        "view_name": "vw_ts_stock_company_base",
        "columns": [
            ("text", "exchange"),
            ("text", "chairman"),
            ("text", "manager"),
            ("text", "secretary"),
            ("numeric", "reg_capital"),
            ("date", "setup_date"),
            ("text", "province"),
            ("text", "city"),
            ("text", "website"),
            ("text", "email"),
            ("text", "office"),
            ("integer", "employees"),
            ("coalesce_text", "excel_main_business|main_business|main_business"),
            ("coalesce_text", "excel_product|business_scope|business_scope"),
            ("text", "main_business|ts_main_business"),
            ("text", "business_scope|ts_business_scope"),
            ("text", "excel_main_business|excel_main_business"),
            ("text", "excel_product|excel_product"),
            ("text", "introduction"),
        ],
    },
    "income": {
        "view_name": "vw_ts_stock_income",
        "columns": [
            ("date", "f_ann_date"),
            ("text", "report_type"),
            ("text", "comp_type"),
            ("numeric", "basic_eps"),
            ("numeric", "diluted_eps"),
            ("numeric", "total_revenue"),
            ("numeric", "revenue"),
            ("numeric", "operate_profit"),
            ("numeric", "total_profit"),
            ("numeric", "income_tax"),
            ("numeric", "n_income"),
            ("numeric", "n_income_attr_p"),
            ("numeric", "minority_gain"),
            ("numeric", "oth_compr_income"),
            ("numeric", "t_compr_income"),
            ("numeric", "ebit"),
            ("numeric", "ebitda"),
            ("numeric", "distable_profit"),
            ("text", "update_flag"),
        ],
    },
    "balancesheet": {
        "view_name": "vw_ts_stock_balancesheet",
        "columns": [
            ("date", "f_ann_date"),
            ("text", "report_type"),
            ("text", "comp_type"),
            ("numeric", "total_share"),
            ("numeric", "cap_rese"),
            ("numeric", "undistr_porfit"),
            ("numeric", "surplus_rese"),
            ("numeric", "money_cap"),
            ("numeric", "trad_asset"),
            ("numeric", "notes_receiv"),
            ("numeric", "accounts_receiv"),
            ("numeric", "oth_receiv"),
            ("numeric", "prepayment"),
            ("numeric", "inventories"),
            ("numeric", "total_cur_assets"),
            ("numeric", "fix_assets"),
            ("numeric", "total_assets"),
            ("numeric", "st_borr"),
            ("numeric", "lt_borr"),
            ("numeric", "notes_payable"),
            ("numeric", "acct_payable"),
            ("numeric", "total_cur_liab"),
            ("numeric", "total_ncl"),
            ("numeric", "total_liab"),
            ("numeric", "treasury_share"),
            ("numeric", "total_hldr_eqy_exc_min_int"),
            ("numeric", "minority_int"),
            ("numeric", "total_hldr_eqy_inc_min_int"),
            ("numeric", "total_liab_hldr_eqy"),
            ("text", "update_flag"),
        ],
    },
    "cashflow": {
        "view_name": "vw_ts_stock_cashflow",
        "columns": [
            ("date", "f_ann_date"),
            ("text", "report_type"),
            ("text", "comp_type"),
            ("numeric", "net_profit"),
            ("numeric", "finan_exp"),
            ("numeric", "c_fr_sale_sg"),
            ("numeric", "recp_tax_rends"),
            ("numeric", "c_paid_goods_s"),
            ("numeric", "c_paid_to_for_empl"),
            ("numeric", "c_paid_for_taxes"),
            ("numeric", "n_cashflow_act"),
            ("numeric", "stot_inflows_inv_act"),
            ("numeric", "stot_out_inv_act"),
            ("numeric", "n_cashflow_inv_act"),
            ("numeric", "stot_cash_in_fnc_act"),
            ("numeric", "stot_cashout_fnc_act"),
            ("numeric", "n_cash_flows_fnc_act"),
            ("numeric", "eff_fx_flu_cash"),
            ("numeric", "c_cash_equ_beg_period"),
            ("numeric", "c_cash_equ_end_period"),
            ("text", "update_flag"),
        ],
    },
    "daily": {
        "view_name": "vw_ts_stock_daily",
        "columns": [
            ("numeric", "open"),
            ("numeric", "high"),
            ("numeric", "low"),
            ("numeric", "close"),
            ("numeric", "pre_close"),
            ("numeric", "change"),
            ("numeric", "pct_chg"),
            ("numeric", "vol"),
            ("numeric", "amount"),
        ],
    },
    "fina_indicator": {
        "view_name": "vw_ts_stock_fina_indicator",
        "columns": [
            ("numeric", "eps"),
            ("numeric", "dt_eps"),
            ("numeric", "total_revenue_ps"),
            ("numeric", "revenue_ps"),
            ("numeric", "capital_rese_ps"),
            ("numeric", "surplus_rese_ps"),
            ("numeric", "undist_profit_ps"),
            ("numeric", "extra_item"),
            ("numeric", "profit_dedt"),
            ("numeric", "gross_margin"),
            ("numeric", "current_ratio"),
            ("numeric", "quick_ratio"),
            ("numeric", "cash_ratio"),
            ("numeric", "ar_turn"),
            ("numeric", "ca_turn"),
            ("numeric", "fa_turn"),
            ("numeric", "assets_turn"),
            ("numeric", "ebit"),
            ("numeric", "ebitda"),
            ("numeric", "fcff"),
            ("numeric", "fcfe"),
            ("numeric", "interestdebt"),
            ("numeric", "netdebt"),
            ("numeric", "tangible_asset"),
            ("numeric", "working_capital"),
            ("numeric", "invest_capital"),
            ("numeric", "retained_earnings"),
            ("numeric", "bps"),
            ("numeric", "ocfps"),
            ("numeric", "cfps"),
            ("numeric", "netprofit_margin"),
            ("numeric", "grossprofit_margin"),
            ("numeric", "roa"),
            ("numeric", "roe"),
            ("numeric", "roe_dt"),
            ("numeric", "roic"),
            ("numeric", "debt_to_assets"),
            ("text", "update_flag"),
        ],
    },
    "daily_basic": {
        "view_name": "vw_ts_stock_daily_basic",
        "columns": [
            ("numeric", "close"),
            ("numeric", "turnover_rate"),
            ("numeric", "turnover_rate_f"),
            ("numeric", "volume_ratio"),
            ("numeric", "pe"),
            ("numeric", "pe_ttm"),
            ("numeric", "pb"),
            ("numeric", "ps"),
            ("numeric", "ps_ttm"),
            ("numeric", "dv_ratio"),
            ("numeric", "dv_ttm"),
            ("numeric", "total_share"),
            ("numeric", "float_share"),
            ("numeric", "free_share"),
            ("numeric", "total_mv"),
            ("numeric", "circ_mv"),
        ],
    },
    "index_dailybasic": {
        "view_name": "vw_ts_index_dailybasic",
        "columns": [
            ("numeric", "close"),
            ("numeric", "turnover_rate"),
            ("numeric", "turnover_rate_f"),
            ("numeric", "pe"),
            ("numeric", "pe_ttm"),
            ("numeric", "pb"),
            ("numeric", "total_share"),
            ("numeric", "float_share"),
            ("numeric", "free_share"),
            ("numeric", "total_mv"),
            ("numeric", "float_mv"),
        ],
    },
    "namechange": {
        "view_name": "vw_ts_stock_namechange",
        "columns": [
            ("text", "name"),
            ("date", "start_date"),
            ("date", "end_date|nc_end_date"),
            ("date", "ann_date|nc_ann_date"),
            ("text", "change_reason"),
        ],
    },
    "stk_week_month_adj": {
        "view_name": "vw_ts_stk_week_month_adj",
        "columns": [
            ("numeric", "w_open"),
            ("numeric", "w_high"),
            ("numeric", "w_low"),
            ("numeric", "w_close"),
            ("numeric", "w_vol"),
            ("numeric", "w_amount"),
            ("numeric", "m_open"),
            ("numeric", "m_high"),
            ("numeric", "m_low"),
            ("numeric", "m_close"),
            ("numeric", "m_vol"),
            ("numeric", "m_amount"),
        ],
    },
    "cn_gdp": {
        "view_name": "vw_ts_macro_cn_gdp",
        "columns": [
            ("text", "quarter"),
            ("numeric", "gdp"),
            ("numeric", "gdp_yoy"),
            ("numeric", "pi"),
            ("numeric", "pi_yoy"),
            ("numeric", "si"),
            ("numeric", "si_yoy"),
            ("numeric", "ti"),
            ("numeric", "ti_yoy"),
        ],
    },
    "cn_cpi": {
        "view_name": "vw_ts_macro_cn_cpi",
        "columns": [
            ("text", "month"),
            ("numeric", "nt_val"),
            ("numeric", "nt_yoy"),
            ("numeric", "nt_mom"),
            ("numeric", "nt_accu"),
            ("numeric", "town_val"),
            ("numeric", "town_yoy"),
            ("numeric", "town_mom"),
            ("numeric", "town_accu"),
            ("numeric", "cnt_val"),
            ("numeric", "cnt_yoy"),
            ("numeric", "cnt_mom"),
            ("numeric", "cnt_accu"),
        ],
    },
    "cn_ppi": {
        "view_name": "vw_ts_macro_cn_ppi",
        "columns": [
            ("text", "month"),
            ("numeric", "ppi_yoy"),
            ("numeric", "ppi_mom"),
            ("numeric", "ppi_accu"),
        ],
    },
    "cn_m": {
        "view_name": "vw_ts_macro_cn_m",
        "columns": [
            ("text", "month"),
            ("numeric", "m0"),
            ("numeric", "m0_yoy"),
            ("numeric", "m0_mom"),
            ("numeric", "m1"),
            ("numeric", "m1_yoy"),
            ("numeric", "m1_mom"),
            ("numeric", "m2"),
            ("numeric", "m2_yoy"),
            ("numeric", "m2_mom"),
        ],
    },
    "shibor": {
        "view_name": "vw_ts_macro_shibor",
        "columns": [
            ("date", "date|published_date"),
            ("numeric", "on|rate_on"),
            ("numeric", "1w|rate_1w"),
            ("numeric", "2w|rate_2w"),
            ("numeric", "1m|rate_1m"),
            ("numeric", "3m|rate_3m"),
            ("numeric", "6m|rate_6m"),
            ("numeric", "9m|rate_9m"),
            ("numeric", "1y|rate_1y"),
        ],
    },
    "shibor_lpr": {
        "view_name": "vw_ts_macro_shibor_lpr",
        "columns": [
            ("date", "date|published_date"),
            ("numeric", "1y|lpr_1y"),
            ("numeric", "5y|lpr_5y"),
        ],
    },
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
FINANCIAL_DATASETS = {"income", "balancesheet", "cashflow", "fina_indicator"}
DAILY_DATASETS = {"daily", "daily_basic", "index_dailybasic", "stk_week_month_adj"}
MACRO_DATASETS = {"cn_gdp", "cn_cpi", "cn_ppi", "cn_m", "shibor", "shibor_lpr"}
BACKFILL_SKIP_TS_CODES = {
    "ts_stock_income": {
        "302132.SZ",
        "601268.SH",
        "920167.BJ",
        "920445.BJ",
        "920489.BJ",
        "920682.BJ",
        "920799.BJ",
        "920819.BJ",
    },
    "ts_stock_balancesheet": {
        "302132.SZ",
        "601268.SH",
        "920167.BJ",
        "920445.BJ",
        "920489.BJ",
        "920682.BJ",
        "920799.BJ",
        "920819.BJ",
    },
    "ts_stock_cashflow": {
        "302132.SZ",
        "601268.SH",
        "920167.BJ",
        "920445.BJ",
        "920489.BJ",
        "920682.BJ",
        "920799.BJ",
        "920819.BJ",
    },
}


def normalize_date_string(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).replace("-", "").strip()
    if not cleaned:
        return None
    datetime.strptime(cleaned, "%Y%m%d")
    return cleaned


def get_today_string() -> str:
    return datetime.now().strftime("%Y%m%d")


def shift_date_string(value: str, days: int) -> str:
    return (datetime.strptime(value, "%Y%m%d") + timedelta(days=days)).strftime("%Y%m%d")


def to_date_value(value):
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.date()
    if pd.isna(value):
        return None
    text_value = str(value).strip()
    if not text_value or text_value.lower() == "nan":
        return None
    if len(text_value) == 8 and text_value.isdigit():
        return datetime.strptime(text_value, "%Y%m%d").date()
    try:
        return pd.to_datetime(text_value).date()
    except Exception:
        return None


def quarter_to_trade_date(value: str | None):
    if value is None:
        return None
    text_value = str(value).strip().upper()
    if len(text_value) != 6 or not text_value.startswith(("19", "20")) or "Q" not in text_value:
        return None
    year = int(text_value[:4])
    quarter = text_value[-1]
    mapping = {
        "1": datetime(year, 3, 31),
        "2": datetime(year, 6, 30),
        "3": datetime(year, 9, 30),
        "4": datetime(year, 12, 31),
    }
    return mapping.get(quarter).date() if mapping.get(quarter) else None


def month_to_trade_date(value: str | None):
    if value is None:
        return None
    text_value = str(value).strip().replace("-", "")
    if len(text_value) != 6 or not text_value.isdigit():
        return None
    month_start = datetime.strptime(f"{text_value}01", "%Y%m%d")
    month_end = month_start + pd.offsets.MonthEnd(0)
    return pd.Timestamp(month_end).date()


def normalize_scalar(value):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    return value


def build_db_url():
    direct_url = os.getenv("ETF_PG_URL") or os.getenv("DATABASE_URL")
    if direct_url:
        return direct_url

    password = os.getenv("ETF_PG_PASSWORD") or os.getenv("PGPASSWORD")
    if not password:
        raise RuntimeError("未配置数据库密码，请设置 ETF_PG_PASSWORD 或 PGPASSWORD")

    return URL.create(
        "postgresql+psycopg2",
        username=os.getenv("ETF_PG_USER", DEFAULT_DB_USER),
        password=password,
        host=os.getenv("ETF_PG_HOST", DEFAULT_DB_HOST),
        port=int(os.getenv("ETF_PG_PORT", str(DEFAULT_DB_PORT))),
        database=os.getenv("ETF_PG_DATABASE", DEFAULT_DB_NAME),
        query={"sslmode": os.getenv("ETF_PG_SSLMODE", DEFAULT_DB_SSLMODE)},
    )


def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)


def build_json_text_expr(field_name: str) -> str:
    key, alias = field_name.split("|") if "|" in field_name else (field_name, field_name)
    return f"NULLIF(payload->>'{key}', '') AS {alias}"


def build_json_numeric_expr(field_name: str) -> str:
    key, alias = field_name.split("|") if "|" in field_name else (field_name, field_name)
    return (
        f"CASE WHEN NULLIF(payload->>'{key}', '') ~ '^[-+]?(\\d+(\\.\\d+)?|\\.\\d+)$' "
        f"THEN (payload->>'{key}')::numeric END AS {alias}"
    )


def build_json_integer_expr(field_name: str) -> str:
    key, alias = field_name.split("|") if "|" in field_name else (field_name, field_name)
    return (
        f"CASE WHEN NULLIF(payload->>'{key}', '') ~ '^[-+]?\\d+$' "
        f"THEN (payload->>'{key}')::integer END AS {alias}"
    )


def build_json_date_expr(field_name: str) -> str:
    key, alias = field_name.split("|") if "|" in field_name else (field_name, field_name)
    return (
        f"CASE WHEN NULLIF(payload->>'{key}', '') ~ '^\\d{{8}}$' "
        f"THEN TO_DATE(payload->>'{key}', 'YYYYMMDD') END AS {alias}"
    )


def build_json_coalesce_text_expr(field_name: str) -> str:
    parts = field_name.split("|")
    alias = parts[-1]
    keys = parts[:-1]
    coalesce_args = ", ".join(f"NULLIF(payload->>'{k}', '')" for k in keys)
    return f"COALESCE({coalesce_args}) AS {alias}"


def build_view_column_expr(column_type: str, field_name: str) -> str:
    builders = {
        "text": build_json_text_expr,
        "numeric": build_json_numeric_expr,
        "integer": build_json_integer_expr,
        "date": build_json_date_expr,
        "coalesce_text": build_json_coalesce_text_expr,
    }
    if column_type not in builders:
        raise ValueError(f"不支持的视图字段类型: {column_type}")
    return builders[column_type](field_name)


def ensure_normalized_views(engine: Engine):
    common_columns = [
        "business_key",
        "dataset_name",
        "ts_code",
        "trade_date",
        "ann_date",
        "end_date",
        "period",
        "record_hash",
        "ingested_at",
        "payload",
    ]
    with engine.begin() as conn:
        for dataset_name, view_spec in NORMALIZED_VIEW_SPECS.items():
            table_name = DATASET_TABLES[dataset_name]
            select_columns = common_columns + [
                build_view_column_expr(column_type, field_name)
                for column_type, field_name in view_spec["columns"]
            ]
            sql = f"""
            CREATE OR REPLACE VIEW {view_spec["view_name"]} AS
            SELECT
                {", ".join(select_columns)}
            FROM {table_name}
            """
            conn.execute(text(sql))


def ensure_storage_objects(engine: Engine):
    ensure_job_table(engine)
    for table_name in DATASET_TABLES.values():
        ensure_landing_table(engine, table_name)
    ensure_normalized_views(engine)
    ensure_custom_table_and_view(engine)


def ensure_landing_table(engine: Engine, table_name: str):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        business_key TEXT PRIMARY KEY,
        dataset_name VARCHAR(64) NOT NULL,
        ts_code VARCHAR(20),
        trade_date DATE,
        ann_date DATE,
        end_date DATE,
        period VARCHAR(20),
        record_hash VARCHAR(64) NOT NULL,
        payload JSONB NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_ts_code ON {table_name}(ts_code);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_end_date ON {table_name}(end_date);
    """
    with engine.begin() as conn:
        for statement in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(statement))


def ensure_job_table(engine: Engine):
    sql = """
    CREATE TABLE IF NOT EXISTS tushare_sync_jobs (
        dataset_name VARCHAR(64) PRIMARY KEY,
        table_name VARCHAR(128) NOT NULL,
        rows_written INTEGER NOT NULL DEFAULT 0,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        status VARCHAR(20) NOT NULL,
        message TEXT
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def update_job_status(
    engine: Engine,
    dataset_name: str,
    table_name: str,
    rows_written: int,
    started_at: datetime,
    status: str,
    message: str | None = None,
):
    sql = """
    INSERT INTO tushare_sync_jobs (
        dataset_name, table_name, rows_written, started_at, finished_at, status, message
    ) VALUES (
        :dataset_name, :table_name, :rows_written, :started_at, NOW(), :status, :message
    )
    ON CONFLICT (dataset_name) DO UPDATE SET
        table_name = EXCLUDED.table_name,
        rows_written = EXCLUDED.rows_written,
        started_at = EXCLUDED.started_at,
        finished_at = EXCLUDED.finished_at,
        status = EXCLUDED.status,
        message = EXCLUDED.message;
    """
    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "dataset_name": dataset_name,
                "table_name": table_name,
                "rows_written": rows_written,
                "started_at": started_at,
                "status": status,
                "message": message,
            },
        )


def get_max_date(engine: Engine, table_name: str, column_name: str) -> str | None:
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT MAX({column_name}) FROM {table_name}")).fetchone()
    if row and row[0]:
        return row[0].strftime("%Y%m%d")
    return None


def get_min_dates_by_ts_code(engine: Engine, table_name: str, column_name: str) -> dict[str, str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT ts_code, MIN({column_name}) AS min_value
                FROM {table_name}
                WHERE ts_code IS NOT NULL
                  AND {column_name} IS NOT NULL
                GROUP BY ts_code
                """
            )
        ).fetchall()
    result = {}
    for ts_code, min_value in rows:
        if ts_code and min_value:
            result[str(ts_code).strip()] = min_value.strftime("%Y%m%d")
    return result


def resolve_incremental_start_date(
    base_start: str,
    existing_max: str | None,
    lookback_days: int = 0,
    next_day: bool = False,
) -> str:
    if not existing_max:
        return base_start

    if next_day:
        candidate = shift_date_string(existing_max, 1)
    else:
        candidate = shift_date_string(existing_max, -lookback_days)
    return max(base_start, candidate)


def build_stock_backfill_targets(
    pro,
    engine: Engine,
    table_name: str,
    column_name: str,
    run_end_date: str,
) -> list[tuple[str, str, str]]:
    listing_dates = get_stock_listing_dates(pro)
    existing_min_dates = get_min_dates_by_ts_code(engine, table_name, column_name)
    skip_ts_codes = BACKFILL_SKIP_TS_CODES.get(table_name, set())
    targets = []

    for ts_code, listing_date in listing_dates.items():
        if ts_code in skip_ts_codes:
            continue
        if listing_date > run_end_date:
            continue
        existing_min_date = existing_min_dates.get(ts_code)
        backfill_end_date = shift_date_string(existing_min_date, -1) if existing_min_date else run_end_date
        if listing_date > backfill_end_date:
            continue
        targets.append((ts_code, listing_date, backfill_end_date))

    targets.sort(key=lambda item: (item[1], item[0]))
    return targets


def resolve_business_key(dataset_name: str, payload: dict) -> str:
    preferred_keys = {
        "stock_basic": ["ts_code"],
        "stock_company": ["ts_code"],
        "daily": ["ts_code", "trade_date"],
        "daily_basic": ["ts_code", "trade_date"],
        "index_dailybasic": ["ts_code", "trade_date"],
        "stk_week_month_adj": ["ts_code", "trade_date"],
        "cn_gdp": ["period", "trade_date"],
        "cn_cpi": ["period", "trade_date"],
        "cn_ppi": ["period", "trade_date"],
        "cn_m": ["period", "trade_date"],
        "shibor": ["trade_date"],
        "shibor_lpr": ["trade_date"],
        "income": ["ts_code", "ann_date", "end_date", "report_type", "comp_type"],
        "balancesheet": ["ts_code", "ann_date", "end_date", "report_type", "comp_type"],
        "cashflow": ["ts_code", "ann_date", "end_date", "report_type", "comp_type"],
        "fina_indicator": ["ts_code", "ann_date", "end_date"],
    }
    values = [str(payload.get(key)).strip() for key in preferred_keys.get(dataset_name, []) if payload.get(key) not in (None, "", "nan")]
    if values:
        return "|".join([dataset_name, *values])
    payload_text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return f"{dataset_name}|{hashlib.sha256(payload_text.encode('utf-8')).hexdigest()}"


def prepare_records(dataset_name: str, df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "business_key",
                "dataset_name",
                "ts_code",
                "trade_date",
                "ann_date",
                "end_date",
                "period",
                "record_hash",
                "payload",
            ]
        )

    rows = []
    for record in df.to_dict(orient="records"):
        normalized = {str(key): normalize_scalar(value) for key, value in record.items()}
        payload_text = json.dumps(normalized, ensure_ascii=False, sort_keys=True, default=str)
        rows.append(
            {
                "business_key": resolve_business_key(dataset_name, normalized),
                "dataset_name": dataset_name,
                "ts_code": normalized.get("ts_code"),
                "trade_date": to_date_value(normalized.get("trade_date")),
                "ann_date": to_date_value(normalized.get("ann_date")),
                "end_date": to_date_value(normalized.get("end_date") or normalized.get("period")),
                "period": str(normalized.get("period")).strip() if normalized.get("period") not in (None, "", "nan") else None,
                "record_hash": hashlib.sha256(payload_text.encode("utf-8")).hexdigest(),
                "payload": payload_text,
            }
        )
    prepared = pd.DataFrame(rows).drop_duplicates(subset=["business_key"], keep="last")
    return prepared


def upsert_records(engine: Engine, table_name: str, records: pd.DataFrame) -> int:
    if records.empty:
        return 0

    tmp_table = f"_tmp_{table_name}_{uuid.uuid4().hex[:8]}"
    with engine.begin() as conn:
        records.to_sql(tmp_table, conn, if_exists="replace", index=False, method="multi", chunksize=1000)
        conn.execute(
            text(
                f"""
                INSERT INTO {table_name} (
                    business_key, dataset_name, ts_code, trade_date, ann_date, end_date, period, record_hash, payload
                )
                SELECT
                    business_key,
                    dataset_name,
                    ts_code,
                    NULLIF(trade_date::text, '')::date,
                    NULLIF(ann_date::text, '')::date,
                    NULLIF(end_date::text, '')::date,
                    period,
                    record_hash,
                    payload::jsonb
                FROM {tmp_table}
                ON CONFLICT (business_key) DO UPDATE SET
                    dataset_name = EXCLUDED.dataset_name,
                    ts_code = EXCLUDED.ts_code,
                    trade_date = EXCLUDED.trade_date,
                    ann_date = EXCLUDED.ann_date,
                    end_date = EXCLUDED.end_date,
                    period = EXCLUDED.period,
                    record_hash = EXCLUDED.record_hash,
                    payload = EXCLUDED.payload,
                    ingested_at = NOW()
                """
            )
        )
        conn.execute(text(f"DROP TABLE IF EXISTS {tmp_table}"))
    return len(records)


def write_dataset_records(engine: Engine, dataset_name: str, table_name: str, df: pd.DataFrame) -> int:
    prepared = prepare_records(dataset_name, df)
    return upsert_records(engine, table_name, prepared)


def ensure_custom_table_and_view(engine: Engine):
    sql = """
    CREATE TABLE IF NOT EXISTS ts_stock_custom_info (
        ts_code VARCHAR(20) PRIMARY KEY,
        custom_main_business TEXT,
        custom_product TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE OR REPLACE VIEW vw_ts_stock_company AS
    SELECT
        t.business_key,
        t.dataset_name,
        t.ts_code,
        t.trade_date,
        t.ann_date,
        t.end_date,
        t.period,
        t.record_hash,
        t.ingested_at,
        t.payload,
        t.exchange,
        t.chairman,
        t.manager,
        t.secretary,
        t.reg_capital,
        t.setup_date,
        t.province,
        t.city,
        t.website,
        t.email,
        t.office,
        t.employees,
        COALESCE(c.custom_main_business, t.main_business) AS main_business,
        COALESCE(c.custom_product, t.business_scope) AS business_scope,
        t.ts_main_business,
        t.ts_business_scope,
        t.excel_main_business,
        t.excel_product,
        t.introduction
    FROM vw_ts_stock_company_base t
    LEFT JOIN ts_stock_custom_info c ON t.ts_code = c.ts_code;
    """
    with engine.begin() as conn:
        for statement in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(statement))


def combine_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    valid_frames = [frame for frame in frames if frame is not None and not frame.empty]
    if not valid_frames:
        return pd.DataFrame()
    return pd.concat(valid_frames, ignore_index=True)


def get_open_trade_dates(pro, start_date: str, end_date: str) -> list[str]:
    calendar_df = pro.trade_cal(exchange="SSE", start_date=start_date, end_date=end_date)
    if calendar_df is None or calendar_df.empty:
        return []
    filtered = calendar_df[calendar_df["is_open"] == 1].copy()
    if filtered.empty:
        return []
    return sorted(filtered["cal_date"].astype(str).tolist())


def get_required_trade_dates(pro, start_date: str, end_date: str, publish_cutoff_hour: int) -> list[str]:
    trade_dates = get_open_trade_dates(pro, start_date, end_date)
    if not trade_dates:
        return []

    now = datetime.now()
    today_str = now.strftime("%Y%m%d")
    required_dates = []
    for trade_date in trade_dates:
        if trade_date > today_str:
            continue
        if trade_date == today_str and now.hour < publish_cutoff_hour:
            continue
        required_dates.append(trade_date)
    return required_dates


def generate_quarter_periods(start_date: str, end_date: str) -> list[str]:
    start = datetime.strptime(start_date, "%Y%m%d").date()
    end = datetime.strptime(end_date, "%Y%m%d").date()
    periods = []
    for year in range(start.year, end.year + 1):
        for suffix in ["0331", "0630", "0930", "1231"]:
            current = datetime.strptime(f"{year}{suffix}", "%Y%m%d").date()
            if start <= current <= end:
                periods.append(current.strftime("%Y%m%d"))
    return periods


def fetch_stock_basic(pro) -> pd.DataFrame:
    global STOCK_BASIC_CACHE

    if STOCK_BASIC_CACHE is not None:
        return STOCK_BASIC_CACHE.copy()

    frames = []
    for status in ["L", "D", "P"]:
        df = pro.stock_basic(list_status=status, fields=STOCK_BASIC_FIELDS)
        if df is not None and not df.empty:
            frames.append(df)
        time.sleep(DEFAULT_API_SLEEP)
    result = combine_frames(frames)
    if not result.empty and "ts_code" in result.columns:
        result = result.drop_duplicates(subset=["ts_code"], keep="last")
    STOCK_BASIC_CACHE = result.copy()
    return result


def fetch_stock_company(pro) -> pd.DataFrame:
    frames = []
    for exchange in ["SSE", "SZSE", "BSE"]:
        df = pro.stock_company(exchange=exchange)
        if df is not None and not df.empty:
            frames.append(df)
        time.sleep(DEFAULT_API_SLEEP)
    result = combine_frames(frames)
    if not result.empty and "ts_code" in result.columns:
        result = result.drop_duplicates(subset=["ts_code"], keep="last")

    try:
        excel_path = os.path.join(PROJECT_ROOT, "resources", "股票基本信息汇总表_20260331 .xlsx")
        if os.path.exists(excel_path):
            excel_df = pd.read_excel(excel_path, usecols=["股票代码", "主要业务", "产品"])
            excel_df = excel_df.rename(columns={
                "股票代码": "ts_code",
                "主要业务": "excel_main_business",
                "产品": "excel_product"
            })
            excel_df["ts_code"] = excel_df["ts_code"].astype(str).str.strip()
            excel_df = excel_df.drop_duplicates(subset=["ts_code"], keep="last")

            if not result.empty:
                result = pd.merge(result, excel_df, on="ts_code", how="left")
            else:
                result = excel_df
    except Exception as e:
        logger.warning(f"合并股票基本信息汇总表_20260331数据失败: {e}")

    return result

def fetch_namechange(pro) -> pd.DataFrame:
    frames = []
    limit = 5000
    offset = 0
    while True:
        try:
            df = pro.namechange(limit=limit, offset=offset)
            if df is None or df.empty:
                break
            frames.append(df)
            if len(df) < limit:
                break
            offset += limit
            time.sleep(DEFAULT_API_SLEEP)
        except Exception as e:
            logger.warning(f"fetch_namechange failed at offset {offset}: {e}")
            break
    return combine_frames(frames)


def filter_active_stock_basic(stock_basic_df: pd.DataFrame) -> pd.DataFrame:
    if stock_basic_df is None or stock_basic_df.empty:
        return pd.DataFrame()

    filtered = stock_basic_df.copy()
    if "list_status" in filtered.columns:
        status_series = filtered["list_status"].fillna("").astype(str).str.strip().str.upper()
        if "delist_date" in filtered.columns:
            delist_series = filtered["delist_date"].fillna("").astype(str).str.strip()
            filtered = filtered.loc[
                (status_series == "L")
                | ((status_series == "") & (delist_series == ""))
            ].copy()
        else:
            filtered = filtered.loc[(status_series == "L") | (status_series == "")].copy()
    elif "delist_date" in filtered.columns:
        delist_series = filtered["delist_date"].fillna("").astype(str).str.strip()
        filtered = filtered.loc[delist_series == ""].copy()

    return filtered


def get_stock_codes(pro) -> list[str]:
    global STOCK_CODES_CACHE

    if STOCK_CODES_CACHE is not None:
        return STOCK_CODES_CACHE

    stock_basic_df = filter_active_stock_basic(fetch_stock_basic(pro))
    if stock_basic_df.empty or "ts_code" not in stock_basic_df.columns:
        raise RuntimeError("无法获取股票列表，不能继续抓取财务数据")

    STOCK_CODES_CACHE = sorted(
        {
            str(value).strip()
            for value in stock_basic_df["ts_code"].tolist()
            if str(value).strip() and str(value).strip().lower() != "nan"
        }
    )
    return STOCK_CODES_CACHE


def get_stock_listing_dates(pro) -> dict[str, str]:
    stock_basic_df = filter_active_stock_basic(fetch_stock_basic(pro))
    if stock_basic_df.empty or "ts_code" not in stock_basic_df.columns or "list_date" not in stock_basic_df.columns:
        raise RuntimeError("无法获取股票上市日期，不能继续回补历史数据")

    listing_dates = {}
    for record in stock_basic_df[["ts_code", "list_date"]].to_dict(orient="records"):
        ts_code = str(record.get("ts_code")).strip()
        list_date = normalize_date_string(record.get("list_date"))
        if ts_code and ts_code.lower() != "nan" and list_date:
            listing_dates[ts_code] = list_date
    return listing_dates


def validate_daily_basic_result(pro, df: pd.DataFrame, required_trade_dates: list[str], min_coverage_ratio: float):
    if not required_trade_dates:
        return

    returned_counts: dict[str, int] = {}
    if df is not None and not df.empty and {"trade_date", "ts_code"}.issubset(df.columns):
        count_df = df[["trade_date", "ts_code"]].copy()
        count_df["trade_date"] = count_df["trade_date"].astype(str).str.replace("-", "", regex=False)
        count_df["ts_code"] = count_df["ts_code"].astype(str).str.strip()
        count_df = count_df.loc[count_df["ts_code"] != ""]
        returned_counts = count_df.groupby("trade_date")["ts_code"].nunique().to_dict()

    active_stock_count = len(get_stock_codes(pro))
    minimum_required = max(1, math.ceil(active_stock_count * min_coverage_ratio))
    insufficient_dates = []
    for trade_date in required_trade_dates:
        row_count = int(returned_counts.get(trade_date, 0))
        if row_count < minimum_required:
            insufficient_dates.append(f"{trade_date}={row_count}")

    if insufficient_dates:
        raise RuntimeError(
            "daily_basic 数据覆盖不足，"
            f"预期每个交易日至少返回 {minimum_required} 只股票"
            f"（当前活跃股票 {active_stock_count}，阈值 {min_coverage_ratio:.0%}），"
            f"异常日期: {', '.join(insufficient_dates[:10])}"
        )


def validate_index_dailybasic_result(df: pd.DataFrame, required_trade_dates: list[str], index_codes: list[str]):
    if not required_trade_dates or not index_codes:
        return

    existing_pairs: set[tuple[str, str]] = set()
    if df is not None and not df.empty and {"trade_date", "ts_code"}.issubset(df.columns):
        pair_df = df[["trade_date", "ts_code"]].copy()
        pair_df["trade_date"] = pair_df["trade_date"].astype(str).str.replace("-", "", regex=False)
        pair_df["ts_code"] = pair_df["ts_code"].astype(str).str.strip()
        existing_pairs = {
            (str(record["ts_code"]).strip(), str(record["trade_date"]).strip())
            for record in pair_df.to_dict(orient="records")
            if str(record["ts_code"]).strip() and str(record["trade_date"]).strip()
        }

    missing_pairs = []
    for trade_date in required_trade_dates:
        for ts_code in index_codes:
            if (ts_code, trade_date) not in existing_pairs:
                missing_pairs.append(f"{ts_code}@{trade_date}")

    if missing_pairs:
        raise RuntimeError(
            "index_dailybasic 数据缺失，"
            f"缺少 {len(missing_pairs)} 条预期记录，示例: {', '.join(missing_pairs[:10])}"
        )


def filter_by_report_period(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    for column_name in ["end_date", "period", "trade_date", "ann_date"]:
        if column_name not in df.columns:
            continue
        normalized = df[column_name].astype(str).str.replace("-", "", regex=False).str.slice(0, 8)
        mask = normalized.str.fullmatch(r"\d{8}") & (normalized >= start_date) & (normalized <= end_date)
        filtered = df.loc[mask].copy()
        return filtered

    return df.copy()


def fetch_financial_dataset_by_stock(pro, endpoint_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    endpoint = getattr(pro, endpoint_name)
    stock_codes = get_stock_codes(pro)
    frames = []
    failures = []

    for index, ts_code in enumerate(stock_codes, start=1):
        success = False
        for attempt in range(1, 4):
            try:
                logger.info("抓取 %s ts_code=%s (%s/%s) attempt=%s", endpoint_name, ts_code, index, len(stock_codes), attempt)
                df = endpoint(ts_code=ts_code, start_date=start_date, end_date=end_date)
                df = filter_by_report_period(df, start_date, end_date)
                if df is not None and not df.empty:
                    frames.append(df)
                    logger.info("%s ts_code=%s 返回 %s 行", endpoint_name, ts_code, len(df))
                success = True
                break
            except Exception as exc:
                logger.warning("%s ts_code=%s attempt=%s 调用失败: %s", endpoint_name, ts_code, attempt, exc)
                time.sleep(DEFAULT_API_SLEEP)
        if not success:
            failures.append(ts_code)
        time.sleep(DEFAULT_API_SLEEP)

    if failures:
        logger.warning("%s 有 %s 只股票抓取失败，示例: %s", endpoint_name, len(failures), ", ".join(failures[:10]))

    return combine_frames(frames)


def fetch_financial_dataset_missing_history(pro, engine: Engine, endpoint_name: str, table_name: str, run_end_date: str) -> pd.DataFrame:
    endpoint = getattr(pro, endpoint_name)
    targets = build_stock_backfill_targets(pro, engine, table_name, "end_date", run_end_date)
    failures = []
    total_written = 0

    logger.info("%s 进入安全历史回补模式，目标股票数=%s", endpoint_name, len(targets))
    for index, (ts_code, start_date, end_date) in enumerate(targets, start=1):
        success = False
        for attempt in range(1, 4):
            try:
                logger.info(
                    "回补 %s ts_code=%s range=%s~%s (%s/%s) attempt=%s",
                    endpoint_name,
                    ts_code,
                    start_date,
                    end_date,
                    index,
                    len(targets),
                    attempt,
                )
                df = endpoint(ts_code=ts_code, start_date=start_date, end_date=end_date)
                df = filter_by_report_period(df, start_date, end_date)
                if df is not None and not df.empty:
                    written = write_dataset_records(engine, endpoint_name, table_name, df)
                    total_written += written
                    logger.info("%s ts_code=%s 回补 %s 行，累计写入 %s 行", endpoint_name, ts_code, len(df), total_written)
                success = True
                break
            except Exception as exc:
                logger.warning("%s ts_code=%s attempt=%s 历史回补失败: %s", endpoint_name, ts_code, attempt, exc)
                time.sleep(DEFAULT_API_SLEEP)
        if not success:
            failures.append(ts_code)
        time.sleep(DEFAULT_API_SLEEP)

    if failures:
        logger.warning("%s 历史回补有 %s 只股票失败，示例: %s", endpoint_name, len(failures), ", ".join(failures[:10]))

    return total_written


def fetch_financial_dataset(pro, endpoint_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    endpoint_names = [name for name in [f"{endpoint_name}_vip", endpoint_name] if hasattr(pro, name)]
    if not endpoint_names:
        raise RuntimeError(f"Tushare 客户端不存在接口: {endpoint_name}")

    periods = generate_quarter_periods(start_date, end_date)
    frames = []
    vip_error = None
    for period in periods:
        last_error = None
        for current_name in endpoint_names:
            endpoint = getattr(pro, current_name)
            try:
                logger.info("抓取 %s period=%s", current_name, period)
                df = endpoint(period=period)
                if df is not None and not df.empty:
                    frames.append(df)
                    logger.info("%s period=%s 返回 %s 行", current_name, period, len(df))
                else:
                    logger.info("%s period=%s 无数据", current_name, period)
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                logger.warning("%s period=%s 调用失败: %s", current_name, period, exc)
                time.sleep(DEFAULT_API_SLEEP)
        if last_error is not None:
            vip_error = last_error
            frames = []
            break
        time.sleep(DEFAULT_API_SLEEP)
    combined = combine_frames(frames)
    if not combined.empty:
        return combined

    if hasattr(pro, endpoint_name):
        if vip_error is not None:
            logger.info("%s 改为按股票逐只抓取", endpoint_name)
        return fetch_financial_dataset_by_stock(pro, endpoint_name, start_date, end_date)

    raise RuntimeError(f"{endpoint_name} 无法抓取，请确认当前 Tushare 账号权限") from vip_error


def fetch_daily(pro, start_date: str, end_date: str) -> pd.DataFrame:
    trade_dates = get_open_trade_dates(pro, start_date, end_date)
    frames = []
    for trade_date in trade_dates:
        logger.info("抓取 daily trade_date=%s", trade_date)
        df = pro.daily(trade_date=trade_date)
        if df is not None and not df.empty:
            frames.append(df)
            logger.info("daily trade_date=%s 返回 %s 行", trade_date, len(df))
        else:
            logger.info("daily trade_date=%s 无数据", trade_date)
        time.sleep(DEFAULT_API_SLEEP)
    return combine_frames(frames)


def fetch_daily_missing_history(pro, engine: Engine, table_name: str, run_end_date: str) -> pd.DataFrame:
    return fetch_daily_dataset_missing_history(pro, engine, "daily", table_name, run_end_date)


def fetch_daily_basic(pro, start_date: str, end_date: str) -> pd.DataFrame:
    trade_dates = get_open_trade_dates(pro, start_date, end_date)
    frames = []
    for trade_date in trade_dates:
        logger.info("抓取 daily_basic trade_date=%s", trade_date)
        df = pro.daily_basic(trade_date=trade_date)
        if df is not None and not df.empty:
            frames.append(df)
            logger.info("daily_basic trade_date=%s 返回 %s 行", trade_date, len(df))
        else:
            logger.info("daily_basic trade_date=%s 无数据", trade_date)
        time.sleep(DEFAULT_API_SLEEP)
    return combine_frames(frames)


def normalize_stk_week_month_adj_frame(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["ts_code", "trade_date"])

    selected = df.copy()
    rename_map = {
        "open": f"{prefix}_open",
        "high": f"{prefix}_high",
        "low": f"{prefix}_low",
        "close": f"{prefix}_close",
        "vol": f"{prefix}_vol",
        "amount": f"{prefix}_amount",
        "end_date": f"{prefix}_end_date",
    }
    keep_columns = ["ts_code", "trade_date", *rename_map.keys()]
    existing_columns = [column for column in keep_columns if column in selected.columns]
    selected = selected[existing_columns].rename(columns=rename_map)
    return selected.drop_duplicates(subset=["ts_code", "trade_date"], keep="last")


def normalize_macro_period_frame(df: pd.DataFrame, period_col: str, date_resolver) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    normalized = df.copy()
    normalized[period_col] = normalized[period_col].astype(str).str.strip()
    normalized["trade_date"] = normalized[period_col].map(date_resolver)
    normalized["end_date"] = normalized["trade_date"]
    normalized["period"] = normalized[period_col]
    normalized = normalized.dropna(subset=["trade_date"])
    return normalized.drop_duplicates(subset=[period_col], keep="last")


def normalize_macro_daily_frame(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    normalized = df.copy()
    normalized[date_col] = normalized[date_col].astype(str).str.strip()
    normalized["trade_date"] = normalized[date_col].map(to_date_value)
    normalized["end_date"] = normalized["trade_date"]
    normalized["period"] = normalized[date_col]
    normalized = normalized.dropna(subset=["trade_date"])
    return normalized.drop_duplicates(subset=[date_col], keep="last")


def merge_stk_week_month_adj_frames(weekly_df: pd.DataFrame, monthly_df: pd.DataFrame) -> pd.DataFrame:
    weekly_normalized = normalize_stk_week_month_adj_frame(weekly_df, "w")
    monthly_normalized = normalize_stk_week_month_adj_frame(monthly_df, "m")

    if weekly_normalized.empty and monthly_normalized.empty:
        return pd.DataFrame()
    if weekly_normalized.empty:
        merged = monthly_normalized.copy()
    elif monthly_normalized.empty:
        merged = weekly_normalized.copy()
    else:
        merged = weekly_normalized.merge(monthly_normalized, on=["ts_code", "trade_date"], how="outer")

    for column in ["w_end_date", "m_end_date"]:
        if column not in merged.columns:
            merged[column] = None

    merged["end_date"] = merged["m_end_date"].fillna(merged["w_end_date"])
    return merged


def fetch_stk_week_month_adj(pro, start_date: str, end_date: str) -> pd.DataFrame:
    trade_dates = get_open_trade_dates(pro, start_date, end_date)
    frames = []
    for trade_date in trade_dates:
        logger.info("抓取 stk_week_month_adj trade_date=%s", trade_date)
        weekly_df = pro.stk_week_month_adj(trade_date=trade_date, freq="week")
        monthly_df = pro.stk_week_month_adj(trade_date=trade_date, freq="month")
        df = merge_stk_week_month_adj_frames(weekly_df, monthly_df)
        if df is not None and not df.empty:
            frames.append(df)
            logger.info("stk_week_month_adj trade_date=%s 返回 %s 行", trade_date, len(df))
        else:
            logger.info("stk_week_month_adj trade_date=%s 无数据", trade_date)
        time.sleep(DEFAULT_API_SLEEP)
    return combine_frames(frames)


def fetch_cn_gdp(pro) -> pd.DataFrame:
    df = pro.cn_gdp()
    return normalize_macro_period_frame(df, "quarter", quarter_to_trade_date)


def fetch_cn_cpi(pro) -> pd.DataFrame:
    df = pro.cn_cpi()
    return normalize_macro_period_frame(df, "month", month_to_trade_date)


def fetch_cn_ppi(pro) -> pd.DataFrame:
    df = pro.cn_ppi()
    return normalize_macro_period_frame(df, "month", month_to_trade_date)


def fetch_cn_m(pro) -> pd.DataFrame:
    df = pro.cn_m()
    return normalize_macro_period_frame(df, "month", month_to_trade_date)


def fetch_shibor(pro) -> pd.DataFrame:
    df = pro.shibor()
    return normalize_macro_daily_frame(df, "date")


def fetch_shibor_lpr(pro) -> pd.DataFrame:
    df = pro.shibor_lpr()
    return normalize_macro_daily_frame(df, "date")


def fetch_stk_week_month_adj_missing_history(pro, engine: Engine, table_name: str, run_end_date: str) -> int:
    targets = build_stock_backfill_targets(pro, engine, table_name, "trade_date", run_end_date)
    failures = []
    total_written = 0

    logger.info("stk_week_month_adj 进入安全历史回补模式，目标股票数=%s", len(targets))
    for index, (ts_code, start_date, end_date) in enumerate(targets, start=1):
        success = False
        for attempt in range(1, 4):
            try:
                logger.info(
                    "回补 stk_week_month_adj ts_code=%s range=%s~%s (%s/%s) attempt=%s",
                    ts_code,
                    start_date,
                    end_date,
                    index,
                    len(targets),
                    attempt,
                )
                weekly_df = pro.stk_week_month_adj(ts_code=ts_code, start_date=start_date, end_date=end_date, freq="week")
                monthly_df = pro.stk_week_month_adj(ts_code=ts_code, start_date=start_date, end_date=end_date, freq="month")
                df = merge_stk_week_month_adj_frames(weekly_df, monthly_df)
                if df is not None and not df.empty:
                    written = write_dataset_records(engine, "stk_week_month_adj", table_name, df)
                    total_written += written
                    logger.info("stk_week_month_adj ts_code=%s 回补 %s 行，累计写入 %s 行", ts_code, len(df), total_written)
                success = True
                break
            except Exception as exc:
                logger.warning("stk_week_month_adj ts_code=%s attempt=%s 历史回补失败: %s", ts_code, attempt, exc)
                time.sleep(DEFAULT_API_SLEEP)
        if not success:
            failures.append(ts_code)
        time.sleep(DEFAULT_API_SLEEP)

    if failures:
        logger.warning("stk_week_month_adj 历史回补有 %s 只股票失败，示例: %s", len(failures), ", ".join(failures[:10]))

    return total_written


def fetch_daily_dataset_missing_history(pro, engine: Engine, endpoint_name: str, table_name: str, run_end_date: str) -> pd.DataFrame:
    targets = build_stock_backfill_targets(pro, engine, table_name, "trade_date", run_end_date)
    failures = []
    total_written = 0

    logger.info("%s 进入安全历史回补模式，目标股票数=%s", endpoint_name, len(targets))
    endpoint = getattr(pro, endpoint_name)
    for index, (ts_code, start_date, end_date) in enumerate(targets, start=1):
        success = False
        for attempt in range(1, 4):
            try:
                logger.info(
                    "回补 %s ts_code=%s range=%s~%s (%s/%s) attempt=%s",
                    endpoint_name,
                    ts_code,
                    start_date,
                    end_date,
                    index,
                    len(targets),
                    attempt,
                )
                df = endpoint(ts_code=ts_code, start_date=start_date, end_date=end_date)
                df = filter_by_report_period(df, start_date, end_date)
                if df is not None and not df.empty:
                    written = write_dataset_records(engine, endpoint_name, table_name, df)
                    total_written += written
                    logger.info("%s ts_code=%s 回补 %s 行，累计写入 %s 行", endpoint_name, ts_code, len(df), total_written)
                success = True
                break
            except Exception as exc:
                logger.warning("%s ts_code=%s attempt=%s 历史回补失败: %s", endpoint_name, ts_code, attempt, exc)
                time.sleep(DEFAULT_API_SLEEP)
        if not success:
            failures.append(ts_code)
        time.sleep(DEFAULT_API_SLEEP)

    if failures:
        logger.warning("%s 历史回补有 %s 只股票失败，示例: %s", endpoint_name, len(failures), ", ".join(failures[:10]))

    return total_written


def fetch_daily_basic_missing_history(pro, engine: Engine, table_name: str, run_end_date: str) -> pd.DataFrame:
    return fetch_daily_dataset_missing_history(pro, engine, "daily_basic", table_name, run_end_date)


def fetch_index_dailybasic(pro, start_date: str, end_date: str, index_codes: list[str]) -> pd.DataFrame:
    frames = []
    for ts_code in index_codes:
        logger.info("抓取 index_dailybasic ts_code=%s", ts_code)
        df = pro.index_dailybasic(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df is not None and not df.empty:
            frames.append(df)
            logger.info("index_dailybasic ts_code=%s 返回 %s 行", ts_code, len(df))
        else:
            logger.info("index_dailybasic ts_code=%s 无数据", ts_code)
        time.sleep(DEFAULT_API_SLEEP)
    return combine_frames(frames)


def resolve_sync_window(engine: Engine, dataset_name: str, table_name: str, args, run_end_date: str) -> tuple[str | None, str]:
    if dataset_name in FINANCIAL_DATASETS:
        start_date = resolve_incremental_start_date(
            args.financial_start,
            get_max_date(engine, table_name, "end_date"),
            lookback_days=args.financial_lookback_days,
        )
        return start_date, run_end_date

    if dataset_name in DAILY_DATASETS:
        start_date = resolve_incremental_start_date(
            args.daily_start,
            get_max_date(engine, table_name, "trade_date"),
            lookback_days=args.daily_lookback_days,
        )
        return start_date, run_end_date

    return None, run_end_date


def sync_dataset(engine: Engine, pro, dataset_name: str, args, run_end_date: str) -> int:
    table_name = DATASET_TABLES[dataset_name]
    ensure_landing_table(engine, table_name)
    start_date, end_date = resolve_sync_window(engine, dataset_name, table_name, args, run_end_date)

    started_at = datetime.now()
    try:
        if dataset_name == "stock_basic":
            raw_df = fetch_stock_basic(pro)
        elif dataset_name == "stock_company":
            raw_df = fetch_stock_company(pro)
        elif dataset_name == "namechange":
            raw_df = fetch_namechange(pro)
        elif dataset_name in FINANCIAL_DATASETS:
            if args.backfill_missing_history:
                written = fetch_financial_dataset_missing_history(pro, engine, dataset_name, table_name, run_end_date)
                update_job_status(engine, dataset_name, table_name, written, started_at, "success", None)
                logger.info("%s -> %s 完成，写入 %s 行", dataset_name, table_name, written)
                return written
            elif start_date is None or start_date > end_date:
                raw_df = pd.DataFrame()
            else:
                raw_df = fetch_financial_dataset(pro, dataset_name, start_date, end_date)
        elif dataset_name == "daily":
            if args.backfill_missing_history:
                written = fetch_daily_missing_history(pro, engine, table_name, run_end_date)
                update_job_status(engine, dataset_name, table_name, written, started_at, "success", None)
                logger.info("%s -> %s 完成，写入 %s 行", dataset_name, table_name, written)
                return written
            elif start_date is None or start_date > end_date:
                raw_df = pd.DataFrame()
            else:
                raw_df = fetch_daily(pro, start_date, end_date)
        elif dataset_name == "daily_basic":
            if args.backfill_missing_history:
                written = fetch_daily_basic_missing_history(pro, engine, table_name, run_end_date)
                update_job_status(engine, dataset_name, table_name, written, started_at, "success", None)
                logger.info("%s -> %s 完成，写入 %s 行", dataset_name, table_name, written)
                return written
            elif start_date is None or start_date > end_date:
                raw_df = pd.DataFrame()
            else:
                raw_df = fetch_daily_basic(pro, start_date, end_date)
                validate_daily_basic_result(
                    pro,
                    raw_df,
                    get_required_trade_dates(pro, start_date, end_date, args.daily_publish_cutoff_hour),
                    args.daily_min_coverage_ratio,
                )
        elif dataset_name == "stk_week_month_adj":
            if args.backfill_missing_history:
                written = fetch_stk_week_month_adj_missing_history(pro, engine, table_name, run_end_date)
                update_job_status(engine, dataset_name, table_name, written, started_at, "success", None)
                logger.info("%s -> %s 完成，写入 %s 行", dataset_name, table_name, written)
                return written
            elif start_date is None or start_date > end_date:
                raw_df = pd.DataFrame()
            else:
                raw_df = fetch_stk_week_month_adj(pro, start_date, end_date)
        elif dataset_name == "index_dailybasic":
            if start_date is None or start_date > end_date:
                raw_df = pd.DataFrame()
            else:
                raw_df = fetch_index_dailybasic(pro, start_date, end_date, args.index_codes)
                validate_index_dailybasic_result(
                    raw_df,
                    get_required_trade_dates(pro, start_date, end_date, args.index_publish_cutoff_hour),
                    args.index_codes,
                )
        elif dataset_name == "cn_gdp":
            raw_df = fetch_cn_gdp(pro)
        elif dataset_name == "cn_cpi":
            raw_df = fetch_cn_cpi(pro)
        elif dataset_name == "cn_ppi":
            raw_df = fetch_cn_ppi(pro)
        elif dataset_name == "cn_m":
            raw_df = fetch_cn_m(pro)
        elif dataset_name == "shibor":
            raw_df = fetch_shibor(pro)
        elif dataset_name == "shibor_lpr":
            raw_df = fetch_shibor_lpr(pro)
        else:
            raise ValueError(f"不支持的数据集: {dataset_name}")

        written = write_dataset_records(engine, dataset_name, table_name, raw_df)
        update_job_status(engine, dataset_name, table_name, written, started_at, "success", None)
        logger.info("%s -> %s 完成，写入 %s 行", dataset_name, table_name, written)
        return written
    except Exception as exc:
        update_job_status(engine, dataset_name, table_name, 0, started_at, "failed", str(exc))
        raise


def parse_args():
    parser = argparse.ArgumentParser(description="同步 Tushare 股票/指数基础与指标数据到 PostgreSQL")
    parser.add_argument(
        "--datasets",
        nargs="+",
        choices=list(DATASET_TABLES.keys()),
        default=list(DATASET_TABLES.keys()),
        help="指定需要同步的数据集",
    )
    parser.add_argument("--daily-start", default=DEFAULT_DAILY_START, help="daily_basic 与 index_dailybasic 的起始日期 YYYYMMDD")
    parser.add_argument("--financial-start", default=DEFAULT_FINANCIAL_START, help="财务类数据的起始日期 YYYYMMDD")
    parser.add_argument("--end-date", default=None, help="同步结束日期 YYYYMMDD，未指定时每轮自动取当天")
    parser.add_argument("--index-codes", nargs="+", default=DEFAULT_INDEX_CODES, help="大盘指数每日指标代码列表")
    parser.add_argument("--schedule", action="store_true", help="开启定时增量同步循环")
    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=DEFAULT_SCHEDULE_INTERVAL_MINUTES,
        help="定时模式下的执行间隔分钟数",
    )
    parser.add_argument(
        "--daily-lookback-days",
        type=int,
        default=DEFAULT_DAILY_LOOKBACK_DAYS,
        help="日频数据增量回看天数",
    )
    parser.add_argument(
        "--financial-lookback-days",
        type=int,
        default=DEFAULT_FINANCIAL_LOOKBACK_DAYS,
        help="财务数据增量回看天数",
    )
    parser.add_argument(
        "--daily-min-coverage-ratio",
        type=float,
        default=DEFAULT_DAILY_MIN_COVERAGE_RATIO,
        help="daily_basic 每个交易日最小覆盖率阈值，按当前活跃股票数的比例校验",
    )
    parser.add_argument(
        "--daily-publish-cutoff-hour",
        type=int,
        default=DEFAULT_DAILY_PUBLISH_CUTOFF_HOUR,
        help="daily_basic 当日数据的最晚发布时间小时，早于该时间不强制校验当日",
    )
    parser.add_argument(
        "--index-publish-cutoff-hour",
        type=int,
        default=DEFAULT_INDEX_PUBLISH_CUTOFF_HOUR,
        help="index_dailybasic 当日数据的最晚发布时间小时，早于该时间不强制校验当日",
    )
    parser.add_argument(
        "--backfill-missing-history",
        action="store_true",
        help="按股票从上市日向前安全回补缺失历史，仅补各表当前最早记录之前的数据",
    )
    return parser.parse_args()


def validate_args(args):
    args.daily_start = normalize_date_string(args.daily_start)
    args.financial_start = normalize_date_string(args.financial_start)
    args.end_date = normalize_date_string(args.end_date) if args.end_date else None
    args.index_codes = [str(code).strip() for code in args.index_codes if str(code).strip()]

    if args.interval_minutes <= 0:
        raise ValueError("--interval-minutes 必须大于 0")
    if args.daily_lookback_days < 0:
        raise ValueError("--daily-lookback-days 不能小于 0")
    if args.financial_lookback_days < 0:
        raise ValueError("--financial-lookback-days 不能小于 0")
    if not (0 < args.daily_min_coverage_ratio <= 1):
        raise ValueError("--daily-min-coverage-ratio 必须大于 0 且不超过 1")
    if not (0 <= args.daily_publish_cutoff_hour <= 23):
        raise ValueError("--daily-publish-cutoff-hour 必须在 0 到 23 之间")
    if not (0 <= args.index_publish_cutoff_hour <= 23):
        raise ValueError("--index-publish-cutoff-hour 必须在 0 到 23 之间")
    if args.schedule and args.backfill_missing_history:
        raise ValueError("--backfill-missing-history 不能与 --schedule 同时使用")
    if not args.index_codes and "index_dailybasic" in args.datasets:
        raise ValueError("同步 index_dailybasic 时必须提供至少一个 --index-codes")
    if args.end_date:
        if args.daily_start > args.end_date:
            raise ValueError(f"daily-start 不能晚于 end-date: {args.daily_start} > {args.end_date}")
        if args.financial_start > args.end_date:
            raise ValueError(f"financial-start 不能晚于 end-date: {args.financial_start} > {args.end_date}")
    return args


def run_sync_once(engine: Engine, pro, args) -> int:
    run_end_date = args.end_date or get_today_string()
    logger.info(
        "开始执行同步，结束日期=%s，数据集=%s，模式=%s",
        run_end_date,
        ",".join(args.datasets),
        "安全历史回补" if args.backfill_missing_history else "增量同步",
    )

    total_rows = 0
    for dataset_name in args.datasets:
        total_rows += sync_dataset(engine, pro, dataset_name, args, run_end_date)

    logger.info("本轮同步完成，累计写入 %s 行", total_rows)
    
    if "stk_week_month_adj" in args.datasets:
        try:
            logger.info("开始自动计算 EMA 因子...")
            from src.calculate_ema_factors import process_all_stocks
            process_all_stocks()
            logger.info("EMA 因子计算并落库完成。")
        except Exception as e:
            logger.error(f"EMA 因子计算失败: {e}", exc_info=True)
            
    return total_rows


def run_schedule_loop(engine: Engine, pro, args):
    interval_seconds = args.interval_minutes * 60
    round_index = 0

    while True:
        round_index += 1
        round_started_at = time.time()
        logger.info("开始第 %s 轮定时同步", round_index)
        try:
            run_sync_once(engine, pro, args)
        except Exception:
            logger.exception("第 %s 轮定时同步失败", round_index)

        next_run_at = datetime.fromtimestamp(round_started_at + interval_seconds)
        sleep_seconds = max(1, int(round_started_at + interval_seconds - time.time()))
        logger.info("第 %s 轮结束，下一轮执行时间 %s", round_index, next_run_at.strftime("%Y-%m-%d %H:%M:%S"))
        time.sleep(sleep_seconds)


def main():
    args = validate_args(parse_args())

    engine = get_engine()
    ensure_storage_objects(engine)
    pro = _init_tushare()

    if args.schedule:
        run_schedule_loop(engine, pro, args)
    else:
        run_sync_once(engine, pro, args)


if __name__ == "__main__":
    main()
