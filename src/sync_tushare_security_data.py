from __future__ import annotations

import argparse
import hashlib
import json
import logging
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
DEFAULT_INDEX_CODES = [
    "000001.SH",
    "399001.SZ",
    "000016.SH",
    "000905.SH",
    "399005.SZ",
    "399006.SZ",
]
STOCK_CODES_CACHE: list[str] | None = None

DATASET_TABLES = {
    "stock_basic": "ts_stock_basic",
    "stock_company": "ts_stock_company",
    "income": "ts_stock_income",
    "balancesheet": "ts_stock_balancesheet",
    "cashflow": "ts_stock_cashflow",
    "fina_indicator": "ts_stock_fina_indicator",
    "daily_basic": "ts_stock_daily_basic",
    "index_dailybasic": "ts_index_dailybasic",
}
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
        "view_name": "vw_ts_stock_company",
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
            ("text", "main_business"),
            ("text", "business_scope"),
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
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
FINANCIAL_DATASETS = {"income", "balancesheet", "cashflow", "fina_indicator"}
DAILY_DATASETS = {"daily_basic", "index_dailybasic"}


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
    return f"NULLIF(payload->>'{field_name}', '') AS {field_name}"


def build_json_numeric_expr(field_name: str) -> str:
    return (
        f"CASE WHEN NULLIF(payload->>'{field_name}', '') ~ '^[-+]?(\\d+(\\.\\d+)?|\\.\\d+)$' "
        f"THEN (payload->>'{field_name}')::numeric END AS {field_name}"
    )


def build_json_integer_expr(field_name: str) -> str:
    return (
        f"CASE WHEN NULLIF(payload->>'{field_name}', '') ~ '^[-+]?\\d+$' "
        f"THEN (payload->>'{field_name}')::integer END AS {field_name}"
    )


def build_json_date_expr(field_name: str) -> str:
    return (
        f"CASE WHEN NULLIF(payload->>'{field_name}', '') ~ '^\\d{{8}}$' "
        f"THEN TO_DATE(payload->>'{field_name}', 'YYYYMMDD') END AS {field_name}"
    )


def build_view_column_expr(column_type: str, field_name: str) -> str:
    builders = {
        "text": build_json_text_expr,
        "numeric": build_json_numeric_expr,
        "integer": build_json_integer_expr,
        "date": build_json_date_expr,
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


def resolve_business_key(dataset_name: str, payload: dict) -> str:
    preferred_keys = {
        "stock_basic": ["ts_code"],
        "stock_company": ["ts_code"],
        "daily_basic": ["ts_code", "trade_date"],
        "index_dailybasic": ["ts_code", "trade_date"],
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
    frames = []
    for status in ["L", "D", "P"]:
        df = pro.stock_basic(list_status=status)
        if df is not None and not df.empty:
            frames.append(df)
        time.sleep(DEFAULT_API_SLEEP)
    result = combine_frames(frames)
    if not result.empty and "ts_code" in result.columns:
        result = result.drop_duplicates(subset=["ts_code"], keep="last")
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
    return result


def get_stock_codes(pro) -> list[str]:
    global STOCK_CODES_CACHE

    if STOCK_CODES_CACHE is not None:
        return STOCK_CODES_CACHE

    stock_basic_df = fetch_stock_basic(pro)
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
        elif dataset_name in FINANCIAL_DATASETS:
            if start_date is None or start_date > end_date:
                raw_df = pd.DataFrame()
            else:
                raw_df = fetch_financial_dataset(pro, dataset_name, start_date, end_date)
        elif dataset_name == "daily_basic":
            if start_date is None or start_date > end_date:
                raw_df = pd.DataFrame()
            else:
                raw_df = fetch_daily_basic(pro, start_date, end_date)
        elif dataset_name == "index_dailybasic":
            if start_date is None or start_date > end_date:
                raw_df = pd.DataFrame()
            else:
                raw_df = fetch_index_dailybasic(pro, start_date, end_date, args.index_codes)
        else:
            raise ValueError(f"不支持的数据集: {dataset_name}")

        prepared = prepare_records(dataset_name, raw_df)
        written = upsert_records(engine, table_name, prepared)
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
    logger.info("开始执行同步，结束日期=%s，数据集=%s", run_end_date, ",".join(args.datasets))

    total_rows = 0
    for dataset_name in args.datasets:
        total_rows += sync_dataset(engine, pro, dataset_name, args, run_end_date)

    logger.info("本轮同步完成，累计写入 %s 行", total_rows)
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
