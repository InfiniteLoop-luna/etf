from __future__ import annotations

from datetime import date
from typing import Mapping

import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

from src.user_watchlist_store import ensure_user_watchlist_table


TABLE_NAME = "app_fund_estimate_snapshots"
SNAPSHOT_COLUMNS = """
    fund_code,
    estimate_date,
    estimate_pct,
    covered_weight_pct,
    top10_coverage_pct,
    quote_count,
    holding_count,
    quote_time,
    holding_end_date,
    source,
    updated_at
"""


def _optional_float(value):
    number = pd.to_numeric(value, errors="coerce")
    return None if pd.isna(number) else float(number)


def _optional_int(value):
    number = pd.to_numeric(value, errors="coerce")
    return None if pd.isna(number) else int(number)


def _required_date(value) -> date:
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        raise ValueError("estimate_date 不能为空")
    return timestamp.date()


def _optional_datetime(value):
    timestamp = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(timestamp) else timestamp.to_pydatetime()


def _normalize_snapshot_row(row) -> dict:
    if row is None:
        return {}
    snapshot = dict(row)
    snapshot["estimate_date"] = pd.to_datetime(
        snapshot.get("estimate_date"), errors="coerce"
    )
    snapshot["quote_time"] = pd.to_datetime(
        snapshot.get("quote_time"), errors="coerce"
    )
    snapshot["holding_end_date"] = pd.to_datetime(
        snapshot.get("holding_end_date"), errors="coerce"
    )
    for field in ("estimate_pct", "covered_weight_pct", "top10_coverage_pct"):
        snapshot[field] = _optional_float(snapshot.get(field))
    for field in ("quote_count", "holding_count"):
        snapshot[field] = _optional_int(snapshot.get(field))
    return snapshot


def ensure_fund_estimate_snapshot_table(engine: Engine) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        fund_code VARCHAR(20) NOT NULL,
        estimate_date DATE NOT NULL,
        estimate_pct NUMERIC(14, 6) NOT NULL,
        covered_weight_pct NUMERIC(14, 6),
        top10_coverage_pct NUMERIC(14, 6),
        quote_count INTEGER,
        holding_count INTEGER,
        quote_time TIMESTAMPTZ,
        holding_end_date DATE,
        source VARCHAR(120),
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (fund_code, estimate_date)
    );

    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_estimate_date
        ON {TABLE_NAME} (estimate_date DESC, fund_code);
    """
    with engine.begin() as conn:
        for statement in [item.strip() for item in sql.split(";") if item.strip()]:
            conn.execute(text(statement))


def list_distinct_fund_watchlist_codes(engine: Engine) -> list[str]:
    ensure_user_watchlist_table(engine)
    sql = """
    SELECT DISTINCT UPPER(TRIM(ts_code)) AS fund_code
    FROM app_user_watchlist
    WHERE security_type = 'fund'
      AND NULLIF(TRIM(ts_code), '') IS NOT NULL
    ORDER BY fund_code
    """
    with engine.connect() as conn:
        return [str(row[0]).strip().upper() for row in conn.execute(text(sql))]


def upsert_fund_estimate_snapshot(
    engine: Engine,
    snapshot: Mapping,
    *,
    ensure_table: bool = True,
) -> None:
    if ensure_table:
        ensure_fund_estimate_snapshot_table(engine)

    fund_code = str(snapshot.get("fund_code") or "").strip().upper()
    if not fund_code:
        raise ValueError("fund_code 不能为空")
    estimate_pct = _optional_float(snapshot.get("estimate_pct"))
    if estimate_pct is None:
        raise ValueError("estimate_pct 不能为空")

    holding_end_date = pd.to_datetime(
        snapshot.get("holding_end_date"), errors="coerce"
    )
    params = {
        "fund_code": fund_code,
        "estimate_date": _required_date(snapshot.get("estimate_date")),
        "estimate_pct": estimate_pct,
        "covered_weight_pct": _optional_float(snapshot.get("covered_weight_pct")),
        "top10_coverage_pct": _optional_float(snapshot.get("top10_coverage_pct")),
        "quote_count": _optional_int(snapshot.get("quote_count")),
        "holding_count": _optional_int(snapshot.get("holding_count")),
        "quote_time": _optional_datetime(snapshot.get("quote_time")),
        "holding_end_date": (
            None if pd.isna(holding_end_date) else holding_end_date.date()
        ),
        "source": str(snapshot.get("source") or "").strip()[:120],
    }
    sql = f"""
    INSERT INTO {TABLE_NAME} (
        fund_code,
        estimate_date,
        estimate_pct,
        covered_weight_pct,
        top10_coverage_pct,
        quote_count,
        holding_count,
        quote_time,
        holding_end_date,
        source,
        created_at,
        updated_at
    ) VALUES (
        :fund_code,
        :estimate_date,
        :estimate_pct,
        :covered_weight_pct,
        :top10_coverage_pct,
        :quote_count,
        :holding_count,
        :quote_time,
        :holding_end_date,
        :source,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    )
    ON CONFLICT (fund_code, estimate_date) DO UPDATE SET
        estimate_pct = EXCLUDED.estimate_pct,
        covered_weight_pct = EXCLUDED.covered_weight_pct,
        top10_coverage_pct = EXCLUDED.top10_coverage_pct,
        quote_count = EXCLUDED.quote_count,
        holding_count = EXCLUDED.holding_count,
        quote_time = EXCLUDED.quote_time,
        holding_end_date = EXCLUDED.holding_end_date,
        source = EXCLUDED.source,
        updated_at = CURRENT_TIMESTAMP
    WHERE {TABLE_NAME}.quote_time IS NULL
       OR (
            EXCLUDED.quote_time IS NOT NULL
            AND EXCLUDED.quote_time <= {TABLE_NAME}.quote_time
       )
    """
    with engine.begin() as conn:
        conn.execute(text(sql), params)


def get_fund_estimate_snapshot(
    engine: Engine,
    fund_code: str,
    estimate_date,
    *,
    ensure_table: bool = True,
) -> dict:
    if ensure_table:
        ensure_fund_estimate_snapshot_table(engine)
    normalized_code = str(fund_code or "").strip().upper()
    normalized_date = _required_date(estimate_date)
    sql = f"""
    SELECT
        {SNAPSHOT_COLUMNS}
    FROM {TABLE_NAME}
    WHERE fund_code = :fund_code
      AND estimate_date = :estimate_date
    LIMIT 1
    """
    with engine.connect() as conn:
        row = conn.execute(
            text(sql),
            {"fund_code": normalized_code, "estimate_date": normalized_date},
        ).mappings().first()
    return _normalize_snapshot_row(row)


def list_latest_fund_estimate_snapshots(
    engine: Engine,
    fund_codes,
    *,
    ensure_table: bool = True,
) -> dict[str, dict]:
    if ensure_table:
        ensure_fund_estimate_snapshot_table(engine)
    codes = sorted(
        {
            str(code or "").strip().upper()
            for code in fund_codes
            if str(code or "").strip()
        }
    )
    if not codes:
        return {}
    placeholders = ", ".join(f":code_{index}" for index in range(len(codes)))
    params = {f"code_{index}": code for index, code in enumerate(codes)}
    sql = f"""
    WITH ranked AS (
        SELECT
            {SNAPSHOT_COLUMNS},
            ROW_NUMBER() OVER (
                PARTITION BY fund_code
                ORDER BY estimate_date DESC
            ) AS row_number
        FROM {TABLE_NAME}
        WHERE fund_code IN ({placeholders})
    )
    SELECT {SNAPSHOT_COLUMNS}
    FROM ranked
    WHERE row_number = 1
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return {
        str(row["fund_code"]).strip().upper(): _normalize_snapshot_row(row)
        for row in rows
    }


def list_fund_estimate_snapshots_for_date(
    engine: Engine,
    fund_codes,
    estimate_date,
    *,
    ensure_table: bool = True,
) -> dict[str, dict]:
    if ensure_table:
        ensure_fund_estimate_snapshot_table(engine)

    normalized_date = _required_date(estimate_date)
    codes = sorted(
        {
            str(code or "").strip().upper()
            for code in fund_codes
            if str(code or "").strip()
        }
    )
    if not codes:
        return {}

    stmt = text(
        f"""
        SELECT {SNAPSHOT_COLUMNS}
        FROM {TABLE_NAME}
        WHERE estimate_date = :estimate_date
          AND fund_code IN :codes
        """
    ).bindparams(bindparam("codes", expanding=True))

    with engine.connect() as conn:
        rows = conn.execute(
            stmt,
            {"estimate_date": normalized_date, "codes": codes},
        ).mappings().all()

    return {
        str(row["fund_code"]).strip().upper(): _normalize_snapshot_row(row)
        for row in rows
    }


def get_latest_fund_estimate_snapshot(
    engine: Engine,
    fund_code: str,
    *,
    ensure_table: bool = True,
) -> dict:
    normalized_code = str(fund_code or "").strip().upper()
    return list_latest_fund_estimate_snapshots(
        engine,
        [normalized_code],
        ensure_table=ensure_table,
    ).get(normalized_code, {})
