"""
游资名录与每日明细查询模块
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .moneyflow_fetcher import _get_engine_cached


def _to_date(s: str):
    return datetime.strptime(str(s).replace("-", ""), "%Y%m%d").date()


def get_hotmoney_latest_detail_date(engine: Optional[Engine] = None) -> Optional[str]:
    if engine is None:
        engine = _get_engine_cached()
    sql = "SELECT TO_CHAR(MAX(trade_date), 'YYYYMMDD') FROM ts_hm_detail"
    with engine.connect() as conn:
        row = conn.execute(text(sql)).fetchone()
    return row[0] if row and row[0] else None


def get_hotmoney_sync_meta(engine: Optional[Engine] = None) -> dict:
    if engine is None:
        engine = _get_engine_cached()

    sql = """
    SELECT
      (SELECT COUNT(*) FROM ts_hm_list) AS hm_list_count,
      (SELECT COUNT(*) FROM ts_hm_detail) AS hm_detail_count,
      (SELECT MAX(trade_date) FROM ts_hm_detail) AS latest_trade_date,
      GREATEST(
        COALESCE((SELECT MAX(ingested_at) FROM ts_hm_list), '1970-01-01'::timestamptz),
        COALESCE((SELECT MAX(ingested_at) FROM ts_hm_detail), '1970-01-01'::timestamptz)
      ) AS latest_ingested_at
    """
    with engine.connect() as conn:
        row = conn.execute(text(sql)).fetchone()

    latest_trade_date = None
    if row and row[2] is not None:
        latest_trade_date = row[2].strftime("%Y%m%d")

    return {
        "hm_list_count": int(row[0] or 0) if row else 0,
        "hm_detail_count": int(row[1] or 0) if row else 0,
        "latest_trade_date": latest_trade_date,
        "latest_ingested_at": row[3] if row else None,
    }


def query_hotmoney_list(name: Optional[str] = None,
                        limit: int = 200,
                        engine: Optional[Engine] = None) -> pd.DataFrame:
    if engine is None:
        engine = _get_engine_cached()

    sql = """
    SELECT
      COALESCE(payload->>'name', ts_code, '未知游资') AS hm_name,
      COALESCE(payload->>'desc', '') AS hm_desc,
      COALESCE(payload->>'orgs', '') AS hm_orgs,
      ingested_at
    FROM ts_hm_list
    WHERE (:name IS NULL OR COALESCE(payload->>'name','') ILIKE ('%' || :name || '%'))
    ORDER BY hm_name
    LIMIT :limit
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params={"name": name, "limit": int(limit)})


def query_hotmoney_detail(start_date: str,
                          end_date: str,
                          hm_name: Optional[str] = None,
                          ts_code: Optional[str] = None,
                          limit: int = 2000,
                          engine: Optional[Engine] = None) -> pd.DataFrame:
    if engine is None:
        engine = _get_engine_cached()

    s_val = _to_date(start_date)
    e_val = _to_date(end_date)

    sql = """
    SELECT
      trade_date,
      ts_code,
      COALESCE(payload->>'ts_name', '') AS ts_name,
      COALESCE(payload->>'hm_name', '') AS hm_name,
      COALESCE(payload->>'hm_orgs', '') AS hm_orgs,
      COALESCE(payload->>'tag', '') AS tag,
      COALESCE(
        CASE WHEN COALESCE(payload->>'buy_amount','') ~ '^-?\\d+(\\.\\d+)?$' THEN (payload->>'buy_amount')::numeric ELSE 0 END,
        0
      ) AS buy_amount,
      COALESCE(
        CASE WHEN COALESCE(payload->>'sell_amount','') ~ '^-?\\d+(\\.\\d+)?$' THEN (payload->>'sell_amount')::numeric ELSE 0 END,
        0
      ) AS sell_amount,
      COALESCE(
        CASE WHEN COALESCE(payload->>'net_amount','') ~ '^-?\\d+(\\.\\d+)?$' THEN (payload->>'net_amount')::numeric ELSE 0 END,
        0
      ) AS net_amount
    FROM ts_hm_detail
    WHERE trade_date BETWEEN :start_date AND :end_date
      AND (:hm_name IS NULL OR COALESCE(payload->>'hm_name', '') ILIKE ('%' || :hm_name || '%'))
      AND (:ts_code IS NULL OR ts_code = :ts_code)
    ORDER BY trade_date DESC, ABS(
      COALESCE(
        CASE WHEN COALESCE(payload->>'net_amount','') ~ '^-?\\d+(\\.\\d+)?$' THEN (payload->>'net_amount')::numeric ELSE 0 END,
        0
      )
    ) DESC
    LIMIT :limit
    """
    with engine.connect() as conn:
        df = pd.read_sql(
            text(sql),
            conn,
            params={
                "start_date": s_val,
                "end_date": e_val,
                "hm_name": hm_name,
                "ts_code": ts_code,
                "limit": int(limit),
            },
        )

    if not df.empty:
        df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def query_hotmoney_top_active(start_date: str,
                              end_date: str,
                              top_n: int = 20,
                              engine: Optional[Engine] = None) -> pd.DataFrame:
    if engine is None:
        engine = _get_engine_cached()

    s_val = _to_date(start_date)
    e_val = _to_date(end_date)

    sql = """
    SELECT
      COALESCE(payload->>'hm_name', '未知游资') AS hm_name,
      COUNT(*) AS hit_count,
      COUNT(DISTINCT ts_code) AS stock_count,
      SUM(
        COALESCE(
          CASE WHEN COALESCE(payload->>'net_amount','') ~ '^-?\\d+(\\.\\d+)?$' THEN (payload->>'net_amount')::numeric ELSE 0 END,
          0
        )
      ) AS total_net_amount
    FROM ts_hm_detail
    WHERE trade_date BETWEEN :start_date AND :end_date
    GROUP BY COALESCE(payload->>'hm_name', '未知游资')
    ORDER BY hit_count DESC, 4 DESC
    LIMIT :top_n
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params={"start_date": s_val, "end_date": e_val, "top_n": int(top_n)})


def query_hotmoney_top_stocks(start_date: str,
                              end_date: str,
                              top_n: int = 20,
                              order_by: str = "hit_count",
                              engine: Optional[Engine] = None) -> pd.DataFrame:
    if engine is None:
        engine = _get_engine_cached()

    s_val = _to_date(start_date)
    e_val = _to_date(end_date)

    order_by = (order_by or "hit_count").lower()
    if order_by == "hm_count":
        order_sql = "hm_count DESC, hit_count DESC, total_net_amount_abs DESC"
    elif order_by == "net_amount_abs":
        order_sql = "total_net_amount_abs DESC, hit_count DESC, hm_count DESC"
    else:
        order_sql = "hit_count DESC, hm_count DESC, total_net_amount_abs DESC"

    sql = f"""
    SELECT *
    FROM (
      SELECT
        ts_code,
        COALESCE(payload->>'ts_name', ts_code) AS ts_name,
        COUNT(*) AS hit_count,
        COUNT(DISTINCT COALESCE(payload->>'hm_name', '未知游资')) AS hm_count,
        SUM(
          COALESCE(
            CASE WHEN COALESCE(payload->>'net_amount','') ~ '^-?\\d+(\\.\\d+)?$' THEN (payload->>'net_amount')::numeric ELSE 0 END,
            0
          )
        ) AS total_net_amount,
        ABS(
          SUM(
            COALESCE(
              CASE WHEN COALESCE(payload->>'net_amount','') ~ '^-?\\d+(\\.\\d+)?$' THEN (payload->>'net_amount')::numeric ELSE 0 END,
              0
            )
          )
        ) AS total_net_amount_abs
      FROM ts_hm_detail
      WHERE trade_date BETWEEN :start_date AND :end_date
      GROUP BY ts_code, COALESCE(payload->>'ts_name', ts_code)
    ) t
    ORDER BY {order_sql}
    LIMIT :top_n
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params={"start_date": s_val, "end_date": e_val, "top_n": int(top_n)})
