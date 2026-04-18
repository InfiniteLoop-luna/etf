"""
打板情绪与接力监控 - P1

数据源：Tushare 打板专题相关表
- ts_limit_list_d
- ts_limit_step
- ts_limit_cpt_list
- ts_kpl_list
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


def get_limitup_latest_date(engine: Optional[Engine] = None) -> Optional[str]:
    if engine is None:
        engine = _get_engine_cached()
    sql = """
    SELECT TO_CHAR(MAX(dt), 'YYYYMMDD') AS latest_date
    FROM (
      SELECT MAX(trade_date) AS dt FROM ts_limit_list_d
      UNION ALL
      SELECT MAX(trade_date) AS dt FROM ts_limit_step
      UNION ALL
      SELECT MAX(trade_date) AS dt FROM ts_limit_cpt_list
      UNION ALL
      SELECT MAX(trade_date) AS dt FROM ts_kpl_list
    ) t
    """
    with engine.connect() as conn:
        row = conn.execute(text(sql)).fetchone()
    return row[0] if row and row[0] else None


def get_limitup_sync_meta(engine: Optional[Engine] = None) -> dict:
    if engine is None:
        engine = _get_engine_cached()

    sql = """
    SELECT
      COALESCE(SUM(cnt), 0) AS total_rows,
      MAX(latest_trade_date) AS latest_trade_date,
      MAX(latest_ingested_at) AS latest_ingested_at
    FROM (
      SELECT COUNT(*) AS cnt, MAX(trade_date) AS latest_trade_date, MAX(ingested_at) AS latest_ingested_at FROM ts_limit_list_d
      UNION ALL
      SELECT COUNT(*) AS cnt, MAX(trade_date) AS latest_trade_date, MAX(ingested_at) AS latest_ingested_at FROM ts_limit_step
      UNION ALL
      SELECT COUNT(*) AS cnt, MAX(trade_date) AS latest_trade_date, MAX(ingested_at) AS latest_ingested_at FROM ts_limit_cpt_list
      UNION ALL
      SELECT COUNT(*) AS cnt, MAX(trade_date) AS latest_trade_date, MAX(ingested_at) AS latest_ingested_at FROM ts_kpl_list
    ) t
    """
    with engine.connect() as conn:
        row = conn.execute(text(sql)).fetchone()

    latest_trade_date = None
    if row and row[1] is not None:
        latest_trade_date = row[1].strftime("%Y%m%d")

    return {
        "total_rows": int(row[0] or 0) if row else 0,
        "latest_trade_date": latest_trade_date,
        "latest_ingested_at": row[2] if row else None,
    }


def query_limitup_emotion_daily(start_date: str,
                                end_date: Optional[str] = None,
                                engine: Optional[Engine] = None) -> pd.DataFrame:
    if engine is None:
        engine = _get_engine_cached()

    s_val = _to_date(start_date)
    e_val = _to_date(end_date or start_date)

    sql = """
    WITH lim AS (
      SELECT
        trade_date,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(payload->>'limit', '')) = 'U') AS up_cnt,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(payload->>'limit', '')) = 'D') AS down_cnt,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(payload->>'limit', '')) = 'Z') AS zha_cnt,
        COUNT(*) AS total_cnt
      FROM ts_limit_list_d
      WHERE trade_date BETWEEN :start_date AND :end_date
      GROUP BY trade_date
    ),
    step AS (
      SELECT
        trade_date,
        MAX(
          CASE
            WHEN COALESCE(payload->>'nums', '') ~ '^\\d+$' THEN (payload->>'nums')::int
            ELSE 0
          END
        ) AS high_days,
        COUNT(*) FILTER (
          WHERE CASE
            WHEN COALESCE(payload->>'nums', '') ~ '^\\d+$' THEN (payload->>'nums')::int
            ELSE 0
          END >= 2
        ) AS lb_cnt
      FROM ts_limit_step
      WHERE trade_date BETWEEN :start_date AND :end_date
      GROUP BY trade_date
    ),
    cpt AS (
      SELECT
        trade_date,
        COUNT(*) AS strong_cpt_cnt
      FROM ts_limit_cpt_list
      WHERE trade_date BETWEEN :start_date AND :end_date
      GROUP BY trade_date
    )
    SELECT
      COALESCE(l.trade_date, s.trade_date, c.trade_date) AS trade_date,
      COALESCE(l.up_cnt, 0) AS up_cnt,
      COALESCE(l.down_cnt, 0) AS down_cnt,
      COALESCE(l.zha_cnt, 0) AS zha_cnt,
      COALESCE(l.total_cnt, 0) AS total_cnt,
      COALESCE(s.high_days, 0) AS high_days,
      COALESCE(s.lb_cnt, 0) AS lb_cnt,
      COALESCE(c.strong_cpt_cnt, 0) AS strong_cpt_cnt
    FROM lim l
    FULL JOIN step s ON l.trade_date = s.trade_date
    FULL JOIN cpt c ON COALESCE(l.trade_date, s.trade_date) = c.trade_date
    ORDER BY trade_date ASC
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params={"start_date": s_val, "end_date": e_val})

    if df.empty:
        return df

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["zha_rate"] = (
        pd.to_numeric(df["zha_cnt"], errors="coerce").fillna(0)
        / pd.to_numeric(df["up_cnt"], errors="coerce").replace(0, pd.NA)
    ).fillna(0)

    def _z(series: pd.Series) -> pd.Series:
        s = pd.to_numeric(series, errors='coerce').fillna(0)
        std = s.std(ddof=0)
        if std == 0 or pd.isna(std):
            return s * 0
        return (s - s.mean()) / std

    score = (
        _z(df["up_cnt"])
        + _z(df["high_days"])
        + _z(df["strong_cpt_cnt"])
        - _z(df["zha_rate"])
        - _z(df["down_cnt"])
    )
    df["emotion_score"] = score

    def _stage(v: float) -> str:
        if v >= 1.2:
            return "高潮"
        if v >= 0.5:
            return "强势"
        if v >= -0.2:
            return "震荡"
        if v >= -0.8:
            return "转弱"
        return "退潮"

    df["emotion_stage"] = df["emotion_score"].map(_stage)
    return df


def query_limitup_sector_relay_daily(trade_date: str,
                                     top_n: int = 20,
                                     engine: Optional[Engine] = None) -> pd.DataFrame:
    if engine is None:
        engine = _get_engine_cached()

    d_val = _to_date(trade_date)
    sql = """
    SELECT
      trade_date,
      COALESCE(payload->>'name', ts_code, '未知概念') AS concept_name,
      COALESCE(
        CASE WHEN COALESCE(payload->>'up_nums', '') ~ '^\\d+$' THEN (payload->>'up_nums')::int ELSE 0 END,
        0
      ) AS up_cnt,
      0 AS zha_cnt,
      COALESCE(
        CASE WHEN COALESCE(payload->>'cons_nums', '') ~ '^\\d+$' THEN (payload->>'cons_nums')::int ELSE 0 END,
        0
      ) AS lead_cnt,
      COALESCE(
        CASE WHEN COALESCE(payload->>'days', '') ~ '^\\d+$' THEN (payload->>'days')::int ELSE 0 END,
        0
      ) AS max_height
    FROM ts_limit_cpt_list
    WHERE trade_date = :trade_date
    ORDER BY up_cnt DESC, lead_cnt DESC, max_height DESC
    LIMIT :top_n
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params={"trade_date": d_val, "top_n": int(top_n)})


def query_limitup_leader_daily(trade_date: str,
                               top_n: int = 30,
                               engine: Optional[Engine] = None) -> pd.DataFrame:
    if engine is None:
        engine = _get_engine_cached()

    d_val = _to_date(trade_date)
    sql = """
    SELECT
      s.trade_date,
      s.ts_code,
      COALESCE(s.payload->>'name', k.payload->>'name', l.payload->>'name', s.ts_code) AS name,
      COALESCE(
        CASE WHEN COALESCE(s.payload->>'nums', '') ~ '^\\d+$' THEN (s.payload->>'nums')::int ELSE 0 END,
        0
      ) AS high_days,
      COALESCE(k.payload->>'status', '未知') AS status,
      COALESCE(
        CASE WHEN COALESCE(l.payload->>'open_times', '') ~ '^\\d+$' THEN (l.payload->>'open_times')::int ELSE 0 END,
        0
      ) AS open_num,
      COALESCE(
        CASE WHEN COALESCE(l.payload->>'fd_amount', '') ~ '^-?\\d+(\\.\\d+)?$' THEN (l.payload->>'fd_amount')::numeric ELSE 0 END,
        0
      ) AS fd_amount
    FROM ts_limit_step s
    LEFT JOIN ts_kpl_list k
      ON s.trade_date = k.trade_date AND s.ts_code = k.ts_code
    LEFT JOIN ts_limit_list_d l
      ON s.trade_date = l.trade_date AND s.ts_code = l.ts_code
    WHERE s.trade_date = :trade_date
    ORDER BY high_days DESC, fd_amount DESC
    LIMIT :top_n
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params={"trade_date": d_val, "top_n": int(top_n)})
