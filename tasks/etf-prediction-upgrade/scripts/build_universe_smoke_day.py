from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text

from src.ml_stock_dataset import get_engine


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


SQL = """
WITH financial_ann AS (
    SELECT ts_code, MIN(ann_date) AS first_financial_ann_date
    FROM (
        SELECT ts_code, ann_date FROM vw_ts_stock_income WHERE ann_date IS NOT NULL
        UNION ALL
        SELECT ts_code, ann_date FROM vw_ts_stock_balancesheet WHERE ann_date IS NOT NULL
        UNION ALL
        SELECT ts_code, ann_date FROM vw_ts_stock_cashflow WHERE ann_date IS NOT NULL
        UNION ALL
        SELECT ts_code, ann_date FROM vw_ts_stock_fina_indicator WHERE ann_date IS NOT NULL
    ) t
    GROUP BY ts_code
),
st_history AS (
    SELECT DISTINCT ts_code
    FROM vw_ts_stock_namechange
    WHERE (
        COALESCE(name, '') LIKE 'ST%'
        OR COALESCE(name, '') LIKE '*ST%'
        OR COALESCE(name, '') LIKE 'S*ST%'
        OR COALESCE(name, '') LIKE 'SST%'
    )
),
target_day AS (
    SELECT
        d.trade_date,
        d.ts_code,
        b.symbol,
        COALESCE(nc_hist.name, b.name) AS historical_name,
        b.name,
        b.industry,
        b.market,
        COALESCE(c.exchange, b.exchange) AS exchange,
        b.list_date,
        b.list_status,
        b.delist_date,
        d.close,
        db.close AS daily_basic_close,
        fa.first_financial_ann_date,
        (st.ts_code IS NOT NULL) AS has_ever_st
    FROM vw_ts_stock_daily d
    JOIN vw_ts_stock_basic b
      ON d.ts_code = b.ts_code
    LEFT JOIN vw_ts_stock_company c
      ON d.ts_code = c.ts_code
    LEFT JOIN vw_ts_stock_daily_basic db
      ON db.ts_code = d.ts_code
     AND db.trade_date = d.trade_date
    LEFT JOIN financial_ann fa
      ON fa.ts_code = d.ts_code
    LEFT JOIN st_history st
      ON st.ts_code = d.ts_code
    LEFT JOIN LATERAL (
        SELECT nc.name
        FROM vw_ts_stock_namechange nc
        WHERE nc.ts_code = d.ts_code
          AND nc.start_date IS NOT NULL
          AND nc.start_date <= d.trade_date
          AND (
                COALESCE(nc.nc_end_date, nc.end_date) IS NULL
                OR COALESCE(nc.nc_end_date, nc.end_date) >= d.trade_date
          )
        ORDER BY nc.start_date DESC, COALESCE(nc.nc_ann_date, nc.ann_date) DESC NULLS LAST
        LIMIT 1
    ) nc_hist ON TRUE
    WHERE d.trade_date = :trade_date
),
price_history AS (
    SELECT ts_code, COUNT(*) AS price_history_bars
    FROM vw_ts_stock_daily
    WHERE trade_date <= :trade_date
    GROUP BY ts_code
)
SELECT
    t.trade_date,
    t.ts_code,
    t.symbol,
    t.historical_name,
    t.name,
    t.industry,
    t.market,
    t.exchange,
    t.list_date,
    t.list_status,
    t.delist_date,
    t.close,
    t.daily_basic_close,
    t.first_financial_ann_date,
    t.has_ever_st,
    COALESCE(p.price_history_bars, 0) AS price_history_bars
FROM target_day t
LEFT JOIN price_history p
  ON p.ts_code = t.ts_code
ORDER BY t.trade_date, t.ts_code
"""

INSERT_SQL = text(
    """
    INSERT INTO ml_stock_universe_daily (
        trade_date, ts_code, symbol, name, industry, market, exchange, list_date,
        listing_days, list_status, is_current_st, has_ever_st, has_price,
        has_daily_basic, has_financial, min_history_ok, sample_eligible,
        created_at, updated_at
    ) VALUES (
        :trade_date, :ts_code, :symbol, :name, :industry, :market, :exchange, :list_date,
        :listing_days, :list_status, :is_current_st, :has_ever_st, :has_price,
        :has_daily_basic, :has_financial, :min_history_ok, :sample_eligible,
        :created_at, :updated_at
    )
    ON CONFLICT (trade_date, ts_code) DO UPDATE SET
        symbol = EXCLUDED.symbol,
        name = EXCLUDED.name,
        industry = EXCLUDED.industry,
        market = EXCLUDED.market,
        exchange = EXCLUDED.exchange,
        list_date = EXCLUDED.list_date,
        listing_days = EXCLUDED.listing_days,
        list_status = EXCLUDED.list_status,
        is_current_st = EXCLUDED.is_current_st,
        has_ever_st = EXCLUDED.has_ever_st,
        has_price = EXCLUDED.has_price,
        has_daily_basic = EXCLUDED.has_daily_basic,
        has_financial = EXCLUDED.has_financial,
        min_history_ok = EXCLUDED.min_history_ok,
        sample_eligible = EXCLUDED.sample_eligible,
        updated_at = NOW()
    """
)


def is_st_name(name) -> bool:
    if name is None:
        return False
    text_value = str(name).strip().upper()
    return (
        text_value.startswith("ST")
        or text_value.startswith("*ST")
        or text_value.startswith("S*ST")
        or text_value.startswith("SST")
    )


def is_active_stock(list_status, delist_date=None) -> bool:
    status = str(list_status or "").strip().upper()
    return status == "L" or (status == "" and pd.isna(delist_date))


def main() -> int:
    trade_date = pd.Timestamp("2026-05-08").date()
    now = utcnow()
    engine = get_engine()

    with engine.connect() as conn:
        df = pd.read_sql(text(SQL), conn, params={"trade_date": trade_date})

    rows = []
    for record in df.to_dict(orient="records"):
        list_date = pd.to_datetime(record.get("list_date"), errors="coerce")
        trade_ts = pd.to_datetime(record.get("trade_date"), errors="coerce")
        listing_days = None
        if pd.notna(list_date) and pd.notna(trade_ts) and trade_ts.date() >= list_date.date():
            listing_days = (trade_ts.date() - list_date.date()).days + 1
        has_price = pd.notna(record.get("close")) and float(record.get("close") or 0) > 0
        has_daily_basic = pd.notna(record.get("daily_basic_close"))
        has_financial = pd.notna(record.get("first_financial_ann_date"))
        min_history_ok = int(record.get("price_history_bars") or 0) >= 60
        sample_eligible = is_active_stock(record.get("list_status"), record.get("delist_date")) and has_price and min_history_ok
        rows.append(
            {
                "trade_date": trade_ts.date() if pd.notna(trade_ts) else None,
                "ts_code": record.get("ts_code"),
                "symbol": record.get("symbol"),
                "name": record.get("historical_name") or record.get("name"),
                "industry": record.get("industry"),
                "market": record.get("market"),
                "exchange": record.get("exchange"),
                "list_date": list_date.date() if pd.notna(list_date) else None,
                "listing_days": listing_days,
                "list_status": record.get("list_status"),
                "is_current_st": is_st_name(record.get("historical_name") or record.get("name")),
                "has_ever_st": bool(record.get("has_ever_st")),
                "has_price": has_price,
                "has_daily_basic": has_daily_basic,
                "has_financial": has_financial,
                "min_history_ok": min_history_ok,
                "sample_eligible": sample_eligible,
                "created_at": now,
                "updated_at": now,
            }
        )

    with engine.begin() as conn:
        conn.execute(text("delete from ml_stock_universe_daily where trade_date = :trade_date"), {"trade_date": trade_date})
        if rows:
            conn.execute(INSERT_SQL, rows)

    print(json.dumps({
        "trade_date": str(trade_date),
        "rows_fetched": int(len(df)),
        "rows_written": int(len(rows)),
        "eligible_rows": int(sum(1 for row in rows if row["sample_eligible"])),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
