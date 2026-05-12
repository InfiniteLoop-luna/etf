from __future__ import annotations

import json
import time
import pandas as pd
from sqlalchemy import text

from src.ml_stock_dataset import get_engine, _build_financial_ann_cte_sql, _to_date

engine = get_engine()
start_dt = _to_date("2026-05-08")
end_dt = _to_date("2026-05-08")

sql = f"""
WITH
{_build_financial_ann_cte_sql()},
st_history AS (
    SELECT DISTINCT ts_code
    FROM vw_ts_stock_namechange
    WHERE (
        COALESCE(name, '') LIKE 'ST%%'
        OR COALESCE(name, '') LIKE '*ST%%'
        OR COALESCE(name, '') LIKE 'S*ST%%'
        OR COALESCE(name, '') LIKE 'SST%%'
    )
),
daily_enriched AS (
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
        (st.ts_code IS NOT NULL) AS has_ever_st,
        ROW_NUMBER() OVER (PARTITION BY d.ts_code ORDER BY d.trade_date) AS price_history_bars
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
)
SELECT
    trade_date,
    ts_code,
    symbol,
    historical_name,
    name,
    industry,
    market,
    exchange,
    list_date,
    list_status,
    delist_date,
    close,
    daily_basic_close,
    first_financial_ann_date,
    has_ever_st,
    price_history_bars
FROM daily_enriched
WHERE (:start_date IS NULL OR trade_date >= :start_date)
  AND (:end_date IS NULL OR trade_date <= :end_date)
ORDER BY trade_date, ts_code
LIMIT 200
"""

with engine.connect() as conn:
    start = time.time()
    df = pd.read_sql(text(sql), conn, params={"start_date": start_dt, "end_date": end_dt})
    elapsed = time.time() - start

print(json.dumps({
    "elapsed_seconds": round(elapsed, 3),
    "row_count": int(len(df)),
    "head": json.loads(df.head(5).to_json(orient="records", date_format="iso")),
}, ensure_ascii=False, indent=2))
