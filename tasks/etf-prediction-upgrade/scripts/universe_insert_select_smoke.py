from __future__ import annotations

import json

from sqlalchemy import text

from src.ml_stock_dataset import get_engine

SQL = text("""
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
price_history AS (
    SELECT ts_code, COUNT(*) AS price_history_bars
    FROM vw_ts_stock_daily
    WHERE trade_date <= :trade_date
    GROUP BY ts_code
),
payload AS (
    SELECT
        d.trade_date,
        d.ts_code,
        b.symbol,
        COALESCE(nc_hist.name, b.name) AS name,
        b.industry,
        b.market,
        COALESCE(c.exchange, b.exchange) AS exchange,
        b.list_date,
        CASE
            WHEN b.list_date IS NOT NULL AND d.trade_date >= b.list_date THEN (d.trade_date - b.list_date) + 1
            ELSE NULL
        END AS listing_days,
        b.list_status,
        CASE
            WHEN UPPER(COALESCE(nc_hist.name, b.name, '')) LIKE 'ST%'
              OR UPPER(COALESCE(nc_hist.name, b.name, '')) LIKE '*ST%'
              OR UPPER(COALESCE(nc_hist.name, b.name, '')) LIKE 'S*ST%'
              OR UPPER(COALESCE(nc_hist.name, b.name, '')) LIKE 'SST%'
            THEN TRUE ELSE FALSE
        END AS is_current_st,
        (st.ts_code IS NOT NULL) AS has_ever_st,
        (d.close IS NOT NULL AND d.close > 0) AS has_price,
        (db.close IS NOT NULL) AS has_daily_basic,
        (fa.first_financial_ann_date IS NOT NULL) AS has_financial,
        (COALESCE(ph.price_history_bars, 0) >= 60) AS min_history_ok,
        (
            (COALESCE(ph.price_history_bars, 0) >= 60)
            AND (d.close IS NOT NULL AND d.close > 0)
            AND (b.list_status = 'L' OR (b.list_status IS NULL AND b.delist_date IS NULL))
        ) AS sample_eligible
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
    LEFT JOIN price_history ph
      ON ph.ts_code = d.ts_code
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
)
, deleted AS (
    DELETE FROM ml_stock_universe_daily WHERE trade_date = :trade_date RETURNING 1
), inserted AS (
    INSERT INTO ml_stock_universe_daily (
        trade_date, ts_code, symbol, name, industry, market, exchange, list_date,
        listing_days, list_status, is_current_st, has_ever_st, has_price,
        has_daily_basic, has_financial, min_history_ok, sample_eligible,
        created_at, updated_at
    )
    SELECT
        trade_date, ts_code, symbol, name, industry, market, exchange, list_date,
        listing_days, list_status, is_current_st, has_ever_st, has_price,
        has_daily_basic, has_financial, min_history_ok, sample_eligible,
        NOW(), NOW()
    FROM payload
    RETURNING sample_eligible
)
SELECT
    (SELECT COUNT(*) FROM payload) AS payload_rows,
    (SELECT COUNT(*) FROM inserted) AS inserted_rows,
    (SELECT COUNT(*) FROM inserted WHERE sample_eligible) AS eligible_rows
""")


def main() -> int:
    engine = get_engine()
    with engine.begin() as conn:
        row = conn.execute(SQL, {"trade_date": "2026-05-08"}).mappings().one()
    print(json.dumps(dict(row), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
