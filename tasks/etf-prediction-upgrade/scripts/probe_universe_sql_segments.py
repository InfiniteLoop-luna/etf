from __future__ import annotations

import json
import time
import pandas as pd
from sqlalchemy import text

from src.ml_stock_dataset import get_engine

engine = get_engine()

queries = {
    "daily_count": """
        select count(*) as row_count
        from vw_ts_stock_daily d
        where d.trade_date = :trade_date
    """,
    "daily_join_basic_count": """
        select count(*) as row_count
        from vw_ts_stock_daily d
        join vw_ts_stock_basic b on d.ts_code = b.ts_code
        where d.trade_date = :trade_date
    """,
    "daily_join_basic_db_count": """
        select count(*) as row_count
        from vw_ts_stock_daily d
        join vw_ts_stock_basic b on d.ts_code = b.ts_code
        left join vw_ts_stock_daily_basic db
          on db.ts_code = d.ts_code
         and db.trade_date = d.trade_date
        where d.trade_date = :trade_date
    """,
    "namechange_lateral_sample": """
        select count(*) as row_count
        from (
            select d.ts_code
            from vw_ts_stock_daily d
            join vw_ts_stock_basic b on d.ts_code = b.ts_code
            left join lateral (
                select nc.name
                from vw_ts_stock_namechange nc
                where nc.ts_code = d.ts_code
                  and nc.start_date is not null
                  and nc.start_date <= d.trade_date
                  and (
                        coalesce(nc.nc_end_date, nc.end_date) is null
                        or coalesce(nc.nc_end_date, nc.end_date) >= d.trade_date
                  )
                order by nc.start_date desc, coalesce(nc.nc_ann_date, nc.ann_date) desc nulls last
                limit 1
            ) nc_hist on true
            where d.trade_date = :trade_date
            limit 100
        ) t
    """,
    "financial_ann_sample": """
        with financial_ann as (
            select ts_code, min(ann_date) as first_financial_ann_date
            from (
                select ts_code, ann_date from vw_ts_stock_income where ann_date is not null
                union all
                select ts_code, ann_date from vw_ts_stock_balancesheet where ann_date is not null
                union all
                select ts_code, ann_date from vw_ts_stock_cashflow where ann_date is not null
                union all
                select ts_code, ann_date from vw_ts_stock_fina_indicator where ann_date is not null
            ) t
            group by ts_code
        )
        select count(*) as row_count
        from financial_ann
    """,
}

params = {"trade_date": "2026-05-08"}
results = []
with engine.connect() as conn:
    for name, sql in queries.items():
        start = time.time()
        df = pd.read_sql(text(sql), conn, params=params)
        elapsed = time.time() - start
        results.append(
            {
                "name": name,
                "elapsed_seconds": round(elapsed, 3),
                "result": json.loads(df.to_json(orient="records", date_format="iso")),
            }
        )

print(json.dumps(results, ensure_ascii=False, indent=2))
