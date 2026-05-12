from __future__ import annotations

import json
import pandas as pd

from src.ml_stock_dataset import get_engine

engine = get_engine()

objects = {
    "vw_ts_stock_daily": "select min(trade_date) as min_date, max(trade_date) as max_date, count(*) as row_count from vw_ts_stock_daily",
    "vw_ts_stock_daily_basic": "select min(trade_date) as min_date, max(trade_date) as max_date, count(*) as row_count from vw_ts_stock_daily_basic",
    "vw_ts_stock_basic": "select count(*) as row_count from vw_ts_stock_basic",
    "vw_ts_stock_company": "select count(*) as row_count from vw_ts_stock_company",
    "vw_ts_stock_namechange": "select count(*) as row_count from vw_ts_stock_namechange",
    "ts_stock_technical_signals": "select min(trade_date) as min_date, max(trade_date) as max_date, count(*) as row_count from ts_stock_technical_signals",
    "vw_ts_stock_income": "select count(*) as row_count, min(ann_date) as min_ann_date, max(ann_date) as max_ann_date from vw_ts_stock_income",
    "vw_ts_stock_balancesheet": "select count(*) as row_count, min(ann_date) as min_ann_date, max(ann_date) as max_ann_date from vw_ts_stock_balancesheet",
    "vw_ts_stock_cashflow": "select count(*) as row_count, min(ann_date) as min_ann_date, max(ann_date) as max_ann_date from vw_ts_stock_cashflow",
    "vw_ts_stock_fina_indicator": "select count(*) as row_count, min(ann_date) as min_ann_date, max(ann_date) as max_ann_date from vw_ts_stock_fina_indicator",
}

result = {}
for name, sql in objects.items():
    df = pd.read_sql(sql, engine)
    result[name] = json.loads(df.to_json(orient="records", date_format="iso"))[0]

print(json.dumps(result, ensure_ascii=False, indent=2))
