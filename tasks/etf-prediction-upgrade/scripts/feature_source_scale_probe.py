from __future__ import annotations

import json
import pandas as pd
from sqlalchemy import text

from src.ml_stock_dataset import get_engine

sql = text("""
WITH target_codes AS (
    SELECT DISTINCT ts_code
    FROM ml_stock_universe_daily
    WHERE trade_date = :trade_date
)
SELECT
    COUNT(*) AS source_rows,
    COUNT(DISTINCT d.ts_code) AS code_count,
    MIN(d.trade_date) AS min_trade_date,
    MAX(d.trade_date) AS max_trade_date
FROM vw_ts_stock_daily d
JOIN target_codes tc
  ON tc.ts_code = d.ts_code
WHERE d.trade_date >= :feature_start_date
  AND d.trade_date <= :feature_end_date
""")

engine = get_engine()
with engine.connect() as conn:
    df = pd.read_sql(
        sql,
        conn,
        params={
            "trade_date": "2026-05-08",
            "feature_start_date": "2026-02-27",
            "feature_end_date": "2026-05-08",
        },
    )
print(json.dumps(json.loads(df.to_json(orient="records", date_format="iso")), ensure_ascii=False, indent=2))
