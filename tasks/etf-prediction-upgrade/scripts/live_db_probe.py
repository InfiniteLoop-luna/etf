from __future__ import annotations

import json
import pandas as pd

from src.ml_stock_dataset import get_engine

engine = get_engine()

summary_sql = """
select
  min(trade_date) as min_date,
  max(trade_date) as max_date,
  count(*) as row_count,
  sum(case when sample_eligible then 1 else 0 end) as eligible_count,
  min(case when sample_eligible then trade_date end) as eligible_min_date,
  max(case when sample_eligible then trade_date end) as eligible_max_date
from ml_stock_sample_daily
"""

per_date_sql = """
select
  trade_date,
  count(*) as row_count,
  sum(case when sample_eligible then 1 else 0 end) as eligible_count
from ml_stock_sample_daily
group by trade_date
order by trade_date
"""

summary_df = pd.read_sql(summary_sql, engine)
per_date_df = pd.read_sql(per_date_sql, engine)

result = {
    "summary": json.loads(summary_df.to_json(orient="records", date_format="iso")),
    "per_date_head": json.loads(per_date_df.head(10).to_json(orient="records", date_format="iso")),
    "per_date_tail": json.loads(per_date_df.tail(10).to_json(orient="records", date_format="iso")),
    "date_count": int(len(per_date_df)),
}

print(json.dumps(result, ensure_ascii=False, indent=2))
