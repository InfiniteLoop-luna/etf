from __future__ import annotations

import json
import pandas as pd

from src.ml_stock_dataset import get_engine

engine = get_engine()

tables = [
    "ml_stock_universe_daily",
    "ml_stock_feature_daily",
    "ml_stock_label_daily",
]

result: dict[str, object] = {"tables": {}, "sample_preview": None}

for table_name in tables:
    sql = f"""
    select
      min(trade_date) as min_date,
      max(trade_date) as max_date,
      count(*) as row_count
    from {table_name}
    """
    df = pd.read_sql(sql, engine)
    result["tables"][table_name] = json.loads(df.to_json(orient="records", date_format="iso"))[0]

sample_sql = """
select
  trade_date,
  ts_code,
  sample_eligible,
  quality_flag
from ml_stock_sample_daily
order by trade_date desc, ts_code asc
limit 5
"""

sample_df = pd.read_sql(sample_sql, engine)
result["sample_preview"] = json.loads(sample_df.to_json(orient="records", date_format="iso"))

print(json.dumps(result, ensure_ascii=False, indent=2))
