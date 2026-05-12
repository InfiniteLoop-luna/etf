from __future__ import annotations

import json
import pandas as pd

from src.ml_stock_dataset import get_engine

engine = get_engine()

sql = """
select
  pid,
  state,
  wait_event_type,
  wait_event,
  now() - query_start as runtime,
  left(query, 200) as query
from pg_stat_activity
where datname = current_database()
  and usename = current_user
  and state <> 'idle'
order by query_start asc
"""

df = pd.read_sql(sql, engine)
print(json.dumps(json.loads(df.to_json(orient="records", date_format="iso")), ensure_ascii=False, indent=2))
