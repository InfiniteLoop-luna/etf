from __future__ import annotations

import json
import pandas as pd

from src.ml_stock_dataset import get_engine

engine = get_engine()

sql = """
select
  a.pid,
  a.state,
  a.wait_event_type,
  a.wait_event,
  a.application_name,
  a.client_addr,
  a.backend_start,
  a.xact_start,
  a.query_start,
  now() - a.query_start as query_runtime,
  now() - a.xact_start as xact_runtime,
  pg_blocking_pids(a.pid) as blocking_pids,
  left(a.query, 300) as query
from pg_stat_activity a
where a.datname = current_database()
  and a.usename = current_user
  and a.state <> 'idle'
order by a.query_start asc
"""

df = pd.read_sql(sql, engine)
print(json.dumps(json.loads(df.to_json(orient="records", date_format="iso")), ensure_ascii=False, indent=2))
