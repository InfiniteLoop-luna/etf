from src.fund_hot_stocks import get_engine, ensure_all_tables, discover_missing_funds_from_aux_sources, search_funds
from src.volume_fetcher import _init_tushare

engine = get_engine()
ensure_all_tables(engine)
pro = _init_tushare()
written = discover_missing_funds_from_aux_sources(engine, pro, fund_codes=['007491.OF'], api_sleep=0)
print('written', written)
df = search_funds('007491', limit=10, engine=engine)
print('rows', 0 if df is None else len(df))
if df is not None and not df.empty:
    print(df.to_dict(orient='records'))
