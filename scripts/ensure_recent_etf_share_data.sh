#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${ETF_APP_DIR:-/opt/etf-app}"
cd "$APP_DIR"

LATEST_SHARE_DATE=$(./.venv/bin/python -c "from src.fetch_etf_share_size import get_engine, get_latest_date; print(get_latest_date(get_engine()) or '')")
LATEST_AGG_DATE=$(./.venv/bin/python -c "from src.aggregate_etf_categories import get_engine, get_latest_agg_date; print((get_latest_agg_date(get_engine()) or '').replace('-', ''))")
TODAY_SH=$(TZ=Asia/Shanghai date +%Y%m%d)
YESTERDAY_SH=$(TZ=Asia/Shanghai date -d 'yesterday' +%Y%m%d)

echo "[$(date -Is)] ensure_recent_etf_share_data: share_latest=${LATEST_SHARE_DATE:-none} agg_latest=${LATEST_AGG_DATE:-none} yesterday=$YESTERDAY_SH today=$TODAY_SH"

if [[ -n "$LATEST_SHARE_DATE" && "$LATEST_SHARE_DATE" -lt "$YESTERDAY_SH" ]]; then
  echo "[$(date -Is)] ensure_recent_etf_share_data: share latest behind yesterday, run targeted backfill"
  python src/fetch_etf_share_size.py --start-date "$YESTERDAY_SH" --end-date "$TODAY_SH" --skip-verify || \
    echo "[$(date -Is)] ensure_recent_etf_share_data: warning - targeted ETF share backfill failed, continue"
  python src/aggregate_etf_categories.py --start-date "$YESTERDAY_SH" --end-date "$TODAY_SH" || \
    echo "[$(date -Is)] ensure_recent_etf_share_data: warning - targeted ETF aggregate backfill failed, continue"
  LATEST_SHARE_DATE=$(./.venv/bin/python -c "from src.fetch_etf_share_size import get_engine, get_latest_date; print(get_latest_date(get_engine()) or '')")
  LATEST_AGG_DATE=$(./.venv/bin/python -c "from src.aggregate_etf_categories import get_engine, get_latest_agg_date; print((get_latest_agg_date(get_engine()) or '').replace('-', ''))")
fi

echo "[$(date -Is)] ensure_recent_etf_share_data: final share_latest=${LATEST_SHARE_DATE:-none} agg_latest=${LATEST_AGG_DATE:-none}"
