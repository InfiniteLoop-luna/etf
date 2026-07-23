#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${ETF_APP_DIR:-/opt/etf-app}"
cd "$APP_DIR"

LATEST_SHARE_DATE=$(./.venv/bin/python -c "from src.fetch_etf_share_size import get_engine, get_latest_date; print(get_latest_date(get_engine()) or '')")
LATEST_AGG_DATE=$(./.venv/bin/python -c "from src.aggregate_etf_categories import get_engine, get_latest_agg_date; print((get_latest_agg_date(get_engine()) or '').replace('-', ''))")
TODAY_SH=$(TZ=Asia/Shanghai date +%Y%m%d)
YESTERDAY_SH=$(TZ=Asia/Shanghai date -d 'yesterday' +%Y%m%d)

echo "[$(date -Is)] ensure_recent_etf_share_data: share_latest=${LATEST_SHARE_DATE:-none} agg_latest=${LATEST_AGG_DATE:-none} yesterday=$YESTERDAY_SH today=$TODAY_SH"

SHOULD_BACKFILL=0
if [[ -z "$LATEST_SHARE_DATE" || "$LATEST_SHARE_DATE" -lt "$TODAY_SH" ]]; then
  SHOULD_BACKFILL=1
fi
if [[ -z "$LATEST_AGG_DATE" || "$LATEST_AGG_DATE" -lt "$TODAY_SH" ]]; then
  SHOULD_BACKFILL=1
fi

if [[ "$SHOULD_BACKFILL" == "1" ]]; then
  echo "[$(date -Is)] ensure_recent_etf_share_data: ETF data not caught up to today, run targeted backfill"
  python src/fetch_etf_share_size.py --start-date "$YESTERDAY_SH" --end-date "$TODAY_SH" --skip-verify || \
    echo "[$(date -Is)] ensure_recent_etf_share_data: warning - targeted ETF share backfill failed, continue"
  python src/aggregate_etf_categories.py --start-date "$YESTERDAY_SH" --end-date "$TODAY_SH" || \
    echo "[$(date -Is)] ensure_recent_etf_share_data: warning - targeted ETF aggregate backfill failed, continue"
  LATEST_SHARE_DATE=$(./.venv/bin/python -c "from src.fetch_etf_share_size import get_engine, get_latest_date; print(get_latest_date(get_engine()) or '')")
  LATEST_AGG_DATE=$(./.venv/bin/python -c "from src.aggregate_etf_categories import get_engine, get_latest_agg_date; print((get_latest_agg_date(get_engine()) or '').replace('-', ''))")
fi

echo "[$(date -Is)] ensure_recent_etf_share_data: final share_latest=${LATEST_SHARE_DATE:-none} agg_latest=${LATEST_AGG_DATE:-none}"
