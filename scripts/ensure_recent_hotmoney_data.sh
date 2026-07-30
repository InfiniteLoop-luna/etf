#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${ETF_APP_DIR:-/opt/etf-app}"
cd "$APP_DIR"

LATEST_DETAIL_DATE=$(./.venv/bin/python -c "from src.hotmoney_monitor import get_hotmoney_latest_detail_date; print(get_hotmoney_latest_detail_date() or '')")
TODAY_SH=$(TZ=Asia/Shanghai date +%Y%m%d)
YESTERDAY_SH=$(TZ=Asia/Shanghai date -d 'yesterday' +%Y%m%d)
TARGET_SH=$YESTERDAY_SH

HOTMONEY_DETAIL_BATCH_DAYS="${ETF_HM_DETAIL_BATCH_DAYS:-10}"
HOTMONEY_DETAIL_SLEEP_SECONDS="${ETF_HM_DETAIL_SLEEP_SECONDS:-35}"
HOTMONEY_DETAIL_LOOKBACK_DAYS="${ETF_HM_DETAIL_LOOKBACK_DAYS:-0}"
HOTMONEY_DETAIL_MAX_DAYS="${ETF_HM_DETAIL_MAX_DAYS:-10}"

echo "[$(date -Is)] ensure_recent_hotmoney_data: latest_detail=${LATEST_DETAIL_DATE:-none} target=$TARGET_SH yesterday=$YESTERDAY_SH today=$TODAY_SH"

SHOULD_BACKFILL=0
if [[ -z "$LATEST_DETAIL_DATE" || "$LATEST_DETAIL_DATE" -lt "$TARGET_SH" ]]; then
  SHOULD_BACKFILL=1
fi

if [[ "$SHOULD_BACKFILL" == "1" ]]; then
  echo "[$(date -Is)] ensure_recent_hotmoney_data: hotmoney detail lagging, run targeted backfill"
  if ! python update_hotmoney.py --datasets hm_detail --start "$YESTERDAY_SH" --end "$TODAY_SH" --detail-batch-days "$HOTMONEY_DETAIL_BATCH_DAYS" --detail-sleep "$HOTMONEY_DETAIL_SLEEP_SECONDS" --detail-lookback-days "$HOTMONEY_DETAIL_LOOKBACK_DAYS" --detail-max-days "$HOTMONEY_DETAIL_MAX_DAYS"; then
    echo "[$(date -Is)] ensure_recent_hotmoney_data: warning - targeted hotmoney backfill failed or rate-limited"
  fi
  LATEST_DETAIL_DATE=$(./.venv/bin/python -c "from src.hotmoney_monitor import get_hotmoney_latest_detail_date; print(get_hotmoney_latest_detail_date() or '')")
fi

echo "[$(date -Is)] ensure_recent_hotmoney_data: final latest_detail=${LATEST_DETAIL_DATE:-none} target=$TARGET_SH"

if [[ -z "$LATEST_DETAIL_DATE" || "$LATEST_DETAIL_DATE" -lt "$TARGET_SH" ]]; then
  echo "[$(date -Is)] ensure_recent_hotmoney_data: latest detail still behind target after retry" >&2
  exit 2
fi
