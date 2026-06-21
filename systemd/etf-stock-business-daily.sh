#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${ETF_APP_DIR:-/opt/etf-app}"
cd "$APP_DIR"

LOG_DIR="$APP_DIR/output/stock_business/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/stock-business-daily_$(date +%Y%m%d_%H%M%S).log"

exec > >(tee -a "$LOG_FILE") 2>&1

echo "[$(date -Is)] etf-stock-business-daily: start"

if [[ -f "$APP_DIR/.env" ]]; then
  set -a
  source "$APP_DIR/.env"
  set +a
fi

if [[ ! -x "$APP_DIR/.venv/bin/python" ]]; then
  echo "[$(date -Is)] etf-stock-business-daily: missing virtualenv python at $APP_DIR/.venv/bin/python"
  exit 1
fi

TZ=Asia/Shanghai PYTHONPATH="$APP_DIR" "$APP_DIR/.venv/bin/python" \
  "$APP_DIR/scripts/export_stock_business_to_excel.py" --json

bash "$APP_DIR/scripts/backup_stock_business_to_github.sh"

echo "[$(date -Is)] etf-stock-business-daily: done"
