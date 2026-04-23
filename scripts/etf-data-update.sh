#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$APP_DIR"

echo "[$(date -Is)] etf-data-update: started"

if [[ -f "$APP_DIR/.env" ]]; then
  set -a
  source "$APP_DIR/.env"
  set +a
fi

if [[ ! -x "$APP_DIR/.venv/bin/python" ]]; then
  echo "[$(date -Is)] etf-data-update: missing virtualenv python at $APP_DIR/.venv/bin/python"
  exit 1
fi

source "$APP_DIR/.venv/bin/activate"

if [[ -z "${TUSHARE_TOKEN:-}" ]]; then
  echo "[$(date -Is)] etf-data-update: TUSHARE_TOKEN is missing in $APP_DIR/.env, skip DB aggregation update"
  exit 0
fi

echo "[$(date -Is)] etf-data-update: run sync_tushare_security_data.py"
python src/sync_tushare_security_data.py --datasets daily_basic index_dailybasic

echo "[$(date -Is)] etf-data-update: run fetch_etf_share_size.py"
python src/fetch_etf_share_size.py

echo "[$(date -Is)] etf-data-update: run update_moneyflow.py (incremental)"
python update_moneyflow.py --datasets moneyflow,moneyflow_hsgt,moneyflow_ind_ths,moneyflow_dc_ind --lookback-days 1

echo "[$(date -Is)] etf-data-update: run update_limitup_monitor.py (incremental)"
python update_limitup_monitor.py --datasets limit_list_d,limit_step,limit_cpt_list,kpl_list,limit_list_ths

echo "[$(date -Is)] etf-data-update: run update_hotmoney.py (safe incremental)"
python update_hotmoney.py --datasets hm_list
python update_hotmoney.py --datasets hm_detail --detail-batch-days 1 --detail-sleep 35 --detail-lookback-days 0

echo "[$(date -Is)] etf-data-update: run generate_daily_trend_reco_from_pyc.py"
TZ=Asia/Shanghai PYTHONPATH="$APP_DIR" python scripts/generate_daily_trend_reco_from_pyc.py

echo "[$(date -Is)] etf-data-update: restart streamlit"
systemctl restart etf-streamlit

echo "[$(date -Is)] etf-data-update: completed"
