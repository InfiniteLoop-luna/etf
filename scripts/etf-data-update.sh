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

echo "[$(date -Is)] etf-data-update: capture fund watchlist 15:00 estimate snapshots (fallback)"
if ! TZ=Asia/Shanghai PYTHONPATH="$APP_DIR" "$APP_DIR/.venv/bin/python" scripts/capture_fund_watchlist_estimates.py; then
  echo "[$(date -Is)] etf-data-update: warning - fund estimate snapshot capture failed, skip and continue"
fi

if [[ -z "${TUSHARE_TOKEN:-}" ]]; then
  echo "[$(date -Is)] etf-data-update: TUSHARE_TOKEN is missing in $APP_DIR/.env, skip DB aggregation update"
  exit 0
fi

echo "[$(date -Is)] etf-data-update: run sync_tushare_security_data.py"
python src/sync_tushare_security_data.py --datasets stock_basic stock_company stock_holdernumber namechange daily daily_basic index_dailybasic stk_week_month_adj

echo "[$(date -Is)] etf-data-update: run fetch_etf_share_size.py"
ETF_SHARE_SIZE_ARGS=()
if [[ "${ETF_SHARE_SIZE_SKIP_VERIFY:-0}" == "1" ]]; then
  ETF_SHARE_SIZE_ARGS+=(--skip-verify)
fi
python src/fetch_etf_share_size.py "${ETF_SHARE_SIZE_ARGS[@]}"

echo "[$(date -Is)] etf-data-update: run aggregate_etf_categories.py"
python src/aggregate_etf_categories.py

echo "[$(date -Is)] etf-data-update: run update_moneyflow.py (incremental)"
python update_moneyflow.py --datasets moneyflow,moneyflow_hsgt,moneyflow_ind_ths,moneyflow_dc_ind --lookback-days 1

echo "[$(date -Is)] etf-data-update: run update_margin.py (incremental)"
python update_margin.py --datasets margin,margin_detail --lookback-days 2

echo "[$(date -Is)] etf-data-update: run update_limitup_monitor.py (incremental)"
python update_limitup_monitor.py --datasets limit_list_d,limit_step,limit_cpt_list,kpl_list,limit_list_ths

echo "[$(date -Is)] etf-data-update: run update_lhb_monitor.py (safe incremental)"
LHB_BATCH_DAYS="${ETF_LHB_BATCH_DAYS:-3}"
LHB_SLEEP_SECONDS="${ETF_LHB_SLEEP_SECONDS:-0.35}"
LHB_LOOKBACK_DAYS="${ETF_LHB_LOOKBACK_DAYS:-2}"
python update_lhb_monitor.py --datasets top_list,top_inst --batch-days "$LHB_BATCH_DAYS" --sleep "$LHB_SLEEP_SECONDS" --lookback-days "$LHB_LOOKBACK_DAYS"

echo "[$(date -Is)] etf-data-update: run update_hotmoney.py (safe incremental)"
python update_hotmoney.py --datasets hm_list
HOTMONEY_DETAIL_BATCH_DAYS="${ETF_HM_DETAIL_BATCH_DAYS:-1}"
python update_hotmoney.py --datasets hm_detail --detail-batch-days "$HOTMONEY_DETAIL_BATCH_DAYS" --detail-sleep 35 --detail-lookback-days 0


echo "[$(date -Is)] etf-data-update: run generate_daily_trend_reco_from_pyc.py"
TREND_RECO_CALIBRATION_ANCHORS="${ETF_TREND_RECO_CALIBRATION_ANCHORS:-0}"
TZ=Asia/Shanghai PYTHONPATH="$APP_DIR" "$APP_DIR/.venv/bin/python" scripts/generate_daily_trend_reco_from_pyc.py --probability-calibration-anchors "$TREND_RECO_CALIBRATION_ANCHORS"

echo "[$(date -Is)] etf-data-update: run write_reco_candidate_score_snapshot.py"
if ! TZ=Asia/Shanghai PYTHONPATH="$APP_DIR" "$APP_DIR/.venv/bin/python" scripts/write_reco_candidate_score_snapshot.py --lookback-days 60 --min-train-rows 2000 --max-candidates 30 --recent-train-rows 6000; then
  echo "[$(date -Is)] etf-data-update: warning - write_reco_candidate_score_snapshot.py failed, skip and continue"
fi

AUTHOR_UID="${EASTMONEY_AUTHOR_UID:-4348595203199492}"
echo "[$(date -Is)] etf-data-update: run sync_eastmoney_author.py for uid=${AUTHOR_UID}"
if ! TZ=Asia/Shanghai PYTHONPATH="$APP_DIR" "$APP_DIR/.venv/bin/python" scripts/sync_eastmoney_author.py --author-uid "$AUTHOR_UID" --max-pages 30 --page-size 20 --unchanged-post-stop-count 10 --reply-cutoff-date 2026-04-01; then
  echo "[$(date -Is)] etf-data-update: warning - sync_eastmoney_author.py failed, skip and continue"
fi

if command -v tesseract >/dev/null 2>&1 && TZ=Asia/Shanghai PYTHONPATH="$APP_DIR" "$APP_DIR/.venv/bin/python" -c "import pytesseract" >/dev/null 2>&1; then
  echo "[$(date -Is)] etf-data-update: enrich pending OCR for uid=${AUTHOR_UID}"
  if ! TZ=Asia/Shanghai PYTHONPATH="$APP_DIR" "$APP_DIR/.venv/bin/python" scripts/sync_eastmoney_author.py --author-uid "$AUTHOR_UID" --use-tesseract --enrich-pending-ocr --skip-sync --ocr-limit 50; then
    echo "[$(date -Is)] etf-data-update: warning - OCR enrichment failed, skip and continue"
  fi
else
  echo "[$(date -Is)] etf-data-update: OCR prerequisites missing, skip OCR enrichment"
fi

echo "[$(date -Is)] etf-data-update: run update_distribution_alerts.py"
if ! TZ=Asia/Shanghai PYTHONPATH="$APP_DIR" "$APP_DIR/.venv/bin/python" scripts/update_distribution_alerts.py; then
  echo "[$(date -Is)] etf-data-update: warning - update_distribution_alerts.py failed, skip and continue"
fi

WATCHLIST_DISTRIBUTION_TIMEOUT_SECONDS="${ETF_WATCHLIST_DISTRIBUTION_TIMEOUT_SECONDS:-1800}"
WATCHLIST_STOCK_RESEARCH_TIMEOUT_SECONDS="${ETF_WATCHLIST_STOCK_RESEARCH_TIMEOUT_SECONDS:-1800}"

echo "[$(date -Is)] etf-data-update: run update_watchlist_distribution_reports.py"
if ! timeout "${WATCHLIST_DISTRIBUTION_TIMEOUT_SECONDS}s" env TZ=Asia/Shanghai PYTHONPATH="$APP_DIR" "$APP_DIR/.venv/bin/python" scripts/update_watchlist_distribution_reports.py; then
  echo "[$(date -Is)] etf-data-update: warning - update_watchlist_distribution_reports.py failed or timed out, skip and continue"
fi

echo "[$(date -Is)] etf-data-update: run update_watchlist_stock_research_reports.py"
if ! timeout "${WATCHLIST_STOCK_RESEARCH_TIMEOUT_SECONDS}s" env TZ=Asia/Shanghai PYTHONPATH="$APP_DIR" "$APP_DIR/.venv/bin/python" scripts/update_watchlist_stock_research_reports.py; then
  echo "[$(date -Is)] etf-data-update: warning - update_watchlist_stock_research_reports.py failed or timed out, skip and continue"
fi

echo "[$(date -Is)] etf-data-update: restart streamlit"
systemctl restart etf-streamlit

echo "[$(date -Is)] etf-data-update: completed"
