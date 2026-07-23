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

step_log() {
  local level="$1"
  local step="$2"
  echo "STEP|${level}|${step}|$(date -Is)"
}

echo "[$(date -Is)] etf-data-update: capture fund watchlist 15:00 estimate snapshots (fallback)"
step_log START capture_fund_watchlist_estimates
if ! TZ=Asia/Shanghai PYTHONPATH="$APP_DIR" "$APP_DIR/.venv/bin/python" scripts/capture_fund_watchlist_estimates.py; then
  echo "[$(date -Is)] etf-data-update: warning - fund estimate snapshot capture failed, skip and continue"
  step_log WARN capture_fund_watchlist_estimates
else
  step_log OK capture_fund_watchlist_estimates
fi

if [[ -z "${TUSHARE_TOKEN:-}" ]]; then
  echo "[$(date -Is)] etf-data-update: TUSHARE_TOKEN is missing in $APP_DIR/.env, skip DB aggregation update"
  exit 0
fi

echo "[$(date -Is)] etf-data-update: run sync_tushare_security_data.py"
step_log START sync_tushare_security_data
python src/sync_tushare_security_data.py --datasets stock_basic stock_company stock_holdernumber namechange daily daily_basic index_dailybasic stk_week_month_adj
step_log OK sync_tushare_security_data

echo "[$(date -Is)] etf-data-update: run fetch_etf_share_size.py"
step_log START fetch_etf_share_size
ETF_SHARE_SIZE_ARGS=()
if [[ "${ETF_SHARE_SIZE_SKIP_VERIFY:-0}" == "1" ]]; then
  ETF_SHARE_SIZE_ARGS+=(--skip-verify)
fi
if ! python src/fetch_etf_share_size.py "${ETF_SHARE_SIZE_ARGS[@]}"; then
  echo "[$(date -Is)] etf-data-update: warning - fetch_etf_share_size.py failed, skip and continue"
  step_log WARN fetch_etf_share_size
else
  echo "[$(date -Is)] etf-data-update: fetch_etf_share_size.py done"
  step_log OK fetch_etf_share_size
fi

echo "[$(date -Is)] etf-data-update: run aggregate_etf_categories.py"
if ! python src/aggregate_etf_categories.py; then
  echo "[$(date -Is)] etf-data-update: warning - aggregate_etf_categories.py failed, skip and continue"
else
  echo "[$(date -Is)] etf-data-update: aggregate_etf_categories.py done"
fi

echo "[$(date -Is)] etf-data-update: ensure recent ETF share data"
if ! bash "$APP_DIR/scripts/ensure_recent_etf_share_data.sh"; then
  echo "[$(date -Is)] etf-data-update: warning - ensure_recent_etf_share_data.sh failed, skip and continue"
else
  echo "[$(date -Is)] etf-data-update: ensure_recent_etf_share_data.sh done"
fi

echo "[$(date -Is)] etf-data-update: run update_fund_hot_stocks.py (fund holdings)"
FUND_HOT_STOCKS_ARGS=(--sync-portfolio-dynamic --rebuild-agg)
if [[ "${ETF_FUND_HOLDING_REFRESH_BASIC:-0}" != "1" ]]; then
  FUND_HOT_STOCKS_ARGS+=(--no-refresh-basic)
fi
if [[ -n "${ETF_FUND_HOLDING_FUND_LIMIT:-}" ]]; then
  FUND_HOT_STOCKS_ARGS+=(--fund-limit "${ETF_FUND_HOLDING_FUND_LIMIT}")
fi
if [[ -n "${ETF_FUND_HOLDING_PERIOD:-}" ]]; then
  FUND_HOT_STOCKS_ARGS+=(--period "${ETF_FUND_HOLDING_PERIOD}")
fi
if ! TZ=Asia/Shanghai PYTHONPATH="$APP_DIR" "$APP_DIR/.venv/bin/python" update_fund_hot_stocks.py "${FUND_HOT_STOCKS_ARGS[@]}"; then
  echo "[$(date -Is)] etf-data-update: warning - update_fund_hot_stocks.py failed, skip and continue"
fi

echo "[$(date -Is)] etf-data-update: run update_moneyflow.py (incremental)"
step_log START update_moneyflow
if ! python update_moneyflow.py --datasets moneyflow,moneyflow_hsgt,moneyflow_ind_ths,moneyflow_dc_ind --lookback-days 1; then
  echo "[$(date -Is)] etf-data-update: warning - update_moneyflow.py failed, skip and continue"
  step_log WARN update_moneyflow
else
  echo "[$(date -Is)] etf-data-update: update_moneyflow.py done"
  step_log OK update_moneyflow
fi

echo "[$(date -Is)] etf-data-update: run update_margin.py (incremental)"
step_log START update_margin
if ! python update_margin.py --datasets margin,margin_detail --lookback-days 2; then
  echo "[$(date -Is)] etf-data-update: warning - update_margin.py failed, skip and continue"
  step_log WARN update_margin
else
  echo "[$(date -Is)] etf-data-update: update_margin.py done"
  step_log OK update_margin
fi

echo "[$(date -Is)] etf-data-update: run update_limitup_monitor.py (incremental)"
if ! python update_limitup_monitor.py --datasets limit_list_d,limit_step,limit_cpt_list,kpl_list,limit_list_ths; then
  echo "[$(date -Is)] etf-data-update: warning - update_limitup_monitor.py failed, skip and continue"
else
  echo "[$(date -Is)] etf-data-update: update_limitup_monitor.py done"
fi

echo "[$(date -Is)] etf-data-update: run update_lhb_monitor.py (safe incremental)"
LHB_BATCH_DAYS="${ETF_LHB_BATCH_DAYS:-3}"
LHB_SLEEP_SECONDS="${ETF_LHB_SLEEP_SECONDS:-0.35}"
LHB_LOOKBACK_DAYS="${ETF_LHB_LOOKBACK_DAYS:-2}"
if ! python update_lhb_monitor.py --datasets top_list,top_inst --batch-days "$LHB_BATCH_DAYS" --sleep "$LHB_SLEEP_SECONDS" --lookback-days "$LHB_LOOKBACK_DAYS"; then
  echo "[$(date -Is)] etf-data-update: warning - update_lhb_monitor.py failed, skip and continue"
else
  echo "[$(date -Is)] etf-data-update: update_lhb_monitor.py done"
fi

echo "[$(date -Is)] etf-data-update: run update_hotmoney.py (safe incremental)"
if ! python update_hotmoney.py --datasets hm_list; then
  echo "[$(date -Is)] etf-data-update: warning - update_hotmoney.py hm_list failed, skip and continue"
else
  echo "[$(date -Is)] etf-data-update: update_hotmoney.py hm_list done"
fi
HOTMONEY_DETAIL_BATCH_DAYS="${ETF_HM_DETAIL_BATCH_DAYS:-1}"
if ! python update_hotmoney.py --datasets hm_detail --detail-batch-days "$HOTMONEY_DETAIL_BATCH_DAYS" --detail-sleep 35 --detail-lookback-days 0; then
  echo "[$(date -Is)] etf-data-update: warning - update_hotmoney.py hm_detail failed, skip and continue"
else
  echo "[$(date -Is)] etf-data-update: update_hotmoney.py hm_detail done"
fi

echo "[$(date -Is)] etf-data-update: run generate_daily_trend_reco_from_pyc.py"
TREND_RECO_CALIBRATION_ANCHORS="${ETF_TREND_RECO_CALIBRATION_ANCHORS:-0}"
if ! TZ=Asia/Shanghai PYTHONPATH="$APP_DIR" "$APP_DIR/.venv/bin/python" scripts/generate_daily_trend_reco_from_pyc.py --probability-calibration-anchors "$TREND_RECO_CALIBRATION_ANCHORS"; then
  echo "[$(date -Is)] etf-data-update: warning - generate_daily_trend_reco_from_pyc.py failed, skip and continue"
else
  echo "[$(date -Is)] etf-data-update: generate_daily_trend_reco_from_pyc.py done"
fi

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

if [[ -f "$APP_DIR/主要ETF基金份额变动情况.xlsx" ]]; then
  echo "[$(date -Is)] etf-data-update: build ETF share cache"
  if ! TZ=Asia/Shanghai PYTHONPATH="$APP_DIR" "$APP_DIR/.venv/bin/python" scripts/build_etf_share_cache.py --skip-if-fresh; then
    echo "[$(date -Is)] etf-data-update: warning - build_etf_share_cache.py failed, skip and continue"
  fi
fi

echo "[$(date -Is)] etf-data-update: restart streamlit"
systemctl restart etf-streamlit

step_log OK etf_data_update_completed
echo "[$(date -Is)] etf-data-update: completed"
