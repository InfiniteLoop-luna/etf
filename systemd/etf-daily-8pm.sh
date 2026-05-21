#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/etf-app"
cd "$APP_DIR"

echo "[$(date -Is)] etf-daily-8pm: start"

# 1) Safe deploy: allow deploy-by-commit to stash tracked changes if needed.
"$APP_DIR/scripts/deploy-by-commit.sh" origin/main --skip-install

# 2) Nightly hotfix params:
#    - skip ETF share verification so a verify failure does not block the chain
#    - widen hotmoney detail batch days to cross weekend/holiday gaps
export ETF_SHARE_SIZE_SKIP_VERIFY=1
export ETF_HM_DETAIL_BATCH_DAYS=10

# 3) Data update chain.
"$APP_DIR/scripts/etf-data-update.sh"

echo "[$(date -Is)] etf-daily-8pm: done"
