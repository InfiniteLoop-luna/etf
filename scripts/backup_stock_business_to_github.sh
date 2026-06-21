#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${ETF_APP_DIR:-/opt/etf-app}"
SOURCE_DIR="${STOCK_BUSINESS_OUTPUT_DIR:-$APP_DIR/output/stock_business}"
BACKUP_GIT_URL="${STOCK_BUSINESS_BACKUP_GIT_URL:-}"
BACKUP_DIR="${STOCK_BUSINESS_BACKUP_DIR:-/opt/etf-stock-business-backup}"
BACKUP_BRANCH="${STOCK_BUSINESS_BACKUP_BRANCH:-main}"
BACKUP_SUBDIR="${STOCK_BUSINESS_BACKUP_SUBDIR:-stock_business}"
GIT_USER_NAME="${STOCK_BUSINESS_BACKUP_GIT_USER_NAME:-etf-backup-bot}"
GIT_USER_EMAIL="${STOCK_BUSINESS_BACKUP_GIT_USER_EMAIL:-etf-backup-bot@users.noreply.github.com}"

if [[ -z "$BACKUP_GIT_URL" ]]; then
  echo "[$(date -Is)] stock-business-github-backup: STOCK_BUSINESS_BACKUP_GIT_URL not set, skip GitHub backup"
  exit 0
fi

if ! command -v git >/dev/null 2>&1; then
  echo "[$(date -Is)] stock-business-github-backup: git command not found"
  exit 1
fi

if [[ ! -d "$SOURCE_DIR" ]]; then
  echo "[$(date -Is)] stock-business-github-backup: source directory not found: $SOURCE_DIR"
  exit 1
fi

shopt -s nullglob
excel_files=("$SOURCE_DIR"/*.xlsx)
if [[ ${#excel_files[@]} -eq 0 ]]; then
  echo "[$(date -Is)] stock-business-github-backup: no Excel files under $SOURCE_DIR"
  exit 0
fi

if [[ -d "$BACKUP_DIR/.git" ]]; then
  cd "$BACKUP_DIR"
else
  if [[ -d "$BACKUP_DIR" && -n "$(find "$BACKUP_DIR" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "[$(date -Is)] stock-business-github-backup: backup dir exists but is not a git repo: $BACKUP_DIR"
    exit 1
  fi
  mkdir -p "$(dirname "$BACKUP_DIR")"
  git clone "$BACKUP_GIT_URL" "$BACKUP_DIR"
  cd "$BACKUP_DIR"
fi

git config user.name "$GIT_USER_NAME"
git config user.email "$GIT_USER_EMAIL"

if git ls-remote --exit-code --heads origin "$BACKUP_BRANCH" >/dev/null 2>&1; then
  git fetch origin "$BACKUP_BRANCH"
  git checkout "$BACKUP_BRANCH"
  git pull --ff-only origin "$BACKUP_BRANCH"
else
  git checkout -B "$BACKUP_BRANCH"
fi

mkdir -p "$BACKUP_SUBDIR"
for file_path in "${excel_files[@]}"; do
  cp -f "$file_path" "$BACKUP_SUBDIR/"
done

git add "$BACKUP_SUBDIR"
if git diff --cached --quiet; then
  echo "[$(date -Is)] stock-business-github-backup: no changes to commit"
  exit 0
fi

commit_date="$(TZ=Asia/Shanghai date +%Y-%m-%d)"
git commit -m "Backup stock business export $commit_date"
git push origin "$BACKUP_BRANCH"

echo "[$(date -Is)] stock-business-github-backup: pushed ${#excel_files[@]} file(s) to $BACKUP_BRANCH/$BACKUP_SUBDIR"
