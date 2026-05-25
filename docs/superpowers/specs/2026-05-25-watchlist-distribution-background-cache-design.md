# Watchlist Distribution Background Cache Design

Date: 2026-05-25
Status: Draft for review
Scope: Build a shared background cache for "深度出货分析" so watchlist stocks are incrementally refreshed from database data, reports are shared across users, and the watchlist button is enabled only when a ready report exists.

## Context

The current watchlist page in [app.py](D:/sourcecode/etf/app.py) generates distribution reports on demand. Even after the recent database-first work, the button path still does too much work at click time:

- it can still wait on report generation
- it has to decide cache readiness on the fly
- it mixes UI concerns with refresh concerns

The user confirmed the desired product behavior:

- background refresh should cover the union of all users' stock watchlists
- report readiness should be shared across users
- the button should be disabled unless a ready report already exists
- if no fresh report exists, the UI should show "后台更新中" or a similar status
- the report should be generated entirely from database data
- "最近一次可用报告" is the button enable condition, not "today only"

The existing codebase already contains useful pieces:

- `app_user_watchlist` for user watchlists
- `ts_distribution_reports` for report markdown cache
- `ts_stock_intraday_mins` and `vw_ts_stock_daily` for database market data
- `scripts/update_distribution_alerts.py` as a precedent for background stock-wide processing

## Goals

- Refresh the union of all stock watchlist symbols in the background
- Store distribution reports as shared, reusable database cache entries
- Make the watchlist button state-driven instead of compute-driven
- Keep report generation DB-only
- Preserve the last ready report when a new refresh is still pending or fails

## Non-Goals

- Per-user report copies
- Live report generation on button click
- Reintroducing `mootdx` as a fallback inside report generation
- A full task queue or distributed worker system in this pass
- Changing the visual design of the rest of the dashboard

## Confirmed Product Decisions

The following were explicitly confirmed during conversation:

1. The background refresh should process the **union** of all users' stock watchlists.
2. The button should be enabled when the **most recent ready report** exists, not only when today's report exists.
3. If a report is not ready, the button should be **disabled** and the UI should show its current status.
4. The recommended disabled-state behavior is:
   - show the button as disabled
   - show the latest report date or a background-refresh message next to it
5. The report should be generated **entirely from database data**.
6. The report date should be based on the **latest source trading date**, not the script runtime date.

## Recommended Architecture

Use a shared, stock-level cache with a status table:

1. A nightly or post-market background job collects the union of all stock watchlist symbols.
2. For each symbol, the job ensures the required market-data caches are present in the database.
3. The job generates or refreshes the markdown report from database data only.
4. The job writes the report body to `ts_distribution_reports`.
5. The job updates a per-symbol status row that the UI can read quickly.
6. The watchlist UI reads only the status and cached report content.

This keeps expensive work out of the user-click path and lets all users share the same cached report for the same stock and source trade date.

## Data Model

### `ts_distribution_reports`

Purpose: store the final markdown report body for a stock and the source trading date it was generated from.

Recommended columns:

- `ts_code`
- `asof_trade_date`
- `report_md`
- `generated_at`
- `source_updated_at`
- `report_version`

Primary key:

- `(ts_code, asof_trade_date)`

### `ts_distribution_report_status`

Purpose: store the current display state for each stock.

Recommended columns:

- `ts_code`
- `status` with values `pending`, `running`, `ready`, `failed`, `stale`
- `target_trade_date`
- `latest_ready_trade_date`
- `latest_report_generated_at`
- `last_attempt_at`
- `last_success_at`
- `duration_ms`
- `error_message`

Primary key:

- `ts_code`

## Status Semantics

- `ready`
  - The button is enabled.
  - The UI opens the latest ready report.
- `pending` / `running`
  - If a ready report exists, the button stays enabled and the UI shows the latest report date plus a refresh message.
  - If no ready report exists, the button is disabled.
- `failed`
  - If a ready report exists, the button stays enabled and the UI shows the latest report date plus a failure note.
  - If no ready report exists, the button is disabled.
- `stale`
  - A newer source trade date exists, but the current refresh has not completed yet.
  - Behavior matches `pending`.

## Data Flow

1. Collect stock symbols from `app_user_watchlist` using `DISTINCT ts_code`.
2. Determine each symbol's latest source trade date from database market data.
3. Skip symbols whose latest ready report already matches the current source trade date.
4. Ensure the source caches exist in the database.
5. Generate the markdown report from cached database data only.
6. Save the report to `ts_distribution_reports`.
7. Update the status row in `ts_distribution_report_status`.
8. On the watchlist page, read status rows and enable the button only when `ready` or when a ready fallback exists.

## Background Job

Create a dedicated background script for shared watchlist cache refresh.

Recommended behavior:

- run after market close or on a daily schedule
- process stock watchlist symbols in deterministic order
- use incremental checks to skip unchanged symbols
- keep going even if one symbol fails
- store failure details in the status table

The existing `scripts/update_distribution_alerts.py` can serve as a style reference for a stock-wide watchlist scan, but this new job must target report cache generation instead of alert generation.

## UI Behavior

In the watchlist page:

- each stock row reads its report status
- the button is disabled unless the report is ready or a fallback ready report exists
- the label area shows one of:
  - `已就绪`
  - `后台更新中`
  - `最近报告 YYYY-MM-DD`
  - `生成失败`
- clicking the button only opens the cached markdown report
- the UI no longer performs live report generation

## Implementation Boundaries

- `src/distribution_analyzer.py`
  - refactor report generation into a DB-only path
  - remove live fallback from the report-generation flow
- `src/distribution_report_store.py`
  - extend report storage for `asof_trade_date` and status records
- `app.py`
  - read status rows for button enable/disable behavior
  - stop generating reports on click
- `scripts/`
  - add a dedicated shared refresh script

## Testing

Required tests:

- report generation uses database caches only
- stale dates do not trigger live fallback fetching
- background refresh skips stocks that already have a ready report for the current source date
- the union of watchlist symbols is deduplicated
- the watchlist button is disabled when status is not ready and no ready fallback exists
- the button remains enabled when a prior ready report exists even if the latest refresh is pending or failed

## Open Questions

None. The remaining work is implementation detail only.
