# Watchlist Distribution Background Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a shared background refresh pipeline for watchlist distribution reports so reports are precomputed from database data, shared across users, and the watchlist button is enabled only when a ready report exists.

**Architecture:** Add a stock-level report status table, a shared background refresh script, and a DB-only report generation path. The watchlist page becomes a thin reader of report status and cached markdown, while the background job handles incremental refresh for the union of all stock watchlist symbols. No live report generation should remain in the button click path.

**Tech Stack:** Python, pandas, SQLAlchemy, PostgreSQL, Streamlit, pytest/unittest, existing `scripts/` job pattern

---

### Task 1: Add report status storage and DB-only cache access helpers

**Files:**
- Modify: `D:/sourcecode/etf/src/distribution_report_store.py`
- Modify: `D:/sourcecode/etf/tests/test_distribution_analyzer.py`

- [ ] **Step 1: Write the failing tests**

```python
@patch("src.distribution_report_store.ensure_tables")
def test_distribution_report_status_round_trip(self, _mock_ensure_tables):
    # add status row, read it back, and verify the fields survive unchanged
    ...
```

```python
@patch("src.distribution_analyzer.create_client", side_effect=AssertionError("client should not be created"))
def test_generate_detailed_report_uses_db_only_when_cache_exists(self, _mock_client):
    # ensure cached report returns without live fallback
    ...
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_distribution_analyzer.py -q`
Expected: fail because report status helpers and DB-only status access do not exist yet.

- [ ] **Step 3: Write minimal implementation**

Add `ts_distribution_report_status` table helpers and extend report cache helpers to support `asof_trade_date`-style access and status reads.

```python
REPORT_STATUS_TABLE = "ts_distribution_report_status"

def ensure_tables(engine: Engine):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {REPORT_TABLE} (
        ts_code VARCHAR(20) NOT NULL,
        asof_trade_date VARCHAR(20) NOT NULL,
        report_md TEXT NOT NULL,
        generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        source_updated_at TIMESTAMPTZ,
        report_version VARCHAR(32) NOT NULL DEFAULT 'v1',
        PRIMARY KEY (ts_code, asof_trade_date)
    );

    CREATE TABLE IF NOT EXISTS {REPORT_STATUS_TABLE} (
        ts_code VARCHAR(20) PRIMARY KEY,
        status VARCHAR(20) NOT NULL,
        target_trade_date VARCHAR(20),
        latest_ready_trade_date VARCHAR(20),
        latest_report_generated_at TIMESTAMPTZ,
        last_attempt_at TIMESTAMPTZ,
        last_success_at TIMESTAMPTZ,
        duration_ms INTEGER,
        error_message TEXT
    );
    ...
    """
```

```python
def get_report_status(engine: Engine, ts_code: str) -> dict | None:
    ...

def upsert_report_status(engine: Engine, ts_code: str, **fields) -> None:
    ...
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_distribution_analyzer.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/distribution_report_store.py tests/test_distribution_analyzer.py
git commit -m "feat: add distribution report status storage"
```

### Task 2: Make report generation DB-only and date-aware

**Files:**
- Modify: `D:/sourcecode/etf/src/distribution_analyzer.py`
- Modify: `D:/sourcecode/etf/tests/test_distribution_analyzer.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_fetch_transactions_skips_live_fallback_for_stale_dates(self):
    # older than lookback should return empty without creating mootdx client
    ...
```

```python
def test_fetch_minutes_skips_live_fallback_for_stale_dates(self):
    # older than lookback should return empty without fetching live minutes
    ...
```

```python
def test_generate_detailed_report_uses_only_recent_expensive_dates(self):
    # verify only the latest 4 dates are used for expensive tick/minute analysis
    ...
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_distribution_analyzer.py -q`
Expected: fail because the stale-date guard and date limiter do not yet exist.

- [ ] **Step 3: Write minimal implementation**

Add helpers to limit expensive analysis and stop live fallback for stale dates.

```python
MAX_EXPENSIVE_TARGET_DATES = 4
RECENT_LIVE_FETCH_LOOKBACK_DAYS = 7

def should_attempt_live_fetch(trade_date_str: str, today: datetime | None = None) -> bool:
    ...

def select_expensive_target_dates(valid_dates: list[str]) -> list[str]:
    ...
```

Use those helpers in `fetch_transactions`, `fetch_minutes`, and the report-generation loop so only the most recent 4 key dates are analyzed.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_distribution_analyzer.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/distribution_analyzer.py tests/test_distribution_analyzer.py
git commit -m "perf: bound expensive distribution analysis dates"
```

### Task 3: Add the shared background refresh script

**Files:**
- Create: `D:/sourcecode/etf/scripts/update_watchlist_distribution_reports.py`
- Modify: `D:/sourcecode/etf/src/distribution_report_store.py`

- [ ] **Step 1: Write the failing test**

```python
def test_watchlist_union_is_deduplicated(self):
    # script should process DISTINCT ts_code only once
    ...
```

```python
def test_ready_reports_skip_recompute(self):
    # if latest ready trade date matches current source date, skip the symbol
    ...
```

- [ ] **Step 2: Run the script tests and verify they fail**

Run: `pytest tests/test_watchlist_distribution_refresh.py -q`
Expected: fail because the script and refresh helpers do not exist yet.

- [ ] **Step 3: Write minimal implementation**

Create a shared refresh script that:

- reads `DISTINCT ts_code` from `app_user_watchlist`
- computes the latest source trade date from cached market data
- skips symbols whose current ready report already matches that source date
- updates status rows to `pending`, `running`, `ready`, or `failed`
- writes markdown reports to `ts_distribution_reports`

```python
def load_watchlist_stock_symbols(engine) -> list[str]:
    ...

def refresh_watchlist_distribution_reports(engine) -> None:
    ...
```

- [ ] **Step 4: Run the script tests and verify they pass**

Run: `pytest tests/test_watchlist_distribution_refresh.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/update_watchlist_distribution_reports.py src/distribution_report_store.py tests/test_watchlist_distribution_refresh.py
git commit -m "feat: add watchlist distribution refresh job"
```

### Task 4: Wire the watchlist UI to report status

**Files:**
- Modify: `D:/sourcecode/etf/app.py`
- Modify: `D:/sourcecode/etf/tests/etf_tab_widget_state_test.py` or add a focused watchlist UI test if needed

- [ ] **Step 1: Write the failing tests**

```python
def test_watchlist_row_disables_button_when_report_not_ready(self):
    # button should be disabled and show background-refresh status
    ...
```

```python
def test_watchlist_row_enables_button_when_latest_ready_report_exists(self):
    # button should be enabled when any latest ready report exists
    ...
```

- [ ] **Step 2: Run the UI tests and verify they fail**

Run: `pytest tests/etf_tab_widget_state_test.py -q`
Expected: fail because the watchlist row does not yet read status-driven report metadata.

- [ ] **Step 3: Write minimal implementation**

Update the watchlist page to read report status rows and control the report button state:

```python
status = get_report_status(engine, row["代码"])
ready_trade_date = status.get("latest_ready_trade_date") if status else None
button_disabled = not status or status.get("status") not in {"ready", "failed"} and not ready_trade_date
```

Render the label area with:

- `已就绪`
- `后台更新中`
- `最近报告 YYYY-MM-DD`
- `生成失败`

The click path should only open cached markdown from `ts_distribution_reports`.

- [ ] **Step 4: Run the UI tests and verify they pass**

Run: `pytest tests/etf_tab_widget_state_test.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app.py tests/etf_tab_widget_state_test.py
git commit -m "feat: make watchlist distribution button status-driven"
```

### Task 5: Verify the end-to-end background cache flow

**Files:**
- Modify: `D:/sourcecode/etf/tests/test_distribution_analyzer.py`
- Modify: `D:/sourcecode/etf/scripts/update_watchlist_distribution_reports.py`

- [ ] **Step 1: Add end-to-end regression coverage**

```python
def test_background_refresh_generates_report_and_marks_ready(self):
    # run one symbol through the refresh path and assert ready state + cached markdown
    ...
```

- [ ] **Step 2: Run the full focused test suite**

Run:

```bash
pytest tests/test_distribution_analyzer.py tests/etf_tab_widget_state_test.py -q
```

Expected: PASS

- [ ] **Step 3: Smoke test the background script**

Run:

```bash
python scripts/update_watchlist_distribution_reports.py
```

Expected:

- processes only distinct watchlist stocks
- completes without live report generation on click
- writes ready rows to the report status table

- [ ] **Step 4: Commit**

```bash
git add app.py src/distribution_analyzer.py src/distribution_report_store.py scripts/update_watchlist_distribution_reports.py tests/test_distribution_analyzer.py tests/etf_tab_widget_state_test.py
git commit -m "feat: add shared watchlist distribution cache"
```
