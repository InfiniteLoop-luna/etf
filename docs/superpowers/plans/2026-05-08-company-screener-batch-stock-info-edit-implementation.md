# Company Screener Batch Stock Info Edit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add batch editing for main-business and product info to the company screener so one authorized submission can update the current filtered company result set.

**Architecture:** Keep the feature local to `app.py`, but move validation and batch-write helpers into `src/etf_stats.py` so the UI can reuse the same rules as the existing single-stock correction path. Persist the current screener result set in `st.session_state`, then render a batch editor below the result table.

**Tech Stack:** Python 3.14, Streamlit, Pandas, SQLAlchemy, PostgreSQL, pytest

---

## File Structure

- Modify: `src/etf_stats.py`
- Modify: `app.py`
- Create: `tests/test_stock_info_edit.py`

### Task 1: Add helper tests first

**Files:**
- Create: `tests/test_stock_info_edit.py`
- Modify: `src/etf_stats.py`

- [ ] **Step 1: Write the failing test**

Add tests for:

- blank values becoming a `clear` action
- too-short paired values becoming `invalid`
- valid trimmed values becoming `save`
- batch helper calling the single-row updater for every code

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_stock_info_edit.py -v`

Expected: FAIL because the new helper functions do not exist yet.

- [ ] **Step 3: Write minimal implementation**

Add:

- `validate_stock_custom_info_inputs(...)`
- `update_stock_custom_info_batch(...)`

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_stock_info_edit.py -v`

Expected: PASS

### Task 2: Wire the company screener batch editor

**Files:**
- Modify: `app.py`

- [ ] **Step 1: Persist screener results in session state**

Store the latest raw company search dataframe plus the filter snapshot after a successful search.

- [ ] **Step 2: Render the batch editor below results**

Add:

- result count message
- result table
- `批量订正主营与产品信息` expander
- permission block matching the single-security correction flow
- form fields for unified batch values

- [ ] **Step 3: Submit batch writes**

On submit:

- check permission again
- validate through `validate_stock_custom_info_inputs(...)`
- call `update_stock_custom_info_batch(...)`
- show success count

- [ ] **Step 4: Run focused verification**

Run: `python -m py_compile app.py src/etf_stats.py`

Expected: PASS

### Task 3: Regression verification

**Files:**
- Modify: none

- [ ] **Step 1: Run targeted tests**

Run: `python -m pytest tests/test_stock_info_edit.py tests/test_navigation_config.py tests/test_fund_monitor_importer.py tests/test_fund_monitor_store.py tests/test_index_monitor_importer.py tests/test_index_monitor_store.py -v`

Expected: PASS

- [ ] **Step 2: Commit**

Run:

```bash
git add app.py src/etf_stats.py tests/test_stock_info_edit.py docs/superpowers/specs/2026-05-08-company-screener-batch-stock-info-edit-design.md docs/superpowers/plans/2026-05-08-company-screener-batch-stock-info-edit-implementation.md
git commit -m "feat: add batch stock info edit for company screener"
```
