# Factor Stock Workbench Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a dedicated factor stock workbench page that supports date-scoped universe selection, opt-in hard filters, and weighted factor ranking for daily stock screening.

**Architecture:** Put all factor metadata, SQL loading, alpha095 snapshot calculation, hard-filter logic, and scoring logic into a new focused backend module. Keep `app.py` responsible for page composition and reuse the existing jump-table helpers for navigation into the stock detail page.

**Tech Stack:** Python 3.14, Streamlit, Pandas, SQLAlchemy, PostgreSQL, pytest

---

## File Structure

- Create: `src/factor_workbench.py`
- Modify: `app.py`
- Modify: `src/navigation_config.py`
- Create: `tests/test_factor_workbench.py`
- Modify: `tests/test_navigation_config.py`

### Task 1: Add pure-logic tests for filtering and scoring

**Files:**
- Create: `tests/test_factor_workbench.py`
- Create: `src/factor_workbench.py`

- [ ] **Step 1: Write the failing test**

Add tests for:

- hard filters only applying when explicitly enabled
- score normalization honoring factor direction
- missing factor values receiving a neutral percentile score
- weighted scores ranking stronger names above weaker names

```python
import unittest
import pandas as pd

from src.factor_workbench import (
    apply_factor_filters,
    compute_factor_scores,
)


class FactorWorkbenchLogicTests(unittest.TestCase):
    def test_apply_factor_filters_respects_enabled_flags(self):
        df = pd.DataFrame(
            [
                {"ts_code": "A", "turnover_rate": 2.0, "roe": 8.0},
                {"ts_code": "B", "turnover_rate": 6.0, "roe": 12.0},
            ]
        )
        filters = {
            "min_turnover_rate_enabled": True,
            "min_turnover_rate": 5.0,
            "min_roe_enabled": False,
            "min_roe": 10.0,
        }
        filtered = apply_factor_filters(df, filters)
        self.assertEqual(filtered["ts_code"].tolist(), ["B"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_factor_workbench.py -q`

Expected: FAIL because the new module and functions do not exist yet.

- [ ] **Step 3: Write minimal implementation**

Create `src/factor_workbench.py` with:

- factor metadata for the V1 core factors
- `apply_factor_filters(...)`
- `compute_factor_scores(...)`
- small helper functions for percentile normalization and weighted average scoring

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_factor_workbench.py -q`

Expected: PASS

### Task 2: Add database query helpers and alpha095 snapshot loading

**Files:**
- Modify: `src/factor_workbench.py`
- Modify: `tests/test_factor_workbench.py`

- [ ] **Step 1: Write the failing test**

Add tests for:

- factor presets returning non-empty positive weights
- alpha095-family merge helpers preserving input index order for synthetic snapshots

```python
def test_compute_factor_scores_fills_missing_values_neutrally(self):
    df = pd.DataFrame(
        [
            {"ts_code": "A", "roe": 10.0, "pb": 2.0},
            {"ts_code": "B", "roe": None, "pb": 1.0},
        ]
    )
    scored = compute_factor_scores(
        df,
        factor_weights={"roe": 1.0, "pb": 1.0},
    )
    self.assertIn("final_score", scored.columns)
    self.assertTrue(scored["final_score"].notna().all())
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_factor_workbench.py -q`

Expected: FAIL because the preset and helper behavior is not implemented yet.

- [ ] **Step 3: Write minimal implementation**

Extend `src/factor_workbench.py` with:

- V1 preset definitions
- base SQL loader for the workbench date anchor
- data-freshness query helper
- alpha095-family history loader using `vw_ts_stock_daily`

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_factor_workbench.py -q`

Expected: PASS

### Task 3: Wire the new page into the stock module

**Files:**
- Modify: `src/navigation_config.py`
- Modify: `app.py`
- Modify: `tests/test_navigation_config.py`

- [ ] **Step 1: Write the failing test**

Extend navigation tests so the stock-page options must include the new workbench label.

```python
def test_stock_page_options_include_factor_workbench(self):
    self.assertIn("🧠 因子选股工作台", STOCK_PAGE_OPTIONS)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_navigation_config.py -q`

Expected: FAIL because the new option is not yet present.

- [ ] **Step 3: Write minimal implementation**

Update:

- `src/navigation_config.py` to add the new stock subpage label
- `app.py` to route the new label to `render_factor_workbench_tab()`
- `app.py` to add the new page renderer using:
  - a date selector
  - universe filters
  - opt-in hard filter controls
  - preset scoring model controls
  - result table via `render_security_jump_table(...)`

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_navigation_config.py -q`

Expected: PASS

### Task 4: Add factor dictionary and data freshness tabs

**Files:**
- Modify: `app.py`
- Modify: `src/factor_workbench.py`

- [ ] **Step 1: Write the failing test**

Add a small pure-logic test asserting that the factor metadata exported by the backend module contains the V1 core factors and direction flags.

```python
def test_factor_catalog_contains_v1_core_factors(self):
    catalog = get_factor_catalog()
    keys = {item["key"] for item in catalog}
    self.assertIn("alpha095_cv", keys)
    self.assertIn("net_mf_amount_rate", keys)
    self.assertIn("roe", keys)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_factor_workbench.py -q`

Expected: FAIL because the catalog helper is not complete.

- [ ] **Step 3: Write minimal implementation**

Expose:

- `get_factor_catalog()`
- `get_factor_workbench_data_freshness(...)`

Render two auxiliary tabs in `app.py`:

- factor dictionary
- data freshness summary

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_factor_workbench.py -q`

Expected: PASS

### Task 5: Verification and visual QA

**Files:**
- Modify: none

- [ ] **Step 1: Run targeted tests**

Run: `python -m pytest tests/test_factor_workbench.py tests/test_navigation_config.py -q`

Expected: PASS

- [ ] **Step 2: Run compile verification**

Run: `python -m py_compile src/factor_workbench.py app.py src/navigation_config.py tests/test_factor_workbench.py tests/test_navigation_config.py`

Expected: PASS

- [ ] **Step 3: Launch the local app and visually verify the new page**

Run the Streamlit app, open the stock module, and confirm:

- the new page appears in stock navigation
- the workbench date selector loads
- filters rerun correctly
- results render with jump links
- factor dictionary and data freshness tabs populate

- [ ] **Step 4: Commit**

```bash
git add app.py src/navigation_config.py src/factor_workbench.py tests/test_factor_workbench.py tests/test_navigation_config.py docs/superpowers/specs/2026-05-17-factor-stock-workbench-design.md docs/superpowers/plans/2026-05-17-factor-stock-workbench-implementation.md
git commit -m "feat: add factor stock workbench"
```
