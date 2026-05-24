# Stock Holder Number Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add stock holder-number history storage, incremental Tushare sync, nightly scheduling, and latest holder-number display in stock query.

**Architecture:** Extend the existing `sync_tushare_security_data.py` landing-table pipeline with a new `stock_holdernumber` dataset and normalized view, then surface the latest snapshot through `src/etf_stats.py` so `app.py` can render it in the stock profile area. Nightly updates flow through the existing `scripts/etf-data-update.sh` entry so the new dataset is refreshed alongside other stock fundamentals.

**Tech Stack:** Python, pandas, SQLAlchemy, PostgreSQL JSONB landing tables, Streamlit, pytest/unittest

---

### Task 1: Add red tests for holder-number sync metadata and profile query

**Files:**
- Create: `D:/sourcecode/etf/tests/test_stock_holdernumber.py`

- [ ] **Step 1: Write the failing tests**

```python
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from src import etf_stats
from src import sync_tushare_security_data as sync_mod


class StockHolderNumberQueryTests(unittest.TestCase):
    def test_get_latest_stock_holder_number_queries_latest_snapshot(self):
        captured = {}

        def fake_read_sql(query, engine, params=None):
            captured["sql"] = str(query)
            captured["params"] = params
            return pd.DataFrame(
                [
                    {
                        "ts_code": "000001.SZ",
                        "holder_num": 45678,
                        "holder_ann_date": "2026-05-20",
                        "holder_end_date": "2026-05-15",
                    }
                ]
            )

        with patch("src.etf_stats.pd.read_sql", side_effect=fake_read_sql):
            df = etf_stats.get_latest_stock_holder_number("000001.SZ", engine=object())

        self.assertEqual(int(df.iloc[0]["holder_num"]), 45678)
        self.assertIn("vw_ts_stock_holdernumber", captured["sql"])
        self.assertEqual(captured["params"]["ts_code"], "000001.SZ")


class StockHolderNumberSyncTests(unittest.TestCase):
    def test_stock_holdernumber_metadata_registered(self):
        self.assertEqual(sync_mod.DATASET_TABLES["stock_holdernumber"], "ts_stock_holdernumber")
        self.assertEqual(
            sync_mod.NORMALIZED_VIEW_SPECS["stock_holdernumber"]["view_name"],
            "vw_ts_stock_holdernumber",
        )

    def test_resolve_business_key_uses_ts_code_end_date_ann_date_for_holdernumber(self):
        business_key = sync_mod.resolve_business_key(
            "stock_holdernumber",
            {
                "ts_code": "000001.SZ",
                "end_date": "20260515",
                "ann_date": "20260520",
                "holder_num": 45678,
            },
        )

        self.assertEqual(business_key, "stock_holdernumber|000001.SZ|20260515|20260520")

    def test_resolve_sync_window_advances_from_latest_ann_date_for_holdernumber(self):
        args = SimpleNamespace(
            financial_start="20240101",
            financial_lookback_days=30,
            daily_start="20250101",
            daily_lookback_days=1,
        )

        with patch("src.sync_tushare_security_data.get_max_date", return_value="20260520"):
            start_date, end_date = sync_mod.resolve_sync_window(
                engine=object(),
                dataset_name="stock_holdernumber",
                table_name="ts_stock_holdernumber",
                args=args,
                run_end_date="20260524",
            )

        self.assertEqual(start_date, "20260521")
        self.assertEqual(end_date, "20260524")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_stock_holdernumber.py -v`
Expected: FAIL because `get_latest_stock_holder_number` and `stock_holdernumber` dataset registration do not exist yet.

- [ ] **Step 3: Write minimal implementation**

Add dataset metadata and a latest-snapshot query function in existing modules.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_stock_holdernumber.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_stock_holdernumber.py src/etf_stats.py src/sync_tushare_security_data.py
git commit -m "feat: add stock holder number sync metadata"
```

### Task 2: Add red tests for fetch logic and nightly scheduling

**Files:**
- Modify: `D:/sourcecode/etf/tests/test_stock_holdernumber.py`
- Modify: `D:/sourcecode/etf/scripts/etf-data-update.sh`

- [ ] **Step 1: Write the failing tests**

```python
class StockHolderNumberFetchTests(unittest.TestCase):
    def test_fetch_stock_holdernumber_calls_tushare_endpoint(self):
        calls = []

        class FakePro:
            def stk_holdernumber(self, **kwargs):
                calls.append(kwargs)
                return pd.DataFrame(
                    [
                        {
                            "ts_code": "000001.SZ",
                            "ann_date": "20260520",
                            "end_date": "20260515",
                            "holder_num": 45678,
                        }
                    ]
                )

        df = sync_mod.fetch_stock_holdernumber(FakePro(), "20260501", "20260524")

        self.assertEqual(len(df), 1)
        self.assertEqual(calls, [{"start_date": "20260501", "end_date": "20260524"}])

    def test_nightly_script_includes_stock_holdernumber_dataset(self):
        script_path = "D:/sourcecode/etf/scripts/etf-data-update.sh"
        with open(script_path, "r", encoding="utf-8") as handle:
            content = handle.read()

        self.assertIn("stock_holdernumber", content)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_stock_holdernumber.py -v`
Expected: FAIL because `fetch_stock_holdernumber` does not exist and nightly script does not include the dataset.

- [ ] **Step 3: Write minimal implementation**

Implement the fetch function and add `stock_holdernumber` to the nightly dataset list in `scripts/etf-data-update.sh`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_stock_holdernumber.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_stock_holdernumber.py src/sync_tushare_security_data.py scripts/etf-data-update.sh
git commit -m "feat: schedule incremental stock holder number sync"
```

### Task 3: Add red tests for stock profile enrichment

**Files:**
- Modify: `D:/sourcecode/etf/tests/test_stock_holdernumber.py`
- Modify: `D:/sourcecode/etf/src/etf_stats.py`

- [ ] **Step 1: Write the failing test**

```python
class StockProfileHolderNumberTests(unittest.TestCase):
    def test_get_stock_profile_selects_holder_number_fields(self):
        captured = {}

        def fake_read_sql(query, engine, params=None):
            captured["sql"] = str(query)
            return pd.DataFrame(
                [
                    {
                        "ts_code": "000001.SZ",
                        "name": "平安银行",
                        "holder_num": 45678,
                        "holder_ann_date": "2026-05-20",
                        "holder_end_date": "2026-05-15",
                    }
                ]
            )

        with patch("src.etf_stats.pd.read_sql", side_effect=fake_read_sql):
            df = etf_stats.get_stock_profile("000001.SZ", engine=object())

        self.assertEqual(int(df.iloc[0]["holder_num"]), 45678)
        self.assertIn("holder_num", captured["sql"])
        self.assertIn("vw_ts_stock_holdernumber", captured["sql"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_stock_holdernumber.py::StockProfileHolderNumberTests::test_get_stock_profile_selects_holder_number_fields -v`
Expected: FAIL because stock profile SQL does not yet join holder-number data.

- [ ] **Step 3: Write minimal implementation**

Join the latest holder-number snapshot into `get_stock_profile`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_stock_holdernumber.py::StockProfileHolderNumberTests::test_get_stock_profile_selects_holder_number_fields -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_stock_holdernumber.py src/etf_stats.py
git commit -m "feat: enrich stock profile with holder number snapshot"
```

### Task 4: Show latest holder number in stock query UI

**Files:**
- Modify: `D:/sourcecode/etf/app.py`

- [ ] **Step 1: Add a focused UI assertion target**

Use the existing stock profile fields in `app.py` so no new state machinery is required.

- [ ] **Step 2: Write minimal implementation**

Render:

```python
metric_cols_bottom[4].metric("最新股东人数", format_optional_number(profile.get("holder_num"), digits=0))
```

and add rows for `holder_end_date` / `holder_ann_date` in the stock info table.

- [ ] **Step 3: Run targeted tests**

Run: `pytest tests/test_stock_holdernumber.py -v`
Expected: PASS

- [ ] **Step 4: Smoke-check syntax**

Run: `python -m compileall app.py src`
Expected: exit 0

- [ ] **Step 5: Commit**

```bash
git add app.py
git commit -m "feat: display latest stock holder number in security search"
```

### Task 5: Verify end-to-end sync and database write path

**Files:**
- Modify: `D:/sourcecode/etf/src/sync_tushare_security_data.py`

- [ ] **Step 1: Run the full holder-number test file**

Run: `pytest tests/test_stock_holdernumber.py -v`
Expected: all PASS

- [ ] **Step 2: Run regression tests for sync module**

Run: `pytest tests/test_sync_tushare_security_data.py -v`
Expected: PASS

- [ ] **Step 3: Run syntax verification**

Run: `python -m compileall app.py src`
Expected: exit 0

- [ ] **Step 4: Run one real incremental sync command**

Run: `python src/sync_tushare_security_data.py --datasets stock_holdernumber --financial-start 20240101 --end-date 20260524`
Expected: either rows written successfully, or a clear token/auth failure that confirms code path is wired correctly.

- [ ] **Step 5: Summarize operational prerequisite**

Document that a valid `TUSHARE_TOKEN` must be present in `.env` or the runtime environment for nightly incremental updates to succeed.
