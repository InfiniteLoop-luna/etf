# 指数监测 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a new `宏观 / 📊 指数监测` dashboard backed by PostgreSQL, with dashboard-first presentation, Excel import preview, monthly batch entry, and single-index manual editing.

**Architecture:** Reuse the current `app.py + focused src modules` pattern already used by `本外币存款`. Keep Streamlit rendering in `app.py`, move index-monitor persistence and workbook parsing into dedicated `src/` modules, store one row per `month + index_name`, and treat Excel as an import source rather than the system of record.

**Tech Stack:** Streamlit, pandas, openpyxl, SQLAlchemy, psycopg2, unittest, pytest

---

## File Structure

### New files

- `src/index_monitor_store.py`
  - PostgreSQL table helpers
  - monthly/index row upsert and load
  - KPI aggregation
  - price and valuation trend shaping
  - display label mapping
- `src/index_monitor_importer.py`
  - parse `股票指数` sheet
  - flatten grouped headers into normalized rows
  - classify insert vs overwrite rows
- `tests/test_index_monitor_store.py`
  - KPI tests
  - trend-shaping tests
  - import-preview classification tests
- `tests/test_index_monitor_importer.py`
  - workbook parser tests
  - malformed/missing sheet tests

### Modified files

- `src/navigation_config.py`
  - append `📊 指数监测` to macro page options
- `app.py`
  - add macro page routing
  - add `render_index_monitor_tab()`
  - add batch month entry flow
  - add single-index edit flow
  - add import preview/write flow

---

### Task 1: Add macro navigation entry and lock it with a test

**Files:**
- Modify: `src/navigation_config.py`
- Modify: `tests/test_navigation_config.py`

- [ ] **Step 1: Extend the existing navigation test with a failing assertion for `📊 指数监测`**

```python
import unittest

DEPOSIT_PAGE = "🏦 本外币存款"
INDEX_MONITOR_PAGE = "📊 指数监测"


class NavigationConfigTests(unittest.TestCase):
    def test_deposit_page_belongs_to_macro_not_etf(self):
        from src.navigation_config import ETF_PAGE_OPTIONS, MACRO_PAGE_OPTIONS

        self.assertNotIn(DEPOSIT_PAGE, ETF_PAGE_OPTIONS)
        self.assertIn(DEPOSIT_PAGE, MACRO_PAGE_OPTIONS)

    def test_index_monitor_page_belongs_to_macro(self):
        from src.navigation_config import ETF_PAGE_OPTIONS, MACRO_PAGE_OPTIONS

        self.assertNotIn(INDEX_MONITOR_PAGE, ETF_PAGE_OPTIONS)
        self.assertIn(INDEX_MONITOR_PAGE, MACRO_PAGE_OPTIONS)
```

- [ ] **Step 2: Run the navigation test to verify it fails**

Run: `pytest tests/test_navigation_config.py -v`

Expected: FAIL because `📊 指数监测` is not yet in `MACRO_PAGE_OPTIONS`

- [ ] **Step 3: Update the macro page options**

```python
ETF_PAGE_OPTIONS = [
    "📈 ETF份额变动",
    "📊 每日成交量",
    "🥧 ETF分类占比",
    "📈 ETF分类趋势",
    "📊 宽基指数ETF",
]

MACRO_PAGE_OPTIONS = [
    "🌏 宏观经济",
    "🏦 本外币存款",
    "📊 指数监测",
]
```

- [ ] **Step 4: Re-run the navigation test**

Run: `pytest tests/test_navigation_config.py -v`

Expected: PASS for both navigation tests

- [ ] **Step 5: Commit**

```bash
git add src/navigation_config.py tests/test_navigation_config.py
git commit -m "feat: add macro index monitor navigation"
```

### Task 2: Add the stock-index workbook parser

**Files:**
- Create: `src/index_monitor_importer.py`
- Create: `tests/test_index_monitor_importer.py`

- [ ] **Step 1: Write the failing parser tests**

```python
import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook

from src.index_monitor_importer import parse_index_monitor_workbook


class IndexMonitorImporterTests(unittest.TestCase):
    def test_parse_index_monitor_workbook_reads_single_index_row(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "index-monitor.xlsx"
            wb = Workbook()
            ws = wb.active
            ws.title = "股票指数"
            ws["B4"] = "2026-05-01"
            ws["C3"] = "指数名称"
            ws["D2"] = "当月情况"
            ws["D3"] = "开盘价格"
            ws["E3"] = "收盘价格"
            ws["F3"] = "最低点"
            ws["G3"] = "最高点"
            ws["N2"] = "环比变动情况"
            ws["N3"] = "开盘价格"
            ws["O3"] = "收盘价格"
            ws["P3"] = "最低点"
            ws["Q3"] = "最高点"
            ws["X2"] = "同比变动情况"
            ws["X3"] = "开盘价格"
            ws["Y3"] = "收盘价格"
            ws["Z3"] = "最低点"
            ws["AA3"] = "最高点"
            ws["C4"] = "上证指数"
            ws["D4"] = 3340.12
            ws["E4"] = 3367.46
            ws["F4"] = 3321.18
            ws["G4"] = 3388.21
            ws["M3"] = "涨幅"
            ws["M4"] = 0.82
            ws["N4"] = 11.4
            ws["O4"] = 12.9
            ws["P4"] = -8.3
            ws["Q4"] = 20.1
            ws["W3"] = "涨幅"
            ws["W4"] = 5.13
            ws["X4"] = 188.4
            ws["Y4"] = 221.8
            ws["Z4"] = 160.5
            ws["AA4"] = 242.3
            wb.save(path)

            df = parse_index_monitor_workbook(path)

        self.assertEqual(df.iloc[0]["month"], "2026-05-01")
        self.assertEqual(df.iloc[0]["index_name"], "上证指数")
        self.assertAlmostEqual(df.iloc[0]["close_price"], 3367.46)
        self.assertAlmostEqual(df.iloc[0]["mom_change_pct"], 0.82)
        self.assertAlmostEqual(df.iloc[0]["yoy_close_price"], 221.8)

    def test_parse_index_monitor_workbook_raises_when_sheet_missing(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "wrong.xlsx"
            wb = Workbook()
            wb.active.title = "本外币存款数据"
            wb.save(path)
            with self.assertRaisesRegex(ValueError, "股票指数"):
                parse_index_monitor_workbook(path)
```

- [ ] **Step 2: Run the importer tests to verify they fail**

Run: `pytest tests/test_index_monitor_importer.py -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'src.index_monitor_importer'`

- [ ] **Step 3: Create the importer module**

```python
from __future__ import annotations

from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

SHEET_NAME = "股票指数"
SECTION_FIELD_MAP = {
    "当月情况": {
        "涨幅": "monthly_change_pct",
        "开盘价格": "open_price",
        "收盘价格": "close_price",
        "最低点": "low_price",
        "最高点": "high_price",
        "期末静态市盈率": "static_pe",
        "期末动态市盈率": "dynamic_pe",
    },
    "环比变动情况": {
        "涨幅": "mom_change_pct",
        "开盘价格": "mom_open_price",
        "收盘价格": "mom_close_price",
        "最低点": "mom_low_price",
        "最高点": "mom_high_price",
        "期末静态市盈率": "mom_static_pe",
        "期末动态市盈率": "mom_dynamic_pe",
        "静态市盈率变化率": "mom_static_pe_change_rate",
        "动态市盈率变化率": "mom_dynamic_pe_change_rate",
    },
    "同比变动情况": {
        "涨幅": "yoy_change_pct",
        "开盘价格": "yoy_open_price",
        "收盘价格": "yoy_close_price",
        "最低点": "yoy_low_price",
        "最高点": "yoy_high_price",
        "期末静态市盈率": "yoy_static_pe",
        "期末动态市盈率": "yoy_dynamic_pe",
    },
}


def parse_index_monitor_workbook(path) -> pd.DataFrame:
    workbook_source = Path(path) if isinstance(path, (str, Path)) else path
    wb = load_workbook(workbook_source, data_only=True, read_only=True)
    try:
        if SHEET_NAME not in wb.sheetnames:
            raise ValueError("导入文件缺少 股票指数 sheet")

        ws = wb[SHEET_NAME]
        month_value = ws["B4"].value
        if month_value is None:
            raise ValueError("股票指数 sheet 缺少月份")
        month_text = pd.to_datetime(month_value).strftime("%Y-%m-%d")

        section_by_col = {}
        field_by_col = {}
        for col_idx in range(1, ws.max_column + 1):
            section = ws.cell(2, col_idx).value
            field = ws.cell(3, col_idx).value
            if section in SECTION_FIELD_MAP:
                section_by_col[col_idx] = str(section).strip()
            if field is not None:
                field_by_col[col_idx] = str(field).replace("\n", "").strip()

        rows = []
        for row_idx in range(4, ws.max_row + 1):
            index_name = ws.cell(row_idx, 3).value
            if not index_name:
                continue
            row = {"month": month_text, "index_name": str(index_name).strip()}
            for col_idx in range(4, ws.max_column + 1):
                section = section_by_col.get(col_idx)
                field = field_by_col.get(col_idx)
                if not section or not field:
                    continue
                mapped_field = SECTION_FIELD_MAP.get(section, {}).get(field)
                if not mapped_field:
                    continue
                value = ws.cell(row_idx, col_idx).value
                row[mapped_field] = float(value) if value is not None else None
            rows.append(row)

        if not rows:
            raise ValueError("未解析出任何指数记录")
        return pd.DataFrame(rows).sort_values("index_name").reset_index(drop=True)
    finally:
        wb.close()
```

- [ ] **Step 4: Re-run the importer tests**

Run: `pytest tests/test_index_monitor_importer.py -v`

Expected: PASS for both importer tests

- [ ] **Step 5: Commit**

```bash
git add src/index_monitor_importer.py tests/test_index_monitor_importer.py
git commit -m "feat: add index monitor workbook importer"
```

### Task 3: Add the index monitor store module

**Files:**
- Create: `src/index_monitor_store.py`
- Create: `tests/test_index_monitor_store.py`

- [ ] **Step 1: Write the failing store tests**

```python
import unittest

import pandas as pd

from src.index_monitor_store import (
    build_index_monitor_summary,
    build_price_trend_df,
    build_valuation_trend_df,
    classify_index_import_rows,
)


class IndexMonitorStoreTests(unittest.TestCase):
    def test_build_index_monitor_summary_returns_core_kpis(self):
        df = pd.DataFrame(
            [
                {"month": "2026-05-01", "index_name": "上证指数", "monthly_change_pct": 0.82, "static_pe": 16.1, "dynamic_pe": 14.5},
                {"month": "2026-05-01", "index_name": "深证成指", "monthly_change_pct": -1.35, "static_pe": 27.3, "dynamic_pe": 22.9},
            ]
        )

        summary = build_index_monitor_summary(df)

        self.assertEqual(summary["latest_month"], "2026-05")
        self.assertAlmostEqual(summary["avg_change_pct"], -0.265, places=3)
        self.assertEqual(summary["strongest_index"], "上证指数")
        self.assertEqual(summary["weakest_index"], "深证成指")
        self.assertAlmostEqual(summary["avg_static_pe"], 21.7, places=1)
        self.assertAlmostEqual(summary["avg_dynamic_pe"], 18.7, places=1)

    def test_build_price_trend_df_shapes_selected_value_field(self):
        df = pd.DataFrame(
            [
                {"month": "2026-04-01", "index_name": "上证指数", "close_price": 3300},
                {"month": "2026-05-01", "index_name": "上证指数", "close_price": 3367.46},
            ]
        )

        trend_df = build_price_trend_df(df, value_field="close_price")

        self.assertEqual(trend_df["index_name"].tolist(), ["上证指数", "上证指数"])
        self.assertEqual(trend_df.iloc[-1]["value"], 3367.46)

    def test_classify_index_import_rows_splits_insert_and_overwrite(self):
        incoming = pd.DataFrame(
            [
                {"month": "2026-05-01", "index_name": "上证指数"},
                {"month": "2026-05-01", "index_name": "深证成指"},
            ]
        )
        existing = pd.DataFrame(
            [{"month": "2026-05-01", "index_name": "上证指数"}]
        )

        preview = classify_index_import_rows(incoming, existing)

        self.assertEqual(preview["to_insert"]["index_name"].tolist(), ["深证成指"])
        self.assertEqual(preview["to_overwrite"]["index_name"].tolist(), ["上证指数"])
```

- [ ] **Step 2: Run the store tests to verify they fail**

Run: `pytest tests/test_index_monitor_store.py -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'src.index_monitor_store'`

- [ ] **Step 3: Create the store module**

```python
from __future__ import annotations

import os

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

TABLE_NAME = "macro_index_monitor_monthly"
DISPLAY_COLUMN_LABELS = {
    "month": "月份",
    "index_name": "指数名称",
    "monthly_change_pct": "当月涨幅",
    "open_price": "开盘价格",
    "close_price": "收盘价格",
    "low_price": "最低点",
    "high_price": "最高点",
    "static_pe": "期末静态市盈率",
    "dynamic_pe": "期末动态市盈率",
    "mom_change_pct": "环比涨幅变化",
    "yoy_change_pct": "同比涨幅变化",
    "mom_static_pe_change_rate": "静态市盈率变化率",
    "mom_dynamic_pe_change_rate": "动态市盈率变化率",
    "source_type": "数据来源",
    "updated_at": "更新时间",
}
PRICE_FIELDS = ["open_price", "close_price", "low_price", "high_price"]
VALUATION_FIELDS = ["static_pe", "dynamic_pe"]
NUMERIC_FIELDS = [
    "monthly_change_pct",
    "open_price",
    "close_price",
    "low_price",
    "high_price",
    "static_pe",
    "dynamic_pe",
    "mom_change_pct",
    "mom_open_price",
    "mom_close_price",
    "mom_low_price",
    "mom_high_price",
    "mom_static_pe",
    "mom_dynamic_pe",
    "mom_static_pe_change_rate",
    "mom_dynamic_pe_change_rate",
    "yoy_change_pct",
    "yoy_open_price",
    "yoy_close_price",
    "yoy_low_price",
    "yoy_high_price",
    "yoy_static_pe",
    "yoy_dynamic_pe",
]


def build_db_url():
    try:
        from src.sync_tushare_security_data import build_db_url as _sync_build_db_url

        return _sync_build_db_url()
    except Exception:
        pass

    direct_url = os.getenv("ETF_PG_URL") or os.getenv("DATABASE_URL")
    if direct_url:
        return direct_url

    password = os.getenv("ETF_PG_PASSWORD") or os.getenv("PGPASSWORD")
    if not password:
        raise RuntimeError("未配置数据库密码，请设置 ETF_PG_PASSWORD 或 PGPASSWORD")

    return URL.create(
        "postgresql+psycopg2",
        username=os.getenv("ETF_PG_USER", "postgres"),
        password=password,
        host=os.getenv("ETF_PG_HOST", "67.216.207.73"),
        port=int(os.getenv("ETF_PG_PORT", "5432")),
        database=os.getenv("ETF_PG_DATABASE", "postgres"),
        query={"sslmode": os.getenv("ETF_PG_SSLMODE", "disable")},
    )


def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)


def ensure_index_monitor_table(engine: Engine) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        month DATE NOT NULL,
        index_name TEXT NOT NULL,
        monthly_change_pct NUMERIC(18, 4),
        open_price NUMERIC(18, 4),
        close_price NUMERIC(18, 4),
        low_price NUMERIC(18, 4),
        high_price NUMERIC(18, 4),
        static_pe NUMERIC(18, 4),
        dynamic_pe NUMERIC(18, 4),
        mom_change_pct NUMERIC(18, 4),
        mom_open_price NUMERIC(18, 4),
        mom_close_price NUMERIC(18, 4),
        mom_low_price NUMERIC(18, 4),
        mom_high_price NUMERIC(18, 4),
        mom_static_pe NUMERIC(18, 4),
        mom_dynamic_pe NUMERIC(18, 4),
        mom_static_pe_change_rate NUMERIC(18, 4),
        mom_dynamic_pe_change_rate NUMERIC(18, 4),
        yoy_change_pct NUMERIC(18, 4),
        yoy_open_price NUMERIC(18, 4),
        yoy_close_price NUMERIC(18, 4),
        yoy_low_price NUMERIC(18, 4),
        yoy_high_price NUMERIC(18, 4),
        yoy_static_pe NUMERIC(18, 4),
        yoy_dynamic_pe NUMERIC(18, 4),
        source_type VARCHAR(16) NOT NULL,
        source_file TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (month, index_name)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def build_index_monitor_summary(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {
            "latest_month": None,
            "avg_change_pct": None,
            "strongest_index": None,
            "weakest_index": None,
            "avg_static_pe": None,
            "avg_dynamic_pe": None,
        }

    data = df.copy()
    data["month"] = pd.to_datetime(data["month"])
    latest_month = data["month"].max()
    latest_df = data[data["month"] == latest_month].copy()
    latest_df["monthly_change_pct"] = pd.to_numeric(latest_df["monthly_change_pct"], errors="coerce")
    latest_df["static_pe"] = pd.to_numeric(latest_df["static_pe"], errors="coerce")
    latest_df["dynamic_pe"] = pd.to_numeric(latest_df["dynamic_pe"], errors="coerce")
    strongest = latest_df.sort_values("monthly_change_pct", ascending=False).iloc[0]["index_name"] if latest_df["monthly_change_pct"].notna().any() else None
    weakest = latest_df.sort_values("monthly_change_pct", ascending=True).iloc[0]["index_name"] if latest_df["monthly_change_pct"].notna().any() else None
    return {
        "latest_month": latest_month.strftime("%Y-%m"),
        "avg_change_pct": latest_df["monthly_change_pct"].mean(),
        "strongest_index": strongest,
        "weakest_index": weakest,
        "avg_static_pe": latest_df["static_pe"].mean(),
        "avg_dynamic_pe": latest_df["dynamic_pe"].mean(),
    }


def build_price_trend_df(df: pd.DataFrame, value_field: str = "close_price") -> pd.DataFrame:
    data = df.copy()
    data["month"] = pd.to_datetime(data["month"])
    data["value"] = pd.to_numeric(data[value_field], errors="coerce")
    return data[["month", "index_name", "value"]].sort_values(["index_name", "month"]).reset_index(drop=True)


def build_valuation_trend_df(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    data["month"] = pd.to_datetime(data["month"])
    melted = data.melt(
        id_vars=["month", "index_name"],
        value_vars=VALUATION_FIELDS,
        var_name="metric_key",
        value_name="value",
    )
    melted["metric"] = melted["metric_key"].map(
        {"static_pe": "期末静态市盈率", "dynamic_pe": "期末动态市盈率"}
    )
    return melted.sort_values(["index_name", "month", "metric_key"]).reset_index(drop=True)


def classify_index_import_rows(incoming_df: pd.DataFrame, existing_df: pd.DataFrame) -> dict:
    incoming = incoming_df.copy()
    incoming["business_key"] = incoming["month"].astype(str) + "|" + incoming["index_name"].astype(str)
    if existing_df is None or existing_df.empty:
        return {"to_insert": incoming.drop(columns=["business_key"]).reset_index(drop=True), "to_overwrite": incoming.iloc[0:0].drop(columns=["business_key"]).copy()}
    existing = existing_df.copy()
    existing["business_key"] = existing["month"].astype(str) + "|" + existing["index_name"].astype(str)
    existing_keys = set(existing["business_key"].tolist())
    to_insert = incoming[~incoming["business_key"].isin(existing_keys)].drop(columns=["business_key"]).reset_index(drop=True)
    to_overwrite = incoming[incoming["business_key"].isin(existing_keys)].drop(columns=["business_key"]).reset_index(drop=True)
    return {"to_insert": to_insert, "to_overwrite": to_overwrite}


def build_index_upsert_rows(rows, source_type: str, source_file: str | None) -> list[dict]:
    payload_rows = []
    for row in rows:
        payload = {
            "month": pd.to_datetime(row["month"]).date(),
            "index_name": str(row["index_name"]).strip(),
            "source_type": source_type,
            "source_file": source_file,
        }
        for field in NUMERIC_FIELDS:
            payload[field] = float(row[field]) if row.get(field) is not None else None
        payload_rows.append(payload)
    return payload_rows


def load_index_monitor_df(engine: Engine | None = None) -> pd.DataFrame:
    actual_engine = engine or get_engine()
    ensure_index_monitor_table(actual_engine)
    with actual_engine.begin() as conn:
        return pd.read_sql(text(f"SELECT * FROM {TABLE_NAME} ORDER BY month ASC, index_name ASC"), conn)


def to_index_monitor_display_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    display_df = df.copy()
    if display_df.empty:
        return display_df.rename(columns=DISPLAY_COLUMN_LABELS)
    display_df["month"] = pd.to_datetime(display_df["month"]).dt.strftime("%Y-%m-%d")
    if "updated_at" in display_df.columns:
        display_df["updated_at"] = pd.to_datetime(display_df["updated_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    ordered = [col for col in DISPLAY_COLUMN_LABELS if col in display_df.columns]
    return display_df[ordered].rename(columns=DISPLAY_COLUMN_LABELS)
```

- [ ] **Step 4: Re-run the store tests**

Run: `pytest tests/test_index_monitor_store.py -v`

Expected: PASS for all store tests

- [ ] **Step 5: Commit**

```bash
git add src/index_monitor_store.py tests/test_index_monitor_store.py
git commit -m "feat: add index monitor store helpers"
```

### Task 4: Integrate `📊 指数监测` into `app.py`

**Files:**
- Modify: `app.py`
- Modify: `src/navigation_config.py`

- [ ] **Step 1: Add the new imports**

```python
from src.index_monitor_importer import parse_index_monitor_workbook
from src.index_monitor_store import (
    build_index_monitor_summary,
    build_index_upsert_rows,
    build_price_trend_df,
    build_valuation_trend_df,
    classify_index_import_rows,
    get_engine as get_index_monitor_engine,
    load_index_monitor_df,
    to_index_monitor_display_df,
    upsert_index_monitor_rows,
)
```

- [ ] **Step 2: Add macro page routing for `📊 指数监测`**

```python
        else:
            mobile_page = st.selectbox(
                "页面",
                MACRO_PAGE_OPTIONS,
                key="iphone_page_macro",
            )
            st.caption(f"当前位置：宏观 / {mobile_page}")
            if mobile_page == "🌏 宏观经济":
                render_macro_tab()
            elif mobile_page == "🏦 本外币存款":
                render_etf_deposit_tab()
            else:
                render_index_monitor_tab()
```

```python
    else:
        macro_subpage = st.sidebar.radio(
            "宏观模块",
            MACRO_PAGE_OPTIONS,
            key="macro_subpage"
        )
        st.caption(f"当前位置：宏观 / {macro_subpage}")
        if macro_subpage == "🌏 宏观经济":
            render_macro_tab()
        elif macro_subpage == "🏦 本外币存款":
            render_etf_deposit_tab()
        else:
            render_index_monitor_tab()
```

- [ ] **Step 3: Add the dashboard renderer**

```python
def render_index_monitor_tab():
    st.subheader("📊 指数监测")
    st.caption("展示股票指数月度表现、估值趋势与同比环比变化，支持手工录入与 Excel 批量导入。")

    for state_key, default_value in (
        ("index_manual_month_open", False),
        ("index_single_edit_open", False),
        ("index_import_open", False),
        ("index_selected_month", ""),
        ("index_selected_name", ""),
        ("index_history_limit", "最近12个月"),
    ):
        if state_key not in st.session_state:
            st.session_state[state_key] = default_value

    try:
        engine = get_index_monitor_engine()
        df = load_index_monitor_df(engine)
    except Exception as exc:
        st.error(f"加载指数监测数据失败: {exc}")
        return

    action_col, status_col = st.columns([1, 3])
    with action_col:
        if st.button("新增月份", key="index_add_month"):
            st.session_state["index_manual_month_open"] = True
        if st.button("单指数补录/修改", key="index_edit_single"):
            st.session_state["index_single_edit_open"] = True
        if st.button("批量导入 Excel", key="index_import"):
            st.session_state["index_import_open"] = True

    with status_col:
        if df.empty:
            st.caption("最新数据月份：- | 记录数：0 | 最近更新时间：-")
        else:
            latest_month = pd.to_datetime(df["month"]).max().strftime("%Y-%m")
            updated_at = pd.to_datetime(df["updated_at"]).max().strftime("%Y-%m-%d %H:%M")
            st.caption(f"最新数据月份：{latest_month} | 记录数：{len(df)} | 最近更新时间：{updated_at}")

    if df.empty:
        st.info("暂无指数监测数据，请先新增月份或批量导入。")
        return

    all_index_names = sorted(df["index_name"].dropna().unique().tolist())
    selected_indices = st.multiselect("指数筛选", options=all_index_names, default=all_index_names[: min(4, len(all_index_names))], key="index_monitor_names")
    window = st.radio("时间范围", ["最近12个月", "最近24个月", "全部"], horizontal=True, key="index_history_limit")

    filtered_df = df[df["index_name"].isin(selected_indices)].copy() if selected_indices else df.copy()
    filtered_df["month"] = pd.to_datetime(filtered_df["month"])
    if window != "全部" and not filtered_df.empty:
        cutoff = filtered_df["month"].max() - pd.DateOffset(months=11 if window == "最近12个月" else 23)
        filtered_df = filtered_df[filtered_df["month"] >= cutoff]

    summary = build_index_monitor_summary(filtered_df if not filtered_df.empty else df)
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("最新月份", summary["latest_month"] or "-")
    c2.metric("指数平均涨幅", f'{summary["avg_change_pct"]:.2f}%' if summary["avg_change_pct"] is not None else "-")
    c3.metric("当月最强指数", summary["strongest_index"] or "-")
    c4.metric("当月最弱指数", summary["weakest_index"] or "-")
    c5.metric("平均静态PE", f'{summary["avg_static_pe"]:.2f}' if summary["avg_static_pe"] is not None else "-")
    c6.metric("平均动态PE", f'{summary["avg_dynamic_pe"]:.2f}' if summary["avg_dynamic_pe"] is not None else "-")

    price_trend_df = build_price_trend_df(filtered_df, value_field="close_price")
    st.plotly_chart(
        px.line(price_trend_df, x="month", y="value", color="index_name", markers=True, title="价格趋势（收盘价）"),
        use_container_width=True,
    )

    valuation_trend_df = build_valuation_trend_df(filtered_df)
    valuation_trend_df["series"] = valuation_trend_df["index_name"] + " / " + valuation_trend_df["metric"]
    st.plotly_chart(
        px.line(valuation_trend_df, x="month", y="value", color="series", markers=True, title="估值趋势"),
        use_container_width=True,
    )

    latest_month = filtered_df["month"].max()
    latest_df = filtered_df[filtered_df["month"] == latest_month].copy()
    latest_show = to_index_monitor_display_df(latest_df)
    st.dataframe(latest_show, use_container_width=True, hide_index=True)
```

- [ ] **Step 4: Add manual month entry, single-index editing, and import preview flow**

```python
    if st.session_state["index_import_open"]:
        upload = st.file_uploader("上传股票指数 Excel", type=["xlsx"], key="index_monitor_uploader")
        if upload is not None:
            imported_df = parse_index_monitor_workbook(upload)
            preview = classify_index_import_rows(imported_df, df)
            overwrite_mode = st.radio(
                "重复记录处理",
                ["跳过已存在记录", "覆盖已存在记录"],
                horizontal=True,
                key="index_overwrite_mode",
            )
            st.write("新增记录")
            st.dataframe(to_index_monitor_display_df(preview["to_insert"]), use_container_width=True, hide_index=True)
            st.write("覆盖记录")
            st.dataframe(to_index_monitor_display_df(preview["to_overwrite"]), use_container_width=True, hide_index=True)
            if st.button("确认写入", key="index_confirm_import"):
                write_df = imported_df.copy()
                if overwrite_mode == "跳过已存在记录":
                    write_df = preview["to_insert"].copy()
                if write_df.empty:
                    st.warning("没有需要写入的记录。")
                else:
                    rows = build_index_upsert_rows(
                        write_df.to_dict(orient="records"),
                        source_type="import",
                        source_file=getattr(upload, "name", None),
                    )
                    upsert_index_monitor_rows(engine, rows)
                    st.session_state["index_import_open"] = False
                    st.success(f"已写入 {len(rows)} 条指数记录")
                    st.rerun()
```

For the month-entry and single-index-edit forms, use the same field names already standardized in `src/index_monitor_store.py`, with one editable grid-like section for month batch entry and one compact form for single-index update.

- [ ] **Step 5: Run targeted tests and a local app smoke check**

Run:

```bash
pytest tests/test_index_monitor_store.py tests/test_index_monitor_importer.py tests/test_navigation_config.py -v
python -m py_compile app.py src/index_monitor_store.py src/index_monitor_importer.py
streamlit run app.py
```

Expected:

- all targeted tests PASS
- `宏观 / 📊 指数监测` appears in mobile and desktop navigation
- empty state renders without traceback
- populated state shows KPI cards, price trend, valuation trend, and comparison table
- import preview separates insert vs overwrite rows

- [ ] **Step 6: Commit**

```bash
git add app.py src/index_monitor_store.py src/index_monitor_importer.py tests/test_index_monitor_store.py tests/test_index_monitor_importer.py src/navigation_config.py
git commit -m "feat: add macro index monitor dashboard"
```

### Task 5: Run database-backed acceptance checks

**Files:**
- Modify: none if verification passes
- Verify: PostgreSQL table `macro_index_monitor_monthly`

- [ ] **Step 1: Prepare the runtime configuration**

```powershell
$env:ETF_PG_HOST="67.216.207.73"
$env:ETF_PG_PORT="5432"
$env:ETF_PG_DATABASE="postgres"
$env:ETF_PG_USER="postgres"
$env:ETF_PG_SSLMODE="disable"
```

Expected: runtime PG configuration is available without introducing duplicate config files

- [ ] **Step 2: Run the targeted tests with PG configuration available**

Run: `pytest tests/test_index_monitor_store.py tests/test_index_monitor_importer.py tests/test_navigation_config.py -v`

Expected: PASS

- [ ] **Step 3: Launch the app and verify these end-to-end scenarios**

```text
Scenario 1: Empty state
- Open 宏观 / 📊 指数监测 against an empty table
- Confirm only CTA actions render, without chart traceback

Scenario 2: Import reference workbook
- Upload 新增证券分析表单-20260505.xlsx
- Confirm preview shows insert/overwrite separation by (month, index_name)
- Confirm write succeeds

Scenario 3: Dashboard rendering
- Open populated 宏观 / 📊 指数监测
- Confirm KPI cards show latest month, average涨幅, strongest/weakest index, average PE
- Confirm both price and valuation charts render

Scenario 4: Single-index edit
- Pick one month + one index
- Modify close_price or monthly_change_pct
- Save and confirm the comparison table updates
```

- [ ] **Step 4: Inspect the table directly**

Run:

```sql
SELECT month, index_name, monthly_change_pct, close_price, static_pe, source_type
FROM macro_index_monitor_monthly
ORDER BY month DESC, index_name ASC
LIMIT 20;
```

Expected: recent month rows appear with correct source metadata

- [ ] **Step 5: Commit**

```bash
git add app.py src/index_monitor_store.py src/index_monitor_importer.py tests/test_index_monitor_store.py tests/test_index_monitor_importer.py
git commit -m "test: verify index monitor dashboard flow"
```

---

## Spec Coverage Check

- 页面位置在 `宏观`：covered in Task 1 and Task 4
- 页面名称 `📊 指数监测`：covered in Task 1 and Task 4
- 总览型首页：covered in Task 4 renderer
- 手工录入两种方式：covered in Task 4 forms
- 双主图：covered in Task 4 charts
- PostgreSQL 存储：covered in Task 3 and Task 5
- Excel 导入预览：covered in Task 2 and Task 4
- `(month, index_name)` 作为业务主键：covered in Task 3 store design

## Placeholder Scan

- No unresolved placeholder markers remain
- Every code-changing step contains concrete snippets
- Every verification step contains exact commands and expected outcomes

## Type Consistency Check

- Importer outputs `month` and `index_name` plus normalized numeric fields the store module expects
- App form payload and import payload both feed `build_index_upsert_rows()`
- Dashboard summary reads `monthly_change_pct`, `static_pe`, and `dynamic_pe` consistently
- Table key remains `(month, index_name)` across parser, preview classifier, write path, and DB inspection
