# 基金监测 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `宏观` 分类下新增 `📈 基金监测` 页面，支持从 `新增证券分析表单-20260505.xlsx` 的 `公募&私募基金` sheet 解析月度数据、写入 PostgreSQL、展示总览仪表盘，并提供手工维护与 Excel 导入能力。

**Architecture:** 复用 `指数监测` 的实现套路，把功能拆成三层：`src/fund_monitor_importer.py` 负责按纵向月份区块解析 Excel，`src/fund_monitor_store.py` 负责建表、读写、趋势构造与展示转换，`app.py` 负责把页面挂到 `宏观` 导航中并渲染仪表盘与维护交互。测试分为 importer、store、navigation 三类，页面层以编译与定向回归为主验证。

**Tech Stack:** Python 3.14、Streamlit、Pandas、OpenPyXL、SQLAlchemy、PostgreSQL、Pytest

---

## File Structure

### New files

- `src/fund_monitor_importer.py`
  - 解析 `公募&私募基金` sheet
  - 识别月份区块、分类名称、字段映射
  - 将 Excel 错误值和空值统一转为 `None`
- `src/fund_monitor_store.py`
  - 基金监测表结构
  - PostgreSQL 连接与建表
  - upsert、导入预览分类、KPI/趋势数据构造、中文展示列映射
- `tests/test_fund_monitor_importer.py`
  - 解析器测试
- `tests/test_fund_monitor_store.py`
  - store 层测试

### Modified files

- `src/navigation_config.py`
  - 增加 `📈 基金监测`
- `tests/test_navigation_config.py`
  - 增加宏观导航归属测试
- `app.py`
  - 导入 fund monitor 模块
  - 新增宏观路由
  - 新增 `render_fund_monitor_tab()`
- `docs/superpowers/specs/2026-05-06-fund-monitor-design.md`
  - 仅作为实现参考，不在本计划中修改

## Implementation Notes

- 数据表采用长表，一条记录唯一对应 `month + category_name`
- 页面默认主维度为 `净值（亿元）`
- 导入器必须按月份区块解析，不读取顶部摘要为唯一月份
- `#DIV/0!`、空白、缺失字段统一落为 `None`
- 手工维护模式与 `指数监测` 尽量一致，降低 UI 与实现复杂度

## Task 1: Build the Fund Monitor Importer

**Files:**
- Create: `tests/test_fund_monitor_importer.py`
- Create: `src/fund_monitor_importer.py`

- [ ] **Step 1: Write the failing parser tests**

Create `tests/test_fund_monitor_importer.py` with:

```python
import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook

from src.fund_monitor_importer import parse_fund_monitor_workbook


class FundMonitorImporterTests(unittest.TestCase):
    def test_parse_fund_monitor_workbook_reads_repeated_month_blocks(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "fund-monitor.xlsx"
            wb = Workbook()
            ws = wb.active
            ws.title = "公募&私募基金"
            ws["D2"] = "当月情况"
            ws["H2"] = "环比变动情况"
            ws["L2"] = "同比变动情况"
            headers = ["基金数量（只）", "份额（亿份）", "净值（亿元）", "单位净值（元）"]
            for idx, header in enumerate(headers, start=4):
                ws.cell(3, idx).value = header
                ws.cell(3, idx + 4).value = header
                ws.cell(3, idx + 8).value = header

            ws["B16"] = "2026-05-01"
            ws["C16"] = "其中：股票基金"
            ws["D16"] = 3585
            ws["E16"] = 39349.05
            ws["F16"] = 51128.57
            ws["G16"] = 1.2993
            ws["J16"] = -5168.49
            ws["N16"] = 6467.17

            ws["B22"] = "2026-04-01"
            ws["C22"] = "私募证券投资基金"
            ws["D22"] = 81745
            ws["F22"] = 74600
            ws["J22"] = 1100
            wb.save(path)

            df = parse_fund_monitor_workbook(path)

        self.assertEqual(df[["month", "category_name"]].values.tolist(), [
            ["2026-04-01", "私募证券投资基金"],
            ["2026-05-01", "其中：股票基金"],
        ])
        self.assertAlmostEqual(df.iloc[1]["nav_amount"], 51128.57)
        self.assertAlmostEqual(df.iloc[0]["mom_nav_amount"], 1100)

    def test_parse_fund_monitor_workbook_converts_excel_errors_to_none(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "fund-monitor-errors.xlsx"
            wb = Workbook()
            ws = wb.active
            ws.title = "公募&私募基金"
            ws["D2"] = "当月情况"
            ws["D3"] = "基金数量（只）"
            ws["E3"] = "份额（亿份）"
            ws["F3"] = "净值（亿元）"
            ws["G3"] = "单位净值（元）"
            ws["B16"] = "2026-05-01"
            ws["C16"] = "其中：货币基金"
            ws["G16"] = "#DIV/0!"
            wb.save(path)

            df = parse_fund_monitor_workbook(path)

        self.assertIsNone(df.iloc[0]["unit_nav"])

    def test_parse_fund_monitor_workbook_raises_when_sheet_missing(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "wrong.xlsx"
            wb = Workbook()
            wb.active.title = "股票指数"
            wb.save(path)
            with self.assertRaisesRegex(ValueError, "公募&私募基金"):
                parse_fund_monitor_workbook(path)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the importer tests to verify they fail**

Run:

```bash
python -m pytest tests/test_fund_monitor_importer.py -v
```

Expected: FAIL with `ModuleNotFoundError` or `ImportError` for `src.fund_monitor_importer`

- [ ] **Step 3: Write the minimal importer implementation**

Create `src/fund_monitor_importer.py` with:

```python
from __future__ import annotations

from pathlib import Path

import pandas as pd
from openpyxl import load_workbook


SHEET_NAME = "公募&私募基金"
SECTION_FIELD_MAP = {
    "当月情况": {
        "基金数量（只）": "fund_count",
        "份额（亿份）": "share_amount",
        "净值（亿元）": "nav_amount",
        "单位净值（元）": "unit_nav",
    },
    "环比变动情况": {
        "基金数量（只）": "mom_fund_count",
        "份额（亿份）": "mom_share_amount",
        "净值（亿元）": "mom_nav_amount",
        "单位净值（元）": "mom_unit_nav",
    },
    "同比变动情况": {
        "基金数量（只）": "yoy_fund_count",
        "份额（亿份）": "yoy_share_amount",
        "净值（亿元）": "yoy_nav_amount",
        "单位净值（元）": "yoy_unit_nav",
    },
}


def _normalize_text(value) -> str:
    if value is None:
        return ""
    return str(value).replace("\n", "").strip()


def _coerce_optional_float(value):
    if value is None:
        return None
    text = _normalize_text(value)
    if text in {"", "#DIV/0!", "#N/A"}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_fund_monitor_workbook(path) -> pd.DataFrame:
    workbook_source = Path(path) if isinstance(path, (str, Path)) else path
    wb = load_workbook(workbook_source, data_only=True, read_only=True)
    try:
        if SHEET_NAME not in wb.sheetnames:
            raise ValueError("导入文件缺少 公募&私募基金 sheet")

        ws = wb[SHEET_NAME]
        section_by_col = {}
        current_section = ""
        for col_idx in range(1, ws.max_column + 1):
            section = _normalize_text(ws.cell(2, col_idx).value)
            if section in SECTION_FIELD_MAP:
                current_section = section
            section_by_col[col_idx] = current_section

        field_by_col = {}
        for col_idx in range(1, ws.max_column + 1):
            field_by_col[col_idx] = _normalize_text(ws.cell(3, col_idx).value)

        rows = []
        current_month_text = None
        for row_idx in range(4, ws.max_row + 1):
            month_value = ws.cell(row_idx, 2).value
            if month_value is not None:
                current_month_text = pd.to_datetime(month_value).strftime("%Y-%m-%d")

            category_name = _normalize_text(ws.cell(row_idx, 3).value)
            if not category_name:
                continue
            if current_month_text is None:
                continue

            row = {"month": current_month_text, "category_name": category_name}
            for col_idx in range(4, ws.max_column + 1):
                section = section_by_col.get(col_idx, "")
                field = field_by_col.get(col_idx, "")
                mapped = SECTION_FIELD_MAP.get(section, {}).get(field)
                if not mapped:
                    continue
                row[mapped] = _coerce_optional_float(ws.cell(row_idx, col_idx).value)

            rows.append(row)

        if not rows:
            raise ValueError("未解析出任何基金监测记录")
        return pd.DataFrame(rows).sort_values(["month", "category_name"]).reset_index(drop=True)
    finally:
        wb.close()
```

- [ ] **Step 4: Run the importer tests to verify they pass**

Run:

```bash
python -m pytest tests/test_fund_monitor_importer.py -v
```

Expected: PASS all tests in `FundMonitorImporterTests`

- [ ] **Step 5: Commit the importer task**

```bash
git add src/fund_monitor_importer.py tests/test_fund_monitor_importer.py
git commit -m "feat: add fund monitor workbook importer"
```

## Task 2: Build the Fund Monitor Store Layer

**Files:**
- Create: `tests/test_fund_monitor_store.py`
- Create: `src/fund_monitor_store.py`

- [ ] **Step 1: Write the failing store tests**

Create `tests/test_fund_monitor_store.py` with:

```python
import unittest

import pandas as pd

from src.fund_monitor_store import (
    build_fund_monitor_change_trend_df,
    build_fund_monitor_summary,
    build_fund_monitor_trend_df,
    classify_fund_monitor_import_rows,
)


class FundMonitorStoreTests(unittest.TestCase):
    def test_build_fund_monitor_summary_returns_core_kpis(self):
        df = pd.DataFrame([
            {"month": "2026-05-01", "category_name": "合计", "nav_amount": 375322.31},
            {"month": "2026-05-01", "category_name": "其中：股票基金", "nav_amount": 51128.57},
            {"month": "2026-05-01", "category_name": "私募证券投资基金", "nav_amount": 74600},
        ])
        summary = build_fund_monitor_summary(df)
        self.assertEqual(summary["latest_month"], "2026-05")
        self.assertAlmostEqual(summary["public_total_nav"], 375322.31)
        self.assertAlmostEqual(summary["equity_fund_nav"], 51128.57)
        self.assertAlmostEqual(summary["private_nav"], 74600)

    def test_build_fund_monitor_trend_df_shapes_selected_metric(self):
        df = pd.DataFrame([
            {"month": "2026-04-01", "category_name": "合计", "nav_amount": 386051.94},
            {"month": "2026-05-01", "category_name": "合计", "nav_amount": 375322.31},
        ])
        trend_df = build_fund_monitor_trend_df(df, value_field="nav_amount")
        self.assertEqual(trend_df.iloc[-1]["value"], 375322.31)

    def test_build_fund_monitor_change_trend_df_shapes_mom_and_yoy_series(self):
        df = pd.DataFrame([
            {"month": "2026-04-01", "category_name": "其中：股票基金", "mom_nav_amount": -790.35, "yoy_nav_amount": 11452.52},
            {"month": "2026-05-01", "category_name": "其中：股票基金", "mom_nav_amount": -5168.49, "yoy_nav_amount": 6467.17},
        ])
        trend_df = build_fund_monitor_change_trend_df(df, metric_key="nav_amount")
        self.assertEqual(trend_df["change_type"].tolist(), ["环比", "同比", "环比", "同比"])
        self.assertEqual(trend_df["metric"].unique().tolist(), ["净值（亿元）"])

    def test_classify_fund_monitor_import_rows_splits_insert_and_overwrite(self):
        incoming = pd.DataFrame([
            {"month": "2026-05-01", "category_name": "合计"},
            {"month": "2026-05-01", "category_name": "私募证券投资基金"},
        ])
        existing = pd.DataFrame([
            {"month": "2026-05-01", "category_name": "合计"},
        ])
        preview = classify_fund_monitor_import_rows(incoming, existing)
        self.assertEqual(preview["to_insert"]["category_name"].tolist(), ["私募证券投资基金"])
        self.assertEqual(preview["to_overwrite"]["category_name"].tolist(), ["合计"])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the store tests to verify they fail**

Run:

```bash
python -m pytest tests/test_fund_monitor_store.py -v
```

Expected: FAIL with import errors for `src.fund_monitor_store`

- [ ] **Step 3: Write the minimal store implementation**

Create `src/fund_monitor_store.py` with:

```python
from __future__ import annotations

import os

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL


TABLE_NAME = "macro_fund_monitor_monthly"
DISPLAY_COLUMN_LABELS = {
    "month": "月份",
    "category_name": "分类名称",
    "fund_count": "基金数量（只）",
    "share_amount": "份额（亿份）",
    "nav_amount": "净值（亿元）",
    "unit_nav": "单位净值（元）",
    "mom_fund_count": "环比基金数量变动",
    "mom_share_amount": "环比份额变动",
    "mom_nav_amount": "环比净值变动",
    "yoy_fund_count": "同比基金数量变动",
    "yoy_share_amount": "同比份额变动",
    "yoy_nav_amount": "同比净值变动",
    "source_type": "数据来源",
    "updated_at": "更新时间",
}
NUMERIC_FIELDS = [
    "fund_count", "share_amount", "nav_amount", "unit_nav",
    "mom_fund_count", "mom_share_amount", "mom_nav_amount", "mom_unit_nav",
    "yoy_fund_count", "yoy_share_amount", "yoy_nav_amount", "yoy_unit_nav",
]
CHANGE_TREND_FIELDS = {
    "fund_count": {"label": "基金数量（只）", "mom": "mom_fund_count", "yoy": "yoy_fund_count"},
    "share_amount": {"label": "份额（亿份）", "mom": "mom_share_amount", "yoy": "yoy_share_amount"},
    "nav_amount": {"label": "净值（亿元）", "mom": "mom_nav_amount", "yoy": "yoy_nav_amount"},
    "unit_nav": {"label": "单位净值（元）", "mom": "mom_unit_nav", "yoy": "yoy_unit_nav"},
}
CHANGE_TREND_FIELD_LABELS = {key: value["label"] for key, value in CHANGE_TREND_FIELDS.items()}


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


def build_fund_monitor_summary(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {
            "latest_month": None,
            "public_total_nav": None,
            "equity_fund_nav": None,
            "hybrid_fund_nav": None,
            "private_nav": None,
            "wealth_nav": None,
        }

    data = df.copy()
    data["month"] = pd.to_datetime(data["month"])
    latest_month = data["month"].max()
    latest_df = data[data["month"] == latest_month].copy()
    latest_df["nav_amount"] = pd.to_numeric(latest_df["nav_amount"], errors="coerce")

    def nav_of(name: str):
        rows = latest_df.loc[latest_df["category_name"] == name, "nav_amount"]
        return rows.iloc[0] if not rows.empty else None

    return {
        "latest_month": latest_month.strftime("%Y-%m"),
        "public_total_nav": nav_of("合计"),
        "equity_fund_nav": nav_of("其中：股票基金"),
        "hybrid_fund_nav": nav_of("其中：混合基金"),
        "private_nav": nav_of("私募证券投资基金"),
        "wealth_nav": nav_of("权益类理财产品"),
    }


def build_fund_monitor_trend_df(df: pd.DataFrame, value_field: str = "nav_amount") -> pd.DataFrame:
    data = df.copy()
    data["month"] = pd.to_datetime(data["month"])
    data["value"] = pd.to_numeric(data[value_field], errors="coerce")
    return data[["month", "category_name", "value"]].sort_values(["category_name", "month"]).reset_index(drop=True)


def build_fund_monitor_change_trend_df(df: pd.DataFrame, metric_key: str = "nav_amount") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["month", "category_name", "change_type", "metric", "value"])
    config = CHANGE_TREND_FIELDS[metric_key]
    data = df.copy()
    data["month"] = pd.to_datetime(data["month"])
    frames = []
    for change_type, field_name in (("环比", config["mom"]), ("同比", config["yoy"])):
        trend_df = data[["month", "category_name", field_name]].copy()
        trend_df["value"] = pd.to_numeric(trend_df[field_name], errors="coerce")
        trend_df["change_type"] = change_type
        trend_df["metric"] = config["label"]
        frames.append(trend_df.drop(columns=[field_name]))
    result = pd.concat(frames, ignore_index=True)
    result["change_order"] = result["change_type"].map({"环比": 0, "同比": 1})
    return result.sort_values(["category_name", "month", "change_order"]).drop(columns=["change_order"]).reset_index(drop=True)


def classify_fund_monitor_import_rows(incoming_df: pd.DataFrame, existing_df: pd.DataFrame) -> dict:
    incoming = incoming_df.copy()
    incoming["business_key"] = incoming["month"].astype(str) + "|" + incoming["category_name"].astype(str)
    if existing_df is None or existing_df.empty:
        return {
            "to_insert": incoming.drop(columns=["business_key"]).reset_index(drop=True),
            "to_overwrite": incoming.iloc[0:0].drop(columns=["business_key"]).copy(),
        }
    existing = existing_df.copy()
    existing["business_key"] = existing["month"].astype(str) + "|" + existing["category_name"].astype(str)
    existing_keys = set(existing["business_key"].tolist())
    to_insert = incoming[~incoming["business_key"].isin(existing_keys)].drop(columns=["business_key"]).reset_index(drop=True)
    to_overwrite = incoming[incoming["business_key"].isin(existing_keys)].drop(columns=["business_key"]).reset_index(drop=True)
    return {"to_insert": to_insert, "to_overwrite": to_overwrite}
```

- [ ] **Step 4: Extend the store with DB and display helpers**

Add to `src/fund_monitor_store.py`:

```python
def ensure_fund_monitor_table(engine: Engine) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        month DATE NOT NULL,
        category_name TEXT NOT NULL,
        category_group VARCHAR(32),
        category_level VARCHAR(16),
        sort_order INTEGER,
        fund_count NUMERIC(18, 4),
        share_amount NUMERIC(18, 4),
        nav_amount NUMERIC(18, 4),
        unit_nav NUMERIC(18, 8),
        mom_fund_count NUMERIC(18, 4),
        mom_share_amount NUMERIC(18, 4),
        mom_nav_amount NUMERIC(18, 4),
        mom_unit_nav NUMERIC(18, 8),
        yoy_fund_count NUMERIC(18, 4),
        yoy_share_amount NUMERIC(18, 4),
        yoy_nav_amount NUMERIC(18, 4),
        yoy_unit_nav NUMERIC(18, 8),
        source_type VARCHAR(16) NOT NULL,
        source_file TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (month, category_name)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def build_fund_monitor_upsert_rows(rows, source_type: str, source_file: str | None) -> list[dict]:
    payload_rows = []
    for row in rows:
        payload = {
            "month": pd.to_datetime(row["month"]).date(),
            "category_name": str(row["category_name"]).strip(),
            "category_group": row.get("category_group"),
            "category_level": row.get("category_level"),
            "sort_order": row.get("sort_order"),
            "source_type": source_type,
            "source_file": source_file,
        }
        for field in NUMERIC_FIELDS:
            payload[field] = float(row[field]) if row.get(field) is not None else None
        payload_rows.append(payload)
    return payload_rows


def upsert_fund_monitor_rows(engine: Engine, rows: list[dict]) -> int:
    ensure_fund_monitor_table(engine)
    columns = [
        "month", "category_name", "category_group", "category_level", "sort_order",
        *NUMERIC_FIELDS, "source_type", "source_file",
    ]
    update_columns = ["category_group", "category_level", "sort_order", *NUMERIC_FIELDS, "source_type", "source_file"]
    insert_sql = text(
        f"""
        INSERT INTO {TABLE_NAME} ({", ".join(columns)})
        VALUES ({", ".join(f":{col}" for col in columns)})
        ON CONFLICT (month, category_name) DO UPDATE SET
            {", ".join(f"{col} = EXCLUDED.{col}" for col in update_columns)},
            updated_at = NOW();
        """
    )
    with engine.begin() as conn:
        conn.execute(insert_sql, rows)
    return len(rows)


def load_fund_monitor_df(engine: Engine | None = None) -> pd.DataFrame:
    actual_engine = engine or get_engine()
    ensure_fund_monitor_table(actual_engine)
    with actual_engine.begin() as conn:
        return pd.read_sql(text(f"SELECT * FROM {TABLE_NAME} ORDER BY month ASC, sort_order ASC, category_name ASC"), conn)


def to_fund_monitor_display_df(df: pd.DataFrame) -> pd.DataFrame:
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

- [ ] **Step 5: Run the store tests to verify they pass**

Run:

```bash
python -m pytest tests/test_fund_monitor_store.py -v
```

Expected: PASS all tests in `FundMonitorStoreTests`

- [ ] **Step 6: Commit the store task**

```bash
git add src/fund_monitor_store.py tests/test_fund_monitor_store.py
git commit -m "feat: add fund monitor store helpers"
```

## Task 3: Add Navigation Entry and Coverage

**Files:**
- Modify: `src/navigation_config.py`
- Modify: `tests/test_navigation_config.py`

- [ ] **Step 1: Write the failing navigation test**

Add to `tests/test_navigation_config.py`:

```python
def test_fund_monitor_page_belongs_to_macro(self):
    self.assertIn("📈 基金监测", MACRO_PAGE_OPTIONS)
    self.assertNotIn("📈 基金监测", ETF_PAGE_OPTIONS)
```

- [ ] **Step 2: Run the navigation test to verify it fails**

Run:

```bash
python -m pytest tests/test_navigation_config.py::NavigationConfigTests::test_fund_monitor_page_belongs_to_macro -v
```

Expected: FAIL because the new page is not yet in `MACRO_PAGE_OPTIONS`

- [ ] **Step 3: Update the macro navigation config**

Modify `src/navigation_config.py`:

```python
MACRO_PAGE_OPTIONS = [
    "🌏 宏观经济",
    "🏦 本外币存款",
    "📊 指数监测",
    "📈 基金监测",
]
```

- [ ] **Step 4: Run the navigation tests to verify they pass**

Run:

```bash
python -m pytest tests/test_navigation_config.py -v
```

Expected: PASS all navigation config tests

- [ ] **Step 5: Commit the navigation task**

```bash
git add src/navigation_config.py tests/test_navigation_config.py
git commit -m "feat: add fund monitor navigation"
```

## Task 4: Integrate the Dashboard into `app.py`

**Files:**
- Modify: `app.py`

- [ ] **Step 1: Add the new imports and route wiring**

Update the import block in `app.py` with:

```python
from src.fund_monitor_importer import parse_fund_monitor_workbook
from src.fund_monitor_store import (
    CHANGE_TREND_FIELD_LABELS as FUND_CHANGE_TREND_FIELD_LABELS,
    build_fund_monitor_change_trend_df,
    build_fund_monitor_summary,
    build_fund_monitor_trend_df,
    build_fund_monitor_upsert_rows,
    classify_fund_monitor_import_rows,
    get_engine as get_fund_monitor_engine,
    load_fund_monitor_df,
    to_fund_monitor_display_df,
    upsert_fund_monitor_rows,
)
```

Update the macro routing branches to:

```python
if mobile_page == "🌏 宏观经济":
    render_macro_tab()
elif mobile_page == "🏦 本外币存款":
    render_etf_deposit_tab()
elif mobile_page == "📊 指数监测":
    render_index_monitor_tab()
else:
    render_fund_monitor_tab()
```

Apply the same branching for the desktop `macro_subpage` route.

- [ ] **Step 2: Add the page constants and editor helpers**

Before `render_fund_hot_stocks_tab()`, add:

```python
FUND_MONITOR_DEFAULT_NAMES = [
    "合计",
    "其中：股票基金",
    "其中：混合基金",
    "其中：债券基金",
    "其中：货币基金",
    "其中：QDII基金",
    "私募证券投资基金",
    "私募资管权益基金",
    "权益类理财产品",
]

FUND_MONITOR_FIELD_LABELS = {
    "fund_count": "基金数量（只）",
    "share_amount": "份额（亿份）",
    "nav_amount": "净值（亿元）",
    "unit_nav": "单位净值（元）",
    "mom_fund_count": "环比基金数量变动",
    "mom_share_amount": "环比份额变动",
    "mom_nav_amount": "环比净值变动",
    "mom_unit_nav": "环比单位净值变动",
    "yoy_fund_count": "同比基金数量变动",
    "yoy_share_amount": "同比份额变动",
    "yoy_nav_amount": "同比净值变动",
    "yoy_unit_nav": "同比单位净值变动",
}
```

Also add helper functions mirroring the index monitor pattern:

```python
def _build_fund_monitor_batch_editor_df(existing_df: pd.DataFrame, month_text: str) -> pd.DataFrame:
    ...

def _collect_fund_monitor_batch_rows(editor_df: pd.DataFrame, month_text: str) -> list[dict]:
    ...
```

- [ ] **Step 3: Implement `render_fund_monitor_tab()`**

Add a new page function with this shape:

```python
def render_fund_monitor_tab():
    st.subheader("📈 基金监测")
    st.caption("展示公募、私募与权益理财的月度规模、结构趋势与同比环比变化，支持手工录入与 Excel 批量导入。")

    for state_key, default_value in (
        ("fund_manual_month_open", False),
        ("fund_single_edit_open", False),
        ("fund_import_open", False),
        ("fund_history_limit", "最近12个月"),
        ("fund_metric_field", "nav_amount"),
        ("fund_overwrite_mode", "跳过已存在记录"),
    ):
        if state_key not in st.session_state:
            st.session_state[state_key] = default_value

    engine = get_fund_monitor_engine()
    df = load_fund_monitor_df(engine)

    action_col, status_col = st.columns([1, 3])
    with action_col:
        st.button("新增月份", key="fund_add_month")
        st.button("单分类补录/修改", key="fund_edit_single")
        st.button("批量导入 Excel", key="fund_import_file")

    # 顶部状态、KPI、时间范围、分类筛选
    # 图1：总趋势，图2：公募结构趋势，图3：同比/环比曲线
    # 最新月度快照表
    # 新增月份 data_editor
    # 单分类编辑 form
    # Excel 导入预览 + 确认写入
```

Implementation requirements:

- 默认分类勾选 `合计 / 其中：股票基金 / 其中：混合基金 / 私募证券投资基金 / 私募资管权益基金`
- 维度切换控件应驱动总趋势图和结构趋势图
- 同比 / 环比图使用 `build_fund_monitor_change_trend_df`
- 表格使用 `to_fund_monitor_display_df`
- 所有表头用中文

- [ ] **Step 4: Run a compile check for `app.py`**

Run:

```bash
python -m py_compile app.py src/fund_monitor_importer.py src/fund_monitor_store.py
```

Expected: exit code `0`

- [ ] **Step 5: Commit the UI integration task**

```bash
git add app.py
git commit -m "feat: add macro fund monitor dashboard"
```

## Task 5: Verify the Full Feature End-to-End

**Files:**
- Modify if needed: `src/fund_monitor_importer.py`
- Modify if needed: `src/fund_monitor_store.py`
- Modify if needed: `app.py`

- [ ] **Step 1: Run the full targeted test suite**

Run:

```bash
python -m pytest tests/test_fund_monitor_importer.py tests/test_fund_monitor_store.py tests/test_navigation_config.py -v
```

Expected: PASS all tests

- [ ] **Step 2: Import the real workbook into PostgreSQL**

Run:

```bash
$env:ETF_PG_HOST='67.216.207.73'
$env:ETF_PG_PORT='5432'
$env:ETF_PG_DATABASE='postgres'
$env:ETF_PG_USER='postgres'
$env:ETF_PG_SSLMODE='disable'
$env:ETF_PG_PASSWORD='<from environment>'
@'
from pathlib import Path
from src.fund_monitor_importer import parse_fund_monitor_workbook
from src.fund_monitor_store import get_engine, build_fund_monitor_upsert_rows, upsert_fund_monitor_rows

path = next(Path(r"D:\sourcecode\etf").glob("*20260505*.xlsx"))
df = parse_fund_monitor_workbook(path)
rows = build_fund_monitor_upsert_rows(df.to_dict(orient="records"), source_type="import", source_file=path.name)
engine = get_engine()
print(upsert_fund_monitor_rows(engine, rows))
'@ | python -
```

Expected: print a positive row count

- [ ] **Step 3: Run the Streamlit app and verify the page renders**

Run:

```bash
python -m streamlit run app.py --server.port 8503 --server.headless true
```

Expected checks:

- `宏观 / 📈 基金监测` 可见
- 页面不报数据库或导入错误
- KPI 可见
- 三张图可见
- 快照表列名为中文

- [ ] **Step 4: Fix any issues uncovered by the real-data smoke test**

Apply minimal patches only to the failing area. Typical repairs:

```python
if filtered_df.empty:
    st.info("当前筛选范围内暂无基金监测数据。")
    return
```

and:

```python
value = edit_row.get(field)
initial_value = float(value) if value is not None and not pd.isna(value) else 0.0
```

- [ ] **Step 5: Re-run tests and compile after fixes**

Run:

```bash
python -m py_compile app.py src/fund_monitor_importer.py src/fund_monitor_store.py
python -m pytest tests/test_fund_monitor_importer.py tests/test_fund_monitor_store.py tests/test_navigation_config.py -v
```

Expected: PASS and exit code `0`

- [ ] **Step 6: Commit the verification fixes**

```bash
git add app.py src/fund_monitor_importer.py src/fund_monitor_store.py tests/test_fund_monitor_importer.py tests/test_fund_monitor_store.py tests/test_navigation_config.py
git commit -m "fix: polish fund monitor dashboard integration"
```

## Spec Coverage Check

- 导航位置：Task 3、Task 4 覆盖
- 解析多个月份区块：Task 1 覆盖
- 错误值与空值处理：Task 1 覆盖
- PostgreSQL 长表存储：Task 2 覆盖
- 总览型 KPI：Task 4 覆盖
- 三张主图：Task 4 覆盖
- 中文快照表：Task 2、Task 4 覆盖
- 手工维护与 Excel 导入：Task 4 覆盖
- 实际工作簿导入验证：Task 5 覆盖

## Placeholder Scan

- 没有 `TODO` / `TBD`
- 每个任务都有明确文件路径
- 每个测试步骤都包含明确命令
- 每个提交步骤都包含明确 `git commit` 命令

## Type Consistency Check

- 解析器统一输出 `month + category_name`
- store 层统一使用 `fund_count/share_amount/nav_amount/unit_nav`
- 页面层统一依赖 `category_name` 与上述字段名
- 同比/环比趋势统一走 `build_fund_monitor_change_trend_df`
