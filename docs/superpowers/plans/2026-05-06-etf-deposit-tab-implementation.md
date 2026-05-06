# ETF Deposit Tab Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a new `ETF / 🏦 本外币存款` dashboard page backed by PostgreSQL, with manual monthly entry, Excel import preview, and dashboard-first analysis views.

**Architecture:** Keep dashboard rendering inside `app.py` to match the current Streamlit app structure, but move deposit-specific persistence and Excel parsing into focused `src/` modules. Use a normalized PostgreSQL table with one row per month, compute MoM/YoY at read time, and treat Excel only as an import source.

**Tech Stack:** Streamlit, pandas, openpyxl, SQLAlchemy, psycopg2, unittest, pytest

---

## File Structure

### New files

- `src/etf_deposit_store.py`
  - PostgreSQL connection reuse
  - month normalization
  - table creation
  - monthly row upsert/load helpers
  - dashboard metric helpers
- `src/etf_deposit_importer.py`
  - Excel sheet parsing
  - label-to-field mapping
  - import preview classification
- `tests/test_etf_deposit_store.py`
  - unit tests for month normalization, dashboard metric logic, preview/update shaping
- `tests/test_etf_deposit_importer.py`
  - unit tests for workbook parsing and import preview classification

### Modified files

- `app.py`
  - new ETF subpage entry on mobile and desktop
  - new dashboard renderer
  - manual entry form
  - import preview/write flow
- `docs/superpowers/specs/2026-05-06-etf-deposit-tab-design.md`
  - no implementation edits expected during execution unless scope changes

---

### Task 1: Add deposit store foundation

**Files:**
- Create: `src/etf_deposit_store.py`
- Test: `tests/test_etf_deposit_store.py`

- [ ] **Step 1: Write the failing tests for month normalization and dashboard metrics**

```python
import unittest
import pandas as pd

from src.etf_deposit_store import (
    normalize_month,
    build_deposit_summary,
    build_balance_trend_df,
)


class EtfDepositStoreTests(unittest.TestCase):
    def test_normalize_month_accepts_common_formats(self):
        self.assertEqual(str(normalize_month("2026-03")), "2026-03-01")
        self.assertEqual(str(normalize_month("2026-03-01")), "2026-03-01")
        self.assertEqual(str(normalize_month("20260301")), "2026-03-01")

    def test_build_deposit_summary_computes_mom_and_yoy(self):
        df = pd.DataFrame(
            [
                {"month": "2025-03-01", "total_deposit_balance": 327.12},
                {"month": "2026-02-01", "total_deposit_balance": 345.72},
                {"month": "2026-03-01", "total_deposit_balance": 350.23},
            ]
        )
        summary = build_deposit_summary(df)
        self.assertEqual(summary["latest_month"], "2026-03")
        self.assertAlmostEqual(summary["latest_value"], 350.23)
        self.assertAlmostEqual(summary["mom_delta"], 4.51, places=2)
        self.assertAlmostEqual(summary["yoy_delta"], 23.11, places=2)

    def test_build_balance_trend_df_sorts_months_and_keeps_three_series(self):
        df = pd.DataFrame(
            [
                {
                    "month": "2026-02-01",
                    "rmb_deposit_balance": 337.94,
                    "fx_deposit_balance": 1.12,
                    "total_deposit_balance": 345.72,
                },
                {
                    "month": "2026-03-01",
                    "rmb_deposit_balance": 342.41,
                    "fx_deposit_balance": 1.13,
                    "total_deposit_balance": 350.23,
                },
            ]
        )
        trend_df = build_balance_trend_df(df)
        self.assertEqual(
            trend_df["metric"].tolist(),
            ["人民币存款余额", "外币存款余额", "本外币存款余额"] * 2,
        )
        self.assertEqual(trend_df.iloc[0]["month"].strftime("%Y-%m-%d"), "2026-02-01")
```

- [ ] **Step 2: Run the test file to verify it fails**

Run: `pytest tests/test_etf_deposit_store.py -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'src.etf_deposit_store'`

- [ ] **Step 3: Write the minimal store module**

```python
from __future__ import annotations

from datetime import date, datetime
from typing import Iterable, Optional
import os

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

TABLE_NAME = "macro_fx_rmb_deposits_monthly"
BALANCE_LABELS = {
    "rmb_deposit_balance": "人民币存款余额",
    "fx_deposit_balance": "外币存款余额",
    "total_deposit_balance": "本外币存款余额",
}


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
        try:
            import streamlit as st

            password = (
                st.secrets.get("ETF_PG_PASSWORD")
                or st.secrets.get("PGPASSWORD")
                or st.secrets.get("database", {}).get("password")
            )
            if password and not os.getenv("ETF_PG_PASSWORD"):
                os.environ["ETF_PG_PASSWORD"] = str(password)
        except Exception:
            pass

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


def normalize_month(value) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.replace(day=1)
    if isinstance(value, datetime):
        return value.date().replace(day=1)

    raw = str(value).strip()
    if not raw:
        raise ValueError("month 不能为空")
    candidates = [raw, raw + "-01" if len(raw) == 7 else raw, raw.replace("-", "")]
    for candidate in candidates:
        try:
            if len(candidate) == 10 and candidate.count("-") == 2:
                return datetime.strptime(candidate, "%Y-%m-%d").date().replace(day=1)
            if len(candidate) == 8 and candidate.isdigit():
                return datetime.strptime(candidate, "%Y%m%d").date().replace(day=1)
        except ValueError:
            continue
    raise ValueError(f"无法解析 month: {value}")


def build_deposit_summary(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {"latest_month": None, "latest_value": None, "mom_delta": None, "yoy_delta": None}

    data = df.copy()
    data["month"] = pd.to_datetime(data["month"])
    data = data.sort_values("month")
    latest = data.iloc[-1]
    prev_month = data.iloc[-2] if len(data) >= 2 else None
    yoy_mask = data["month"] == latest["month"] - pd.DateOffset(years=1)
    yoy_row = data.loc[yoy_mask].iloc[-1] if yoy_mask.any() else None

    latest_value = float(latest["total_deposit_balance"])
    mom_delta = latest_value - float(prev_month["total_deposit_balance"]) if prev_month is not None else None
    yoy_delta = latest_value - float(yoy_row["total_deposit_balance"]) if yoy_row is not None else None
    return {
        "latest_month": latest["month"].strftime("%Y-%m"),
        "latest_value": latest_value,
        "mom_delta": mom_delta,
        "yoy_delta": yoy_delta,
    }


def build_balance_trend_df(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    data["month"] = pd.to_datetime(data["month"])
    melted = data.melt(
        id_vars=["month"],
        value_vars=list(BALANCE_LABELS.keys()),
        var_name="metric_key",
        value_name="value",
    )
    melted["metric"] = melted["metric_key"].map(BALANCE_LABELS)
    return melted.sort_values(["month", "metric_key"]).reset_index(drop=True)
```

- [ ] **Step 4: Run the store tests to verify they pass**

Run: `pytest tests/test_etf_deposit_store.py -v`

Expected: PASS for all three tests in `EtfDepositStoreTests`

- [ ] **Step 5: Commit**

```bash
git add tests/test_etf_deposit_store.py src/etf_deposit_store.py
git commit -m "feat: add ETF deposit store helpers"
```

### Task 2: Add PostgreSQL table and upsert/load behavior

**Files:**
- Modify: `src/etf_deposit_store.py`
- Modify: `tests/test_etf_deposit_store.py`

- [ ] **Step 1: Write the failing tests for row shaping and import preview classification**

```python
    def test_build_upsert_rows_normalizes_month_and_source_fields(self):
        rows = build_upsert_rows(
            [
                {
                    "month": "2026-03",
                    "rmb_deposit_balance": 342.41,
                    "fx_deposit_balance": 1.13,
                    "total_deposit_balance": 350.23,
                    "household_deposit_increase": 7.68,
                    "corp_deposit_increase": 2.68,
                    "fiscal_deposit_increase": 0.4606,
                    "nonbank_deposit_increase": 2.03,
                    "total_deposit_increase": 13.73,
                    "household_long_loan_increase": 0.4607,
                }
            ],
            source_type="manual",
            source_file=None,
        )
        self.assertEqual(rows[0]["month"].isoformat(), "2026-03-01")
        self.assertEqual(rows[0]["source_type"], "manual")
        self.assertIsNone(rows[0]["source_file"])

    def test_classify_import_rows_splits_insert_and_overwrite(self):
        incoming = pd.DataFrame(
            [
                {"month": "2026-03-01", "total_deposit_balance": 350.23},
                {"month": "2026-04-01", "total_deposit_balance": 352.10},
            ]
        )
        existing = pd.DataFrame([{"month": "2026-03-01", "total_deposit_balance": 349.50}])
        preview = classify_import_rows(incoming, existing)
        self.assertEqual(preview["to_insert"]["month"].tolist(), ["2026-04-01"])
        self.assertEqual(preview["to_overwrite"]["month"].tolist(), ["2026-03-01"])
```

- [ ] **Step 2: Run the test file to verify it fails**

Run: `pytest tests/test_etf_deposit_store.py -v`

Expected: FAIL with `NameError` or `ImportError` for `build_upsert_rows` and `classify_import_rows`

- [ ] **Step 3: Extend the store module with schema and write helpers**

```python
NUMERIC_FIELDS = [
    "rmb_deposit_balance",
    "fx_deposit_balance",
    "total_deposit_balance",
    "household_deposit_increase",
    "corp_deposit_increase",
    "fiscal_deposit_increase",
    "nonbank_deposit_increase",
    "total_deposit_increase",
    "household_long_loan_increase",
]


def ensure_deposit_table(engine: Engine) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        month DATE PRIMARY KEY,
        rmb_deposit_balance NUMERIC(18, 4) NOT NULL,
        fx_deposit_balance NUMERIC(18, 4) NOT NULL,
        total_deposit_balance NUMERIC(18, 4) NOT NULL,
        household_deposit_increase NUMERIC(18, 4) NOT NULL,
        corp_deposit_increase NUMERIC(18, 4) NOT NULL,
        fiscal_deposit_increase NUMERIC(18, 4) NOT NULL,
        nonbank_deposit_increase NUMERIC(18, 4) NOT NULL,
        total_deposit_increase NUMERIC(18, 4) NOT NULL,
        household_long_loan_increase NUMERIC(18, 4) NOT NULL,
        source_type VARCHAR(16) NOT NULL,
        source_file TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_month_desc ON {TABLE_NAME}(month DESC);
    """
    with engine.begin() as conn:
        for statement in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(statement))


def build_upsert_rows(rows: Iterable[dict], source_type: str, source_file: Optional[str]) -> list[dict]:
    payload_rows = []
    for row in rows:
        payload = {
            "month": normalize_month(row["month"]),
            "source_type": source_type,
            "source_file": source_file,
        }
        for field in NUMERIC_FIELDS:
            payload[field] = float(row[field])
        payload_rows.append(payload)
    return payload_rows


def classify_import_rows(incoming_df: pd.DataFrame, existing_df: pd.DataFrame) -> dict:
    incoming = incoming_df.copy()
    existing = existing_df.copy()
    incoming["month"] = pd.to_datetime(incoming["month"]).dt.strftime("%Y-%m-%d")
    existing_months = set(pd.to_datetime(existing["month"]).dt.strftime("%Y-%m-%d")) if not existing.empty else set()
    to_insert = incoming[~incoming["month"].isin(existing_months)].reset_index(drop=True)
    to_overwrite = incoming[incoming["month"].isin(existing_months)].reset_index(drop=True)
    return {"to_insert": to_insert, "to_overwrite": to_overwrite}


def upsert_deposit_rows(engine: Engine, rows: list[dict]) -> int:
    ensure_deposit_table(engine)
    insert_sql = text(
        f"""
        INSERT INTO {TABLE_NAME} (
            month, rmb_deposit_balance, fx_deposit_balance, total_deposit_balance,
            household_deposit_increase, corp_deposit_increase, fiscal_deposit_increase,
            nonbank_deposit_increase, total_deposit_increase, household_long_loan_increase,
            source_type, source_file
        ) VALUES (
            :month, :rmb_deposit_balance, :fx_deposit_balance, :total_deposit_balance,
            :household_deposit_increase, :corp_deposit_increase, :fiscal_deposit_increase,
            :nonbank_deposit_increase, :total_deposit_increase, :household_long_loan_increase,
            :source_type, :source_file
        )
        ON CONFLICT (month) DO UPDATE SET
            rmb_deposit_balance = EXCLUDED.rmb_deposit_balance,
            fx_deposit_balance = EXCLUDED.fx_deposit_balance,
            total_deposit_balance = EXCLUDED.total_deposit_balance,
            household_deposit_increase = EXCLUDED.household_deposit_increase,
            corp_deposit_increase = EXCLUDED.corp_deposit_increase,
            fiscal_deposit_increase = EXCLUDED.fiscal_deposit_increase,
            nonbank_deposit_increase = EXCLUDED.nonbank_deposit_increase,
            total_deposit_increase = EXCLUDED.total_deposit_increase,
            household_long_loan_increase = EXCLUDED.household_long_loan_increase,
            source_type = EXCLUDED.source_type,
            source_file = EXCLUDED.source_file,
            updated_at = NOW();
        """
    )
    with engine.begin() as conn:
        conn.execute(insert_sql, rows)
    return len(rows)


def load_deposit_monthly_df(engine: Optional[Engine] = None) -> pd.DataFrame:
    engine = engine or get_engine()
    ensure_deposit_table(engine)
    sql = text(f"SELECT * FROM {TABLE_NAME} ORDER BY month ASC")
    with engine.begin() as conn:
        return pd.read_sql(sql, conn)
```

- [ ] **Step 4: Run the store tests again**

Run: `pytest tests/test_etf_deposit_store.py -v`

Expected: PASS for all tests in `tests/test_etf_deposit_store.py`

- [ ] **Step 5: Commit**

```bash
git add tests/test_etf_deposit_store.py src/etf_deposit_store.py
git commit -m "feat: add ETF deposit table persistence"
```

### Task 3: Add Excel importer and preview logic

**Files:**
- Create: `src/etf_deposit_importer.py`
- Create: `tests/test_etf_deposit_importer.py`

- [ ] **Step 1: Write the failing importer tests**

```python
import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook

from src.etf_deposit_importer import parse_deposit_workbook


class EtfDepositImporterTests(unittest.TestCase):
    def test_parse_deposit_workbook_reads_known_month_block(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "deposit.xlsx"
            wb = Workbook()
            ws = wb.active
            ws.title = "本外币存款数据"
            ws["B4"] = "2026-03-01"
            ws["C4"] = "人民币存款余额"
            ws["D4"] = 342.41
            ws["C5"] = "外币存款余额"
            ws["D5"] = 1.13
            ws["C6"] = "本外币存款余额"
            ws["D6"] = 350.23
            ws["C7"] = "住户存款增加额"
            ws["D7"] = 7.68
            ws["C8"] = "非金融企业存款增加额"
            ws["D8"] = 2.68
            ws["C9"] = "财政性存款增加额"
            ws["D9"] = 0.4606
            ws["C10"] = "非银行业金融机构存款增加额"
            ws["D10"] = 2.03
            ws["C11"] = "存款合计增加额"
            ws["D11"] = 13.73
            ws["C12"] = "居民长期贷款增加额"
            ws["D12"] = 0.4607
            wb.save(path)

            df = parse_deposit_workbook(path)

        self.assertEqual(df.iloc[0]["month"], "2026-03-01")
        self.assertAlmostEqual(df.iloc[0]["total_deposit_balance"], 350.23)

    def test_parse_deposit_workbook_raises_when_sheet_missing(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "wrong.xlsx"
            wb = Workbook()
            wb.active.title = "股票指数"
            wb.save(path)
            with self.assertRaisesRegex(ValueError, "本外币存款数据"):
                parse_deposit_workbook(path)
```

- [ ] **Step 2: Run the importer tests to verify they fail**

Run: `pytest tests/test_etf_deposit_importer.py -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'src.etf_deposit_importer'`

- [ ] **Step 3: Write the importer module**

```python
from __future__ import annotations

from pathlib import Path
import pandas as pd
from openpyxl import load_workbook

LABEL_TO_FIELD = {
    "人民币存款余额": "rmb_deposit_balance",
    "外币存款余额": "fx_deposit_balance",
    "本外币存款余额": "total_deposit_balance",
    "住户存款增加额": "household_deposit_increase",
    "非金融企业存款增加额": "corp_deposit_increase",
    "财政性存款增加额": "fiscal_deposit_increase",
    "非银行业金融机构存款增加额": "nonbank_deposit_increase",
    "存款合计增加额": "total_deposit_increase",
    "居民长期贷款增加额": "household_long_loan_increase",
}


def parse_deposit_workbook(path) -> pd.DataFrame:
    workbook_path = Path(path)
    wb = load_workbook(workbook_path, data_only=True, read_only=True)
    if "本外币存款数据" not in wb.sheetnames:
        raise ValueError("导入文件缺少 本外币存款数据 sheet")

    ws = wb["本外币存款数据"]
    rows = []
    current = None
    for row_idx in range(1, ws.max_row + 1):
        month_value = ws.cell(row_idx, 2).value
        label = ws.cell(row_idx, 3).value
        number = ws.cell(row_idx, 4).value

        if month_value is not None:
            if current and len(current) > 1:
                rows.append(current)
            current = {"month": pd.to_datetime(month_value).strftime("%Y-%m-%d")}

        if current is not None and label in LABEL_TO_FIELD:
            current[LABEL_TO_FIELD[label]] = float(number) if number is not None else None

    if current and len(current) > 1:
        rows.append(current)

    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError("未解析出任何月份数据")
    return df.sort_values("month").reset_index(drop=True)
```

- [ ] **Step 4: Run the importer tests again**

Run: `pytest tests/test_etf_deposit_importer.py -v`

Expected: PASS for both importer tests

- [ ] **Step 5: Commit**

```bash
git add tests/test_etf_deposit_importer.py src/etf_deposit_importer.py
git commit -m "feat: add ETF deposit workbook importer"
```

### Task 4: Integrate the new ETF page into app.py

**Files:**
- Modify: `app.py`
- Modify: `src/etf_deposit_store.py`
- Modify: `src/etf_deposit_importer.py`

- [ ] **Step 1: Add the failing UI smoke tests as explicit manual checks**

```text
Manual check A:
1. Start the app.
2. Open ETF sidebar/module.
3. Confirm a new page label `🏦 本外币存款` appears on both desktop and mobile navigation paths.

Manual check B:
1. Open the new page with an empty table.
2. Confirm empty state shows `新增月份` and `批量导入`.

Manual check C:
1. Insert one month manually.
2. Confirm summary cards, trend chart, and detail table update without refresh errors.
```

- [ ] **Step 2: Run the app before code changes to verify the new page is absent**

Run: `streamlit run app.py`

Expected: current ETF navigation does not contain `🏦 本外币存款`

- [ ] **Step 3: Add the page navigation, dashboard renderer, form, and import flow**

```python
from src.etf_deposit_store import (
    build_balance_trend_df,
    build_deposit_summary,
    build_upsert_rows,
    classify_import_rows,
    get_engine,
    load_deposit_monthly_df,
    upsert_deposit_rows,
)
from src.etf_deposit_importer import parse_deposit_workbook


def render_etf_deposit_tab():
    st.subheader("🏦 本外币存款")
    engine = get_engine()
    df = load_deposit_monthly_df(engine)
    for state_key in (
        "deposit_manual_open",
        "deposit_import_open",
        "deposit_edit_month",
        "deposit_history_limit",
    ):
        st.session_state.setdefault(state_key, False if state_key != "deposit_history_limit" else "最近12个月")

    action_col, status_col = st.columns([1, 3])
    with action_col:
        if st.button("新增月份", key="deposit_add_month"):
            st.session_state["deposit_manual_open"] = True
            st.session_state["deposit_edit_month"] = None
        if st.button("批量导入", key="deposit_import_file"):
            st.session_state["deposit_import_open"] = True
    with status_col:
        if df.empty:
            st.caption("最新数据月份：- | 数据来源：- | 最近更新时间：-")
        else:
            latest_row = df.sort_values("month").iloc[-1]
            updated_at = pd.to_datetime(latest_row["updated_at"]).strftime("%Y-%m-%d %H:%M") if pd.notna(latest_row["updated_at"]) else "-"
            st.caption(
                f"最新数据月份：{pd.to_datetime(latest_row['month']).strftime('%Y-%m')} | "
                f"数据来源：{latest_row['source_type']} | 最近更新时间：{updated_at}"
            )

    if df.empty:
        st.info("暂无本外币存款数据，请先新增月份或批量导入。")
    else:
        summary = build_deposit_summary(df)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("最新月份", summary["latest_month"] or "-")
        c2.metric("本外币存款余额", f'{summary["latest_value"]:.2f}' if summary["latest_value"] is not None else "-")
        c3.metric("环比变动", f'{summary["mom_delta"]:.2f}' if summary["mom_delta"] is not None else "-")
        c4.metric("同比变动", f'{summary["yoy_delta"]:.2f}' if summary["yoy_delta"] is not None else "-")

        trend_df = build_balance_trend_df(df)
        window = st.radio(
            "时间范围",
            ["最近12个月", "最近24个月", "全部"],
            horizontal=True,
            key="deposit_history_limit",
        )
        if window == "最近12个月":
            trend_df = trend_df[trend_df["month"] >= trend_df["month"].max() - pd.DateOffset(months=11)]
        elif window == "最近24个月":
            trend_df = trend_df[trend_df["month"] >= trend_df["month"].max() - pd.DateOffset(months=23)]
        st.plotly_chart(
            px.line(trend_df, x="month", y="value", color="metric", markers=True),
            use_container_width=True,
        )

        increment_cols = [
            "household_deposit_increase",
            "corp_deposit_increase",
            "fiscal_deposit_increase",
            "nonbank_deposit_increase",
            "total_deposit_increase",
        ]
        increment_df = (
            df.copy()
            .assign(month=pd.to_datetime(df["month"]))
            .melt(id_vars=["month"], value_vars=increment_cols, var_name="metric", value_name="value")
        )
        st.plotly_chart(
            px.bar(increment_df, x="month", y="value", color="metric", barmode="group"),
            use_container_width=True,
        )

        history_choice = st.radio("明细范围", ["最近12个月", "全部历史"], horizontal=True, key="deposit_detail_window")
        detail_df = df.sort_values("month", ascending=False).copy()
        if history_choice == "最近12个月":
            detail_df = detail_df.head(12)

        edit_options = detail_df["month"].astype(str).tolist()
        edit_month = st.selectbox("编辑已有月份", [""] + edit_options, index=0, key="deposit_edit_select")
        if edit_month and st.button("编辑选中月份", key="deposit_edit_button"):
            st.session_state["deposit_manual_open"] = True
            st.session_state["deposit_edit_month"] = edit_month

        st.dataframe(detail_df, use_container_width=True, hide_index=True)

    if st.session_state["deposit_manual_open"]:
        edit_row = None
        if st.session_state["deposit_edit_month"] and not df.empty:
            month_mask = pd.to_datetime(df["month"]).dt.strftime("%Y-%m-%d") == st.session_state["deposit_edit_month"]
            if month_mask.any():
                edit_row = df.loc[month_mask].iloc[-1].to_dict()
        with st.form("deposit_manual_form", clear_on_submit=False):
            month = st.text_input("月份", value=(pd.to_datetime(edit_row["month"]).strftime("%Y-%m") if edit_row else ""))
            rmb = st.number_input("人民币存款余额", value=float(edit_row["rmb_deposit_balance"]) if edit_row else 0.0, format="%.4f")
            fx = st.number_input("外币存款余额", value=float(edit_row["fx_deposit_balance"]) if edit_row else 0.0, format="%.4f")
            total = st.number_input("本外币存款余额", value=float(edit_row["total_deposit_balance"]) if edit_row else 0.0, format="%.4f")
            household = st.number_input("住户存款增加额", value=float(edit_row["household_deposit_increase"]) if edit_row else 0.0, format="%.4f")
            corp = st.number_input("非金融企业存款增加额", value=float(edit_row["corp_deposit_increase"]) if edit_row else 0.0, format="%.4f")
            fiscal = st.number_input("财政性存款增加额", value=float(edit_row["fiscal_deposit_increase"]) if edit_row else 0.0, format="%.4f")
            nonbank = st.number_input("非银行业金融机构存款增加额", value=float(edit_row["nonbank_deposit_increase"]) if edit_row else 0.0, format="%.4f")
            total_increase = st.number_input("存款合计增加额", value=float(edit_row["total_deposit_increase"]) if edit_row else 0.0, format="%.4f")
            household_loan = st.number_input("居民长期贷款增加额", value=float(edit_row["household_long_loan_increase"]) if edit_row else 0.0, format="%.4f")
            submitted = st.form_submit_button("保存本月数据")
            canceled = st.form_submit_button("取消")

        if canceled:
            st.session_state["deposit_manual_open"] = False
            st.session_state["deposit_edit_month"] = None
            st.rerun()
        if submitted:
            rows = build_upsert_rows(
                [
                    {
                        "month": month,
                        "rmb_deposit_balance": rmb,
                        "fx_deposit_balance": fx,
                        "total_deposit_balance": total,
                        "household_deposit_increase": household,
                        "corp_deposit_increase": corp,
                        "fiscal_deposit_increase": fiscal,
                        "nonbank_deposit_increase": nonbank,
                        "total_deposit_increase": total_increase,
                        "household_long_loan_increase": household_loan,
                    }
                ],
                source_type="manual",
                source_file=None,
            )
            upsert_deposit_rows(engine, rows)
            st.session_state["deposit_manual_open"] = False
            st.session_state["deposit_edit_month"] = None
            st.success("保存成功")
            st.rerun()

    if st.session_state["deposit_import_open"]:
        upload = st.file_uploader("上传本外币存款 Excel", type=["xlsx"], key="deposit_uploader")
        if upload is not None:
            imported_df = parse_deposit_workbook(upload)
            preview = classify_import_rows(imported_df, df)
            overwrite_mode = st.radio("重复月份处理", ["跳过已存在月份", "覆盖已存在月份"], horizontal=True, key="deposit_overwrite_mode")
            st.write("新增月份", preview["to_insert"])
            st.write("覆盖月份", preview["to_overwrite"])
            if st.button("确认写入", key="deposit_confirm_import"):
                write_df = imported_df.copy()
                if overwrite_mode == "跳过已存在月份":
                    write_df = preview["to_insert"].copy()
                rows = build_upsert_rows(
                    write_df.to_dict(orient="records"),
                    source_type="import",
                    source_file=upload.name,
                )
                upsert_deposit_rows(engine, rows)
                st.session_state["deposit_import_open"] = False
                st.success(f"已写入 {len(rows)} 个月份")
                st.rerun()
```

Also update both ETF navigation lists:

```python
["📈 ETF份额变动", "📊 每日成交量", "🥧 ETF分类占比", "📈 ETF分类趋势", "📊 宽基指数ETF", "🏦 本外币存款"]
```

and route the new label on both mobile and desktop selectors to:

```python
elif mobile_page == "🏦 本外币存款":
    render_etf_deposit_tab()

elif etf_subpage == "🏦 本外币存款":
    render_etf_deposit_tab()
```

- [ ] **Step 4: Run targeted tests and a local Streamlit smoke check**

Run:

```bash
pytest tests/test_etf_deposit_store.py tests/test_etf_deposit_importer.py -v
streamlit run app.py
```

Expected:

- both test files PASS
- ETF navigation includes `🏦 本外币存款`
- empty state works without tracebacks
- freshness strip renders latest month, source, and update time
- manual insert, edit-existing-month flow, and import preview render without layout breakage
- overwrite mode switches between insert-only and upsert behavior

- [ ] **Step 5: Commit**

```bash
git add app.py src/etf_deposit_store.py src/etf_deposit_importer.py
git commit -m "feat: add ETF deposit dashboard"
```

### Task 5: Run database-backed acceptance checks

**Files:**
- Modify: none if everything passes
- Verify: PostgreSQL table `macro_fx_rmb_deposits_monthly`

- [ ] **Step 1: Prepare runtime configuration**

Run:

```powershell
$env:ETF_PG_HOST="67.216.207.73"
$env:ETF_PG_PORT="5432"
$env:ETF_PG_DATABASE="postgres"
$env:ETF_PG_USER="postgres"
$env:ETF_PG_SSLMODE="disable"
```

Expected: environment is ready without writing duplicate config into source files

- [ ] **Step 2: Run the targeted test suite with runtime PG settings available**

Run: `pytest tests/test_etf_deposit_store.py tests/test_etf_deposit_importer.py -v`

Expected: PASS

- [ ] **Step 3: Launch the app and verify three end-to-end scenarios**

```text
Scenario 1: Empty state
- Temporarily point to an empty table or empty environment
- Open ETF / 🏦 本外币存款
- Confirm CTA buttons render and no stack trace appears

Scenario 2: Manual month save
- Enter 2026-03 and the sample metric values from the reference workbook
- Click 保存本月数据
- Confirm cards show 2026-03 and 本外币存款余额 350.23

Scenario 3: Excel import preview
- Upload 新增证券分析表单-20260505.xlsx
- Confirm preview separates insert and overwrite months
- Confirm write succeeds and table row count increases or updates as expected
```

- [ ] **Step 4: Inspect the database table**

Run:

```sql
SELECT month, total_deposit_balance, source_type, source_file
FROM macro_fx_rmb_deposits_monthly
ORDER BY month DESC
LIMIT 12;
```

Expected: newest months appear in descending order with correct source metadata

- [ ] **Step 5: Commit**

```bash
git add app.py src/etf_deposit_store.py src/etf_deposit_importer.py tests/test_etf_deposit_store.py tests/test_etf_deposit_importer.py
git commit -m "test: verify ETF deposit dashboard flow"
```

---

## Spec Coverage Check

- New ETF subpage location: covered in Task 4 navigation changes
- Dashboard-first layout: covered in Task 4 renderer
- PostgreSQL storage: covered in Tasks 1 and 2
- Manual monthly entry: covered in Task 4 form flow
- Bulk import with preview: covered in Tasks 3 and 4
- MoM/YoY computed at render time: covered in Task 1 summary helper
- Empty and edge states: covered in Task 4 and Task 5 manual acceptance
- Reuse existing PG config convention: covered in Task 1 and Task 5

## Placeholder Scan

- No `TODO`, `TBD`, or deferred implementation markers remain
- Each code-changing step includes concrete code
- Each verification step includes exact commands and expected outcomes

## Type Consistency Check

- `month` is consistently normalized to first-of-month date values
- Importer outputs the same field names the store module expects
- App form payload uses the same field names the upsert helper consumes
- Dashboard summary reads `total_deposit_balance` consistently for KPI, MoM, and YoY
