# Fund Watchlist Page Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone “⭐ 自选基金” page under the Fund module, using the existing dark cyber watchlist design language for fund holding summaries, focus details, sorting, view switching, and batch deletion.

**Architecture:** Add the page to the shared navigation model, extend the existing fund search query to expose the already-stored scale field, and create a pure `src/fund_watchlist_dashboard.py` presentation-model module. Keep Streamlit state and rendering in `app.py`, but feed every summary, card, table, and focus panel from one session-cached list of normalized fund models so focus and view changes do not requery the database.

**Tech Stack:** Python 3, Streamlit, pandas, SQLAlchemy, pytest/unittest, existing `src.fund_hot_stocks` queries and `src.user_watchlist_store`.

---

## File map

- Modify `src/sidebar_navigation.py`: declare the new Fund-module page.
- Modify `src/navigation_config.py`: expose `ETF_FUND_WATCHLIST_PAGE_LABEL`.
- Modify `src/fund_hot_stocks.py`: include `issue_amount` in fund search results.
- Create `src/fund_watchlist_dashboard.py`: normalize one fund, aggregate the portfolio summary, sort models, and produce table rows.
- Modify `app.py`: add scoped fund-watchlist CSS, session cache, standalone renderer, focus panel, view switching, and batch management; route desktop/mobile navigation; remove the embedded board call.
- Modify `tests/test_navigation_config.py`: lock the public Fund page list.
- Modify `tests/test_sidebar_navigation.py`: lock Fund page metadata and searchability.
- Modify `tests/fund_hot_stocks_sync_test.py`: verify scale is selected by the existing search query.
- Create `tests/test_fund_watchlist_dashboard.py`: unit-test the presentation model and summary/sort helpers.
- Rewrite `tests/test_fund_watchlist_ui.py`: verify standalone route, Chinese page contract, scoped CSS/controls, and removal from the hot-stocks page.

### Task 1: Add the standalone Fund navigation entry

**Files:**
- Modify: `src/sidebar_navigation.py:38-75`
- Modify: `src/navigation_config.py:28-33`
- Modify: `tests/test_navigation_config.py`
- Modify: `tests/test_sidebar_navigation.py`

- [ ] **Step 1: Write failing navigation tests**

Add the constant import and update the stable Fund option assertion in `tests/test_navigation_config.py`:

```python
from src.navigation_config import (
    ETF_FUND_WATCHLIST_PAGE_LABEL,
    ETF_PAGE_OPTIONS,
    STOCK_PAGE_OPTIONS,
)


def test_fund_page_options_include_standalone_watchlist():
    assert ETF_FUND_WATCHLIST_PAGE_LABEL == "⭐ 自选基金"
    assert "⭐ 自选基金" in ETF_PAGE_OPTIONS
```

Change the expected Fund list in `test_navigation_option_labels_remain_stable` to:

```python
self.assertEqual(
    ETF_PAGE_OPTIONS,
    [
        "📈 主要宽基ETF份额",
        "🥧 ETF分类占比",
        "📈 ETF分类趋势",
        "📊 宽基指数ETF",
        "📈 基金监测",
        "⭐ 自选基金",
    ],
)
```

Add to `tests/test_sidebar_navigation.py`:

```python
def test_fund_watchlist_page_belongs_to_fund_module_and_is_searchable(self):
    page = get_page_by_id("fund_watchlist")

    self.assertEqual(page.label, "⭐ 自选基金")
    self.assertEqual(page.description, "管理个人自选基金与持仓结构")
    self.assertEqual(page.toolbar_variant, "standard")
    self.assertEqual(get_module_id_for_page_id("fund_watchlist"), "fund")
    self.assertIn("⭐ 自选基金", get_page_labels("基金"))

    results = search_sidebar_pages("自选基金")
    self.assertGreater(len(results), 0)
    self.assertEqual(results[0].page_id, "fund_watchlist")
    self.assertEqual(results[0].module_id, "fund")
```

- [ ] **Step 2: Run the navigation tests and confirm failure**

Run:

```powershell
pytest tests/test_navigation_config.py tests/test_sidebar_navigation.py -q
```

Expected: failures because `ETF_FUND_WATCHLIST_PAGE_LABEL` and page ID `fund_watchlist` do not exist.

- [ ] **Step 3: Add the page metadata**

Append this page after `fund_monitor` inside the Fund module in `src/sidebar_navigation.py`:

```python
SidebarPage(
    "fund_watchlist",
    "⭐ 自选基金",
    "管理个人自选基金与持仓结构",
    "standard",
),
```

Add this constant after `ETF_FUND_MONITOR_PAGE_LABEL` in `src/navigation_config.py`:

```python
ETF_FUND_WATCHLIST_PAGE_LABEL = _page_label("基金", "fund_watchlist")
```

- [ ] **Step 4: Run the navigation tests and confirm success**

Run:

```powershell
pytest tests/test_navigation_config.py tests/test_sidebar_navigation.py -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit navigation**

```powershell
git add src/sidebar_navigation.py src/navigation_config.py tests/test_navigation_config.py tests/test_sidebar_navigation.py
git commit -m "feat: add standalone fund watchlist navigation"
```

### Task 2: Expose the existing fund scale field

**Files:**
- Modify: `src/fund_hot_stocks.py:1484-1528`
- Modify: `tests/fund_hot_stocks_sync_test.py:203-245`

- [ ] **Step 1: Add a failing SQL contract assertion**

Extend `test_search_funds_keeps_portfolio_only_code_searchable`:

```python
self.assertIn("issue_amount", sql_text)
```

Add `"issue_amount": None` to `portfolio_only_df` so the fixture matches the public result shape:

```python
{
    "fund_code": "159915.SZ",
    "name": "159915.SZ",
    "management": "持仓表补全",
    "fund_type": "场内基金/ETF",
    "invest_type": None,
    "status": None,
    "issue_amount": None,
    "latest_end_date": pd.Timestamp("2026-06-30"),
    "source_priority": 2,
    "holding_priority": 1,
    "match_rank": 0,
    "name_pos": 999999,
    "name_len": 9,
}
```

- [ ] **Step 2: Run the targeted test and confirm failure**

Run:

```powershell
pytest tests/fund_hot_stocks_sync_test.py::FundPortfolioByFundSyncTests::test_search_funds_keeps_portfolio_only_code_searchable -q
```

Expected: failure because the SQL text does not contain `issue_amount`.

- [ ] **Step 3: Select scale in both branches of `search_funds`**

In the `basic` CTE, add the stored field after `status`:

```sql
status,
issue_amount,
1 AS source_priority,
```

In `portfolio_only`, add a compatible null after `status`:

```sql
NULL::text AS status,
NULL::numeric AS issue_amount,
2 AS source_priority,
```

In the final `SELECT`, add:

```sql
issue_amount,
latest_end_date,
```

- [ ] **Step 4: Run fund query tests**

Run:

```powershell
pytest tests/fund_hot_stocks_sync_test.py -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit the query extension**

```powershell
git add src/fund_hot_stocks.py tests/fund_hot_stocks_sync_test.py
git commit -m "feat: expose fund scale in fund search"
```

### Task 3: Build a pure fund-watchlist presentation model

**Files:**
- Create: `src/fund_watchlist_dashboard.py`
- Create: `tests/test_fund_watchlist_dashboard.py`

- [ ] **Step 1: Write failing model tests**

Create `tests/test_fund_watchlist_dashboard.py`:

```python
import pandas as pd

from src.fund_watchlist_dashboard import (
    build_fund_watchlist_item,
    build_fund_watchlist_summary,
    build_fund_watchlist_table,
    sort_fund_watchlist_items,
)


def _watchlist_row():
    return pd.Series(
        {
            "ts_code": "001938.OF",
            "security_name": "中欧时代先锋",
            "created_at": "2026-06-20",
        }
    )


def _meta_df():
    return pd.DataFrame(
        [
            {
                "fund_code": "001938.OF",
                "name": "中欧时代先锋",
                "management": "中欧基金",
                "fund_type": "混合型",
                "issue_amount": 128.4,
                "latest_end_date": "2026-03-31",
            }
        ]
    )


def _holding_df():
    return pd.DataFrame(
        [
            {
                "end_date": "2026-03-31",
                "stock_name": "宁德时代",
                "symbol": "300750.SZ",
                "mkv": 1_820_000_000,
                "stk_mkv_ratio": 7.9,
                "holding_change_flag": "increase",
                "management": "中欧基金",
                "fund_type": "混合型",
            },
            {
                "end_date": "2026-03-31",
                "stock_name": "立讯精密",
                "symbol": "002475.SZ",
                "mkv": 1_450_000_000,
                "stk_mkv_ratio": 6.3,
                "holding_change_flag": "new",
                "management": "中欧基金",
                "fund_type": "混合型",
            },
            {
                "end_date": "2026-03-31",
                "stock_name": "美的集团",
                "symbol": "000333.SZ",
                "mkv": 1_080_000_000,
                "stk_mkv_ratio": 4.7,
                "holding_change_flag": "decrease",
                "management": "中欧基金",
                "fund_type": "混合型",
            },
        ]
    )


def test_build_item_normalizes_existing_fund_and_holding_data():
    item = build_fund_watchlist_item(_watchlist_row(), _meta_df(), _holding_df())

    assert item["fund_code"] == "001938.OF"
    assert item["fund_name"] == "中欧时代先锋"
    assert item["management"] == "中欧基金"
    assert item["fund_type"] == "混合型"
    assert item["issue_amount"] == 128.4
    assert item["holding_market_value"] == 43.5
    assert item["top10_ratio"] == 18.9
    assert item["holding_count"] == 3
    assert item["new_count"] == 1
    assert item["increase_count"] == 1
    assert item["decrease_count"] == 1
    assert item["latest_end_date"] == pd.Timestamp("2026-03-31")
    assert item["added_at"] == pd.Timestamp("2026-06-20")
    assert item["holdings"][0]["stock_name"] == "宁德时代"


def test_build_item_preserves_fund_when_one_query_failed():
    item = build_fund_watchlist_item(
        _watchlist_row(),
        pd.DataFrame(),
        pd.DataFrame(),
        load_error="持仓读取失败",
    )

    assert item["fund_name"] == "中欧时代先锋"
    assert item["top10_ratio"] is None
    assert item["holding_market_value"] is None
    assert item["holdings"] == []
    assert item["load_error"] == "持仓读取失败"


def test_summary_ignores_missing_values_and_counts_changes():
    first = build_fund_watchlist_item(_watchlist_row(), _meta_df(), _holding_df())
    second = {
        **first,
        "fund_code": "005827.OF",
        "top10_ratio": None,
        "latest_end_date": pd.NaT,
        "new_count": 2,
        "increase_count": 0,
        "decrease_count": 3,
    }

    summary = build_fund_watchlist_summary([first, second])

    assert summary["fund_count"] == 2
    assert summary["latest_end_date"] == pd.Timestamp("2026-03-31")
    assert summary["average_top10_ratio"] == 18.9
    assert summary["positive_change_count"] == 4
    assert summary["decrease_count"] == 4


def test_sort_and_table_use_the_same_normalized_models():
    base = build_fund_watchlist_item(_watchlist_row(), _meta_df(), _holding_df())
    other = {
        **base,
        "fund_code": "005827.OF",
        "fund_name": "易方达蓝筹精选",
        "top10_ratio": 62.4,
        "issue_amount": 425.1,
        "holding_market_value": 212.5,
        "latest_end_date": pd.Timestamp("2025-12-31"),
    }

    sorted_items = sort_fund_watchlist_items([base, other], "Top10 集中度")
    table = build_fund_watchlist_table(sorted_items)

    assert [item["fund_code"] for item in sorted_items] == ["005827.OF", "001938.OF"]
    assert table.iloc[0]["基金代码"] == "005827.OF"
    assert table.iloc[0]["Top10 集中度(%)"] == 62.4
```

- [ ] **Step 2: Run the new tests and confirm import failure**

Run:

```powershell
pytest tests/test_fund_watchlist_dashboard.py -q
```

Expected: collection fails because `src.fund_watchlist_dashboard` does not exist.

- [ ] **Step 3: Implement the pure model module**

Create `src/fund_watchlist_dashboard.py` with these public functions and stable dictionary keys:

```python
from __future__ import annotations

from typing import Iterable

import pandas as pd


CHANGE_LABELS = {
    "new": "新进",
    "increase": "增持",
    "decrease": "减持",
    "stable": "稳定",
}

SORT_FIELDS = {
    "Top10 集中度": "top10_ratio",
    "基金规模": "issue_amount",
    "持仓市值": "holding_market_value",
    "披露日期": "latest_end_date",
}


def _first_nonempty(*values, default="-"):
    for value in values:
        if value is None:
            continue
        if isinstance(value, float) and pd.isna(value):
            continue
        text = str(value).strip()
        if text:
            return value
    return default


def _optional_float(value):
    number = pd.to_numeric(value, errors="coerce")
    return None if pd.isna(number) else float(number)


def _optional_timestamp(value):
    timestamp = pd.to_datetime(value, errors="coerce")
    return pd.NaT if pd.isna(timestamp) else timestamp


def build_fund_watchlist_item(
    watchlist_row: pd.Series,
    meta_df: pd.DataFrame,
    holding_df: pd.DataFrame,
    *,
    load_error: str = "",
) -> dict:
    fund_code = str(watchlist_row.get("ts_code") or "").strip().upper()
    meta_row = None
    if meta_df is not None and not meta_df.empty:
        exact = meta_df[
            meta_df["fund_code"].astype(str).str.strip().str.upper() == fund_code
        ]
        meta_row = exact.iloc[0] if not exact.empty else meta_df.iloc[0]

    holding_first = (
        holding_df.iloc[0]
        if holding_df is not None and not holding_df.empty
        else None
    )
    fund_name = str(
        _first_nonempty(
            watchlist_row.get("security_name"),
            meta_row.get("name") if meta_row is not None else None,
            holding_first.get("fund_name") if holding_first is not None else None,
            fund_code,
        )
    )
    management = str(
        _first_nonempty(
            meta_row.get("management") if meta_row is not None else None,
            holding_first.get("management") if holding_first is not None else None,
        )
    )
    fund_type = str(
        _first_nonempty(
            meta_row.get("fund_type") if meta_row is not None else None,
            holding_first.get("fund_type") if holding_first is not None else None,
            holding_first.get("invest_type") if holding_first is not None else None,
        )
    )
    issue_amount = _optional_float(
        meta_row.get("issue_amount") if meta_row is not None else None
    )
    latest_end_date = _optional_timestamp(
        holding_first.get("end_date")
        if holding_first is not None
        else (meta_row.get("latest_end_date") if meta_row is not None else None)
    )
    added_at = _optional_timestamp(watchlist_row.get("created_at"))

    holdings = []
    if holding_df is not None and not holding_df.empty:
        for _, row in holding_df.head(10).iterrows():
            flag = str(row.get("holding_change_flag") or "stable").strip().lower()
            holdings.append(
                {
                    "stock_name": str(row.get("stock_name") or row.get("symbol") or "-"),
                    "symbol": str(row.get("symbol") or "-"),
                    "market_value": _optional_float(row.get("mkv")),
                    "market_value_yi": (
                        _optional_float(row.get("mkv")) / 1e8
                        if _optional_float(row.get("mkv")) is not None
                        else None
                    ),
                    "weight": _optional_float(row.get("stk_mkv_ratio")),
                    "change_flag": flag,
                    "change_label": CHANGE_LABELS.get(flag, "稳定"),
                }
            )

    valid_market_values = [
        row["market_value_yi"]
        for row in holdings
        if row["market_value_yi"] is not None
    ]
    valid_weights = [row["weight"] for row in holdings if row["weight"] is not None]
    flags = [row["change_flag"] for row in holdings]

    return {
        "fund_code": fund_code,
        "safe_code": "".join(ch if ch.isalnum() else "_" for ch in fund_code),
        "fund_name": fund_name,
        "fund_type": fund_type,
        "management": management,
        "issue_amount": issue_amount,
        "latest_end_date": latest_end_date,
        "added_at": added_at,
        "holding_count": len(holdings),
        "holding_market_value": (
            round(sum(valid_market_values), 2) if valid_market_values else None
        ),
        "top10_ratio": round(sum(valid_weights), 2) if valid_weights else None,
        "new_count": flags.count("new"),
        "increase_count": flags.count("increase"),
        "decrease_count": flags.count("decrease"),
        "stable_count": flags.count("stable"),
        "holdings": holdings,
        "load_error": str(load_error or ""),
    }


def build_fund_watchlist_summary(items: Iterable[dict]) -> dict:
    items = list(items)
    dates = [
        item["latest_end_date"]
        for item in items
        if not pd.isna(item.get("latest_end_date"))
    ]
    ratios = [
        float(item["top10_ratio"])
        for item in items
        if item.get("top10_ratio") is not None
    ]
    return {
        "fund_count": len(items),
        "latest_end_date": max(dates) if dates else pd.NaT,
        "average_top10_ratio": (
            round(sum(ratios) / len(ratios), 2) if ratios else None
        ),
        "positive_change_count": sum(
            int(item.get("new_count", 0)) + int(item.get("increase_count", 0))
            for item in items
        ),
        "decrease_count": sum(int(item.get("decrease_count", 0)) for item in items),
    }


def sort_fund_watchlist_items(items: Iterable[dict], sort_label: str) -> list[dict]:
    field = SORT_FIELDS.get(sort_label, "top10_ratio")

    def sort_key(item):
        value = item.get(field)
        if field == "latest_end_date":
            timestamp = pd.to_datetime(value, errors="coerce")
            return pd.Timestamp.min if pd.isna(timestamp) else timestamp
        number = pd.to_numeric(value, errors="coerce")
        return float("-inf") if pd.isna(number) else float(number)

    return sorted(list(items), key=sort_key, reverse=True)


def build_fund_watchlist_table(items: Iterable[dict]) -> pd.DataFrame:
    rows = []
    for item in items:
        rows.append(
            {
                "基金名称": item["fund_name"],
                "基金代码": item["fund_code"],
                "基金类型": item["fund_type"],
                "基金规模(亿份)": item["issue_amount"],
                "持仓市值(亿元)": item["holding_market_value"],
                "Top10 集中度(%)": item["top10_ratio"],
                "新进": item["new_count"],
                "增持": item["increase_count"],
                "减持": item["decrease_count"],
                "最新披露": (
                    item["latest_end_date"].strftime("%Y-%m-%d")
                    if not pd.isna(item["latest_end_date"])
                    else "-"
                ),
                "加入日期": (
                    item["added_at"].strftime("%Y-%m-%d")
                    if not pd.isna(item["added_at"])
                    else "-"
                ),
            }
        )
    return pd.DataFrame(rows)
```

- [ ] **Step 4: Run unit tests and correct only real contract mismatches**

Run:

```powershell
pytest tests/test_fund_watchlist_dashboard.py -q
```

Expected: all four tests pass. If floating-point representation differs, use `pytest.approx` in the assertion; do not alter the displayed two-decimal model contract.

- [ ] **Step 5: Commit the presentation model**

```powershell
git add src/fund_watchlist_dashboard.py tests/test_fund_watchlist_dashboard.py
git commit -m "feat: add fund watchlist presentation model"
```

### Task 4: Route the new standalone page and remove the embedded board

**Files:**
- Modify: `app.py:92-121`
- Modify: `app.py:6473-6489`
- Modify: `app.py:6575-6587`
- Modify: `app.py:16715-16717`
- Rewrite: `tests/test_fund_watchlist_ui.py`

- [ ] **Step 1: Replace source-string tests with standalone route assertions**

Replace `tests/test_fund_watchlist_ui.py` with:

```python
from pathlib import Path


APP_SOURCE = Path("app.py").read_text(encoding="utf-8", errors="ignore")


def _fund_hot_stocks_body():
    start = APP_SOURCE.index("def render_fund_hot_stocks_tab")
    end = APP_SOURCE.index("def render_moneyflow_tab", start)
    return APP_SOURCE[start:end]


def test_app_imports_and_routes_standalone_fund_watchlist_page():
    assert "ETF_FUND_WATCHLIST_PAGE_LABEL" in APP_SOURCE
    assert "elif mobile_page == ETF_FUND_WATCHLIST_PAGE_LABEL:" in APP_SOURCE
    assert "elif selected_page == ETF_FUND_WATCHLIST_PAGE_LABEL:" in APP_SOURCE
    assert APP_SOURCE.count("render_fund_watchlist_tab()") >= 2


def test_fund_hot_stocks_page_no_longer_embeds_watchlist_board():
    body = _fund_hot_stocks_body()

    assert "render_fund_watchlist_tab()" not in body
    assert "render_fund_watchlist_board()" not in body


def test_standalone_page_uses_only_fund_watchlist_rows():
    assert 'list_watchlist_items(current_username, security_type="fund")' in APP_SOURCE
```

- [ ] **Step 2: Run the UI contract tests and confirm failure**

Run:

```powershell
pytest tests/test_fund_watchlist_ui.py -q
```

Expected: failures because the new constant and route branches do not exist, and the old board remains embedded.

- [ ] **Step 3: Import and route the page**

Add `ETF_FUND_WATCHLIST_PAGE_LABEL` to the navigation imports in `app.py`.

In the mobile Fund route, use constants consistently:

```python
if mobile_page == ETF_MAIN_PAGE_LABEL:
    render_etf_tab()
elif mobile_page == ETF_RATIO_PAGE_LABEL:
    render_etf_category_ratio_tab()
elif mobile_page == ETF_TREND_PAGE_LABEL:
    render_etf_trend_tab()
elif mobile_page == ETF_WIDE_INDEX_PAGE_LABEL:
    render_wide_index_tab()
elif mobile_page == ETF_FUND_MONITOR_PAGE_LABEL:
    render_fund_monitor_tab()
elif mobile_page == ETF_FUND_WATCHLIST_PAGE_LABEL:
    render_fund_watchlist_tab()
else:
    render_etf_tab()
```

Add to the desktop Fund branch before the fallback:

```python
elif selected_page == ETF_FUND_WATCHLIST_PAGE_LABEL:
    render_fund_watchlist_tab()
```

Delete the final embedded call from `render_fund_hot_stocks_tab`:

```python
st.markdown("---")
render_fund_watchlist_board()
```

Rename the old `render_fund_watchlist_board` entry point to `render_fund_watchlist_tab`; Task 5 will replace its body.

- [ ] **Step 4: Run route tests**

Run:

```powershell
pytest tests/test_fund_watchlist_ui.py tests/test_navigation_config.py tests/test_sidebar_navigation.py -q
```

Expected: all tests pass except any Task 5 UI-field assertions not yet added.

- [ ] **Step 5: Commit the standalone route**

```powershell
git add app.py tests/test_fund_watchlist_ui.py
git commit -m "feat: route standalone fund watchlist page"
```

### Task 5: Implement one cached load path and the dark dashboard UI

**Files:**
- Modify: `app.py:436` (add scoped CSS next to stock watchlist CSS)
- Modify: `app.py:15802-15938` (replace legacy board implementation)
- Modify: `tests/test_fund_watchlist_ui.py`

- [ ] **Step 1: Add failing page-contract assertions**

Append to `tests/test_fund_watchlist_ui.py`:

```python
def test_fund_watchlist_dashboard_exposes_view_sort_focus_and_batch_controls():
    assert "FUND_WATCHLIST_DASHBOARD_CSS" in APP_SOURCE
    assert "load_fund_watchlist_dashboard_data_session_cached" in APP_SOURCE
    assert "build_fund_watchlist_summary" in APP_SOURCE
    assert "sort_fund_watchlist_items" in APP_SOURCE
    assert "build_fund_watchlist_table" in APP_SOURCE
    assert '"看板", "表格"' in APP_SOURCE
    assert '"Top10 集中度", "基金规模", "持仓市值", "披露日期"' in APP_SOURCE
    assert "render_fund_watchlist_focus_detail" in APP_SOURCE
    assert "fund_watchlist_batch_mode" in APP_SOURCE
    assert "remove_watchlist_items_batch(current_username, pending_items)" in APP_SOURCE


def test_fund_watchlist_copy_and_fields_are_chinese_fund_semantics():
    for text in [
        "请先登录用户名，再查看和管理你的自选基金。",
        "你的自选基金还是空的",
        "追踪自选基金的持仓结构、披露进度与集中度变化",
        "平均 Top10 集中度",
        "持仓变动",
        "基金管理人",
        "前十大持仓明细",
        "持仓市值",
        "持仓变化",
    ]:
        assert text in APP_SOURCE
```

- [ ] **Step 2: Run tests and confirm the UI contract fails**

Run:

```powershell
pytest tests/test_fund_watchlist_ui.py -q
```

Expected: failures for missing CSS, cache, helpers, controls, and Chinese field copy.

- [ ] **Step 3: Import model helpers and add a single session cache**

Add `import time` with the standard-library imports in `app.py`.

Add imports in `app.py`:

```python
from src.fund_watchlist_dashboard import (
    build_fund_watchlist_item,
    build_fund_watchlist_summary,
    build_fund_watchlist_table,
    sort_fund_watchlist_items,
)
```

Add these cache helpers immediately before the page renderer:

```python
FUND_WATCHLIST_SESSION_CACHE_TTL_SECONDS = 900


def _clear_fund_watchlist_session_cache() -> None:
    st.session_state.pop("fund_watchlist_dashboard_cache", None)


def load_fund_watchlist_dashboard_data(
    watchlist_df: pd.DataFrame,
    fund_engine,
) -> list[dict]:
    from src.fund_hot_stocks import query_fund_preference_snapshot, search_funds

    items = []
    for _, watchlist_row in watchlist_df.iterrows():
        fund_code = str(watchlist_row.get("ts_code") or "").strip().upper()
        meta_df = pd.DataFrame()
        holding_df = pd.DataFrame()
        errors = []
        try:
            meta_df = search_funds(fund_code, limit=5, engine=fund_engine)
        except Exception as exc:
            errors.append(f"基础信息读取失败：{exc}")
        try:
            holding_df = query_fund_preference_snapshot(
                fund_code=fund_code,
                top_n=10,
                engine=fund_engine,
            )
        except Exception as exc:
            errors.append(f"持仓读取失败：{exc}")
        items.append(
            build_fund_watchlist_item(
                watchlist_row,
                meta_df,
                holding_df,
                load_error="；".join(errors),
            )
        )
    return items


def load_fund_watchlist_dashboard_data_session_cached(
    username: str,
    watchlist_df: pd.DataFrame,
    fund_engine,
) -> list[dict]:
    codes = tuple(
        watchlist_df["ts_code"].astype(str).str.strip().str.upper().tolist()
    )
    now = time.time()
    cache = st.session_state.get("fund_watchlist_dashboard_cache")
    if (
        isinstance(cache, dict)
        and cache.get("username") == username
        and cache.get("codes") == codes
        and now - float(cache.get("saved_at", 0.0))
        < FUND_WATCHLIST_SESSION_CACHE_TTL_SECONDS
    ):
        return cache["items"]

    items = load_fund_watchlist_dashboard_data(watchlist_df, fund_engine)
    st.session_state["fund_watchlist_dashboard_cache"] = {
        "username": username,
        "codes": codes,
        "saved_at": now,
        "items": items,
    }
    return items
```

- [ ] **Step 4: Add fully scoped cyber CSS**

Define `FUND_WATCHLIST_DASHBOARD_CSS` next to `WATCHLIST_CYBER_DASHBOARD_CSS`. Every selector must begin with `.ws-fund-watchboard` or `.st-key-fund_watchlist_`; do not use unscoped `table`, `button`, `.card`, or `.metric` selectors.

The CSS must include these exact component classes:

```css
.ws-fund-watchboard { ...dark blue-black shell tokens... }
.ws-fund-watchboard__summary { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); ... }
.ws-fund-watchboard__metric { ... }
.ws-fund-watchboard__cards { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); ... }
.ws-fund-watchboard__card { ... }
.ws-fund-watchboard__card.is-active { border-color:var(--fw-cyan); ... }
.ws-fund-watchboard__ratio.is-low { color:var(--fw-green); }
.ws-fund-watchboard__ratio.is-high { color:var(--fw-red); }
.ws-fund-watchboard__focus { display:grid; grid-template-columns:minmax(240px,.85fr) minmax(0,1.7fr); ... }
.ws-fund-watchboard__ring { ... }
.ws-fund-watchboard__holdings { ... }
.st-key-fund_watchlist_card_grid [data-testid="stButton"] > button { ...transparent card hit target... }
@media (max-width: 900px) {
  .ws-fund-watchboard__summary { grid-template-columns:repeat(2,minmax(0,1fr)); }
  .ws-fund-watchboard__cards { grid-template-columns:repeat(2,minmax(0,1fr)); }
  .ws-fund-watchboard__focus { grid-template-columns:1fr; }
}
@media (max-width: 620px) {
  .ws-fund-watchboard__cards { grid-template-columns:1fr; }
}
```

Use the existing stock colors as tokens:

```css
--fw-bg:#030816;
--fw-panel:rgba(5,17,39,.92);
--fw-line:rgba(70,126,255,.54);
--fw-cyan:#22d7ff;
--fw-blue:#2f7bff;
--fw-red:#ff3f55;
--fw-green:#20dfb8;
--fw-text:#f5f9ff;
--fw-muted:#93a9ca;
```

- [ ] **Step 5: Replace `render_fund_watchlist_tab` with page orchestration**

The page renderer must:

1. Show the confirmed Chinese heading/caption.
2. Stop early for no login, list failure, empty list, or database connection failure.
3. Load the model once through `load_fund_watchlist_dashboard_data_session_cached`.
4. Derive summary, view, sort, and focus from that model.
5. Render either cards or the normalized table.
6. Render focus detail for the selected item.

Use this control/state skeleton:

```python
def render_fund_watchlist_tab() -> None:
    from src.fund_hot_stocks import get_engine as get_fund_hot_engine

    st.subheader("⭐ 自选基金")
    st.caption("追踪自选基金的持仓结构、披露进度与集中度变化")

    current_username = get_logged_in_username()
    if not current_username:
        st.info("请先登录用户名，再查看和管理你的自选基金。")
        return

    try:
        watchlist_df = list_watchlist_items(
            current_username,
            security_type="fund",
        )
    except Exception as exc:
        st.error(f"加载自选基金失败：{exc}")
        return

    if watchlist_df is None or watchlist_df.empty:
        st.info("你的自选基金还是空的，请从“公募持仓热股”的基金持仓查询区域添加。")
        return

    try:
        fund_engine = get_fund_hot_engine()
    except Exception as exc:
        st.error(f"连接基金持仓数据库失败：{exc}")
        return

    with st.spinner("正在加载自选基金持仓数据..."):
        items = load_fund_watchlist_dashboard_data_session_cached(
            current_username,
            watchlist_df,
            fund_engine,
        )

    summary = build_fund_watchlist_summary(items)
    st.markdown(FUND_WATCHLIST_DASHBOARD_CSS, unsafe_allow_html=True)
    render_fund_watchlist_summary(summary)

    control_cols = st.columns([1.1, 1.4, 1.2])
    with control_cols[0]:
        view_mode = st.radio(
            "视图模式",
            ["看板", "表格"],
            horizontal=True,
            key="fund_watchlist_view_mode",
        )
    with control_cols[1]:
        sort_label = st.selectbox(
            "排序方式",
            ["Top10 集中度", "基金规模", "持仓市值", "披露日期"],
            key="fund_watchlist_sort_label",
        )

    sorted_items = sort_fund_watchlist_items(items, sort_label)
    valid_codes = [item["fund_code"] for item in sorted_items]
    focus_code = str(
        st.session_state.get("fund_watchlist_focus_code") or ""
    ).strip().upper()
    if focus_code not in valid_codes:
        focus_code = valid_codes[0]
        st.session_state["fund_watchlist_focus_code"] = focus_code

    if view_mode == "看板":
        render_fund_watchlist_cards(
            sorted_items,
            focus_code=focus_code,
            current_username=current_username,
        )
    else:
        st.dataframe(
            build_fund_watchlist_table(sorted_items),
            use_container_width=True,
            hide_index=True,
        )
        focus_labels = {
            f"{item['fund_name']} ({item['fund_code']})": item["fund_code"]
            for item in sorted_items
        }
        selected_label = st.selectbox(
            "详情焦点",
            list(focus_labels),
            index=valid_codes.index(focus_code),
            key="fund_watchlist_table_focus",
        )
        focus_code = focus_labels[selected_label]
        st.session_state["fund_watchlist_focus_code"] = focus_code

    focus_item = next(
        item for item in sorted_items if item["fund_code"] == focus_code
    )
    render_fund_watchlist_focus_detail(focus_item)
```

Implement `render_fund_watchlist_summary(summary)` with four `.ws-fund-watchboard__metric` blocks. Format:

- fund count as `N 只`
- latest date as `%Y-%m-%d` or `-`
- average ratio as `%.2f%%` or `-`
- changes as `+N / -N`

- [ ] **Step 6: Implement cards and focus detail**

`render_fund_watchlist_cards` must render three columns on desktop using `st.columns(3)`, keep each card button accessible, and use `fund_watchlist_focus_code`.

Card HTML must expose:

```html
<div class="ws-fund-watchboard__card is-active">
  <div class="ws-fund-watchboard__card-head">基金名称 / 代码 / 类型</div>
  <div class="ws-fund-watchboard__ratio">Top10 集中度</div>
  <div class="ws-fund-watchboard__card-metrics">基金规模 / 持仓市值</div>
  <div class="ws-fund-watchboard__changes">新进 / 增持 / 减持</div>
  <div class="ws-fund-watchboard__date">最新披露日期或加载错误</div>
</div>
```

Use these deterministic ratio classes:

```python
def _fund_watchlist_ratio_class(value) -> str:
    if value is None:
        return ""
    if value >= 60:
        return " is-high"
    if value <= 40:
        return " is-low"
    return ""
```

`render_fund_watchlist_focus_detail(item)` must show:

- fund name/code, manager, type, latest disclosure, added date, holding count
- one ring with `top10_ratio`
- a dataframe with columns `股票名称`, `股票代码`, `持仓市值(亿元)`, `持仓权重(%)`, `持仓变化`
- a fund-level warning when `load_error` is non-empty
- a fund-level empty message when `holdings` is empty

Build the detail dataframe from `item["holdings"]`; do not query inside the render function.

- [ ] **Step 7: Implement fund-only batch management**

Keep fund keys isolated from stock watchlist keys:

```python
fund_watchlist_batch_mode
fund_watchlist_batch_sel_{safe_code}
fund_watchlist_batch_confirm_pending
fund_watchlist_batch_delete_items
fund_watchlist_batch_delete_names
```

Use the same state flow as the stock dashboard, but every pending item must be:

```python
(item["fund_code"], "fund")
```

After successful deletion:

```python
deleted = remove_watchlist_items_batch(current_username, pending_items)
_clear_fund_watchlist_session_cache()
st.session_state.pop("fund_watchlist_focus_code", None)
st.session_state["fund_watchlist_batch_mode"] = False
```

Clear all pending/selection keys before `st.rerun()`. Show:

```python
st.success(f"已从自选基金中删除 {deleted} 只基金")
```

- [ ] **Step 8: Run focused automated tests**

Run:

```powershell
pytest tests/test_fund_watchlist_dashboard.py tests/test_fund_watchlist_ui.py tests/test_navigation_config.py tests/test_sidebar_navigation.py tests/fund_hot_stocks_sync_test.py -q
```

Expected: all tests pass.

- [ ] **Step 9: Commit the standalone dashboard**

```powershell
git add app.py tests/test_fund_watchlist_ui.py
git commit -m "feat: redesign standalone fund watchlist dashboard"
```

### Task 6: Verify behavior and visual fidelity

**Files:**
- Modify only if verification reveals a defect: `app.py`, `src/fund_watchlist_dashboard.py`, or their tests

- [ ] **Step 1: Run syntax and focused tests**

Run:

```powershell
python -m py_compile app.py src/fund_watchlist_dashboard.py src/fund_hot_stocks.py
pytest tests/test_fund_watchlist_dashboard.py tests/test_fund_watchlist_ui.py tests/test_navigation_config.py tests/test_sidebar_navigation.py tests/fund_hot_stocks_sync_test.py -q
```

Expected: compilation succeeds and all focused tests pass.

- [ ] **Step 2: Run the broader relevant suite**

Run:

```powershell
pytest tests/test_desktop_sidebar_layout.py tests/test_sidebar_tree_layout.py tests/watchlist_excel_importer_test.py tests/test_watchlist_stock_research_refresh.py -q
```

Expected: all tests pass, proving the Fund navigation addition did not break stock watchlist or sidebar behavior.

- [ ] **Step 3: Start Streamlit**

Run in a persistent terminal:

```powershell
streamlit run app.py --server.port 8501
```

Expected: Streamlit reports `http://localhost:8501`.

- [ ] **Step 4: Verify through the in-app browser**

Use the browser control skill and check:

1. Open `http://localhost:8501`.
2. Sign in with the existing local test username used for this workspace.
3. Expand “基金” and open “⭐ 自选基金”.
4. Confirm the hot-stocks page no longer contains the board.
5. Confirm the standalone page shows four summary metrics.
6. Switch all four sort options.
7. Switch “看板” and “表格”.
8. Click at least two fund cards and confirm focus details change without a loading spinner/database requery.
9. Enter batch mode, select/deselect, open deletion confirmation, and cancel without deleting.
10. At desktop width, verify three-card rhythm and two-column focus panel.
11. At approximately 390px width, verify summary becomes two columns, cards become one column, focus stacks vertically, and the page itself does not overflow horizontally.

- [ ] **Step 5: Capture implementation screenshots**

Capture:

- desktop standalone page at a width near the accepted concept
- mobile page around 390px

Save under `.codex_tmp/fund-watchlist-qa/` because they are temporary QA artifacts.

- [ ] **Step 6: Compare concept and implementation**

Use `view_image` on:

- accepted concept/reference: the selected visual companion screenshot or `docs/股票看板.jpg` for the shared cyber language
- latest desktop implementation screenshot
- latest mobile implementation screenshot

Record and fix any mismatch in:

1. dark blue-black palette and blue line treatment
2. four-metric hierarchy
3. three-card desktop density
4. active-card cyan state
5. ratio/change green-red semantics
6. focus overview/table balance
7. Chinese copy and fund-only field names
8. mobile stacking and readable controls

- [ ] **Step 7: Re-run verification after visual fixes**

Run:

```powershell
python -m py_compile app.py src/fund_watchlist_dashboard.py src/fund_hot_stocks.py
pytest tests/test_fund_watchlist_dashboard.py tests/test_fund_watchlist_ui.py tests/test_navigation_config.py tests/test_sidebar_navigation.py tests/fund_hot_stocks_sync_test.py tests/test_desktop_sidebar_layout.py tests/test_sidebar_tree_layout.py -q
```

Expected: all checks pass.

- [ ] **Step 8: Remove temporary QA artifacts and commit fixes**

Remove only files created under `.codex_tmp/fund-watchlist-qa/`, after resolving their absolute paths inside `D:\sourcecode\etf\.codex_tmp\fund-watchlist-qa`.

Then commit any verification fixes:

```powershell
git add app.py src/fund_watchlist_dashboard.py src/fund_hot_stocks.py tests/test_fund_watchlist_dashboard.py tests/test_fund_watchlist_ui.py tests/test_navigation_config.py tests/test_sidebar_navigation.py tests/fund_hot_stocks_sync_test.py
git commit -m "fix: polish fund watchlist responsive behavior"
```

If verification required no code changes, do not create an empty commit.

## Completion checklist

- [ ] “基金” module exposes `fund_watchlist` with label “⭐ 自选基金”.
- [ ] Desktop and mobile routes render `render_fund_watchlist_tab()`.
- [ ] The hot-stocks page does not embed the watchlist.
- [ ] Fund scale comes from existing `issue_amount`.
- [ ] One normalized model drives summary, cards, table, and focus detail.
- [ ] Focus/view/sort changes reuse session data.
- [ ] A single fund load error does not hide other funds.
- [ ] Batch deletion only submits `(fund_code, "fund")`.
- [ ] Cache and focus state clear after deletion.
- [ ] Desktop and mobile browser checks pass.
- [ ] Accepted dark cyber visual direction is faithfully preserved.
