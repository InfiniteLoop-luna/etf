# Sidebar Navigation Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the desktop Streamlit sidebar into a stable navigation shell, move page-local desktop filters into page toolbars, and preserve the existing iPhone mode and jump-to-security behavior.

**Architecture:** Introduce a structured navigation catalog with pure helper functions, then use it to drive a new desktop sidebar helper in `app.py`. Migrate the three current desktop sidebar-heavy pages (`render_volume_tab`, `render_etf_tab`, `render_etf_trend_tab`) to page-local toolbars while keeping their data logic intact. Use recent visits plus curated quick shortcuts in the first pass instead of a user-editable favorites subsystem. Extend the shared theme CSS so the new shell and toolbar layout fit the existing gold/navy visual system.

**Tech Stack:** Python, Streamlit, Pandas, Plotly, existing `src.apple_theme` CSS generator, `unittest`-style tests executed through `pytest`

---

## File Map

- Create: `src/sidebar_navigation.py`
  - Canonical desktop navigation metadata, toolbar variants, quick-jump helpers, and recent-visit session helpers.
- Create: `src/page_filter_utils.py`
  - Pure helper functions for ETF metric grouping and ETF trend category resolution so UI migration is testable without Streamlit.
- Create: `tests/test_sidebar_navigation.py`
  - Tests for module order, legacy option compatibility, toolbar variants, quick-jump lookup, and recent-visit state management.
- Create: `tests/test_page_filter_utils.py`
  - Tests for ETF metric category grouping, quick metric buckets, and ETF trend secondary category resolution.
- Create: `tests/test_desktop_sidebar_layout.py`
  - Source-level regression checks for the new desktop sidebar helper and quick-jump flow.
- Create: `tests/test_page_toolbar_layout.py`
  - Source-level regression checks proving the migrated desktop pages no longer use `st.sidebar`.
- Modify: `src/navigation_config.py`
  - Export the existing option lists from the new navigation catalog so iPhone mode and page routing keep their current labels.
- Modify: `src/apple_theme.py`
  - Add CSS selectors for the new sidebar blocks, recent-visit items, and page toolbar shells.
- Modify: `tests/eastmoney_author_tracker/test_ui.py`
  - Extend existing theme CSS assertions with the new sidebar and page-toolbar selectors.
- Modify: `app.py`
  - Replace the current desktop sidebar radio block with a helper-driven shell, preserve iPhone mode, preserve `security_query` jump behavior, and move desktop filters out of `render_volume_tab`, `render_etf_tab`, and `render_etf_trend_tab`.

### Task 1: Create the structured navigation catalog

**Files:**
- Create: `src/sidebar_navigation.py`
- Modify: `src/navigation_config.py`
- Test: `tests/test_sidebar_navigation.py`

- [ ] **Step 1: Write the failing navigation tests**

```python
import unittest

from src.navigation_config import (
    DECISION_PAGE_OPTIONS,
    ETF_PAGE_OPTIONS,
    MACRO_PAGE_OPTIONS,
    MONEY_PAGE_OPTIONS,
    STOCK_PAGE_OPTIONS,
)
from src.sidebar_navigation import (
    MAX_RECENT_PAGES,
    RECENT_VISITS_KEY,
    ensure_sidebar_state,
    get_default_shortcuts,
    get_module_label_for_page,
    get_module_labels,
    get_page_by_label,
    get_page_labels,
    get_recent_visits,
    record_recent_visit,
)


class SidebarNavigationTests(unittest.TestCase):
    def test_module_labels_keep_expected_order(self):
        self.assertEqual(get_module_labels(), ["决策", "基金", "个股", "资金", "宏观"])

    def test_legacy_option_exports_match_structured_catalog(self):
        self.assertEqual(get_page_labels("决策"), DECISION_PAGE_OPTIONS)
        self.assertEqual(get_page_labels("基金"), ETF_PAGE_OPTIONS)
        self.assertEqual(get_page_labels("个股"), STOCK_PAGE_OPTIONS)
        self.assertEqual(get_page_labels("资金"), MONEY_PAGE_OPTIONS)
        self.assertEqual(get_page_labels("宏观"), MACRO_PAGE_OPTIONS)

    def test_get_page_by_label_exposes_toolbar_variant(self):
        page = get_page_by_label("个股", "🧠 因子选股工作台")

        self.assertEqual(page.id, "factor_workbench")
        self.assertEqual(page.toolbar_variant, "heavy")

    def test_get_module_label_for_page_supports_quick_jump(self):
        self.assertEqual(get_module_label_for_page("💹 资金流向"), "资金")
        self.assertEqual(get_module_label_for_page("🌏 宏观经济"), "宏观")

    def test_default_shortcuts_stay_curated_and_stable(self):
        self.assertEqual(
            get_default_shortcuts(),
            ["💼 今日机会清单", "🔎 个股/指数查询", "💹 资金流向"],
        )

    def test_record_recent_visit_deduplicates_and_trims(self):
        session_state = {}
        ensure_sidebar_state(session_state)

        record_recent_visit(session_state, "决策", "💼 今日机会清单")
        record_recent_visit(session_state, "基金", "📈 基金监测")
        record_recent_visit(session_state, "决策", "💼 今日机会清单")
        record_recent_visit(session_state, "个股", "🔎 个股/指数查询")
        record_recent_visit(session_state, "资金", "💹 资金流向")
        record_recent_visit(session_state, "宏观", "🌏 宏观经济")

        recent = get_recent_visits(session_state)

        self.assertEqual(session_state[RECENT_VISITS_KEY], recent)
        self.assertEqual(len(recent), MAX_RECENT_PAGES)
        self.assertEqual(recent[0], {"module": "宏观", "page": "🌏 宏观经济"})
        self.assertEqual(sum(1 for item in recent if item["page"] == "💼 今日机会清单"), 1)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_sidebar_navigation.py -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'src.sidebar_navigation'`

- [ ] **Step 3: Write the navigation catalog and compatibility exports**

```python
# src/sidebar_navigation.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, MutableMapping


ToolbarVariant = Literal["light", "standard", "heavy"]
RECENT_VISITS_KEY = "sidebar_recent_pages"
MAX_RECENT_PAGES = 4


@dataclass(frozen=True)
class SidebarPage:
    id: str
    label: str
    description: str
    toolbar_variant: ToolbarVariant


@dataclass(frozen=True)
class SidebarModule:
    id: str
    label: str
    session_key: str
    pages: tuple[SidebarPage, ...]


SIDEBAR_MODULES = (
    SidebarModule(
        id="decision",
        label="决策",
        session_key="decision_subpage",
        pages=(
            SidebarPage("commercial_mvp", "💼 今日机会清单", "今天先看什么", "light"),
            SidebarPage("daily_trend_reco", "⭐ 每日趋势推荐", "趋势候选和理由", "light"),
            SidebarPage("reco_eval", "🧪 推荐评估", "回看策略表现", "heavy"),
            SidebarPage("ml_upgrade", "🧠 ML预测升级", "模型结果页", "standard"),
        ),
    ),
    SidebarModule(
        id="fund",
        label="基金",
        session_key="etf_subpage",
        pages=(
            SidebarPage("etf_main", "📈 主要宽基ETF份额", "核心ETF份额走势", "standard"),
            SidebarPage("etf_ratio", "🥧 ETF分类占比", "按分类看占比", "light"),
            SidebarPage("etf_trend", "📈 ETF分类趋势", "分类时间序列", "standard"),
            SidebarPage("wide_index", "📊 宽基指数ETF", "基准指数聚合", "standard"),
            SidebarPage("fund_monitor", "📈 基金监测", "基金监测台账", "standard"),
        ),
    ),
    SidebarModule(
        id="stock",
        label="个股",
        session_key="stock_subpage",
        pages=(
            SidebarPage("security_search", "🔎 个股/指数查询", "查询单只股票或指数", "heavy"),
            SidebarPage("company_screener", "🏢 公司筛选", "按公司维度筛选", "standard"),
            SidebarPage("tech_picker", "🎯 技术选股", "技术面候选清单", "standard"),
            SidebarPage("factor_workbench", "🧠 因子选股工作台", "多因子工作台", "heavy"),
            SidebarPage("author_tracking", "🧭 观点跟踪", "作者观点研究台", "heavy"),
        ),
    ),
    SidebarModule(
        id="money",
        label="资金",
        session_key="money_subpage",
        pages=(
            SidebarPage("moneyflow", "💹 资金流向", "个股与板块资金流", "standard"),
            SidebarPage("volume", "📊 每日成交量", "A股成交量时间序列", "standard"),
            SidebarPage("fund_hot_stocks", "🏦 公募持仓热股", "基金持仓热股透视", "standard"),
            SidebarPage("limitup", "🔥 打板情绪", "涨停接力情绪", "standard"),
            SidebarPage("hotmoney", "🧾 游资名录", "游资活跃明细", "standard"),
        ),
    ),
    SidebarModule(
        id="macro",
        label="宏观",
        session_key="macro_subpage",
        pages=(
            SidebarPage("macro", "🌏 宏观经济", "宏观指标总览", "standard"),
            SidebarPage("deposit", "🏦 本外币存款", "存款月度数据", "standard"),
            SidebarPage("index_monitor", "📊 指数监测", "指数观察台账", "standard"),
        ),
    ),
)

MODULE_BY_LABEL = {module.label: module for module in SIDEBAR_MODULES}
PAGE_TO_MODULE = {
    page.label: module.label
    for module in SIDEBAR_MODULES
    for page in module.pages
}
DEFAULT_SHORTCUTS = ["💼 今日机会清单", "🔎 个股/指数查询", "💹 资金流向"]


def get_module_labels() -> list[str]:
    return [module.label for module in SIDEBAR_MODULES]


def get_module_by_label(module_label: str) -> SidebarModule:
    return MODULE_BY_LABEL[module_label]


def get_page_labels(module_label: str) -> list[str]:
    return [page.label for page in MODULE_BY_LABEL[module_label].pages]


def get_page_by_label(module_label: str, page_label: str) -> SidebarPage:
    module = MODULE_BY_LABEL[module_label]
    for page in module.pages:
        if page.label == page_label:
            return page
    raise KeyError(f"Unknown page {page_label!r} for module {module_label!r}")


def get_module_label_for_page(page_label: str) -> str:
    return PAGE_TO_MODULE[page_label]


def get_default_shortcuts() -> list[str]:
    return list(DEFAULT_SHORTCUTS)


def ensure_sidebar_state(session_state: MutableMapping[str, object]) -> None:
    if RECENT_VISITS_KEY not in session_state:
        session_state[RECENT_VISITS_KEY] = []


def record_recent_visit(session_state: MutableMapping[str, object], module_label: str, page_label: str) -> None:
    ensure_sidebar_state(session_state)
    latest = {"module": module_label, "page": page_label}
    existing = [
        item
        for item in session_state[RECENT_VISITS_KEY]
        if item != latest
    ]
    session_state[RECENT_VISITS_KEY] = [latest] + existing[: MAX_RECENT_PAGES - 1]


def get_recent_visits(session_state: MutableMapping[str, object]) -> list[dict[str, str]]:
    ensure_sidebar_state(session_state)
    return list(session_state[RECENT_VISITS_KEY])
```

```python
# src/navigation_config.py
from src.sidebar_navigation import get_page_labels


DECISION_PAGE_OPTIONS = get_page_labels("决策")
ETF_PAGE_OPTIONS = get_page_labels("基金")
STOCK_PAGE_OPTIONS = get_page_labels("个股")
MONEY_PAGE_OPTIONS = get_page_labels("资金")
MACRO_PAGE_OPTIONS = get_page_labels("宏观")

DECISION_TODAY_PAGE_LABEL = "💼 今日机会清单"
DECISION_DAILY_RECO_PAGE_LABEL = "⭐ 每日趋势推荐"
DECISION_RECO_EVAL_PAGE_LABEL = "🧪 推荐评估"
DECISION_ML_PAGE_LABEL = "🧠 ML预测升级"

ETF_MAIN_PAGE_LABEL = "📈 主要宽基ETF份额"
ETF_RATIO_PAGE_LABEL = "🥧 ETF分类占比"
ETF_TREND_PAGE_LABEL = "📈 ETF分类趋势"
ETF_WIDE_INDEX_PAGE_LABEL = "📊 宽基指数ETF"
ETF_FUND_MONITOR_PAGE_LABEL = "📈 基金监测"

STOCK_SECURITY_SEARCH_LABEL = "🔎 个股/指数查询"
STOCK_COMPANY_SCREENER_LABEL = "🏢 公司筛选"
STOCK_TECH_PICKER_LABEL = "🎯 技术选股"

MONEY_FLOW_PAGE_LABEL = "💹 资金流向"
MONEY_VOLUME_PAGE_LABEL = "📊 每日成交量"
MONEY_FUND_HOT_PAGE_LABEL = "🏦 公募持仓热股"
MONEY_LIMITUP_PAGE_LABEL = "🔥 打板情绪"
MONEY_HOTMONEY_PAGE_LABEL = "🧾 游资名录"

MACRO_MAIN_PAGE_LABEL = "🌏 宏观经济"
MACRO_DEPOSIT_PAGE_LABEL = "🏦 本外币存款"
MACRO_INDEX_MONITOR_PAGE_LABEL = "📊 指数监测"
```

- [ ] **Step 4: Run the navigation tests to verify they pass**

Run: `python -m pytest tests/test_sidebar_navigation.py -v`

Expected: PASS with 6 passed

- [ ] **Step 5: Commit the navigation foundation**

```bash
git add src/sidebar_navigation.py src/navigation_config.py tests/test_sidebar_navigation.py
git commit -m "feat: add structured sidebar navigation catalog"
```

### Task 2: Add pure filter helpers for migrated pages

**Files:**
- Create: `src/page_filter_utils.py`
- Test: `tests/test_page_filter_utils.py`

- [ ] **Step 1: Write the failing filter utility tests**

```python
import unittest

from src.page_filter_utils import (
    build_metric_categories,
    build_quick_metric_groups,
    build_secondary_category_options,
    resolve_trend_category_key,
)


class PageFilterUtilsTests(unittest.TestCase):
    def test_build_metric_categories_groups_known_keywords(self):
        categories = build_metric_categories(["总市值", "基金份额", "份额变动", "涨跌幅", "跟踪误差"])

        self.assertEqual(categories["市值类"], ["总市值"])
        self.assertEqual(categories["份额类"], ["基金份额"])
        self.assertEqual(categories["变动类"], ["份额变动"])
        self.assertEqual(categories["涨跌类"], ["涨跌幅"])
        self.assertEqual(categories["其他"], ["跟踪误差"])

    def test_build_quick_metric_groups_matches_existing_shortcuts(self):
        groups = build_quick_metric_groups(["总市值", "基金份额", "涨跌幅"])

        self.assertEqual(groups["总市值"], ["总市值"])
        self.assertEqual(groups["份额"], ["基金份额"])
        self.assertEqual(groups["涨跌幅"], ["涨跌幅"])

    def test_build_secondary_category_options_returns_subtotal_entry(self):
        options = build_secondary_category_options("指数", {"指数": ["宽基", "港股"]})

        self.assertEqual(options, ["全部(小计)", "宽基", "港股"])

    def test_resolve_trend_category_key_handles_all_and_subtotal(self):
        tree = {"指数": ["宽基", "港股"]}

        self.assertEqual(resolve_trend_category_key("全部", None, tree), "全部")
        self.assertEqual(resolve_trend_category_key("指数", "全部(小计)", tree), "指数")
        self.assertEqual(resolve_trend_category_key("指数", "宽基", tree), "指数-宽基")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_page_filter_utils.py -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'src.page_filter_utils'`

- [ ] **Step 3: Write the pure helper module**

```python
# src/page_filter_utils.py
from __future__ import annotations


def build_metric_categories(metric_types: list[str]) -> dict[str, list[str]]:
    categories = {
        "市值类": [m for m in metric_types if "市值" in m],
        "份额类": [m for m in metric_types if "份额" in m and "变动" not in m],
        "变动类": [m for m in metric_types if "变动" in m or "申赎" in m],
        "比例类": [m for m in metric_types if "比例" in m],
        "涨跌类": [m for m in metric_types if "涨跌" in m],
    }
    known = {metric for values in categories.values() for metric in values}
    categories["其他"] = [m for m in metric_types if m not in known]
    return {name: values for name, values in categories.items() if values}


def build_quick_metric_groups(metric_types: list[str]) -> dict[str, list[str]]:
    return {
        "总市值": [m for m in metric_types if "总市值" in m],
        "份额": [m for m in metric_types if "份额" in m and "总市值" not in m],
        "涨跌幅": [m for m in metric_types if "涨跌" in m],
    }


def build_secondary_category_options(selected_primary: str, category_tree: dict[str, list[str]]) -> list[str]:
    if selected_primary == "全部":
        return []
    if not category_tree.get(selected_primary):
        return []
    return ["全部(小计)"] + list(category_tree[selected_primary])


def resolve_trend_category_key(
    selected_primary: str,
    selected_secondary: str | None,
    category_tree: dict[str, list[str]],
) -> str:
    if selected_primary == "全部":
        return "全部"
    if not category_tree.get(selected_primary):
        return selected_primary
    if not selected_secondary or selected_secondary == "全部(小计)":
        return selected_primary
    return f"{selected_primary}-{selected_secondary}"
```

- [ ] **Step 4: Run the filter utility tests to verify they pass**

Run: `python -m pytest tests/test_page_filter_utils.py -v`

Expected: PASS with 4 passed

- [ ] **Step 5: Commit the filter helper foundation**

```bash
git add src/page_filter_utils.py tests/test_page_filter_utils.py
git commit -m "feat: add page filter helper utilities"
```

### Task 3: Build the desktop sidebar shell and preserve jump behavior

**Files:**
- Modify: `src/apple_theme.py:280-360`
- Modify: `tests/eastmoney_author_tracker/test_ui.py`
- Create: `tests/test_desktop_sidebar_layout.py`
- Modify: `app.py:85`, `app.py:1704-1736`, `app.py:3464-3736`

- [ ] **Step 1: Write the failing desktop sidebar layout tests**

```python
import unittest
from pathlib import Path

from src.apple_theme import build_global_apple_theme_css


class DesktopSidebarLayoutTests(unittest.TestCase):
    def test_theme_css_contains_sidebar_shell_and_toolbar_hooks(self):
        css = build_global_apple_theme_css()

        self.assertIn(".ws-sidebar-block", css)
        self.assertIn(".ws-sidebar-brand", css)
        self.assertIn(".ws-sidebar-recent-item", css)
        self.assertIn(".ws-page-toolbar", css)

    def test_app_routes_desktop_navigation_through_helper(self):
        app_source = Path("app.py").read_text(encoding="utf-8", errors="ignore")

        self.assertIn("def render_desktop_sidebar_navigation()", app_source)
        self.assertIn("selected_module, selected_page = render_desktop_sidebar_navigation()", app_source)
        self.assertIn("record_recent_visit(st.session_state, selected_module, selected_page)", app_source)
        self.assertIn("快速跳转", app_source)


if __name__ == "__main__":
    unittest.main()
```

Append these assertions to the existing theme suite in `tests/eastmoney_author_tracker/test_ui.py`:

```python
    def test_build_global_apple_theme_css_contains_sidebar_navigation_shell_hooks(self):
        css = build_global_apple_theme_css()

        self.assertIn(".ws-sidebar-brand", css)
        self.assertIn(".ws-sidebar-block", css)
        self.assertIn(".ws-sidebar-recent-item", css)
        self.assertIn(".ws-page-toolbar", css)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/test_desktop_sidebar_layout.py tests/eastmoney_author_tracker/test_ui.py -k "sidebar or theme" -v`

Expected: FAIL because `app.py` does not yet define `render_desktop_sidebar_navigation()` and the theme CSS does not yet include the new shell selectors.

- [ ] **Step 3: Add the theme hooks and refactor the desktop navigation branch**

Add these selectors inside `build_global_apple_theme_css()` in `src/apple_theme.py` near the existing sidebar rules:

```python
.ws-sidebar-brand {{
    padding: 1rem 1rem 0.9rem 1rem;
    border-radius: 22px;
    background: linear-gradient(135deg, rgba(212, 175, 55, 0.14) 0%, rgba(255,255,255,0.04) 100%);
    border: 1px solid rgba(212, 175, 55, 0.22);
    margin-bottom: 0.85rem;
}}

.ws-sidebar-block {{
    padding: 0.9rem 1rem;
    border-radius: 20px;
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.06);
    margin-bottom: 0.8rem;
}}

.ws-sidebar-kicker {{
    color: rgba(255,255,255,0.68);
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    margin-bottom: 0.45rem;
}}

.ws-sidebar-helper {{
    color: rgba(255,255,255,0.66);
    font-size: 0.82rem;
    line-height: 1.6;
}}

.ws-sidebar-recent-item {{
    display: block;
    padding: 0.38rem 0;
    color: rgba(255,255,255,0.74) !important;
    text-decoration: none;
    border-bottom: 1px solid rgba(255,255,255,0.06);
}}

.ws-page-toolbar {{
    margin: 0.55rem 0 1rem 0;
    padding: 1rem 1rem 0.35rem 1rem;
    border-radius: 22px;
    background: linear-gradient(180deg, rgba(255,255,255,0.96) 0%, rgba(248,250,248,0.96) 100%);
    border: 1px solid rgba(27, 38, 59, 0.08);
    box-shadow: 0 8px 24px rgba(27, 38, 59, 0.05);
}}
```

Update the navigation imports and helper functions in `app.py`:

```python
from src.navigation_config import (
    DECISION_DAILY_RECO_PAGE_LABEL,
    DECISION_ML_PAGE_LABEL,
    DECISION_PAGE_OPTIONS,
    DECISION_RECO_EVAL_PAGE_LABEL,
    DECISION_TODAY_PAGE_LABEL,
    ETF_FUND_MONITOR_PAGE_LABEL,
    ETF_MAIN_PAGE_LABEL,
    ETF_PAGE_OPTIONS,
    ETF_RATIO_PAGE_LABEL,
    ETF_TREND_PAGE_LABEL,
    ETF_WIDE_INDEX_PAGE_LABEL,
    MACRO_DEPOSIT_PAGE_LABEL,
    MACRO_INDEX_MONITOR_PAGE_LABEL,
    MACRO_MAIN_PAGE_LABEL,
    MACRO_PAGE_OPTIONS,
    MONEY_FLOW_PAGE_LABEL,
    MONEY_FUND_HOT_PAGE_LABEL,
    MONEY_HOTMONEY_PAGE_LABEL,
    MONEY_LIMITUP_PAGE_LABEL,
    MONEY_PAGE_OPTIONS,
    MONEY_VOLUME_PAGE_LABEL,
    STOCK_COMPANY_SCREENER_LABEL,
    STOCK_PAGE_OPTIONS,
    STOCK_SECURITY_SEARCH_LABEL,
    STOCK_TECH_PICKER_LABEL,
)
from src.sidebar_navigation import (
    get_default_shortcuts,
    get_module_by_label,
    get_module_label_for_page,
    get_module_labels,
    get_page_labels,
    get_recent_visits,
    record_recent_visit,
)
```

```python
def render_desktop_sidebar_navigation() -> tuple[str, str]:
    st.sidebar.markdown("---")
    st.sidebar.markdown(
        (
            '<div class="ws-sidebar-brand">'
            '<div class="ws-sidebar-kicker">NAVIGATION</div>'
            '<div style="font-size:1.3rem;font-weight:800;">WealthSpark</div>'
            '<div class="ws-sidebar-helper">先选模块，再选页面；具体筛选会显示在页面标题下方。</div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )

    quick_jump_options = [""] + [
        page_label
        for module_label in get_module_labels()
        for page_label in get_page_labels(module_label)
    ]
    quick_jump = st.sidebar.selectbox(
        "快速跳转",
        options=quick_jump_options,
        index=0,
        key="sidebar_quick_jump",
        format_func=lambda value: value or "选择页面…",
    )
    if quick_jump:
        target_module = get_module_label_for_page(quick_jump)
        target_module_config = get_module_by_label(target_module)
        st.session_state["sidebar_nav_group"] = target_module
        st.session_state[target_module_config.session_key] = quick_jump

    st.sidebar.markdown('<div class="ws-sidebar-block"><div class="ws-sidebar-kicker">MODULES</div></div>', unsafe_allow_html=True)
    selected_module = st.sidebar.radio(
        "选择模块",
        get_module_labels(),
        key="sidebar_nav_group",
        label_visibility="collapsed",
    )
    module_config = get_module_by_label(selected_module)

    st.sidebar.markdown('<div class="ws-sidebar-block"><div class="ws-sidebar-kicker">PAGES</div></div>', unsafe_allow_html=True)
    selected_page = st.sidebar.radio(
        "当前页面",
        get_page_labels(selected_module),
        key=module_config.session_key,
        label_visibility="collapsed",
    )

    record_recent_visit(st.session_state, selected_module, selected_page)
    recent_visits = get_recent_visits(st.session_state)
    if recent_visits:
        st.sidebar.markdown("**最近访问**")
        for item in recent_visits:
            st.sidebar.markdown(
                f'<span class="ws-sidebar-recent-item">{item["module"]} / {item["page"]}</span>',
                unsafe_allow_html=True,
            )

    st.sidebar.markdown("**快捷入口**")
    for shortcut in get_default_shortcuts():
        if st.sidebar.button(shortcut, use_container_width=True, key=f"sidebar_shortcut_{shortcut}"):
            target_module = get_module_label_for_page(shortcut)
            target_module_config = get_module_by_label(target_module)
            st.session_state["sidebar_nav_group"] = target_module
            st.session_state[target_module_config.session_key] = shortcut
            st.rerun()

    return selected_module, selected_page
```

Update the security jump helpers:

```python
    if open_tab == "security":
        st.session_state["sidebar_nav_group"] = "个股"
        st.session_state["stock_subpage"] = STOCK_SECURITY_SEARCH_LABEL
        st.session_state["jump_to_security_tab"] = True
```

```python
    st.session_state["sidebar_nav_group"] = "个股"
    st.session_state["stock_subpage"] = STOCK_SECURITY_SEARCH_LABEL
```

Replace the current desktop radio block near `# ===== 方案B进阶版` with:

```python
    selected_module, selected_page = render_desktop_sidebar_navigation()

    if selected_module == "决策":
        st.caption(f"当前位置：决策 / {selected_page}")
        if selected_page == DECISION_TODAY_PAGE_LABEL:
            render_commercial_mvp_tab()
        elif selected_page == DECISION_DAILY_RECO_PAGE_LABEL:
            render_daily_trend_reco_tab()
        elif selected_page == DECISION_RECO_EVAL_PAGE_LABEL:
            render_reco_effectiveness_tracking_panel()
        elif selected_page == DECISION_ML_PAGE_LABEL:
            render_ml_prediction_upgrade_tab()

    elif selected_module == "基金":
        st.caption(f"当前位置：基金 / {selected_page}")
        if selected_page == ETF_MAIN_PAGE_LABEL:
            render_etf_tab()
        elif selected_page == ETF_RATIO_PAGE_LABEL:
            render_etf_category_ratio_tab()
        elif selected_page == ETF_TREND_PAGE_LABEL:
            render_etf_trend_tab()
        elif selected_page == ETF_FUND_MONITOR_PAGE_LABEL:
            render_fund_monitor_tab()
        elif selected_page == ETF_WIDE_INDEX_PAGE_LABEL:
            render_wide_index_tab()

    elif selected_module == "个股":
        st.caption(f"当前位置：个股 / {selected_page}")
        if selected_page == STOCK_SECURITY_SEARCH_LABEL:
            render_security_search_tab()
        elif selected_page == STOCK_COMPANY_SCREENER_LABEL:
            render_company_screener_tab()
        elif selected_page == FACTOR_WORKBENCH_PAGE_LABEL:
            render_factor_workbench_tab()
        elif selected_page == TRACKING_PAGE_LABEL:
            render_author_tracking_tab()
        elif selected_page == STOCK_TECH_PICKER_LABEL:
            render_tech_picker_tab()

    elif selected_module == "资金":
        st.caption(f"当前位置：资金 / {selected_page}")
        if selected_page == MONEY_FLOW_PAGE_LABEL:
            render_moneyflow_tab()
        elif selected_page == MONEY_VOLUME_PAGE_LABEL:
            render_volume_tab()
        elif selected_page == MONEY_FUND_HOT_PAGE_LABEL:
            render_fund_hot_stocks_tab()
        elif selected_page == MONEY_LIMITUP_PAGE_LABEL:
            render_limitup_monitor_tab()
        elif selected_page == MONEY_HOTMONEY_PAGE_LABEL:
            render_hotmoney_tab()

    else:
        st.caption(f"当前位置：宏观 / {selected_page}")
        if selected_page == MACRO_MAIN_PAGE_LABEL:
            render_macro_tab()
        elif selected_page == MACRO_DEPOSIT_PAGE_LABEL:
            render_etf_deposit_tab()
        elif selected_page == MACRO_INDEX_MONITOR_PAGE_LABEL:
            render_index_monitor_tab()
```

- [ ] **Step 4: Run the desktop sidebar tests to verify they pass**

Run: `python -m pytest tests/test_sidebar_navigation.py tests/test_desktop_sidebar_layout.py tests/eastmoney_author_tracker/test_ui.py -k "sidebar or theme" -v`

Expected: PASS with the new layout test and the added theme assertions passing.

- [ ] **Step 5: Commit the desktop sidebar shell**

```bash
git add app.py src/apple_theme.py src/navigation_config.py src/sidebar_navigation.py tests/test_desktop_sidebar_layout.py tests/eastmoney_author_tracker/test_ui.py tests/test_sidebar_navigation.py
git commit -m "feat: add desktop sidebar navigation shell"
```

### Task 4: Move `render_volume_tab` and `render_etf_trend_tab` filters into page-local toolbars

**Files:**
- Modify: `app.py:3284-3370`
- Modify: `app.py:6538-6689`
- Modify: `app.py:85`
- Create: `tests/test_page_toolbar_layout.py`

- [ ] **Step 1: Write the failing toolbar regression tests for the first two pages**

```python
import re
import unittest
from pathlib import Path


APP_SOURCE = Path("app.py").read_text(encoding="utf-8", errors="ignore")


def function_chunk(name: str) -> str:
    match = re.search(rf"def {name}\(.*?(?=^def |\Z)", APP_SOURCE, flags=re.S | re.M)
    if not match:
        raise AssertionError(f"Function {name} not found")
    return match.group(0)


class PageToolbarLayoutTests(unittest.TestCase):
    def test_render_volume_tab_no_longer_uses_sidebar(self):
        chunk = function_chunk("render_volume_tab")

        self.assertNotIn("st.sidebar", chunk)
        self.assertIn("ws-page-toolbar", chunk)

    def test_render_etf_trend_tab_no_longer_uses_sidebar(self):
        chunk = function_chunk("render_etf_trend_tab")

        self.assertNotIn("st.sidebar", chunk)
        self.assertIn("更多筛选", chunk)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/test_page_toolbar_layout.py -v`

Expected: FAIL because both functions still contain desktop `st.sidebar` controls.

- [ ] **Step 3: Refactor the two page functions to render desktop toolbars inside the main content area**

Import the new pure helpers in `app.py`:

```python
from src.page_filter_utils import (
    build_secondary_category_options,
    resolve_trend_category_key,
)
```

Replace the desktop branch inside `render_volume_tab()` with:

```python
    else:
        st.markdown('<div class="ws-page-toolbar">', unsafe_allow_html=True)
        toolbar_date_col, toolbar_sector_col = st.columns([1.0, 1.4])
        with toolbar_date_col:
            if vol_min_date == vol_max_date:
                st.info(f"📅 当前数据日期: {vol_min_date}")
                vol_date_range = (vol_min_date, vol_max_date)
            else:
                vol_date_range = st.slider(
                    "选择日期范围（成交量）",
                    min_value=vol_min_date,
                    max_value=vol_max_date,
                    value=(vol_min_date, vol_max_date),
                    format="YYYY-MM-DD",
                    key="vol_date_range",
                )
        with toolbar_sector_col:
            all_sectors = sorted(vol_df["ts_name"].unique())
            selected_sectors = st.multiselect(
                "选择板块",
                options=all_sectors,
                default=all_sectors,
                key="vol_sectors",
            )
        st.markdown('</div>', unsafe_allow_html=True)
```

Move the shared available-date fetch above the `if iphone_mode:` split inside `render_etf_trend_tab()`:

```python
    try:
        available = get_available_dates(limit=1000)
    except Exception as e:
        st.error(f"获取可用日期失败: {e}")
        return

    if not available:
        st.warning("暂无可用交易日数据")
        return

    from datetime import datetime as dt
    all_dates = sorted([dt.strptime(d, "%Y-%m-%d").date() for d in available])
    min_d, max_d = all_dates[0], all_dates[-1]
```

Then replace the desktop branch inside `render_etf_trend_tab()` with:

```python

    else:
        st.markdown('<div class="ws-page-toolbar">', unsafe_allow_html=True)
        toolbar_primary_col, toolbar_secondary_col, toolbar_metric_col = st.columns([1.05, 1.05, 0.95])
        with toolbar_primary_col:
            selected_primary = st.selectbox(
                "一级分类",
                options=primary_options,
                index=0,
                key="trend_primary",
            )
        secondary_options = build_secondary_category_options(selected_primary, category_tree)
        with toolbar_secondary_col:
            if secondary_options:
                selected_secondary = st.selectbox(
                    "二级分类",
                    options=secondary_options,
                    index=0,
                    key="trend_secondary",
                )
            else:
                selected_secondary = None
                st.caption("当前一级分类没有二级分类可选")
        with toolbar_metric_col:
            metric = st.radio(
                "查看指标",
                options=["总份额(亿份)", "总规模(亿元)"],
                index=0,
                key="trend_metric",
            )
        metric_col = "total_share_yi" if "份额" in metric else "total_size_yi"
        category_key = resolve_trend_category_key(selected_primary, selected_secondary, category_tree)
        with st.expander("更多筛选", expanded=False):
            date_range = st.slider(
                "时间范围",
                min_value=min_d,
                max_value=max_d,
                value=(min_d, max_d),
                format="YYYY-MM-DD",
                key="trend_date_range",
            )
        st.markdown('</div>', unsafe_allow_html=True)
```

Keep the iPhone branches untouched so the mobile interaction model does not change.

- [ ] **Step 4: Run the targeted toolbar tests to verify they pass**

Run: `python -m pytest tests/test_page_filter_utils.py tests/test_page_toolbar_layout.py -v`

Expected: PASS with both source-level toolbar assertions passing and the pure helper tests still green.

- [ ] **Step 5: Commit the first page migrations**

```bash
git add app.py src/page_filter_utils.py tests/test_page_filter_utils.py tests/test_page_toolbar_layout.py
git commit -m "feat: move desktop volume and trend filters into page toolbars"
```

### Task 5: Move `render_etf_tab` filters into the page toolbar and finish verification

**Files:**
- Modify: `app.py:5730-5920`
- Modify: `app.py:85`
- Modify: `tests/test_page_toolbar_layout.py`

- [ ] **Step 1: Extend the failing toolbar regression test to cover the ETF main page**

Append this test to `tests/test_page_toolbar_layout.py`:

```python
    def test_render_etf_tab_no_longer_uses_sidebar(self):
        chunk = function_chunk("render_etf_tab")

        self.assertNotIn("st.sidebar", chunk)
        self.assertIn("更多筛选", chunk)
        self.assertIn("build_metric_categories", chunk)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_page_toolbar_layout.py -v`

Expected: FAIL because `render_etf_tab()` still uses `st.sidebar` and does not yet reference the new helper functions.

- [ ] **Step 3: Refactor `render_etf_tab()` to use a page toolbar plus expander**

Add the ETF helper imports in `app.py`:

```python
from src.page_filter_utils import (
    build_metric_categories,
    build_quick_metric_groups,
    build_secondary_category_options,
    resolve_trend_category_key,
)
```

Replace the desktop branch inside `render_etf_tab()` with:

```python
    else:
        st.success(f"✅ 已加载 {len(df)} 条数据记录")
        metric_categories = build_metric_categories(metric_types)

        st.markdown('<div class="ws-page-toolbar">', unsafe_allow_html=True)
        toolbar_category_col, toolbar_metric_col, toolbar_chart_col = st.columns([1.0, 1.35, 0.9])
        with toolbar_category_col:
            if len(metric_categories) > 1:
                selected_category = st.selectbox(
                    "指标分类",
                    options=list(metric_categories.keys()),
                    index=0,
                    key="etf_metric_category",
                )
                available_metrics = metric_categories[selected_category]
            else:
                selected_category = next(iter(metric_categories))
                available_metrics = metric_types
        with toolbar_metric_col:
            selected_metric = st.selectbox(
                "选择具体指标",
                options=available_metrics,
                index=0,
                key="etf_selected_metric",
            )
        with toolbar_chart_col:
            chart_type = st.selectbox(
                "图表类型",
                options=["line", "area", "scatter"],
                format_func=lambda value: {
                    "line": "📈 平滑曲线",
                    "area": "📊 面积图",
                    "scatter": "⚫ 散点图",
                }[value],
                index=0,
                key="etf_chart_type",
            )

        quick_metrics = build_quick_metric_groups(metric_types)
        quick_cols = st.columns(3)
        for idx, (label, metrics) in enumerate(quick_metrics.items()):
            if metrics and quick_cols[idx].button(label, use_container_width=True, key=f"etf_quick_{idx}"):
                selected_metric = metrics[0]
                st.session_state["etf_selected_metric"] = selected_metric
                st.rerun()

        metric_df = df[df["metric_type"] == selected_metric].copy()
        has_aggregate = metric_df["is_aggregate"].any()
        contains_total_market_value = "总市值" in selected_metric if selected_metric else False

        with st.expander("更多筛选", expanded=False):
            if has_aggregate and contains_total_market_value:
                st.info("📊 当前显示所有ETF的总和")
                selected_etfs = None
            else:
                etf_names = sorted(metric_df[metric_df["is_aggregate"] == False]["name"].unique())
                selected_etfs = st.multiselect(
                    "选择ETF",
                    options=etf_names,
                    default=etf_names,
                    key="etf_selected_etfs",
                )

            min_date = metric_df["date"].min().date()
            max_date = metric_df["date"].max().date()
            if min_date == max_date:
                st.info(f"📅 当前数据日期: {min_date}")
                date_range = (min_date, max_date)
            else:
                date_range = st.slider(
                    "选择日期范围",
                    min_value=min_date,
                    max_value=max_date,
                    value=(min_date, max_date),
                    format="YYYY-MM-DD",
                    key="etf_date_range",
                )
        st.markdown('</div>', unsafe_allow_html=True)
```

Do not change the iPhone `st.expander("🔍 ETF筛选条件", expanded=True)` branch.

- [ ] **Step 4: Run the focused regression suite and manual smoke checks**

Run: `python -m pytest tests/test_sidebar_navigation.py tests/test_page_filter_utils.py tests/test_desktop_sidebar_layout.py tests/test_page_toolbar_layout.py tests/eastmoney_author_tracker/test_ui.py -k "sidebar or theme or toolbar" -v`

Expected: PASS with the navigation, toolbar, and theme regressions all green.

Run: `streamlit run app.py --server.headless true`

Expected:
- app boots without a traceback
- desktop sidebar shows brand, module list, current module page list, and recent visits
- `💼 今日机会清单` still renders from the desktop sidebar
- `📊 每日成交量` no longer renders any desktop `st.sidebar` controls
- `📈 主要宽基ETF份额` renders filters in the page toolbar and `更多筛选`
- `📈 ETF分类趋势` renders classification controls in the page toolbar
- `?iphone_mode=1` still uses the existing mobile branch
- links that jump into `🔎 个股/指数查询` still land on the stock page

- [ ] **Step 5: Commit the ETF page migration and verification**

```bash
git add app.py src/apple_theme.py src/navigation_config.py src/sidebar_navigation.py src/page_filter_utils.py tests/test_desktop_sidebar_layout.py tests/test_page_filter_utils.py tests/test_page_toolbar_layout.py tests/test_sidebar_navigation.py tests/eastmoney_author_tracker/test_ui.py
git commit -m "feat: move desktop page filters out of the sidebar"
```
