# Sidebar Tree Navigation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current desktop sidebar quick-jump/radio layout with the approved `search + accordion tree + recent visits` navigation model while preserving existing page routing, `security_query` deep links, and the separate mobile branch.

**Architecture:** Keep `src/sidebar_navigation.py` as the canonical navigation catalog and extend it with stable-id lookup, search ranking, recent-visit normalization, and expanded-module resolution helpers. Keep the Streamlit rendering entrypoint in `app.py`, but replace the current desktop sidebar body with state-driven helper functions that render the search mode, tree mode, and recent-visit shortcuts. Extend `src.apple_theme.py` with tree-specific CSS hooks so the new structure can be styled without changing page business logic.

**Tech Stack:** Python, Streamlit, existing `src.apple_theme` CSS generator, `unittest`-style tests executed with `pytest`

---

## File Map

- Modify: `src/sidebar_navigation.py`
  - Keep the navigation catalog, but add stable-id lookup helpers, search ranking, legacy recent-visit upgrade logic, and expanded-module resolution helpers.
- Create: `tests/test_sidebar_navigation.py`
  - Unit tests for pure sidebar navigation logic and state transforms.
- Modify: `src/apple_theme.py`
  - Add CSS hooks for the tree shell, module rows, active page row, search results, empty state, and recent-visit buttons.
- Create: `tests/test_sidebar_tree_theme.py`
  - CSS regression checks for the new tree-navigation selectors.
- Modify: `app.py`
  - Preserve the existing routing and desktop/mobile split, but replace the desktop sidebar body with search, accordion tree, recent visits, and deep-link-aware expanded-module state.
- Create: `tests/test_sidebar_tree_layout.py`
  - Source-level regression checks for the desktop sidebar renderer and the `security_query` deep-link glue.

### Task 1: Extend the sidebar navigation helpers with stable ids, search, and recent-visit normalization

**Files:**
- Modify: `src/sidebar_navigation.py`
- Test: `tests/test_sidebar_navigation.py`

- [ ] **Step 1: Write the failing unit tests for sidebar lookup, search, and state helpers**

```python
import unittest

from src.sidebar_navigation import (
    MAX_RECENT_PAGES,
    RECENT_VISITS_KEY,
    get_module_by_id,
    get_module_id_for_page_id,
    get_page_by_id,
    get_recent_visits,
    record_recent_visit,
    resolve_expanded_module_id,
    search_sidebar_pages,
)


class SidebarNavigationLogicTests(unittest.TestCase):
    def test_lookup_helpers_return_module_and_page_metadata_by_id(self):
        module = get_module_by_id("stock")
        page = get_page_by_id("security_search")

        self.assertEqual(module.id, "stock")
        self.assertEqual(module.session_key, "stock_subpage")
        self.assertEqual(page.id, "security_search")
        self.assertEqual(page.toolbar_variant, "heavy")
        self.assertEqual(get_module_id_for_page_id("security_search"), "stock")

    def test_search_sidebar_pages_prefers_page_hits_over_module_hits(self):
        results = search_sidebar_pages("security")

        self.assertGreaterEqual(len(results), 1)
        self.assertEqual(results[0].page_id, "security_search")

    def test_search_sidebar_pages_returns_pages_when_module_keyword_matches(self):
        results = search_sidebar_pages("stock")

        self.assertTrue(results)
        self.assertTrue(all(result.module_id == "stock" for result in results))
        self.assertTrue(any(result.page_id == "security_search" for result in results))

    def test_record_recent_visit_deduplicates_trims_and_exposes_labels(self):
        session_state = {}

        record_recent_visit(session_state, "decision", "commercial_mvp")
        record_recent_visit(session_state, "fund", "etf_main")
        record_recent_visit(session_state, "stock", "security_search")
        record_recent_visit(session_state, "money", "moneyflow")
        record_recent_visit(session_state, "macro", "macro")
        record_recent_visit(session_state, "money", "hotmoney")
        record_recent_visit(session_state, "stock", "security_search")

        recent = get_recent_visits(session_state)

        self.assertEqual(len(recent), MAX_RECENT_PAGES)
        self.assertEqual(recent[0]["page_id"], "security_search")
        self.assertEqual(recent[0]["module_id"], "stock")
        self.assertIn("page_label", recent[0])
        self.assertIn("module_label", recent[0])
        self.assertEqual(sum(1 for item in recent if item["page_id"] == "security_search"), 1)

    def test_get_recent_visits_upgrades_legacy_label_entries(self):
        stock_module = get_module_by_id("stock")
        stock_page = get_page_by_id("security_search")
        session_state = {
            RECENT_VISITS_KEY: [
                {"module": stock_module.label, "page": stock_page.label},
            ]
        }

        recent = get_recent_visits(session_state)

        self.assertEqual(recent[0]["module_id"], "stock")
        self.assertEqual(recent[0]["page_id"], "security_search")
        self.assertEqual(recent[0]["page_label"], stock_page.label)

    def test_resolve_expanded_module_id_defaults_to_active_page_module(self):
        self.assertEqual(resolve_expanded_module_id("security_search", None), "stock")
        self.assertEqual(resolve_expanded_module_id("security_search", "fund"), "fund")
        self.assertEqual(resolve_expanded_module_id("security_search", "unknown"), "stock")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/test_sidebar_navigation.py -v`

Expected: FAIL with `ImportError` because `get_module_by_id`, `get_page_by_id`, `search_sidebar_pages`, and `resolve_expanded_module_id` do not exist yet, and `record_recent_visit` still uses label payloads.

- [ ] **Step 3: Update `src/sidebar_navigation.py` with stable-id helpers, search ranking, and upgraded recent-visit handling**

Update the imports and constants at the top of `src/sidebar_navigation.py`:

```python
from dataclasses import dataclass
import re
from typing import Literal, MutableMapping

ToolbarVariant = Literal["light", "standard", "heavy"]

RECENT_VISITS_KEY = "sidebar_recent_pages"
MAX_RECENT_PAGES = 6
```

Add this dataclass directly after `SidebarModule`:

```python
@dataclass(frozen=True)
class SidebarSearchResult:
    module_id: str
    module_label: str
    page_id: str
    page_label: str
    description: str
    score: tuple[int, int]
```

Replace the helper block beginning at `MODULE_BY_LABEL = ...` with:

```python
MODULE_BY_ID = {module.id: module for module in SIDEBAR_MODULES}
MODULE_BY_LABEL = {module.label: module for module in SIDEBAR_MODULES}
PAGE_BY_ID = {
    page.id: page
    for module in SIDEBAR_MODULES
    for page in module.pages
}
PAGE_ID_TO_MODULE_ID = {
    page.id: module.id
    for module in SIDEBAR_MODULES
    for page in module.pages
}
PAGE_TO_MODULE = {
    page.label: module.label
    for module in SIDEBAR_MODULES
    for page in module.pages
}
DEFAULT_SHORTCUT_PAGE_IDS = ["commercial_mvp", "security_search", "moneyflow"]


def _normalize_search_text(value: str) -> str:
    text = str(value or "").lower().replace("/", " ").replace("-", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _recent_visit_payload(module: SidebarModule, page: SidebarPage) -> dict[str, str]:
    return {
        "module_id": module.id,
        "module_label": module.label,
        "page_id": page.id,
        "page_label": page.label,
    }


def _coerce_recent_visit(item: object) -> dict[str, str] | None:
    if not isinstance(item, dict):
        return None

    if "module_id" in item and "page_id" in item:
        module_id = str(item["module_id"])
        page_id = str(item["page_id"])
        if module_id not in MODULE_BY_ID or page_id not in PAGE_BY_ID:
            return None
        if PAGE_ID_TO_MODULE_ID[page_id] != module_id:
            return None
        return _recent_visit_payload(MODULE_BY_ID[module_id], PAGE_BY_ID[page_id])

    if "module" in item and "page" in item:
        module_label = str(item["module"])
        page_label = str(item["page"])
        if module_label not in MODULE_BY_LABEL:
            return None
        module = MODULE_BY_LABEL[module_label]
        for page in module.pages:
            if page.label == page_label:
                return _recent_visit_payload(module, page)
        return None

    return None


def _search_rank(module: SidebarModule, page: SidebarPage, query: str) -> tuple[int, int] | None:
    page_label = _normalize_search_text(page.label)
    page_id = _normalize_search_text(page.id)
    description = _normalize_search_text(page.description)
    module_label = _normalize_search_text(module.label)
    module_id = _normalize_search_text(module.id)

    if page_label.startswith(query) or page_id.startswith(query):
        return (0, len(page_label) or len(page_id))
    if query in page_label or query in page_id:
        return (1, len(page_label) or len(page_id))
    if query in module_label or query in module_id:
        return (2, len(page_label) or len(page_id))
    if query in description:
        return (3, len(description) or len(page_label))
    return None


def get_module_labels() -> list[str]:
    return [module.label for module in SIDEBAR_MODULES]


def get_module_by_id(module_id: str) -> SidebarModule:
    return MODULE_BY_ID[module_id]


def get_module_by_label(module_label: str) -> SidebarModule:
    return MODULE_BY_LABEL[module_label]


def get_page_labels(module_label: str) -> list[str]:
    return [page.label for page in MODULE_BY_LABEL[module_label].pages]


def get_page_by_id(page_id: str) -> SidebarPage:
    return PAGE_BY_ID[page_id]


def get_page_by_label(module_label: str, page_label: str) -> SidebarPage:
    module = MODULE_BY_LABEL[module_label]
    for page in module.pages:
        if page.label == page_label:
            return page
    raise KeyError(f"Unknown page {page_label!r} for module {module_label!r}")


def get_module_id_for_page_id(page_id: str) -> str:
    return PAGE_ID_TO_MODULE_ID[page_id]


def get_module_label_for_page(page_label: str) -> str:
    return PAGE_TO_MODULE[page_label]


def get_default_shortcuts() -> list[str]:
    return [get_page_by_id(page_id).label for page_id in DEFAULT_SHORTCUT_PAGE_IDS]


def ensure_sidebar_state(session_state: MutableMapping[str, object]) -> None:
    if RECENT_VISITS_KEY not in session_state:
        session_state[RECENT_VISITS_KEY] = []


def search_sidebar_pages(query: str) -> list[SidebarSearchResult]:
    normalized_query = _normalize_search_text(query)
    if not normalized_query:
        return []

    results: list[SidebarSearchResult] = []
    for module in SIDEBAR_MODULES:
        for page in module.pages:
            score = _search_rank(module, page, normalized_query)
            if score is None:
                continue
            results.append(
                SidebarSearchResult(
                    module_id=module.id,
                    module_label=module.label,
                    page_id=page.id,
                    page_label=page.label,
                    description=page.description,
                    score=score,
                )
            )

    return sorted(results, key=lambda item: (item.score[0], item.score[1], item.page_id))


def resolve_expanded_module_id(active_page_id: str, requested_module_id: str | None) -> str:
    if requested_module_id in MODULE_BY_ID:
        return requested_module_id
    return get_module_id_for_page_id(active_page_id)


def record_recent_visit(session_state: MutableMapping[str, object], module_id: str, page_id: str) -> None:
    ensure_sidebar_state(session_state)

    module = get_module_by_id(module_id)
    page = get_page_by_id(page_id)
    if PAGE_ID_TO_MODULE_ID[page_id] != module_id:
        raise KeyError(f"Page {page_id!r} does not belong to module {module_id!r}")

    latest = _recent_visit_payload(module, page)
    existing: list[dict[str, str]] = []
    for raw_item in session_state[RECENT_VISITS_KEY]:
        normalized = _coerce_recent_visit(raw_item)
        if normalized is None or normalized["page_id"] == page_id:
            continue
        existing.append(normalized)

    session_state[RECENT_VISITS_KEY] = [latest] + existing[: MAX_RECENT_PAGES - 1]


def get_recent_visits(session_state: MutableMapping[str, object]) -> list[dict[str, str]]:
    ensure_sidebar_state(session_state)

    normalized_items: list[dict[str, str]] = []
    seen_page_ids: set[str] = set()
    for raw_item in session_state[RECENT_VISITS_KEY]:
        normalized = _coerce_recent_visit(raw_item)
        if normalized is None:
            continue
        if normalized["page_id"] in seen_page_ids:
            continue
        normalized_items.append(normalized)
        seen_page_ids.add(normalized["page_id"])
        if len(normalized_items) == MAX_RECENT_PAGES:
            break

    session_state[RECENT_VISITS_KEY] = normalized_items
    return [dict(item) for item in normalized_items]
```

- [ ] **Step 4: Run the unit tests to verify the new helpers pass**

Run: `python -m pytest tests/test_sidebar_navigation.py -v`

Expected: PASS with 6 passed

- [ ] **Step 5: Commit the navigation helper foundation**

```bash
git add src/sidebar_navigation.py tests/test_sidebar_navigation.py
git commit -m "feat: add sidebar tree navigation helpers"
```

### Task 2: Add sidebar tree theme hooks before changing the desktop renderer

**Files:**
- Modify: `src/apple_theme.py`
- Test: `tests/test_sidebar_tree_theme.py`

- [ ] **Step 1: Write the failing CSS regression test for the new tree-navigation selectors**

```python
import unittest

from src.apple_theme import build_global_apple_theme_css


class SidebarTreeThemeTests(unittest.TestCase):
    def test_tree_navigation_css_hooks_exist(self):
        css = build_global_apple_theme_css()

        self.assertIn(".ws-sidebar-tree", css)
        self.assertIn(".ws-sidebar-page-description", css)
        self.assertIn(".ws-sidebar-search-result-meta", css)
        self.assertIn(".ws-sidebar-empty", css)
        self.assertIn("st-key-ws-sidebar-module-", css)
        self.assertIn("st-key-ws-sidebar-page-", css)
        self.assertIn("st-key-ws-sidebar-search-result-", css)
        self.assertIn("st-key-ws-sidebar-recent-link-", css)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the CSS test to verify it fails**

Run: `python -m pytest tests/test_sidebar_tree_theme.py -v`

Expected: FAIL because `build_global_apple_theme_css()` does not yet emit the tree-navigation selectors.

- [ ] **Step 3: Append tree-navigation styles to `build_global_apple_theme_css()` in `src/apple_theme.py`**

Add the following block immediately after the existing `.ws-sidebar-recent-page` rules in `src/apple_theme.py`:

```python
[data-testid="stSidebar"] .ws-sidebar-tree {{
    display: flex;
    flex-direction: column;
    gap: 0.45rem;
    margin: 0.3rem 0 0.9rem 0;
}}

[data-testid="stSidebar"] .ws-sidebar-page-description {{
    margin: 0.2rem 0 0.4rem 0.9rem;
    color: rgba(248, 250, 252, 0.72) !important;
    font-size: 0.79rem;
    line-height: 1.45;
}}

[data-testid="stSidebar"] .ws-sidebar-search-result-meta {{
    color: rgba(248, 250, 252, 0.62) !important;
    font-size: 0.72rem;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    margin-bottom: 0.18rem;
}}

[data-testid="stSidebar"] .ws-sidebar-search-result-title {{
    color: var(--ws-text-inverse) !important;
    font-size: 0.92rem;
    font-weight: 700;
    line-height: 1.35;
}}

[data-testid="stSidebar"] .ws-sidebar-search-result-copy {{
    color: rgba(248, 250, 252, 0.74) !important;
    font-size: 0.8rem;
    line-height: 1.45;
    margin-top: 0.18rem;
}}

[data-testid="stSidebar"] .ws-sidebar-empty {{
    padding: 0.92rem 1rem;
    border-radius: 18px;
    background: rgba(255, 255, 255, 0.04);
    border: 1px dashed rgba(255, 255, 255, 0.16);
    color: rgba(248, 250, 252, 0.72) !important;
    font-size: 0.82rem;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] button {{
    min-height: 2.7rem !important;
    justify-content: flex-start !important;
    padding: 0.72rem 0.9rem !important;
    border-radius: 18px !important;
    background: rgba(255, 255, 255, 0.04) !important;
    border: 1px solid rgba(255, 255, 255, 0.06) !important;
    color: var(--ws-text-inverse) !important;
    font-weight: 700 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-expanded"] button {{
    background: linear-gradient(135deg, rgba(212, 175, 55, 0.18) 0%, rgba(255, 255, 255, 0.06) 100%) !important;
    border-color: rgba(212, 175, 55, 0.26) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-current"] button {{
    box-shadow: inset 2px 0 0 var(--ws-color-primary);
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] button {{
    min-height: 2.2rem !important;
    justify-content: flex-start !important;
    padding: 0.55rem 0.9rem 0.55rem 1.4rem !important;
    border-radius: 14px !important;
    background: transparent !important;
    border: 1px solid transparent !important;
    color: rgba(248, 250, 252, 0.84) !important;
    font-weight: 500 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-active"] button {{
    background: rgba(255, 255, 255, 0.08) !important;
    border-color: rgba(212, 175, 55, 0.22) !important;
    color: var(--ws-text-inverse) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] button {{
    min-height: 3.5rem !important;
    justify-content: flex-start !important;
    padding: 0.85rem 0.95rem !important;
    border-radius: 18px !important;
    background: rgba(255, 255, 255, 0.05) !important;
    border: 1px solid rgba(255, 255, 255, 0.08) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] button {{
    min-height: 2rem !important;
    justify-content: flex-start !important;
    padding: 0.45rem 0.75rem !important;
    border-radius: 14px !important;
    background: rgba(255, 255, 255, 0.02) !important;
    border: 1px solid rgba(255, 255, 255, 0.04) !important;
    color: rgba(248, 250, 252, 0.78) !important;
    font-size: 0.82rem !important;
}}
```

- [ ] **Step 4: Run the CSS regression test to verify it passes**

Run: `python -m pytest tests/test_sidebar_tree_theme.py -v`

Expected: PASS with 1 passed

- [ ] **Step 5: Commit the sidebar tree theme hooks**

```bash
git add src/apple_theme.py tests/test_sidebar_tree_theme.py
git commit -m "feat: add sidebar tree navigation styles"
```

### Task 3: Replace the desktop sidebar renderer with search, accordion tree, and recent visits

**Files:**
- Modify: `app.py`
- Test: `tests/test_sidebar_tree_layout.py`

- [ ] **Step 1: Write the failing source-level regression tests for the desktop tree renderer and stock deep-link behavior**

```python
import re
import unittest
from pathlib import Path


APP_SOURCE = Path("app.py").read_text(encoding="utf-8", errors="ignore")


def function_chunk(name: str) -> str:
    match = re.search(rf"def {name}\\(.*?(?=^def |\\Z)", APP_SOURCE, flags=re.S | re.M)
    if not match:
        raise AssertionError(f"Function {name} not found")
    return match.group(0)


class SidebarTreeLayoutTests(unittest.TestCase):
    def test_render_desktop_sidebar_navigation_uses_search_and_expanded_state(self):
        chunk = function_chunk("render_desktop_sidebar_navigation")

        self.assertIn("sidebar_search_query", chunk)
        self.assertIn("sidebar_expanded_module_id", chunk)
        self.assertIn("search_sidebar_pages", chunk)
        self.assertIn("get_recent_visits(st.session_state)", chunk)
        self.assertNotIn("sidebar_quick_jump_", chunk)
        self.assertNotIn('st.sidebar.selectbox(\n        "快速跳转"', chunk)

    def test_security_jump_helpers_expand_the_stock_module(self):
        hydrate_chunk = function_chunk("hydrate_security_jump_from_query_params")
        trigger_chunk = function_chunk("trigger_security_tab_jump_if_needed")

        self.assertIn('st.session_state["sidebar_expanded_module_id"] = "stock"', hydrate_chunk)
        self.assertIn('st.session_state["sidebar_expanded_module_id"] = "stock"', trigger_chunk)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the layout regression tests to verify they fail**

Run: `python -m pytest tests/test_sidebar_tree_layout.py -v`

Expected: FAIL because the current desktop sidebar still contains `sidebar_quick_jump`, radio-based module/page selection, and the stock deep-link helpers do not set `sidebar_expanded_module_id`.

- [ ] **Step 3: Update `app.py` to use the new tree-navigation helpers and preserve current routing**

Replace the `src.sidebar_navigation` import block in `app.py` with:

```python
from src.sidebar_navigation import (
    SIDEBAR_MODULES,
    SidebarModule,
    SidebarPage,
    get_module_by_id,
    get_module_by_label,
    get_module_id_for_page_id,
    get_page_by_id,
    get_page_by_label,
    get_recent_visits,
    record_recent_visit,
    resolve_expanded_module_id,
    search_sidebar_pages,
)
```

Update the stock jump helpers in `app.py`:

```python
def hydrate_security_jump_from_query_params() -> None:
    security_query = get_query_param_value("security_query").strip()
    if not security_query:
        return

    jump_nonce = get_query_param_value("jump_nonce").strip()
    open_tab = get_query_param_value("open_tab").strip().lower()
    security_type = get_query_param_value("security_type").strip().lower()

    if jump_nonce and jump_nonce == st.session_state.get("last_consumed_jump_nonce"):
        return

    st.session_state["security_search_keyword"] = security_query
    if security_type == "stock":
        st.session_state["security_search_type"] = "股票"
    elif security_type == "index":
        st.session_state["security_search_type"] = "指数"

    if open_tab == "security":
        st.session_state["sidebar_nav_group"] = get_module_by_id("stock").label
        st.session_state["stock_subpage"] = STOCK_SECURITY_SEARCH_LABEL
        st.session_state["sidebar_expanded_module_id"] = "stock"
        st.session_state["jump_to_security_tab"] = True

    if jump_nonce:
        st.session_state["last_consumed_jump_nonce"] = jump_nonce


def trigger_security_tab_jump_if_needed() -> None:
    if not st.session_state.get("jump_to_security_tab", False):
        return

    st.session_state["sidebar_nav_group"] = get_module_by_id("stock").label
    st.session_state["stock_subpage"] = STOCK_SECURITY_SEARCH_LABEL
    st.session_state["sidebar_expanded_module_id"] = "stock"
    st.session_state["jump_to_security_tab"] = False
```

Insert these helper functions directly above `render_desktop_sidebar_navigation()`:

```python
def _resolve_active_sidebar_target() -> tuple[SidebarModule, SidebarPage]:
    selected_module_label = st.session_state.get("sidebar_nav_group")
    module_labels = [module.label for module in SIDEBAR_MODULES]

    if selected_module_label not in module_labels:
        selected_module = SIDEBAR_MODULES[0]
        st.session_state["sidebar_nav_group"] = selected_module.label
    else:
        selected_module = get_module_by_label(selected_module_label)

    page_labels = [page.label for page in selected_module.pages]
    selected_page_label = st.session_state.get(selected_module.session_key)
    if selected_page_label not in page_labels:
        selected_page = selected_module.pages[0]
        st.session_state[selected_module.session_key] = selected_page.label
    else:
        selected_page = get_page_by_label(selected_module.label, selected_page_label)

    return selected_module, selected_page


def _select_sidebar_page(module_id: str, page_id: str, *, clear_search: bool = False) -> None:
    module = get_module_by_id(module_id)
    page = get_page_by_id(page_id)

    st.session_state["sidebar_nav_group"] = module.label
    st.session_state[module.session_key] = page.label
    st.session_state["sidebar_expanded_module_id"] = module.id
    if clear_search:
        st.session_state["sidebar_search_query"] = ""

    record_recent_visit(st.session_state, module.id, page.id)
```

Replace `render_desktop_sidebar_navigation()` with:

```python
def render_desktop_sidebar_navigation() -> tuple[str, str]:
    selected_module, selected_page = _resolve_active_sidebar_target()
    record_recent_visit(st.session_state, selected_module.id, selected_page.id)

    expanded_module_id = resolve_expanded_module_id(
        selected_page.id,
        st.session_state.get("sidebar_expanded_module_id"),
    )
    st.session_state["sidebar_expanded_module_id"] = expanded_module_id

    st.sidebar.markdown(
        """
        <div class="ws-sidebar-brand">
            <span class="ws-sidebar-brand-kicker">WealthSpark</span>
            <h2>Navigation</h2>
            <p>Direct search, tree browsing, and lightweight recent-page return paths.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.sidebar.markdown(
        """
        <div class="ws-sidebar-block">
            <div class="ws-sidebar-block-title">Search</div>
            <p class="ws-sidebar-block-copy">Search a module, page, or stable keyword. Active search replaces the tree area.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    search_query = st.sidebar.text_input(
        "Search pages",
        placeholder="security / stock / macro",
        key="sidebar_search_query",
        label_visibility="collapsed",
    )

    st.sidebar.markdown(
        """
        <div class="ws-sidebar-block">
            <div class="ws-sidebar-block-title">Tree</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar.container(key="ws-sidebar-tree"):
        if search_query.strip():
            results = search_sidebar_pages(search_query)
            if not results:
                st.sidebar.markdown(
                    '<div class="ws-sidebar-empty">No matching pages. Try another keyword.</div>',
                    unsafe_allow_html=True,
                )
            for index, result in enumerate(results):
                with st.sidebar.container(key=f"ws-sidebar-search-result-{result.page_id}-{index}"):
                    st.sidebar.markdown(
                        f"""
                        <div class="ws-sidebar-search-result-meta">{escape(result.module_label)}</div>
                        <div class="ws-sidebar-search-result-title">{escape(result.page_label)}</div>
                        <div class="ws-sidebar-search-result-copy">{escape(result.description)}</div>
                        """,
                        unsafe_allow_html=True,
                    )
                    if st.sidebar.button(
                        "Open page",
                        key=f"sidebar_search_open_{result.page_id}_{index}",
                        use_container_width=True,
                    ):
                        _select_sidebar_page(result.module_id, result.page_id, clear_search=True)
                        st.rerun()
        else:
            current_module_id = get_module_id_for_page_id(selected_page.id)
            for module in SIDEBAR_MODULES:
                is_current_module = module.id == current_module_id
                is_expanded = module.id == expanded_module_id
                container_key = f"ws-sidebar-module-{module.id}"
                if is_current_module:
                    container_key += "-current"
                if is_expanded:
                    container_key += "-expanded"

                with st.sidebar.container(key=container_key):
                    header_label = module.label
                    if is_current_module and not is_expanded:
                        header_label = f"{header_label} • current"
                    if st.sidebar.button(
                        header_label,
                        key=f"sidebar_module_{module.id}",
                        use_container_width=True,
                    ):
                        st.session_state["sidebar_expanded_module_id"] = "" if is_expanded else module.id
                        st.rerun()

                if not is_expanded:
                    continue

                for page in module.pages:
                    page_key = f"ws-sidebar-page-{page.id}"
                    if page.id == selected_page.id:
                        page_key += "-active"

                    with st.sidebar.container(key=page_key):
                        if st.sidebar.button(
                            page.label,
                            key=f"sidebar_page_{page.id}",
                            use_container_width=True,
                        ):
                            _select_sidebar_page(module.id, page.id)
                            st.rerun()
                        if page.id == selected_page.id:
                            st.sidebar.markdown(
                                f'<div class="ws-sidebar-page-description">{escape(page.description)}</div>',
                                unsafe_allow_html=True,
                            )

    recent_visits = get_recent_visits(st.session_state)
    st.sidebar.markdown(
        """
        <div class="ws-sidebar-block">
            <div class="ws-sidebar-block-title">Recent</div>
            <p class="ws-sidebar-block-copy">Keep only a few recent pages as lightweight secondary shortcuts.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    for index, recent_item in enumerate(recent_visits):
        with st.sidebar.container(key=f"ws-sidebar-recent-link-{recent_item['page_id']}-{index}"):
            st.sidebar.markdown(
                f'<div class="ws-sidebar-recent-module">{escape(recent_item["module_label"])}</div>',
                unsafe_allow_html=True,
            )
            if st.sidebar.button(
                recent_item["page_label"],
                key=f"sidebar_recent_{recent_item['page_id']}_{index}",
                use_container_width=True,
            ):
                _select_sidebar_page(recent_item["module_id"], recent_item["page_id"], clear_search=True)
                st.rerun()

    return selected_module.label, selected_page.label
```

Keep the existing desktop routing block below `selected_module, selected_page = render_desktop_sidebar_navigation()` unchanged so the page dispatch behavior remains stable.

- [ ] **Step 4: Run the focused regression suite and perform the desktop smoke test**

Run: `python -m pytest tests/test_sidebar_navigation.py tests/test_sidebar_tree_theme.py tests/test_sidebar_tree_layout.py -v`

Expected: PASS with all sidebar helper, theme, and layout regressions green.

Run: `streamlit run app.py --server.headless true --server.port 8767`

Expected:
- the app boots without a traceback
- the desktop sidebar shows `brand + search + tree + recent`
- typing `security` or `stock` into the sidebar search replaces the tree area with direct-jump results
- clearing the search restores the accordion tree
- clicking a result expands the correct module and highlights the destination page
- clicking a recent visit opens the correct page and expands the correct module
- loading `?security_query=600030.SH&security_type=stock&open_tab=security` still lands on the stock security page and expands the stock module
- `?iphone_mode=1` still uses the existing mobile flow

- [ ] **Step 5: Commit the desktop sidebar tree integration**

```bash
git add app.py tests/test_sidebar_tree_layout.py
git commit -m "feat: switch desktop sidebar to tree navigation"
```
