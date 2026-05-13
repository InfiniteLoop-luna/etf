# Apple-Inspired Streamlit Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the app's current financial-dashboard-heavy visual styling with a lightweight Apple-inspired global theme while preserving business logic and page usability.

**Architecture:** Extract the visual system into a dedicated helper module so `app.py` consumes generated CSS and Plotly theme objects instead of embedding the entire theme inline. Apply the new theme globally, then add small tracker-page-specific hooks where global CSS alone is not enough.

**Tech Stack:** Python, Streamlit, Plotly, pytest, unittest

---

### Task 1: Create a testable theme helper seam

**Files:**
- Create: `src/apple_theme.py`
- Modify: `tests/eastmoney_author_tracker/test_ui.py`

- [ ] **Step 1: Write the failing theme helper tests**

Add tests that assert:

```python
from src.apple_theme import build_apple_plotly_template, build_global_apple_theme_css

def test_build_global_apple_theme_css_contains_core_shell_tokens():
    css = build_global_apple_theme_css()
    assert "--ws-bg" in css
    assert "[data-testid=\"stSidebar\"]" in css
    assert "[data-testid=\"stDataFrame\"]" in css
    assert ".stMetric" in css

def test_build_apple_plotly_template_uses_light_backgrounds():
    template = build_apple_plotly_template()
    assert template.layout.paper_bgcolor == "#F5F5F7"
    assert template.layout.plot_bgcolor == "#FFFFFF"
```

- [ ] **Step 2: Run the targeted test command to verify failure**

Run: `python -m pytest tests/eastmoney_author_tracker/test_ui.py -q`
Expected: FAIL with `ImportError` for `src.apple_theme`

- [ ] **Step 3: Implement the minimal theme helper**

Create `src/apple_theme.py` with:

- a `build_apple_plotly_template()` function
- a `build_global_apple_theme_css()` function
- centralized token constants

- [ ] **Step 4: Run the targeted test command to verify pass**

Run: `python -m pytest tests/eastmoney_author_tracker/test_ui.py -q`
Expected: PASS

### Task 2: Replace global `app.py` styling with the shared Apple-inspired theme

**Files:**
- Modify: `app.py`
- Modify: `src/apple_theme.py`

- [ ] **Step 1: Write the failing regression test for app-consumable theme output**

Extend the existing tracked test file with assertions that:

```python
from src.apple_theme import build_global_apple_theme_css

def test_build_global_apple_theme_css_includes_primary_interaction_selectors():
    css = build_global_apple_theme_css()
    assert "button[kind=\"primary\"]" in css or ".stButton" in css
    assert "[data-baseweb=\"select\"]" in css
    assert ".stPlotlyChart" in css
```

- [ ] **Step 2: Run the targeted test command to verify failure**

Run: `python -m pytest tests/eastmoney_author_tracker/test_ui.py -q`
Expected: FAIL because the selectors are not yet emitted

- [ ] **Step 3: Implement the global theme swap in `app.py`**

Update `app.py` to:

- import the helper module
- set the default Plotly template from the helper
- replace the large legacy CSS block with `build_global_apple_theme_css()`

Keep:

- existing page logic
- existing navigation logic
- existing data queries

- [ ] **Step 4: Run compile and targeted tests**

Run:

- `python -m py_compile app.py src/apple_theme.py`
- `python -m pytest tests/eastmoney_author_tracker/test_ui.py -q`

Expected: both PASS

### Task 3: Refine the tracker page for the new global design system

**Files:**
- Modify: `src/eastmoney_author_tracker/ui.py`
- Modify: `tests/eastmoney_author_tracker/test_ui.py`

- [ ] **Step 1: Write the failing tracker styling-hook tests**

Add tests that assert tracker-specific helper output includes stable hook names, for example:

```python
from src.apple_theme import build_author_tracker_apple_css

def test_build_author_tracker_apple_css_contains_tracker_hooks():
    css = build_author_tracker_apple_css()
    assert ".ws-tracker-shell" in css
    assert ".ws-tracker-section" in css
    assert ".ws-evidence-gallery" in css
```

- [ ] **Step 2: Run the targeted tests to verify failure**

Run: `python -m pytest tests/eastmoney_author_tracker/test_ui.py -q`
Expected: FAIL because tracker-specific CSS builder does not exist

- [ ] **Step 3: Implement tracker-page-specific refinement**

Update:

- `src/apple_theme.py` with `build_author_tracker_apple_css()`
- `src/eastmoney_author_tracker/ui.py` to emit lightweight wrappers around:
  - tracker root
  - section headings
  - overview block
  - evidence image area

Preserve the page's existing behavior and data flow.

- [ ] **Step 4: Run tracker tests**

Run: `python -m pytest tests/eastmoney_author_tracker/test_store.py tests/eastmoney_author_tracker/test_ui.py -q`
Expected: PASS

### Task 4: Full regression and visual safety checks

**Files:**
- Modify if needed: `app.py`
- Modify if needed: `src/apple_theme.py`
- Modify if needed: `src/eastmoney_author_tracker/ui.py`

- [ ] **Step 1: Run compile checks**

Run: `python -m py_compile app.py src/apple_theme.py src/eastmoney_author_tracker/ui.py`
Expected: no output

- [ ] **Step 2: Run focused regression tests**

Run: `python -m pytest tests/eastmoney_author_tracker/test_store.py tests/eastmoney_author_tracker/test_service.py tests/eastmoney_author_tracker/test_ui.py -q`
Expected: PASS

- [ ] **Step 3: Run the project test suite**

Run: `python -m pytest -q`
Expected: PASS

- [ ] **Step 4: Inspect the working tree**

Run:

- `git status --short`
- `git diff --stat`

Expected: theme helper, `app.py`, tracker UI, and doc/test files only
