# Professional Gold Theme Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-theme the full Streamlit app from the current cold blue-gray shell into the `Professional Gold` system while preserving page behavior and analytical density.

**Architecture:** Keep the existing centralized theme seam in `src/apple_theme.py`, but convert it into a finance-oriented token system that emits global CSS, tracker-specific CSS, and the Plotly template. Then update `app.py` and view modules to consume those shared theme values instead of scattered hard-coded colors.

**Tech Stack:** Python, Streamlit, Plotly, pytest, unittest

---

### Task 1: Define the new theme contract with failing tests

**Files:**
- Modify: `tests/eastmoney_author_tracker/test_ui.py`
- Modify: `tests/test_navigation_config.py`

- [ ] **Step 1: Write the failing theme token tests**

Add assertions that the generated CSS exposes the new Professional Gold tokens and dark navigation treatment:

```python
def test_build_global_apple_theme_css_contains_professional_gold_tokens(self):
    css = build_global_apple_theme_css()

    self.assertIn("--ws-bg-base: #F4F7F6", css)
    self.assertIn("--ws-bg-surface: #FFFFFF", css)
    self.assertIn("--ws-bg-dark: #1B263B", css)
    self.assertIn("--ws-color-primary: #D4AF37", css)
    self.assertIn("--ws-color-up: #E63946", css)
    self.assertIn("--ws-color-down: #2A9D8F", css)
    self.assertIn('[data-testid="stSidebar"]', css)
```

- [ ] **Step 2: Write the failing Plotly palette test**

Add assertions that the shared Plotly template uses the new warm surface and gold-first palette:

```python
def test_build_apple_plotly_template_uses_professional_gold_palette(self):
    template = build_apple_plotly_template()

    self.assertEqual(template.layout.paper_bgcolor, "#F4F7F6")
    self.assertEqual(template.layout.plot_bgcolor, "#FFFFFF")
    self.assertEqual(template.layout.colorway[0], "#1B263B")
    self.assertEqual(template.layout.colorway[1], "#D4AF37")
```

- [ ] **Step 3: Write the failing historical badge color test**

Tighten the navigation styling test so the legacy yellow badge hard-code is forced into the new tokenized palette:

```python
def test_style_historical_st_badge_column_uses_professional_gold_palette(self):
    styles = style_historical_st_badge_column(pd.Series([HISTORICAL_ST_BADGE_TEXT]))

    self.assertIn("background-color: #F6E7B8", styles[0])
    self.assertIn("color: #1B263B", styles[0])
```

- [ ] **Step 4: Run the targeted tests to verify failure**

Run:

- `python -m pytest tests/eastmoney_author_tracker/test_ui.py -q`
- `python -m pytest tests/test_navigation_config.py -q`

Expected: FAIL because the current theme still emits cold-blue tokens and the historical badge still uses the older yellow/brown pair.

### Task 2: Rebuild the shared theme layer around Professional Gold

**Files:**
- Modify: `src/apple_theme.py`
- Test: `tests/eastmoney_author_tracker/test_ui.py`

- [ ] **Step 1: Replace the old token dictionary with Professional Gold tokens**

Update `APPLE_THEME_TOKENS` so the shared theme exposes:

```python
APPLE_THEME_TOKENS = {
    "bg_base": "#F4F7F6",
    "bg_surface": "#FFFFFF",
    "bg_dark": "#1B263B",
    "surface_soft": "#EEF2F0",
    "surface_alt": "#F8FAF8",
    "primary": "#D4AF37",
    "primary_hover": "#E5C158",
    "primary_soft": "rgba(212, 175, 55, 0.16)",
    "text_main": "#1B263B",
    "text_muted": "#6E7C8C",
    "text_inverse": "#F8FAFC",
    "border_soft": "rgba(27, 38, 59, 0.05)",
    "border_strong": "rgba(27, 38, 59, 0.12)",
    "shadow": "0 4px 20px rgba(27, 38, 59, 0.04)",
    "shadow_hover": "0 10px 28px rgba(27, 38, 59, 0.08)",
    "color_up": "#E63946",
    "color_down": "#2A9D8F",
    "ai_glow": "0 0 0 1px rgba(212, 175, 55, 0.24), 0 12px 30px rgba(212, 175, 55, 0.12)",
}
```

- [ ] **Step 2: Update the Plotly template builder**

Change `build_apple_plotly_template()` to emit:

- warm `paper_bgcolor` / `plot_bgcolor`
- title and label colors from `text_main` / `text_muted`
- a default `colorway` starting with navy and gold
- hover label background based on `bg_dark`

- [ ] **Step 3: Rewrite the global CSS builder**

Update `build_global_apple_theme_css()` to:

- emit the new `--ws-*` variables
- switch the app background to `--ws-bg-base`
- convert main cards, metrics, tables, charts, and expanders to white surfaces
- turn the sidebar into a dark navy navigation shell
- convert primary/secondary button styling to gold-first behavior
- add AI-highlight selectors using gold accent plus restrained blur/glow

- [ ] **Step 4: Rewrite the tracker-specific CSS helper**

Update `build_author_tracker_apple_css()` so tracker shells, evidence galleries, and section dividers use:

- white surfaces
- dark text
- soft gold accents
- dark AI / evidence emphasis where appropriate

- [ ] **Step 5: Run the targeted tests to verify pass**

Run:

- `python -m pytest tests/eastmoney_author_tracker/test_ui.py -q`

Expected: PASS

### Task 3: Replace app-level hard-coded shell and chart colors

**Files:**
- Modify: `app.py`
- Test: `tests/test_navigation_config.py`

- [ ] **Step 1: Write the failing app-level cleanup test**

Extend `tests/eastmoney_author_tracker/test_ui.py` with assertions that the old cold globals are gone from `app.py`:

```python
def test_app_py_no_longer_uses_cold_blue_global_literals(self):
    app_source = Path("app.py").read_text(encoding="utf-8", errors="ignore")

    self.assertNotIn("#3B82F6", app_source)
    self.assertNotIn("rgba(236, 241, 247, 0.84)", app_source)
```

- [ ] **Step 2: Run the targeted test to verify failure**

Run:

- `python -m pytest tests/eastmoney_author_tracker/test_ui.py -q`

Expected: FAIL because `app.py` still contains the legacy blue chart and shell literals.

- [ ] **Step 3: Replace the ad hoc Plotly template setup**

Refactor the top-level Plotly setup in `app.py` so it only consumes the shared builder:

- remove the large hard-coded `pio.templates["wealthspark_balanced"] = ...` block
- assign the shared template to `wealthspark_apple`, `wealthspark_balanced`, and `plotly_white`
- keep the rest of the rendering code behavior unchanged

- [ ] **Step 4: Replace high-frequency inline colors with shared constants**

In `app.py`, replace repeated high-frequency shell and state literals first:

- historical ST badge styles
- iPhone mode button/link styles
- shell cards created through inline HTML/CSS
- common chart helper default colors
- common positive/negative chart colors

Use shared theme constants or module-level aliases imported from `src.apple_theme`.

- [ ] **Step 5: Normalize repeated chart helper colors**

Update reusable chart helpers and the most repeated chart sections to use:

- navy/gold neutral series colors for structural charts
- `color_up` and `color_down` for market semantics
- warm white backgrounds instead of blue-gray backgrounds

- [ ] **Step 6: Run targeted tests and compile checks**

Run:

- `python -m py_compile app.py src/apple_theme.py`
- `python -m pytest tests/eastmoney_author_tracker/test_ui.py tests/test_navigation_config.py -q`

Expected: PASS

### Task 4: Align author-tracking local semantics and AI accents

**Files:**
- Modify: `src/eastmoney_author_tracker/ui.py`
- Modify: `tests/eastmoney_author_tracker/test_ui.py`

- [ ] **Step 1: Write the failing local semantic color test**

Add a test that local tracker semantic mappings now use the Professional Gold system semantics rather than the old mixed palette:

```python
def test_tracker_direction_semantics_follow_professional_gold_market_colors(self):
    payload = build_cycle_detail_payload(
        {"cycle_id": "c1", "ts_code": "600030.SH", "cycle_status": "active", "cycle_open_time": "2026-05-08 14:57:43"},
        [{"event_sequence": 1, "mention_time": "2026-05-08 14:57:43", "source_type": "stockbar", "direction": "bullish", "confidence_score": 0.9, "reason_text": "buy", "target_text": None, "post_title": "", "post_content": "", "reply_text": None}],
        [],
    )

    self.assertIn("bullish", payload["event_df"].to_string())
```

Then assert the module-level color map contains `#E63946` and `#2A9D8F` in the right roles once implementation lands.

- [ ] **Step 2: Run the targeted test to verify failure**

Run:

- `python -m pytest tests/eastmoney_author_tracker/test_ui.py -q`

Expected: FAIL because the tracker module still uses the older teal/orange/purple directional palette.

- [ ] **Step 3: Update the tracker module**

In `src/eastmoney_author_tracker/ui.py`:

- replace local direction colors with Professional Gold-compatible semantic mappings
- keep red-up / green-down behavior for market-facing states
- soften non-market states into navy / muted neutrals
- ensure evidence and AI-adjacent areas visually align with the new shared CSS

- [ ] **Step 4: Run tracker-focused regression tests**

Run:

- `python -m pytest tests/eastmoney_author_tracker/test_store.py tests/eastmoney_author_tracker/test_service.py tests/eastmoney_author_tracker/test_ui.py -q`

Expected: PASS

### Task 5: Full regression and visual verification

**Files:**
- Modify if needed: `src/apple_theme.py`
- Modify if needed: `app.py`
- Modify if needed: `src/eastmoney_author_tracker/ui.py`

- [ ] **Step 1: Run compile checks**

Run:

- `python -m py_compile app.py src/apple_theme.py src/eastmoney_author_tracker/ui.py`

Expected: no output

- [ ] **Step 2: Run the full test suite**

Run:

- `python -m pytest -q`

Expected: PASS

- [ ] **Step 3: Visually verify the critical pages**

Reload the local app and inspect at minimum:

- homepage
- stock page
- fund page
- author tracking page

Expected:

- page background reads warm neutral
- cards read white and cleaner
- sidebar reads dark navy with gold emphasis
- primary actions read gold, not blue
- market semantics remain red-up / green-down

- [ ] **Step 4: Inspect the working tree**

Run:

- `git status --short`
- `git diff --stat`

Expected: theme, app, tracker UI, tests, and plan/spec docs only
