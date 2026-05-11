# Eastmoney Author Stock Tracking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a first working version of Eastmoney author stock tracking with a new isolated package, daily sync foundations, mention extraction, cycle modeling, and a minimal Streamlit UI.

**Architecture:** Add a new package under `src/eastmoney_author_tracker/` so all newly introduced logic stays isolated from existing modules. Implement pure logic first with tests, then add storage/sync orchestration, and finally wire a small read-only UI entry under the stock navigation group.

**Tech Stack:** Python, requests, pandas, SQLAlchemy, Streamlit, Plotly, pytest/unittest

---

### Task 1: Build the new tracker package foundations

**Files:**
- Create: `src/eastmoney_author_tracker/__init__.py`
- Create: `src/eastmoney_author_tracker/models.py`
- Create: `src/eastmoney_author_tracker/extract.py`
- Create: `src/eastmoney_author_tracker/cycles.py`
- Test: `tests/eastmoney_author_tracker/test_extract.py`
- Test: `tests/eastmoney_author_tracker/test_cycles.py`

- [ ] **Step 1: Write the failing extraction tests**

```python
def test_extract_stock_mentions_uses_stockbar_code_and_reply_text():
    post = {
        "post_id": 1001,
        "post_title": "今日一图 301139 元道通信",
        "post_content": "继续观察，暂不追涨。",
        "post_publish_time": "2026-05-09 20:42:17",
        "post_guba": {"stockbar_code": "301139", "stockbar_name": "元道通信吧"},
        "reply_list": [
            {"reply_id": 9001, "reply_is_author": True, "reply_text": "先减一点", "reply_time": "2026-05-11 08:40:29"}
        ],
    }
    mentions = extract_stock_mentions(post, stock_name_aliases={})
    assert [item["source_type"] for item in mentions] == ["stockbar", "author_reply"]
    assert mentions[0]["ts_code"] == "301139.SZ"
    assert mentions[1]["direction"] == "trim_signal"
```

- [ ] **Step 2: Run extraction tests to verify they fail**

Run: `python -m pytest tests/eastmoney_author_tracker/test_extract.py -v`
Expected: FAIL with `ModuleNotFoundError` for `src.eastmoney_author_tracker`

- [ ] **Step 3: Write the minimal extraction implementation**

```python
def extract_stock_mentions(post, stock_name_aliases=None, ocr_records=None, rule_version="v1"):
    # normalize stockbar metadata, title/body code hits, author-reply signals, and OCR hits
    ...
```

- [ ] **Step 4: Write the failing cycle tests**

```python
def test_build_stock_cycles_closes_cycle_on_exit_signal():
    mentions = [
        {"mention_id": "m1", "ts_code": "600030.SH", "mention_time": "2026-05-08 14:57:43", "direction": "bullish"},
        {"mention_id": "m2", "ts_code": "600030.SH", "mention_time": "2026-05-12 10:00:00", "direction": "exit_signal"},
    ]
    cycles = build_stock_cycles(mentions)
    assert len(cycles) == 1
    assert cycles[0]["cycle_status"] == "closed"
    assert cycles[0]["close_reason"] == "explicit_exit"
```

- [ ] **Step 5: Run cycle tests to verify they fail**

Run: `python -m pytest tests/eastmoney_author_tracker/test_cycles.py -v`
Expected: FAIL with `ImportError` or missing `build_stock_cycles`

- [ ] **Step 6: Write minimal cycle and scoring implementation**

```python
def build_stock_cycles(mentions, inactivity_days=30, as_of_date=None):
    # sort mentions, open cycles on bullish signals, append trim signals,
    # and close cycles on exit/bearish or timeout rules
    ...
```

- [ ] **Step 7: Run the package foundation tests**

Run: `python -m pytest tests/eastmoney_author_tracker/test_extract.py tests/eastmoney_author_tracker/test_cycles.py -v`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add src/eastmoney_author_tracker tests/eastmoney_author_tracker
git commit -m "feat: add Eastmoney author tracker core logic"
```

### Task 2: Add client, store, OCR abstraction, and sync orchestration

**Files:**
- Create: `src/eastmoney_author_tracker/client.py`
- Create: `src/eastmoney_author_tracker/store.py`
- Create: `src/eastmoney_author_tracker/ocr.py`
- Create: `src/eastmoney_author_tracker/service.py`
- Create: `scripts/sync_eastmoney_author.py`
- Test: `tests/eastmoney_author_tracker/test_client.py`
- Test: `tests/eastmoney_author_tracker/test_store.py`

- [ ] **Step 1: Write the failing client tests**

```python
def test_parse_userdynamiclist_payload_normalizes_posts():
    payload = {"re": True, "result": [{"post_id": 1, "post_title": "标题", "post_content": "内容"}]}
    posts = parse_userdynamiclist_payload(payload)
    assert len(posts) == 1
    assert posts[0]["post_id"] == 1
```

- [ ] **Step 2: Run client tests to verify they fail**

Run: `python -m pytest tests/eastmoney_author_tracker/test_client.py -v`
Expected: FAIL because parser helpers do not exist

- [ ] **Step 3: Implement client and OCR abstraction**

```python
class OptionalTesseractOcrProvider:
    def extract_text(self, image_bytes: bytes) -> dict:
        # return unavailable status when Pillow / pytesseract are absent
        ...
```

- [ ] **Step 4: Write the failing store tests**

```python
def test_build_summary_and_active_cycle_frames_from_cycle_rows():
    rows = [
        {"cycle_status": "active", "ts_code": "301139.SZ", "total_return": 0.12},
        {"cycle_status": "closed", "ts_code": "600030.SH", "total_return": 0.05},
    ]
    summary = build_author_summary(rows)
    assert summary["cycle_count"] == 2
    assert summary["active_count"] == 1
```

- [ ] **Step 5: Run store tests to verify they fail**

Run: `python -m pytest tests/eastmoney_author_tracker/test_store.py -v`
Expected: FAIL with missing helpers

- [ ] **Step 6: Implement storage helpers and service orchestration**

```python
def sync_author_activity(engine, author_uid, fetch_page_fn=None, ocr_provider=None, stock_name_aliases=None):
    # fetch pages, upsert raw rows, extract mentions, rebuild touched cycles, return summary
    ...
```

- [ ] **Step 7: Run client/store/service tests**

Run: `python -m pytest tests/eastmoney_author_tracker/test_client.py tests/eastmoney_author_tracker/test_store.py tests/eastmoney_author_tracker/test_extract.py tests/eastmoney_author_tracker/test_cycles.py -v`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add src/eastmoney_author_tracker scripts/sync_eastmoney_author.py tests/eastmoney_author_tracker
git commit -m "feat: add Eastmoney author tracker sync pipeline"
```

### Task 3: Wire the tracker into navigation and add a minimal UI

**Files:**
- Modify: `src/navigation_config.py`
- Modify: `app.py`
- Create: `src/eastmoney_author_tracker/ui.py`
- Test: `tests/test_navigation_config.py`
- Test: `tests/eastmoney_author_tracker/test_ui.py`

- [ ] **Step 1: Write the failing navigation and UI tests**

```python
def test_stock_page_options_include_author_tracking():
    from src.navigation_config import STOCK_PAGE_OPTIONS
    assert "🧭 观点跟踪" in STOCK_PAGE_OPTIONS
```

```python
def test_build_dashboard_payload_splits_active_and_closed_cycles():
    payload = build_dashboard_payload([
        {"cycle_status": "active", "ts_code": "301139.SZ"},
        {"cycle_status": "closed", "ts_code": "600030.SH"},
    ])
    assert len(payload["active_cycles"]) == 1
    assert len(payload["closed_cycles"]) == 1
```

- [ ] **Step 2: Run navigation/UI tests to verify they fail**

Run: `python -m pytest tests/test_navigation_config.py tests/eastmoney_author_tracker/test_ui.py -v`
Expected: FAIL because `STOCK_PAGE_OPTIONS` and UI helpers do not exist

- [ ] **Step 3: Implement the minimal Streamlit integration**

```python
STOCK_PAGE_OPTIONS = [
    "🔎 个股/指数查询",
    "🏢 公司筛选",
    "🎯 技术选股",
    "🧭 观点跟踪",
]
```

```python
def render_author_tracking_tab():
    # show author summary cards, active cycles table, closed cycles table,
    # and a detail table for cycle events when data is available
    ...
```

- [ ] **Step 4: Run targeted tests and compile checks**

Run: `python -m pytest tests/eastmoney_author_tracker/test_ui.py tests/test_navigation_config.py tests/eastmoney_author_tracker/test_client.py tests/eastmoney_author_tracker/test_store.py tests/eastmoney_author_tracker/test_extract.py tests/eastmoney_author_tracker/test_cycles.py -v`
Expected: PASS

Run: `python -m py_compile app.py src/navigation_config.py src/eastmoney_author_tracker/*.py scripts/sync_eastmoney_author.py`
Expected: no output

- [ ] **Step 5: Commit**

```bash
git add app.py src/navigation_config.py src/eastmoney_author_tracker tests/eastmoney_author_tracker tests/test_navigation_config.py scripts/sync_eastmoney_author.py
git commit -m "feat: add Eastmoney author tracking page"
```

### Task 4: Final verification

**Files:**
- Modify if needed: `docs/superpowers/plans/2026-05-11-eastmoney-author-stock-tracking-implementation.md`

- [ ] **Step 1: Run the focused verification suite**

Run: `python -m pytest tests/eastmoney_author_tracker/test_client.py tests/eastmoney_author_tracker/test_store.py tests/eastmoney_author_tracker/test_extract.py tests/eastmoney_author_tracker/test_cycles.py tests/eastmoney_author_tracker/test_ui.py tests/test_navigation_config.py -v`
Expected: PASS

- [ ] **Step 2: Run the project baseline suite again**

Run: `python -m pytest -q`
Expected: PASS

- [ ] **Step 3: Review git diff**

Run: `git status --short && git diff --stat`
Expected: only the new tracker package, app integration, tests, and plan/spec touchpoints

