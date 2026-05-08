# Company Screener Batch Stock Info Edit Design

Date: 2026-05-08
Status: Approved in conversation
Scope: Add batch editing for stock main-business and product information to the company screener results area, reusing the same permission gate and validation rules as the single-security correction flow.

## Context

The current company screener in `app.py` can filter companies by industry, product keyword, and business keyword, but it only renders a result table. It does not preserve the current result set across reruns and offers no follow-up action on those results.

The application already supports single-stock correction in the security search tab. That flow includes:

- password-based permission gating through `ETF_STOCK_INFO_EDIT_PASSWORD` or `ETF_EDIT_PASSWORD`
- per-session authorization state
- validation rules for blank clears, too-short content, and unchanged submissions
- persistence through `update_stock_custom_info()`

The new batch-edit feature should only apply to the company screener and only target the current filtered result set.

## Goals

- Keep the batch-edit entry inside `公司主营与产品筛选`
- Reuse the existing permission model and user expectations
- Allow one submission to update every stock in the current filtered result set
- Preserve the current result set in session state so the editor remains available after reruns

## Non-Goals

- Add row-level manual selection in this version
- Extend batch editing to technical stock screening
- Introduce a new permission model or separate credentials

## UX Design

After a successful company search:

1. Store the raw filtered dataframe in `st.session_state`
2. Show the results table as today
3. Show a new expander below the table: `批量订正主营与产品信息`

Inside the expander:

- show the same password configuration warning when no edit password exists
- show the same authorize / revoke experience already used in the security search tab
- show the target count and a short preview of affected companies
- show a batch form with:
  - `新的主要业务`
  - `新的产品及业务范围`
  - submit button

The form applies the same values to every `ts_code` in the current result set.

## Validation Rules

Validation should match the single-stock correction flow:

- no permission: reject submission
- both fields blank: clear custom info
- both trimmed values shorter than 2 characters: reject submission
- otherwise persist the trimmed values

Unlike the single-stock flow, this batch version should not block "unchanged" submissions because the current result set can contain many different existing values.

## Data and Persistence

No schema change is required.

Persist through the existing `ts_stock_custom_info` table using the same upsert path as single-stock correction. A lightweight batch helper may loop over the existing write API so the UI remains simple and consistent.

## Affected Files

- `app.py`
  - persist screener result set in session state
  - render batch edit expander
  - reuse permission state and validation flow
- `src/etf_stats.py`
  - add small reusable helpers for validation and batch update
- `tests/test_stock_info_edit.py`
  - cover validation and batch-update helper behavior

## Verification

- unit tests for the new helper functions
- `python -m py_compile app.py src/etf_stats.py`
- targeted pytest for the new tests plus related existing tests if touched
