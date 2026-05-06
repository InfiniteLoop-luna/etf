# ETF Deposit Tab Design

Date: 2026-05-06
Status: Proposed and user-validated in conversation
Scope: Add a new ETF subpage for domestic and foreign currency deposit data with dashboard-first presentation, manual monthly entry, and bulk import support.

## Context

The current Streamlit app groups product areas under top-level navigation such as `ETF`, `个股`, `资金`, and `宏观`. The reference workbook `新增证券分析表单-20260505.xlsx` contains a dedicated sheet named `本外币存款数据`, but the app does not yet expose this dataset in a way that supports both ongoing maintenance and analysis.

The reference sheet is organized in month-sized blocks and includes:

- `人民币存款余额`
- `外币存款余额`
- `本外币存款余额`
- `住户存款增加额`
- `非金融企业存款增加额`
- `财政性存款增加额`
- `非银行业金融机构存款增加额`
- `存款合计增加额`
- `居民长期贷款增加额`

The user wants this data available inside the `ETF` area as its own tab/page, with two requirements:

1. New monthly data can be registered in the future without reshaping the page.
2. The data is displayed effectively for ongoing analysis.

The user explicitly chose:

- dashboard-first presentation over entry-first presentation
- support for both manual monthly entry and bulk import
- page organization equivalent to "dashboard homepage + secondary entry actions"

## Goals

- Add a new `ETF` subpage for deposit data without disturbing the current top-level information architecture.
- Make the first screen useful for reading trends, not data entry.
- Support stable month-by-month maintenance for future data additions.
- Preserve compatibility with the existing Excel-based workflow through import support.
- Keep the design simple enough to fit the current app style and implementation patterns in `app.py`.

## Non-Goals

- Rebuild the broader macro module around this dataset.
- Introduce advanced analytics such as heatmaps, factor decomposition, or narrative insights in the first version.
- Depend on live external APIs for this dataset in the first version.

## Information Architecture

Add a new ETF subpage:

- `🏦 本外币存款`

Placement:

- mobile `ETF` group: append this page to the existing ETF page selector
- desktop sidebar `ETF模块`: append this page to the existing ETF subpage radio

The page itself is dashboard-first and has five visible regions:

1. Summary cards
2. Main balance trend chart
3. Structure and increment chart area
4. Monthly detail table
5. Top-right maintenance actions

Maintenance actions are always available but never dominate the first viewport:

- `新增月份`
- `批量导入`

## UX Flow

### Default reading flow

1. User opens `ETF / 本外币存款`
2. Page shows the latest month and key changes immediately
3. User reads the main trend chart
4. User checks increment structure and then drills into the monthly detail table

### Manual entry flow

1. User clicks `新增月份`
2. A side panel or modal opens with a full-month form
3. User enters one month of values
4. User saves
5. The dataset refreshes and all cards, charts, and tables update

### Bulk import flow

1. User clicks `批量导入`
2. User uploads an Excel file or pastes a recognized monthly block
3. System parses the `本外币存款数据` sheet or equivalent structured input
4. System shows a preview with new rows, overwritten rows, and failed rows
5. User confirms write
6. The dataset refreshes and all cards, charts, and tables update

### Edit existing month flow

1. User locates a month in the detail table
2. User clicks `编辑`
3. The same entry panel opens in update mode
4. User saves changes
5. Dashboard refreshes using the updated month

## Data Model

Do not store the dataset in the block-style layout used by the reference workbook. Normalize it into one row per month.

Recommended logical schema:

| Field | Type | Meaning |
| --- | --- | --- |
| `month` | date or `YYYY-MM-01` timestamp | reporting month, unique |
| `rmb_deposit_balance` | numeric | 人民币存款余额 |
| `fx_deposit_balance` | numeric | 外币存款余额 |
| `total_deposit_balance` | numeric | 本外币存款余额 |
| `household_deposit_increase` | numeric | 住户存款增加额 |
| `corp_deposit_increase` | numeric | 非金融企业存款增加额 |
| `fiscal_deposit_increase` | numeric | 财政性存款增加额 |
| `nonbank_deposit_increase` | numeric | 非银行业金融机构存款增加额 |
| `total_deposit_increase` | numeric | 存款合计增加额 |
| `household_long_loan_increase` | numeric | 居民长期贷款增加额 |
| `source_type` | text | `manual` or `import` |
| `source_file` | text nullable | import file name when applicable |
| `created_at` | timestamp | first write time |
| `updated_at` | timestamp | latest update time |

Key design decisions:

- `month` is the unique business key.
- `total_deposit_balance` is stored directly, not derived from the other balance fields.
- month-over-month and year-over-year values are computed at render time, not stored.

## Storage Approach

This design does not force a single persistence implementation, but it should use a normalized local data source consistent with the rest of the app.

Recommended order of preference:

1. a local structured file already used by the app for app-managed datasets
2. a lightweight table in the existing project data layer
3. an Excel-backed normalized sheet only if no better local store exists

Regardless of storage implementation, the read shape exposed to the UI should always be the normalized month-row dataframe above.

## Import Parsing Rules

The first version should support import from files shaped like `新增证券分析表单-20260505.xlsx`.

Parsing assumptions from the reference workbook:

- the target sheet is named `本外币存款数据`
- each month is represented by a grouped block
- the month appears once at the block header row
- the following rows represent the known metric labels in fixed order

Importer behavior:

1. locate the deposit sheet
2. identify each monthly block
3. map metric labels to normalized fields
4. emit one normalized row per month
5. compare imported months against existing stored months
6. show a preview before write

Preview must show:

- months to insert
- months that already exist
- months that would be overwritten if the user confirms overwrite mode
- rows that failed to parse and why

Conflict behavior:

- if imported `month` does not exist: insert
- if imported `month` exists: allow `跳过` or `覆盖`

## Entry Form Design

Manual entry should capture one month at a time and use grouped fields in this order:

1. `month`
2. balance metrics
3. increment metrics

Suggested field grouping:

### Basic month

- `月份`

### Balance metrics

- `人民币存款余额`
- `外币存款余额`
- `本外币存款余额`

### Increment metrics

- `住户存款增加额`
- `非金融企业存款增加额`
- `财政性存款增加额`
- `非银行业金融机构存款增加额`
- `存款合计增加额`
- `居民长期贷款增加额`

The form should support:

- create mode
- edit mode
- reset/cancel
- save feedback

## Dashboard Design

### Summary cards

Show four top-level cards:

- `最新月份`
- `本外币存款余额`
- `环比变动`
- `同比变动`

The primary KPI for the page is `本外币存款余额`.

If month-over-month or year-over-year comparison is unavailable, show `-` rather than an error.

### Main balance trend chart

Purpose: answer "where is the balance level now and how has it moved recently?"

Default series:

- `人民币存款余额`
- `外币存款余额`
- `本外币存款余额`

Recommended lightweight controls:

- date window: `最近12个月` / `最近24个月` / `全部`
- series view: `显示全部` / `仅主指标`

### Structure and increment area

Do not mix balance metrics and increment metrics in the same main chart.

Default structure chart:

- column chart for `住户`, `企业`, `财政`, `非银`, `合计`

Optional secondary toggle:

- switch to `居民长期贷款增加额` as a focused single-series view

This separation keeps the chart readable and avoids mixing stock and flow concepts.

### Monthly detail table

The table is the operational drill-down view.

Requirements:

- default sort: descending by month
- default viewport: recent 12 months
- user can expand to full history
- all normalized columns are visible or selectable
- each row provides `编辑`

### Data freshness strip

Show a compact status line near the page top:

- latest data month
- source type
- latest update time

Example shape:

`最新数据月份：2026-03 | 数据来源：导入/手工 | 最近更新时间：2026-05-06 14:30`

## Validation Rules

Required:

- `month`
- all numeric business fields in the first version

Business rules:

- only one active row per month
- numeric fields allow positive and negative values
- duplicate month on manual save triggers `覆盖更新 / 取消`
- duplicate month on import triggers `跳过 / 覆盖`

Rendering rules:

- no stored MoM or YoY fields
- compute comparisons from existing rows
- if prior month is missing, MoM is blank
- if prior-year same month is missing, YoY is blank

Import validation:

- unknown sheet name or missing target sheet is a blocking error
- unmatched labels are surfaced in preview
- partial parse failures do not silently disappear

## Empty and Edge States

### No data

Show an empty state with clear actions:

- `新增月份`
- `批量导入`

Do not raise an application error.

### Only one month of data

- render summary cards
- show the trend chart with one point
- show `-` for MoM and YoY

### Missing comparison months

- display `-` for missing comparisons
- do not block the rest of the dashboard

## Implementation Notes

This feature should follow the current single-file Streamlit app pattern in `app.py` unless the surrounding code already provides a reusable local helper worth reusing.

Expected implementation units:

- a new ETF page renderer for deposit data
- a data loading layer that returns the normalized dataframe
- form handling for create/update
- import parsing logic for the workbook layout
- chart builders for balance and increment views

Keep the write path scoped to this dataset and avoid unrelated refactors to the ETF or macro modules.

## Error Handling

User-visible failure cases should be explicit and actionable:

- import file missing target sheet
- parse failure for one or more blocks
- duplicate month without confirmed overwrite
- invalid number input
- persistence write failure

Recommended messaging style:

- clear error at action point
- preserve preview context when possible
- never discard parsed rows without explanation

## Testing Strategy

Focus tests on riskier behavior rather than broad UI snapshot coverage.

### Parser tests

- parse the known workbook layout into normalized rows
- verify month extraction
- verify label-to-field mapping
- verify handling for duplicate months and malformed blocks

### Data logic tests

- MoM calculation with consecutive months
- YoY calculation with same month in prior year
- missing-comparison behavior

### UI behavior checks

- empty state renders correctly
- latest cards render with known sample data
- detail table sorts by month descending
- create and edit flows update the backing dataset

### Import flow checks

- preview distinguishes insert vs overwrite
- confirmation writes only approved rows

## Recommended Initial Version

Ship the first version with:

- new `🏦 本外币存款` ETF subpage
- summary cards
- main balance trend chart
- increment structure chart
- monthly detail table
- manual monthly entry
- bulk Excel import with preview and overwrite handling

Defer:

- advanced analytics
- commentary generation
- external data synchronization

## Decision Summary

The chosen design is:

- page location: `ETF / 🏦 本外币存款`
- default experience: dashboard-first
- maintenance access: top-right action entry points
- data model: one row per month
- maintenance modes: both manual entry and bulk import
- primary analysis focus: balance trends, increment structure, and monthly detail visibility
