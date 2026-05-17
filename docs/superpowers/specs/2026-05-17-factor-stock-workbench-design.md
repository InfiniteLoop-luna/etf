# Factor Stock Workbench Design

Date: 2026-05-17
Status: Approved in conversation
Scope: Add a dedicated stock-page workbench for daily factor-based stock selection using two-stage screening: hard filters first, then multi-factor scoring and ranking.

## Context

The current stock module has a lightweight `技术选股` page in [app.py](/D:/sourcecode/etf/app.py:5054) that only screens the latest `ts_stock_technical_signals` snapshot for weekly or monthly EMA relationships. It is intentionally fast, but it cannot support the broader "daily post-close factor screening" workflow.

The repository already has enough data to build a useful V1 factor workbench without inventing a new storage system:

- `ml_stock_feature_daily` provides the main daily anchor dataset for price, liquidity, valuation, and technical features
- `vw_moneyflow`, `vw_moneyflow_ths`, and `vw_moneyflow_dc` provide same-day capital-flow signals
- `vw_ts_stock_fina_indicator` provides rich financial snapshot fields, but is effectively keyed by `end_date` and `ann_date` instead of daily `trade_date`
- `vw_ts_stock_basic` and `vw_ts_stock_company` provide security identity, market, industry, and company profile metadata
- `vw_ts_stock_daily` provides trailing amount history that can be used to calculate `alpha095` and its normalized variants on demand

The user wants a dedicated page for "various quantitative factors" and explicitly chose:

- primary workflow: daily post-close quick stock selection
- factor scope: full multi-library factors
- selection method: stage 1 hard filters, then stage 2 weighted factor scoring
- implementation preference: write the design doc, then proceed directly to code

## Goals

- Add a dedicated `个股` subpage for factor-based stock selection without overloading the existing technical screener
- Support a fast daily workflow: choose date and universe, apply hard filters, compute factor scores, output a candidate pool
- Combine multiple factor families in one page:
  - Alpha-style quantity-price factors
  - liquidity and momentum factors
  - capital-flow factors
  - valuation factors
  - financial snapshot factors
  - technical structure factors
- Keep the scoring explainable by exposing factor values, factor percentile scores, and final weighted score
- Reuse existing query helpers and database configuration patterns; do not hardcode database credentials

## Non-Goals

- No backtest engine in V1
- No user-saved strategies or persistent personal presets in V1
- No intraday live screening; this page is for end-of-day or latest-loaded snapshot analysis
- No attempt to create a giant permanently materialized super-table in V1
- No replacement of the existing `技术选股` page

## Product Decision

Create a new stock subpage: `🧠 因子选股工作台`.

Keep `🎯 技术选股` intact as a lightweight, single-purpose screener. The new page is a broader workbench with richer controls, different data semantics, and a more research-oriented results table. This avoids turning the current EMA screener into a mixed-responsibility page.

## Data Model

### Daily anchor

Use `ml_stock_feature_daily.trade_date` as the primary page date selector and row anchor.

Reason:

- it already contains the broadest same-day factor coverage
- it is dense enough for all active A-share stocks
- most fast-screening factors are already stored there

### Same-day joins

Join these sources on `ts_code + trade_date` when the selected workbench date is loaded:

- `vw_moneyflow`
- `vw_moneyflow_ths`
- `vw_moneyflow_dc`

Derived same-day factors should include:

- `net_mf_amount`
- `net_mf_amount_rate = net_mf_amount / amount`
- `ths_net_amount`
- `ths_net_d5_amount`
- `dc_net_amount`
- `dc_net_amount_rate`

### Financial snapshot joins

For financial factors, use the latest available row per `ts_code` from `vw_ts_stock_fina_indicator`, ordered by:

1. `end_date DESC`
2. `ann_date DESC`

This snapshot is not strictly aligned to the selected trading day, so the UI must clearly label the financial snapshot period.

V1 financial factors should include:

- `roe`
- `roa`
- `grossprofit_margin`
- `current_ratio`
- `ocfps`
- `debt_to_assets`

### On-demand Alpha095 family

Compute these on demand from recent `vw_ts_stock_daily.amount` history for the selected date:

- `alpha095`
- `alpha095_cv`
- `alpha095_logstd`
- `alpha095_pctstd`

Implementation approach:

- query a recent calendar buffer ending at the selected date for all target securities in the anchor universe
- compute the factor family in pandas with `groupby('code')` and rolling windows
- merge the selected-date snapshot back into the workbench dataframe

This keeps V1 accurate and immediately useful without waiting for a new persistent factor pipeline.

## V1 Factor Families

### Universe and identity fields

- `ts_code`
- `name`
- `industry`
- `market`
- `exchange`
- `list_date`
- `is_hs`
- `has_ever_st`

### Liquidity and size

- `close`
- `amount`
- `turnover_rate`
- `volume_ratio`
- `total_mv`
- `circ_mv`
- `amount_ma5_ratio`
- `vol_ma5_ratio`

### Technical structure

- `close_over_ma20`
- `ma5_over_ma20`
- `w_ema5_over_30`
- `m_ema5_over_30`
- `distance_to_20d_high`
- `distance_to_20d_low`
- `volatility_20d`

### Valuation

- `pe_ttm`
- `pb`
- `ps_ttm`
- `dv_ratio`

### Capital flow

- `net_mf_amount`
- `net_mf_amount_rate`
- `ths_net_amount`
- `ths_net_d5_amount`
- `dc_net_amount`
- `dc_net_amount_rate`

### Financial quality

- `roe`
- `roa`
- `grossprofit_margin`
- `current_ratio`
- `ocfps`
- `debt_to_assets`

### Alpha amount-volatility family

- `alpha095`
- `alpha095_cv`
- `alpha095_logstd`
- `alpha095_pctstd`

## Page UX

The page should prioritize a repeatable daily workflow rather than a research notebook feel.

### Top-level layout

Use one dedicated page function rendered under the stock module, with three tabs:

1. `工作台`
2. `因子字典`
3. `数据新鲜度`

`工作台` is the operational page. The other two tabs keep explanations close to the action and reduce confusion.

### Workbench flow

#### Section 1: Date and universe

Show:

- selected trade date
- latest anchor date
- number of stocks in the loaded anchor universe

Controls:

- trade-date selectbox sourced from recent `ml_stock_feature_daily` dates
- market multiselect with values such as `主板`, `创业板`, `科创板`, `北交所`
- industry multiselect
- checkbox filters:
  - exclude historical ST
  - require沪深港通标识

#### Section 2: Hard filters

Use expanders grouped by factor family so the screen stays compact.

V1 filter groups:

- `流动性与市值`
- `技术形态`
- `资金流`
- `估值`
- `财务快照`

Each filter should be opt-in through a checkbox. When enabled, show one threshold input or range input. This is more usable than always-on number fields.

Recommended V1 hard-filter controls:

- liquidity and size:
  - min turnover rate
  - min amount
  - total market value range
- technical:
  - close_over_ma20 >= threshold
  - ma5_over_ma20 >= threshold
  - w_ema5_over_30 >= threshold
- capital flow:
  - min net_mf_amount
  - min net_mf_amount_rate
  - min dc_net_amount_rate
- valuation:
  - pe_ttm range
  - max pb
- financial snapshot:
  - min roe
  - min grossprofit_margin
  - max debt_to_assets
  - min current_ratio

#### Section 3: Scoring model

Offer fast presets first, then allow limited custom control.

Preset options:

- `均衡打分`
- `趋势动量`
- `质量价值`
- `资金驱动`
- `自定义`

For the first four presets, pre-fill a curated factor set and weights. For `自定义`, show editable factor weights.

To keep V1 tractable, expose custom weights for a focused set of core factors:

- `alpha095_cv`
- `amount_ma5_ratio`
- `close_over_ma20`
- `w_ema5_over_30`
- `net_mf_amount_rate`
- `dc_net_amount_rate`
- `pe_ttm`
- `pb`
- `roe`
- `grossprofit_margin`

Scoring method:

- convert each factor into a daily cross-sectional percentile score on the filtered universe
- respect factor direction:
  - higher is better for momentum, quality, and inflow metrics
  - lower is better for valuation pressure and leverage metrics
- fill missing factor percentiles with a neutral value `0.5`
- compute weighted average score on a 0-100 scale

Also compute group subtotals where possible so the result table can explain why a stock ranked highly.

#### Section 4: Candidate output

The result area should show:

- counts:
  - anchor universe size
  - post-filter size
  - ranked result size
- a top-candidate summary card
- a result dataframe with jump links into `个股/指数查询`

Result columns should include:

- security identity
- final score
- selected core factor values
- financial snapshot date
- tag columns such as `曾经ST`

Support:

- TopN selectbox
- minimum score threshold
- CSV download

### Factor dictionary tab

Show a compact dataframe or cards describing:

- factor key
- Chinese display name
- source table or view
- direction
- short explanation

This tab keeps the main page fast while helping users remember what each factor means.

### Data freshness tab

Show:

- latest date in `ml_stock_feature_daily`
- latest date in `vw_moneyflow`
- latest date in `vw_moneyflow_ths`
- latest date in `vw_moneyflow_dc`
- latest date in `ts_stock_technical_signals`
- latest financial `end_date`
- latest financial `ann_date`

Also explain that financial factors are latest-snapshot fields rather than trading-day-synchronized fields.

## Architecture

Create a dedicated backend helper module instead of expanding `app.py` with query and scoring logic.

### New module

Add a focused module, for example [src/factor_workbench.py](/D:/sourcecode/etf/src/factor_workbench.py), responsible for:

- factor metadata and preset definitions
- query helpers for trade dates and latest data freshness
- base dataframe loading from SQL
- alpha095-family snapshot calculation
- hard-filter application
- factor scoring and score breakdown generation

This keeps the Streamlit page mostly declarative and makes the factor logic testable with pure unit tests.

### App integration

In [app.py](/D:/sourcecode/etf/app.py), add:

- a page renderer `render_factor_workbench_tab()`
- a new entry in `STOCK_PAGE_OPTIONS`
- routing so the new page appears under the stock module

The page should reuse:

- existing engine and view patterns
- `render_security_jump_table(...)`
- existing styling and table conventions

## Error Handling

- If no feature anchor date is available, show a blocking warning and exit the page
- If financial or moneyflow joins are partially missing, continue with warnings and neutral scoring for missing fields
- If no rows survive hard filters, show a friendly warning and stop before scoring
- If selected custom weights are all zero, block scoring and ask the user to set at least one positive weight

## Testing Strategy

Unit-test the pure logic in the new module.

V1 test targets:

- factor-direction percentile normalization
- neutral handling of missing factor values
- hard-filter behavior for enabled and disabled thresholds
- score aggregation and ordering
- navigation option stability after adding the new stock page

Verification should include:

- targeted pytest for the new pure-logic tests
- existing navigation tests
- `python -m py_compile` for changed modules

## Affected Files

- `src/factor_workbench.py`
  - new query and scoring module
- `app.py`
  - add the new page renderer and stock-module routing
- `src/navigation_config.py`
  - add the new stock subpage label
- `tests/test_factor_workbench.py`
  - new pure-logic unit tests
- `tests/test_navigation_config.py`
  - extend the expected stock page options

## Implementation Notes

- Use the existing environment-based database configuration; do not persist credentials in code, docs, or Streamlit secrets as part of this feature
- Keep V1 focused on screening and ranking, not strategy persistence
- Prefer pandas vectorization and SQL-side narrowing over row-wise Python loops
- Keep result rendering explainable so the page is useful both for decision making and trust building
