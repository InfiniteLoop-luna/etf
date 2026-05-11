# Eastmoney Author Stock Tracking Design

Date: 2026-05-11
Status: Approved in conversation
Scope: Track one Eastmoney author's mentioned stocks from first mention through exit, support daily incremental updates, and provide both real-time monitoring and historical evaluation inside the existing Streamlit + PostgreSQL app.

## Context

The current repository already has:

- a Streamlit application in `app.py`
- PostgreSQL-backed store helpers such as `src/trend_reco_store.py`, `src/fund_monitor_store.py`, and `src/security_intraday_store.py`
- existing stock analytics, stock detail pages, and historical market data that can be reused for post-mention performance evaluation

The referenced Eastmoney author page is front-end rendered, but the page JavaScript exposes a stable activity API:

- `/api/guba/userdynamiclistv2`
- `/api/guba/tauserinfo`

The dynamic list payload already includes enough signal to support this feature:

- post identity and timestamps
- post title and content
- stock-bar code and stock-bar name when a post is published in a stock bar
- author replies embedded in reply lists
- image URLs attached to the post

This makes the feature feasible without browser scraping. The correct first move is to treat Eastmoney activity as an external content stream, archive it, then derive mention events, lifecycle states, and evaluation outputs from that archive.

## Goals

- Track one specified Eastmoney author with daily incremental updates
- Capture stock mentions from post metadata, post text, author replies, and attached images
- Model each stock idea as a lifecycle that starts with a first actionable mention and ends with an explicit or inferred exit
- Provide both:
  - a real-time view of active cycles
  - a historical evaluation view of closed cycles and author quality
- Keep the system explainable by preserving raw evidence for every derived decision
- Fit the implementation into the existing repository patterns instead of creating a separate service

## Non-Goals

- Support many authors in V1
- Infer exact portfolio sizing or exact position percentages
- Interpret complex chart drawings or full technical patterns from images in V1
- Reconstruct intraday execution quality in V1

## Confirmed Product Decisions

The following decisions were confirmed during brainstorming:

1. Product target: both real-time tracking and historical evaluation
2. Exit rule: mixed exit detection
   - first preference: explicit exit / trim / sell language from the author
   - second preference: clear view reversal that invalidates the prior bullish thesis
   - fallback: timeout or price-based closure rules
3. Same-stock modeling: dual-layer structure
   - one cycle for the overall stock idea / holding thesis
   - multiple mention events inside that cycle
4. Mention detection scope: aggressive V1
   - stock-bar code when available
   - title and body text parsing
   - author reply parsing
   - OCR on attached images

## Solution Options Considered

### Option 1: Lightweight snapshot tracker

Re-fetch recent pages daily, rebuild the latest state, and render one or two views.

Pros:

- fastest to ship
- minimal schema

Cons:

- weak traceability
- poor support for OCR backfill
- fragile when posts change or disappear
- hard to evolve rule versions safely

### Option 2: Event-archive pipeline (recommended)

Archive raw Eastmoney activity first, then derive mention events, then derive cycles and scores.

Pros:

- supports daily incremental updates cleanly
- allows OCR and parsing logic to improve over time
- every score and exit decision can point back to evidence
- matches the existing store-helper architecture in this repository

Cons:

- more tables than a snapshot-only design
- requires explicit rule-versioning

### Option 3: Heavy multi-author research platform

Build a generalized crawler and asynchronous research platform for many authors and many content sources.

Pros:

- high long-term ceiling

Cons:

- too heavy for the first version
- much more operational work than needed

## Recommended Architecture

Use the event-archive pipeline and implement it as a new stock-monitor feature inside the existing app.

### Layers

1. Ingestion layer
   - fetch author activity from Eastmoney APIs
   - upsert only new or changed posts, replies, and images
   - preserve raw payloads and fetch timestamps

2. Extraction layer
   - detect stock mentions from:
     - stock-bar metadata
     - title/body parsing
     - author replies
     - OCR from images
   - attach confidence, extraction source, and rule version

3. Lifecycle layer
   - group mention events into stock cycles
   - maintain current state: `new`, `active`, `trimmed`, `closed`, `expired`
   - determine closure reason and closure evidence

4. Evaluation layer
   - compute cycle-level performance and exit quality
   - compute author-level summary metrics
   - compute event-level incremental value metrics

5. Presentation layer
   - expose a new Streamlit page under the stock navigation group
   - support real-time monitoring and historical review

## Data Model

The model should preserve a strict separation between raw evidence and derived judgments.

### Raw archive tables

- `em_author_profiles`
  - tracked author metadata
  - `author_uid`, nickname, page URL, last_sync_at, status

- `em_author_posts`
  - one row per Eastmoney post
  - `post_id`, `author_uid`, `post_publish_time`, `post_last_time`, `post_title`, `post_content`, `post_guba_code`, `post_guba_name`, `post_type`, `raw_payload`, `record_hash`, `first_seen_at`, `last_seen_at`

- `em_author_replies`
  - one row per reply in the embedded reply list
  - `reply_id`, `post_id`, `reply_time`, `reply_text`, `reply_is_author`, `raw_payload`, `record_hash`

- `em_author_post_images`
  - one row per image URL attached to a post
  - `post_id`, `image_index`, `image_url`, `download_status`, `ocr_status`, `ocr_text`, `ocr_provider`, `ocr_version`, `image_hash`

### Derived mention table

- `em_stock_mentions`
  - one row per actionable stock mention
  - suggested fields:
    - `mention_id`
    - `author_uid`
    - `post_id`
    - `reply_id` nullable
    - `ts_code`
    - `symbol`
    - `security_name`
    - `mention_time`
    - `source_type` such as `stockbar`, `title_body`, `author_reply`, `image_ocr`
    - `direction` such as `bullish`, `bearish`, `neutral`, `trim_signal`, `exit_signal`
    - `confidence_score`
    - `target_text`
    - `risk_text`
    - `reason_text`
    - `rule_version`
    - `evidence_payload`

### Cycle tables

- `em_stock_cycles`
  - one row per stock idea cycle
  - suggested fields:
    - `cycle_id`
    - `author_uid`
    - `ts_code`
    - `cycle_open_time`
    - `cycle_close_time` nullable
    - `cycle_status`
    - `open_mention_id`
    - `close_mention_id` nullable
    - `close_reason`
    - `close_reason_detail`
    - `rule_version`
    - `manual_override_flag`
    - `manual_override_note`

- `em_cycle_events`
  - bridge table from cycles to mention events
  - `cycle_id`, `mention_id`, `event_role`, `event_sequence`

### Evaluation tables or materialized snapshots

- `em_cycle_scores`
  - cycle-level metrics such as total return, max drawdown, hold days, benchmark excess return, exit quality

- `em_author_score_snapshots`
  - daily or periodic author-level rollups

V1 may compute the score outputs on demand, but the schema should leave room for cached snapshots once the page grows heavier.

## Incremental Update Design

The feature should run as a daily incremental job with idempotent behavior.

### Daily update flow

1. Fetch newest author activity pages from `userdynamiclistv2`
2. Stop paging when enough already-seen unchanged records are encountered
3. Upsert changed raw posts and replies
4. Upsert post image metadata
5. Run text-based mention extraction immediately
6. Queue image OCR as a follow-up enrichment step
7. Rebuild or update mention events touched by the new raw records
8. Merge new mention events into active cycles or open new cycles when needed
9. Recompute scores for touched cycles
10. Refresh author-level summary snapshot

### Incremental stop rule

Use a practical stop rule instead of scanning the entire history every day:

- continue paging until a configurable number of consecutive posts are already known and unchanged
- still allow periodic full reconciliation jobs, such as weekly or monthly, in case Eastmoney edits older content

### Idempotency requirements

- raw rows should use stable natural keys such as `post_id` and `reply_id`
- derived mention rows should carry deterministic hashes or rule-based unique keys
- cycle rebuild for touched stocks must be safe to rerun

## Mention Detection Rules

V1 should prioritize explainability over aggressive semantic guessing.

### Detection priority

1. `post_guba.stockbar_code`
2. explicit stock code in title/body
3. stock name in title/body with market lookup support
4. author reply mentions
5. OCR hits from images

### OCR strategy

OCR is required by the confirmed scope, but it should not block the main ingestion flow.

Design requirement:

- mark image OCR as asynchronous enrichment
- keep raw image URL and OCR text separately
- when OCR finds a new stock mention, create a new mention event with lower default confidence and explicit `source_type=image_ocr`

### Confidence model

Every mention event should keep a confidence score and structured reason:

- high confidence: stock-bar metadata or explicit code pattern
- medium confidence: explicit stock name in text
- lower confidence: OCR-only hit without supporting text

## Cycle Rules

The dual-layer cycle model is required because the same stock can be mentioned multiple times while still belonging to one overarching thesis.

### Open-cycle rule

Open a new cycle when:

- a stock receives its first actionable bullish mention, or
- the prior cycle is already closed and the author later establishes a new bullish thesis for the same stock

### Add-event rule

Append new mention events to an active cycle when:

- the stock matches the active cycle
- the new event reinforces, updates, trims, or comments on the same still-open thesis

### Close-cycle priority

Close an active cycle by this priority order:

1. explicit exit / trim / sell language from the author
2. clear thesis reversal or invalidation
3. timeout expiration after a configured inactivity window
4. price-based closure fallback

### New-cycle boundary after close

For the same stock, do not automatically merge a later bullish mention into a closed cycle. A closed cycle stays closed. A later fresh thesis opens a new cycle.

## Scoring Model

Scoring should exist at three levels, with cycle score as the primary metric.

### Cycle score (primary)

Suggested metrics:

- total return from cycle open reference price to cycle close reference price
- max drawdown during the cycle
- hold duration in trading days
- excess return versus benchmark such as CSI 300 or sector proxy
- exit quality:
  - after the recorded exit, did the stock underperform, flatten, or fall in the next 5/10/20 trading days
  - if yes, the exit was effective

### Author summary score

Suggested rollups:

- total cycle count
- win rate
- average return
- payoff ratio
- average hold duration
- effective exit rate
- rolling monthly score trend

### Mention incremental value score

This measures whether later updates add value beyond the original mention.

Suggested signals:

- return delta after the later mention
- drawdown avoidance after trim or caution comments
- whether later commentary improves or worsens outcomes compared with holding unchanged

## Price and Benchmark Data

V1 should rely on daily market data that already fits the repository's patterns.

Design assumptions:

- use existing stock history and reference data already available in the project database where possible
- compute cycle performance on daily bars first
- defer intraday precision and trade execution reconstruction to a later version

## Manual Review and Override

V1 should not pretend to be fully self-judging.

Provide lightweight human correction capability for:

- wrongly detected stock symbol
- wrongly merged or split cycles
- wrongly inferred exit
- false-positive OCR mention

Manual overrides should never delete evidence. They should mark the correction on the derived layer and preserve the raw records.

## UI Design

Add a new stock subpage under the existing `个股` group, such as `观点跟踪`.

### V1 page sections

1. Author score summary
   - current score cards
   - rolling trend
   - update freshness

2. Active cycles
   - currently open stock ideas
   - latest mention time
   - unrealized performance
   - latest author stance

3. Closed cycles
   - sortable review table
   - close reason
   - realized performance
   - exit quality

4. Single-stock timeline detail
   - price chart
   - markers for mention events
   - author reply timeline
   - exit evidence
   - OCR evidence when relevant

### Navigation rationale

This fits better under the stock area than under the macro or decision groups because the feature is centered on per-stock idea tracking and stock-detail follow-up.

## Proposed Code Layout

The implementation should reuse the repository's current patterns.

Suggested new modules:

- `src/eastmoney_author_client.py`
  - API fetch helpers for author activity
- `src/eastmoney_author_store.py`
  - schema creation and upsert/query helpers
- `src/eastmoney_author_extract.py`
  - text parsing and mention extraction
- `src/eastmoney_author_ocr.py`
  - OCR abstraction and image enrichment workflow
- `src/eastmoney_author_cycles.py`
  - cycle grouping, close rules, and score calculation
- `scripts/sync_eastmoney_author.py`
  - daily incremental sync entry point

Likely touched existing files:

- `app.py`
- `src/navigation_config.py`
- tests for store, extraction, cycle logic, and navigation

## V1 Scope

Include in V1:

- one configured author UID
- daily incremental sync
- text plus OCR stock detection
- cycle plus event dual-layer modeling
- active and closed cycle views
- author summary score view
- single-stock evidence timeline
- manual correction entry points

Defer beyond V1:

- multi-author ranking
- deep chart-shape interpretation from images
- intraday trade-quality reconstruction
- inferred position sizing

## Risks and Mitigations

### Eastmoney payload drift

Risk:

- API fields may change

Mitigation:

- archive raw payloads
- isolate API mapping in one client module
- add defensive parsing and sync logging

### OCR false positives

Risk:

- image-only mentions can be noisy

Mitigation:

- store confidence and source type
- require evidence display
- allow manual correction

### Ambiguous exits

Risk:

- the author may imply an exit without saying it directly

Mitigation:

- mixed close rule with explicit priority
- preserve `close_reason` and `close_reason_detail`
- support override notes

## Verification Expectations For Implementation

When implementation starts, the plan should require:

- unit tests for raw-store upserts
- unit tests for mention extraction and confidence assignment
- unit tests for cycle open/append/close rules
- unit tests for score calculations
- navigation and UI smoke validation
- at least one replay test from saved Eastmoney payloads
