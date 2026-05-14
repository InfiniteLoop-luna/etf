# Professional Gold Theme Redesign

Date: 2026-05-14
Status: Approved in conversation
Scope: Replace the current cold blue-gray visual language with a warmer finance-oriented global theme while preserving the app's dense analytical workflows and existing page structure.

## Context

The current Streamlit app already has a centralized theme layer in [src/apple_theme.py](D:/sourcecode/etf/src/apple_theme.py), but the active visual direction still leans cold, glassy, and blue-gray. That look is cleaner than the older dashboard skin, but it does not yet communicate enough financial texture, warmth, or brand character.

The new direction should feel:

- more premium
- more trustworthy
- more finance-native
- more AI-assisted without looking flashy

This redesign is a visual system refactor, not a layout rewrite.

## Goals

- Introduce a new global color system named `Professional Gold`
- Reduce the current cold, overly pale blue atmosphere
- Add a stronger sense of wealth, trust, and professional financial polish
- Preserve analytical density and operational usability
- Unify shell surfaces, buttons, states, Plotly charts, semantic colors, and AI modules under one system
- Remove or centralize hard-coded colors currently scattered across `app.py` and local UI modules

## Non-Goals

- Rebuild page layouts or information architecture
- Change business logic, data sources, or navigation behavior
- Turn the product into a marketing-style gold-and-black luxury site
- Apply gold equally everywhere

## Confirmed Product Decisions

The following decisions were confirmed during conversation:

1. Scope is the entire project, not only the author-tracking page
2. This should be a full visual system replacement, not a partial shell recolor
3. The preferred direction is `Professional Gold`
4. The result should remain a serious data product with stronger financial character
5. A-share semantic convention should be used: red for up, green for down

## Design Options Considered

### Option 1: Theme-system-first global replacement (recommended)

Build a new token system and use it to replace:

- global CSS colors
- Plotly defaults
- semantic colors
- local page accent colors
- inline hard-coded component colors

Pros:

- most maintainable
- strongest consistency
- easiest to tune later
- best long-term cleanup of `app.py`

Cons:

- larger first pass
- requires careful regression checks across multiple pages

### Option 2: Incremental direct replacement inside pages

Pros:

- fast visible progress

Cons:

- theme logic stays fragmented
- harder to evolve later
- high chance of inconsistency

### Option 3: Heavy luxury-brand treatment

Pros:

- strongest immediate brand character

Cons:

- too dark and theatrical for a dense analytical tool
- hurts readability if overapplied

## Recommended Approach

Use Option 1.

Upgrade the existing centralized theme layer so it becomes the canonical source for:

- background colors
- surface colors
- brand gold
- dark structural navy
- semantic market colors
- shadows, borders, and AI highlight effects
- Plotly chart defaults

Then refactor app consumers to read from that system instead of local hard-coded colors.

## Visual System

### Theme Name

`Professional Gold`

### Core Palette

- `--bg-base: #F4F7F6`
- `--bg-surface: #FFFFFF`
- `--bg-dark: #1B263B`
- `--color-primary: #D4AF37`
- `--color-primary-hover: #E5C158`
- `--text-main: #1B263B`
- `--text-muted: #6E7C8C`
- `--color-up: #E63946`
- `--color-down: #2A9D8F`

### Visual Principle

The product should use:

- warm pale backgrounds for the workspace
- white surfaces for content-bearing cards
- deep navy for structure and immersion
- gold only for emphasis, activation, and AI/brand identity
- red/green only where true market semantics are being shown

This prevents visual overuse of gold while still giving the product a distinctly financial tone.

## Color Mapping Rules

### Backgrounds

- Page-level background uses `--bg-base`
- The current cold full-page blue-gray shell should be removed
- A very subtle warm haze or navy-gold atmospheric layer is acceptable, but the base should read neutral and breathable

### Surfaces

- Cards, table shells, metric shells, and expanders use `--bg-surface`
- Visible borders should be minimized or replaced with very light navy transparency such as `rgba(27, 38, 59, 0.05)`
- Default card shadow target: `0 4px 20px rgba(27, 38, 59, 0.04)`

### Typography

- Main headings, numeric highlights, and body text use `--text-main`
- Secondary text, timestamps, and helper text use `--text-muted`
- Avoid pure black as the default foreground

### Brand Gold

- Use gold for primary CTAs, active states, premium highlights, important labels, and AI identifiers
- Do not use gold as a large-area background for ordinary data cards

### Dark Structural Color

- Use `--bg-dark` for navigation, immersive panels, and special AI-driven modules
- Text on dark surfaces should be white or light neutral, with gold reserved for active emphasis

### Semantic Market Colors

- Rising / profitable / positive net flow: `--color-up`
- Falling / loss / negative net flow: `--color-down`
- Apply consistently across candlesticks, bars, labels, tags, heatmaps, and PnL-related figures

## Component Styling Rules

### Navigation / Sidebar

- Background uses `--bg-dark`
- Text and icons use white or light gray
- Active nav state uses gold through one or more of:
  - left indicator bar
  - active dot
  - soft gold background accent

### Cards / Containers

- Background uses `--bg-surface`
- Borders become minimal
- Shadows become lighter and cleaner
- The system should feel more like a premium research terminal and less like blue frosted glass

### Buttons

- Primary button:
  - gold gradient from `--color-primary` toward a slightly brighter gold
  - dark navy text for contrast
- Secondary button:
  - white or transparent background
  - gold border and gold text
- Hover states should rely on brightness lift and subtle elevation, not neon glow

### Tabs / Pills / Selected States

- Default state remains quiet and neutral
- Selected state switches from blue-first to gold-first emphasis
- Important tags such as AI, recommendation, or premium insight may use stronger gold treatment than ordinary tags

### AI Modules

Preferred treatment:

- dark navy surface
- gold heading, border, or badge
- restrained glow or highlight effect

Alternative treatment:

- white translucent surface with light gold highlight

Recommendation: prefer the dark navy AI module variant so AI areas feel clearly differentiated from regular data cards.

## Plotly Theme Direction

Plotly should be fully aligned with the new theme.

### Chart Surfaces

- `paper_bgcolor` and `plot_bgcolor` move to warm white / neutral light surfaces
- Remove the cold blue chart shell bias

### Text and Grid

- Titles and primary labels use `--text-main`
- Secondary labels use `--text-muted`
- Grid lines use very light navy transparency

### Default Series Palette

For non-semantic charts, use a restrained finance palette centered on:

- navy
- gold
- muted blue-gray
- supporting teal
- supporting warm accent

### Semantic Charts

For price, funds flow, returns, and PnL-like views:

- strict use of `--color-up`
- strict use of `--color-down`

Non-semantic ranking and structure charts should not overuse red/green.

## Implementation Structure

### Core Theme Layer

Refactor [src/apple_theme.py](D:/sourcecode/etf/src/apple_theme.py) into the canonical theme builder for:

- token constants
- CSS variable emission
- global CSS generation
- tracker-specific CSS helper output
- Plotly template output
- chart palette helpers if needed

The file name can remain temporarily for compatibility, but its contents should become conceptually theme-neutral.

### App Consumer Layer

Update [app.py](D:/sourcecode/etf/app.py) to:

- consume the centralized theme tokens and CSS
- remove remaining global hard-coded shell colors
- replace repeated Plotly color literals with theme-aware values where practical

### Local UI Modules

Update view-specific files such as [src/eastmoney_author_tracker/ui.py](D:/sourcecode/etf/src/eastmoney_author_tracker/ui.py) to:

- replace local semantic colors
- align AI / evidence / status styling with the new theme

## Execution Boundaries

This redesign includes:

- global CSS tokens
- shell and component color mapping
- Plotly template updates
- cleanup of reusable hard-coded colors
- normalization of semantic up/down usage
- alignment of local AI / tracker accents

This redesign does not include:

- page layout rewrites
- component hierarchy changes
- data logic changes
- new business features

## Risks and Mitigations

### Risk: Hard-coded colors remain scattered

Mitigation:

- scan `app.py` and key modules for hex / rgba literals
- classify replacements into:
  - theme tokens
  - plot palettes
  - semantic market colors

### Risk: Inconsistent market semantics

Mitigation:

- explicitly normalize all gain/loss and inflow/outflow visuals to A-share red-up / green-down behavior

### Risk: Gold becomes overused

Mitigation:

- reserve gold for emphasis, activation, AI identity, and premium highlights
- keep ordinary data surfaces white and restrained

### Risk: Regressions on high-density pages

Mitigation:

- prioritize regression checks on:
  - stock pages
  - fund pages
  - author tracking
  - funds-flow and candlestick-heavy views

## Testing Strategy

### Code-Level Tests

- verify new global CSS variables are emitted
- verify primary selectors map to the new token system
- verify Plotly template backgrounds and palette output
- verify semantic up/down colors are exposed consistently

### Regression Tests

Run focused suites for:

- theme and tracker UI helpers
- store and service regressions already covered by the repo

Then run the full pytest suite.

### Visual Verification

Check at minimum:

- homepage shell
- stock page
- fund page
- author tracking page

Success should read as:

- warmer
- more financial
- more premium
- still dense and highly usable

## Success Criteria

The redesign is successful when:

- the app no longer feels dominated by cold blue glass styling
- the product feels more like a premium financial decision tool
- gold reads as strategic brand emphasis rather than decoration
- charts, buttons, navigation, AI modules, and semantic market colors all feel part of one system
- stock, fund, and tracker pages remain operational and readable
- future color tuning can happen primarily through centralized theme tokens
