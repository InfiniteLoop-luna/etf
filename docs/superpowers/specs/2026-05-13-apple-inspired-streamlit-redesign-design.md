# Apple-Inspired Streamlit Redesign

Date: 2026-05-13
Status: Approved in conversation
Scope: Apply a lightweight Apple-inspired visual system across the existing Streamlit app while preserving the dashboard's information density, workflows, and data interactions.

## Context

The current application is a large Streamlit dashboard centered in `app.py` with a broad set of stock, ETF, fund, macro, and author-tracking views. Visual styling is currently defined inline in one large CSS block plus a custom Plotly template.

The current look is intentionally "financial dashboard":

- blue gradient backgrounds
- prominent shadows
- heavy card treatment
- strong sidebar contrast
- dense control styling

This works functionally, but the overall visual language is louder than the user now wants. The confirmed direction is not a literal Apple clone and not a landing-page transformation. The goal is to create an Apple-inspired product aesthetic:

- calmer surfaces
- more restrained color
- more generous spacing
- cleaner type hierarchy
- lower visual noise

while keeping tables, filters, charts, and operational workflows fully usable.

## Goals

- Apply a unified Apple-inspired theme across the entire Streamlit app
- Preserve current page structure and business logic
- Preserve information density for tables, metrics, and charts
- Replace the existing dashboard-heavy styling with a calmer and more premium interface
- Improve the `观点跟踪` page so it feels like a polished research workspace, especially for cycle detail and evidence review
- Keep the redesign maintainable by centralizing theme tokens and generation logic instead of growing the `app.py` CSS block further

## Non-Goals

- Rebuild the app in React, Next.js, or another frontend framework
- Rework core navigation logic or data-fetching architecture
- Reduce the app into a sparse marketing-site layout
- Clone Apple's brand assets, typography licensing, or exact interaction language
- Redesign every page's information architecture in this pass

## Confirmed Product Decisions

The following design decisions were confirmed during conversation:

1. Scope: entire project, not just `观点跟踪`
2. Style direction: Apple-inspired rather than 1:1 Apple clone
3. Density preference: lightweight Apple feel while preserving data usability
4. Coverage: sidebar, top-level shell, buttons, tables, charts, and content containers should all be unified

## Design Options Considered

### Option 1: Global light Apple-inspired reskin (recommended)

Create a shared theme system for:

- colors
- spacing
- corner radii
- borders
- shadows
- Plotly chart defaults
- Streamlit shell selectors

Apply it globally and only refine local views where needed.

Pros:

- lowest regression risk
- largest visual impact per line of code
- keeps existing workflows intact
- easy to evolve later

Cons:

- some deeply custom pages may still reflect legacy layout decisions

### Option 2: Global reskin plus page-by-page layout rewrite

Pros:

- strongest visual coherence

Cons:

- high regression risk
- much larger scope
- likely to break stock and fund pages again

### Option 3: Strong Apple showcase style

Pros:

- most dramatic visual shift

Cons:

- wrong fit for a high-density analytical dashboard
- would harm readability and scanning speed

## Recommended Approach

Use Option 1.

Implement a new theme module that generates:

- design tokens
- Plotly template
- global CSS
- tracker-page-specific CSS helpers

Then update `app.py` to use the shared theme rather than hard-coded financial-dashboard styling.

## Visual System

### Color direction

Replace the current multi-gradient blue dashboard look with a restrained neutral system:

- background: warm white / cool paper
- surface: white
- muted surface: very light gray
- text primary: near-black
- text secondary: slate gray
- border: light gray
- accent: a single calm system blue
- semantic colors: keep green / amber / red for financial meaning, but reduce saturation

### Typography

Primary stack should favor Apple-adjacent system reading:

- `SF Pro Text` where available via `-apple-system`
- `PingFang SC` for Chinese
- `BlinkMacSystemFont`
- fallback sans-serif

Typography should shift toward:

- stronger headline hierarchy
- lighter captions
- less all-caps feeling in utility labels
- tighter control over section spacing

### Surfaces

Cards and panels should use:

- flatter white surfaces
- subtler borders
- smaller and softer shadows
- larger radius than the current dashboard, but still restrained

### Controls

Buttons, select boxes, pills, and tags should feel more native and less dashboard-like:

- soft neutral backgrounds
- thin border
- subtle hover state
- blue reserved for primary emphasis

### Data presentation

Dataframes, metrics, and charts must stay high density. The redesign should:

- preserve row counts and tabular access
- improve spacing around containers
- lighten chart chrome and grid lines
- remove decorative gradients from analytical surfaces

## Plotly Theme Direction

Charts should keep current data semantics but adopt a cleaner visual baseline:

- white paper and plot background
- light grid lines
- dark text
- quiet legend container
- calmer colorway
- hover labels with high contrast but lower visual bulk

The chart theme should feel consistent with the rest of the app rather than like a separate dark financial tool.

## Page Coverage

### Global shell

Apply redesign to:

- app background
- main container
- sidebar
- section headers
- expander shells
- tabs
- buttons
- inputs
- dataframes
- metrics
- plot wrappers

### View-specific refinement

The first explicit local refinement target is `src/eastmoney_author_tracker/ui.py` because it contains:

- summary metrics
- active and closed cycle tables
- timeline charts
- evidence image rendering
- evidence expanders

This page should become a model for the new visual tone:

- cleaner section rhythm
- lighter metric shells
- more premium evidence presentation
- better image integration

## Implementation Structure

Introduce a shared theme helper module rather than placing more CSS directly into `app.py`.

Suggested responsibilities:

- `src/apple_theme.py`
  - centralized color tokens
  - Plotly template builder
  - global CSS builder
  - optional tracker-page CSS builder

- `app.py`
  - import and apply shared theme
  - remove or replace legacy dashboard CSS and template wiring

- `src/eastmoney_author_tracker/ui.py`
  - add light structural hooks or wrappers where global CSS alone is not enough

## Testing Strategy

Because most of this work is visual, the implementation should create testable seams:

- test the generated CSS string for key Apple-inspired tokens and selectors
- test the Plotly template builder for expected colors and backgrounds
- test tracker-specific helper output where appropriate

Then run the existing regression suite to ensure the redesign does not break non-visual behavior.

## Risks and Mitigations

### Risk: visual changes break controls or data tables

Mitigation:

- keep selectors scoped and conservative
- avoid replacing Streamlit layout primitives
- preserve table and chart dimensions
- test stock/fund/tracker pages after each stage

### Risk: `app.py` becomes even harder to maintain

Mitigation:

- move theme generation into a dedicated module
- keep `app.py` as a consumer of theme helpers

### Risk: Apple inspiration becomes generic minimalism

Mitigation:

- use explicit design tokens
- keep typography, spacing, and restraint intentional
- avoid random gradients and decorative color bursts

## Success Criteria

The redesign is successful when:

- the app no longer reads as a blue financial dashboard
- the app feels calmer, lighter, and more premium
- stock, fund, and tracker pages remain fully usable
- `观点跟踪` feels especially polished for research and evidence review
- global styling is centralized enough to evolve safely
