# Sidebar Tree Navigation Design

Date: 2026-05-19
Status: Approved in conversation
Scope: Refine the desktop sidebar into a generalized tree navigation model using `search + accordion tree + recent visits`, without changing page business logic, page-local filter behavior, or the mobile flow.

## Relationship to the Previous Sidebar Spec

This document refines the sidebar portion of [2026-05-18-sidebar-navigation-design.md](D:/sourcecode/etf/docs/superpowers/specs/2026-05-18-sidebar-navigation-design.md).

The earlier spec established the navigation-first direction and the rule that page-local filters should not dominate the global sidebar. This spec narrows that direction into a more specific interaction model:

- a single search entry above navigation
- an accordion tree that expresses the module-to-page relationship directly
- a lightweight recent-visit area at the bottom

It does not replace the broader page-toolbar strategy from the earlier document. It only replaces the earlier desktop sidebar interaction pattern with a more general tree model.

## Context

The current desktop implementation in [app.py](D:/sourcecode/etf/app.py) and [src/sidebar_navigation.py](D:/sourcecode/etf/src/sidebar_navigation.py) already centralizes module/page metadata and records recent visits, but the rendered sidebar still behaves more like a stitched set of controls than a single coherent navigation system.

The user request for this pass was to keep optimizing the left sidebar and move away from the current module/page presentation toward a more general navigation pattern. During brainstorming, the preferred direction converged on a directory-style tree instead of a tab-like or button-wall sidebar.

This matters because the application now contains enough modules and analytical pages that users need a stable mental map more than they need another decorative variation of the same layout.

## Goals

- Make the module-to-page relationship immediately understandable
- Keep the sidebar clearly navigational rather than operational
- Support both browsing and direct page jump behavior
- Preserve fast return paths for repeat workflows
- Reuse the existing centralized navigation metadata where possible
- Make the sidebar renderer more generic than the current page-switching presentation

## Non-Goals

- Change data fetching, chart logic, or page business behavior
- Reintroduce page-specific filters into the sidebar
- Introduce module landing pages as clickable destinations
- Build a global security/content search system in this pass
- Redesign the existing mobile / iPhone navigation flow

## Confirmed Product Decisions

The following decisions were explicitly confirmed during conversation:

1. The sidebar should use a `directory tree` model.
2. The tree should use `accordion behavior`, with only one module expanded at a time.
3. The sidebar should include `search + recent visits`.
4. Module titles should `only expand/collapse` and should not navigate.
5. The preferred overall concept is `search + accordion tree + recent visits`.
6. The agreed section order is:
   - brand header
   - search
   - accordion tree
   - recent visits
7. The preferred node density is `balanced`:
   - generally compact rows
   - the active page may show one extra description line
8. Search should `replace the tree area inline` while active.
9. Search result items should show:
   - module label
   - page label
   - one-line description
10. After clicking a search result:
    - clear the search input
    - restore the tree
    - expand the destination module
    - highlight the destination page
11. Recent visits should be visually subordinate to the main navigation.
12. Deep links such as `security_query` should still expand the correct module and highlight the correct page.

## Recommended Interaction Model

The desktop sidebar becomes a four-layer navigation surface:

1. Brand header
2. Search
3. Accordion tree
4. Recent visits

The sidebar should answer four questions clearly:

- what product am I in
- where can I jump directly
- what module/page am I currently in
- what page did I use recently

It should not act like a mixed control wall.

## Information Architecture

### 1. Brand header

The top block keeps the WealthSpark identity and a short product-supporting line. It should remain compact and should not turn into a metrics panel.

Responsibilities:

- product name
- a short supporting sentence
- optional subtle system freshness cue if already available elsewhere in the shell

### 2. Search

The search field sits directly above the tree and acts as the only direct-jump input inside the sidebar.

Version 1 search scope:

- page labels
- module labels
- page descriptions
- optional static keywords stored in navigation metadata

Version 1 does not search securities, datasets, or page content.

### 3. Accordion tree

The tree is the primary navigation surface.

Structure:

- top level: modules
- second level: pages inside the expanded module

Rules:

- only one module can be expanded at a time
- module headers only expand/collapse
- page rows are the only elements that trigger navigation
- the current page is highlighted on the page row itself
- the current page may show one extra description line

There is no clickable module home in this model.

### 4. Recent visits

The recent-visit block is a secondary shortcut layer.

Rules:

- session-scoped only
- latest-first order
- maximum 6 items
- deduplicated by page
- clicking an item navigates to the page and expands its module

The block should help repeat navigation, but it should never compete visually with the main tree.

## Tree Semantics and Navigation Rules

### Module headers

Module headers represent containers, not destinations.

Click behavior:

- if a collapsed module is clicked, expand it and collapse any previously expanded module
- if the expanded module is clicked again, collapse it

Because module headers do not navigate, the app must never depend on a module click to choose a page.

### Page rows

Page rows are the only navigable nodes in the tree.

Click behavior:

- navigate to the page
- update the page-level session state already used by the app
- mark the page as active
- record the visit in recent visits

### Active location behavior

The active page remains the authoritative navigation state.

Rules:

- the active page row uses the strongest highlight
- only the active page row may show a description line in tree mode
- if the active page belongs to a collapsed module, the module header should still carry a subtle active marker so the user does not lose orientation

This preserves context even when the user temporarily expands another module to browse.

## Search Behavior

Search is a mode switch for the middle portion of the sidebar.

### Activation

- when the trimmed search query is empty, show the accordion tree
- when the trimmed search query is non-empty, replace the tree area with search results

The tree and the search-result list should never appear at the same time in version 1.

### Result shape

Each search result should show:

- page label
- module label
- one-line description

Visual treatment:

- more card-like than tree rows
- clearly scannable as direct-jump results
- still compact enough to keep several results visible

### Matching and ranking

Version 1 should prefer a deterministic lightweight ranking model:

1. exact or prefix match on page label
2. substring match on page label
3. match on module label
4. match on page description or keywords

This is intentionally simple and sufficient for a controlled navigation dataset.

If the query matches a module label, the result list should return the pages inside that module rather than a non-navigable module-only result.

### Selection behavior

When a user clicks a search result:

- navigate to the destination page
- clear the search query
- restore the tree area
- expand the destination module
- highlight the destination page
- record the page in recent visits

### Empty state

If no search results match:

- show a clear empty state in the tree area position
- do not keep the old tree visible behind it

This avoids stale navigation context and accidental mis-clicks.

## Visual Hierarchy and Density

The approved direction is a balanced-density tree rather than an ultra-compact admin list or a card-heavy visual menu.

### Relative emphasis

From strongest to weakest visual weight:

1. current page row
2. module headers
3. normal page rows
4. search result metadata
5. recent visits

### Approved emphasis rules

- module headers should feel structurally heavier than page rows
- only the active page should show a one-line description in tree mode
- search results should feel more like direct-jump cards than like tree rows
- recent visits should be the quietest block in the sidebar

### Practical density guidance

Version 1 should target the following rhythm:

- module row: approximately 40 to 44 px visual height
- page row: approximately 34 to 36 px visual height
- active page row: may grow vertically to accommodate one extra description line
- recent-visit row: approximately 30 to 32 px visual height
- search results: two-line rows or compact cards with a stronger hover state than the tree

These values are design guidance, not hard CSS constants.

## State Model

The tree should remain generic and state-driven.

### Source of truth

The active page should still be derived from the existing page/session routing model. The new tree should not invent a competing navigation state.

### Sidebar-specific state

The desktop sidebar will need to track:

- active page
- expanded module
- search query
- recent visits

Likely state responsibilities:

- existing per-module page selection keys stay as they are or are normalized around stable page ids
- `recent visits` remains in session state
- `expanded module` becomes an explicit sidebar state value
- `search query` is ephemeral UI state

Initial behavior:

- on first render, the expanded module defaults to the module that contains the active page
- after that, user-driven module expansion state may diverge temporarily from the active page's module until the user navigates again

### Recent visit policy

Version 1 should store:

- page identifier
- parent module identifier

It should not store a rendered label as the only identifier, because the navigation renderer should remain independent from decorated display strings.

## Data Model Guidance

To make the sidebar more general than the current presentation, navigation metadata should be renderer-friendly and text-clean.

### Recommended normalization

The navigation config should prefer stable fields such as:

- module id
- module label
- optional module icon
- module session key
- module default page id
- page id
- page label
- optional page icon
- page description
- optional search keywords

### Important constraint

The renderer should not treat emoji-decorated labels as the true identity of a page.

Display text may still include icons in the UI, but the data model should operate on stable ids and clean labels so that:

- search matching is predictable
- recent visits remain stable
- deep-link resolution is reliable
- future sidebar variants can reuse the same config

## Deep Links and Fallback Rules

The sidebar must behave correctly when the user enters through a deep page state rather than through sidebar clicks.

### Deep-link behavior

If the current app state points to a known page:

- resolve its parent module
- expand that module by default on first render
- highlight the page

### Invalid state fallback

If a deep link, restored session value, or recent-visit item points to a page that no longer exists:

- drop the stale recent-visit item silently
- fall back to the module default page if the module is still valid
- otherwise fall back to the first valid module and its default page

The user should never land in a broken navigation state.

## Mapping to the Current Codebase

The current sidebar-related logic already has a useful foundation:

- [src/sidebar_navigation.py](D:/sourcecode/etf/src/sidebar_navigation.py)
- [app.py](D:/sourcecode/etf/app.py)

### What should remain centralized

[src/sidebar_navigation.py](D:/sourcecode/etf/src/sidebar_navigation.py) should remain the single source of truth for:

- module/page definitions
- label and description metadata
- recent-visit utilities
- module/page lookup helpers
- future search indexing helpers

### What should change in the renderer

The desktop renderer in [app.py](D:/sourcecode/etf/app.py) should stop presenting modules and pages through the current pattern and instead render from the tree state model described in this spec.

That renderer should be responsible for:

- search input
- expanded-module resolution
- tree rendering
- search-result rendering
- recent-visit rendering
- click-to-page state transitions

## Risks and Mitigations

### Risk: users lose the currently active page while browsing another module

Mitigation:

- keep a subtle active marker on the collapsed module that contains the active page
- always restore the correct active highlight after navigation

### Risk: search and tree feel like two unrelated widgets

Mitigation:

- place search directly above the tree
- replace the same middle area rather than opening a separate overlay
- clear search immediately after successful navigation

### Risk: recent visits grows into a second primary navigation system

Mitigation:

- keep it visually quieter than the tree
- limit it to 6 items
- do not duplicate the full tree hierarchy there

### Risk: current label-based helpers become fragile as the sidebar gets more generic

Mitigation:

- keep stable ids as the underlying navigation identity
- use labels purely for presentation

## Testing Strategy

Testing should cover navigation behavior, state recovery, and hierarchy clarity.

### Unit-level coverage

Prefer adding or updating tests for:

- resolving a page to its parent module
- computing the initial expanded module from active page state
- recent-visit deduplication and max-length trimming
- search result matching and ordering
- stale page fallback handling

### Interaction checks

Manual or UI-level verification should confirm:

- only one module expands at a time
- module header clicks do not navigate
- page row clicks do navigate
- active page highlight follows the selected page
- search replaces the tree while active
- clicking a search result restores the tree and expands the correct module
- clicking a recent visit opens the correct page and expands the correct module

### Regression checks

Smoke test at least:

- one page in each module
- one deep-link entry path such as the stock query page
- empty search behavior
- stale recent-visit recovery
- desktop-only rendering, with no accidental spillover into the mobile flow

## Success Criteria

This redesign succeeds when:

1. The sidebar reads like a navigation map instead of a mixed control stack.
2. Users can understand the module-to-page relationship without trial clicking.
3. Search provides a fast direct-jump path without opening a separate UI surface.
4. Recent visits helps repeat workflows without competing with the tree.
5. The implementation becomes more generic and reusable than the current presentation pattern.
