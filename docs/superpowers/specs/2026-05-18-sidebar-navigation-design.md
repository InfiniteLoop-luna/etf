# Sidebar Navigation Redesign

Date: 2026-05-18
Status: Approved in conversation
Scope: Redesign the desktop Streamlit sidebar so it becomes a stable navigation surface, and move page-level filters into page-local toolbars without changing business logic or data behavior.

## Context

The current desktop app in [app.py](D:/sourcecode/etf/app.py) uses a sidebar structure that combines three different jobs in one vertical stack:

- top-level module switching
- second-level page switching
- page-specific filters and controls

That structure works functionally, but it creates two recurring usability problems:

1. The first impression is noisy because navigation and operation controls compete for the same space.
2. Once users enter a page, they still need to keep looking back to the sidebar to operate page-local filters.

This is especially visible in a dashboard of this size, where modules such as `决策`, `基金`, `个股`, `资金`, and `宏观` lead to pages with very different analytical depth.

The current theme work in [src/apple_theme.py](D:/sourcecode/etf/src/apple_theme.py) already gives the product a strong shell and visual direction. The next improvement should not be another pure styling pass. It should clarify the information architecture of the left sidebar itself.

## Goals

- Reduce the first-screen confusion of the desktop sidebar
- Separate navigation from page-level analysis controls
- Keep module and page switching fast for frequent cross-page use
- Preserve deep analytical workflows for users who stay in a page for a long session
- Reuse the existing desktop shell and visual language
- Create a consistent rule for where filters belong across pages

## Non-Goals

- Change business logic, data loading, or calculation behavior
- Rework the existing mobile / iPhone mode architecture
- Rebuild the app outside Streamlit
- Redesign every page layout in this pass
- Remove advanced controls from heavy research pages

## Confirmed Product Decisions

The following decisions were confirmed during conversation:

1. The main problem to solve is not just visual polish, but sidebar clarity.
2. The two top priorities are reducing clutter at first glance and separating navigation from filtering.
3. The product must support both frequent page switching and long in-page analysis sessions.
4. The preferred structural direction is the earlier `Option A`.
5. The sidebar should feel more like a directory than an action wall.

## Design Options Considered

### Option A: Navigation-first sidebar with page-local filters (recommended)

Sidebar responsibilities:

- brand and global entry
- top-level module navigation
- current module page navigation
- recent / favorite shortcuts

Page-level filters move into the main content area under the page title.

Pros:

- clearest first impression
- fastest mental model
- works for both cross-page and in-page usage
- sidebar remains stable while page tools can vary by page

Cons:

- requires moving some existing controls out of the sidebar
- users need a short adjustment period

### Option B: Split sidebar into navigation area and filter area

Keep one sidebar, but visually divide it into navigation and filter sections.

Pros:

- lowest migration cost
- smaller implementation change

Cons:

- navigation and filtering still compete vertically
- only partially solves clutter
- remains constrained by Streamlit sidebar density

### Option C: Rail plus drawer structure

Use a thin module rail plus page drawer and separate filter reveal patterns.

Pros:

- strongest product feel
- best long-term expansion path

Cons:

- highest implementation complexity
- too large a jump for the current problem
- unnecessary before the simpler structure is validated

## Recommended Approach

Use Option A.

The desktop sidebar should become a stable navigation map. It should answer:

- where am I
- what module am I in
- what pages can I go to next
- what have I used recently

It should not answer:

- what date range do I want
- what chart mode do I want
- what TopN or filter combination do I want for this page

Those questions belong to the current page, not to the global shell.

## Information Architecture

### Sidebar role

The desktop sidebar becomes a stable, four-block structure:

1. Brand and global entry
2. Top-level module list
3. Page list for the active module
4. Recent visits / favorites / a few global shortcuts

This structure stays stable across module changes. Only block 3 changes based on the active module.

### Block 1: Brand and global entry

This block should include:

- product name
- lightweight data freshness or current status signal
- global search entry for pages, securities, or key workflows

Purpose:

- reassure users the system is current
- give one universal entry point that bypasses navigation when needed

This block should not become a metrics cluster or a large dashboard summary.

### Block 2: Top-level modules

Modules remain the current primary groups:

- `决策`
- `基金`
- `个股`
- `资金`
- `宏观`

Recommended interaction:

- vertical list, fixed order
- strong highlight only for the active module
- quiet inactive states
- no equal-weight card treatment for every item

This should feel like a directory list, not a collection of CTA buttons.

### Block 3: Current module page list

This block only shows the pages for the active module.

Examples:

- `决策`: `今日机会清单`, `每日趋势推荐`, `推荐评估`, `ML 预测升级`
- `个股`: `个股/指数查询`, `公司筛选`, `技术选股`, `因子选股工作台`, `观点跟踪`

Recommended interaction:

- compact list below the active module
- one clear active page state
- optional one-line helper description for pages that benefit from it
- no filter controls mixed into this block

This keeps focus narrow and reduces cognitive spill from unrelated pages.

### Block 4: Recent visits and favorites

This is a lightweight utility block, not a second navigation system.

Recommended contents:

- 2 to 4 recent pages
- 1 to 3 pinned favorites
- optionally one permanent global shortcut such as `今日机会清单`

Purpose:

- improve return speed for repeat workflows
- support expert users without cluttering the main navigation

## Main Content Toolbar Strategy

Once filters leave the sidebar, the main content area needs a clear rule for where those controls go.

### Placement rule

Every page should place its shared page-level filters directly under the page title and breadcrumb.

This creates one coherent work surface:

- page identity at the top
- filters immediately below
- charts, tables, and analysis under those filters

Users can change controls and immediately inspect results without moving their attention back to the sidebar.

### Complexity-based filter taxonomy

Not every page should use the same toolbar weight. The current app should classify pages into three patterns.

#### Type 1: Light-filter pages

Purpose:

- fast browsing
- quick switching
- low operational overhead

Toolbar pattern:

- 2 to 4 high-frequency controls only
- one optional `更多筛选` entry if needed

Suggested examples:

- `今日机会清单`
- `每日趋势推荐`

Typical controls:

- date
- view mode
- sort mode
- short quick toggle

Avoid:

- long multiselects
- large sliders
- many low-frequency controls

#### Type 2: Standard analysis pages

Purpose:

- balanced exploration and efficiency
- charts plus tables plus repeat filtering

Toolbar pattern:

- 3 to 5 high-frequency controls visible by default
- remaining controls inside `更多筛选` collapse / expander / secondary area

Suggested examples:

- `资金流向`
- `ETF分类趋势`
- `宏观经济`
- `基金监测`
- `指数监测`

This pattern should become the default for most analysis pages.

#### Type 3: Heavy workbench pages

Purpose:

- dense, research-heavy, multi-panel workflows

Toolbar pattern:

- top toolbar contains only identity-style controls
- local panels, tabs, or sections contain deeper parameters

Identity-style controls usually include:

- security / entity selection
- date or date range
- mode or horizon

Suggested examples:

- `个股/指数查询`
- `因子选股工作台`
- `推荐评估`

Avoid:

- giant single-row toolbars carrying every parameter
- global controls that only affect one tab or one local panel

If a parameter affects only one tab, it belongs inside that tab, not in the page-global toolbar.

## Visual and Interaction Principles

### Sidebar style

The sidebar should use list logic rather than button-wall logic.

Recommended traits:

- calm vertical rhythm
- restrained inactive states
- one clear active highlight
- dark structural background consistent with the existing gold / navy shell
- shallow hierarchy expressed through spacing and type, not through loud card decoration

### Content contrast

The sidebar remains the structural layer. The main content area remains the analytical layer.

This means:

- dark sidebar for orientation
- light content area for reading and analysis
- filters visually tied to content instead of to the shell

### State behavior

The app should always make the following obvious:

- active module
- active page
- current breadcrumb or location label
- whether extra filters are open

Users should never have to infer current context from charts alone.

## Mapping to the Current Codebase

The current navigation and related logic live primarily in:

- [app.py](D:/sourcecode/etf/app.py)
- [src/navigation_config.py](D:/sourcecode/etf/src/navigation_config.py)
- [src/apple_theme.py](D:/sourcecode/etf/src/apple_theme.py)

### Likely implementation shape

Without prescribing final code structure yet, the implementation will probably need:

1. A dedicated sidebar render helper instead of inline branching around multiple `st.sidebar.radio(...)` blocks
2. A page-toolbar helper pattern for the three filter complexity classes
3. Incremental migration of page-local sidebar filters into their page content sections
4. Theme updates to support the new sidebar list states and page toolbar rhythm

### Files likely involved

- [app.py](D:/sourcecode/etf/app.py)
- [src/navigation_config.py](D:/sourcecode/etf/src/navigation_config.py)
- [src/apple_theme.py](D:/sourcecode/etf/src/apple_theme.py)
- page-specific rendering sections currently using `st.sidebar.*`

## Migration Guidance

This redesign should be applied incrementally rather than through one massive rewrite.

Suggested order:

1. Build the stable sidebar structure first
2. Move the most obvious page-local filters out of the sidebar
3. Introduce toolbar templates for Type 1, Type 2, and Type 3 pages
4. Migrate pages module by module
5. Clean up leftover sidebar-only controls and duplicated context labels

This allows visual validation after each step and reduces regression risk.

## Risks and Mitigations

### Risk: filter relocation confuses existing users

Mitigation:

- keep page titles and breadcrumbs explicit
- keep top toolbar placement consistent
- migrate repeated patterns first so the new mental model becomes obvious

### Risk: some heavy pages lose power-user efficiency

Mitigation:

- use the Type 3 heavy-workbench pattern
- keep deep controls close to the affected panel or tab
- do not force all parameters into a single shared toolbar

### Risk: implementation becomes fragmented page by page

Mitigation:

- define shared patterns before migrating pages
- use explicit page categories
- centralize shared sidebar and toolbar styling

### Risk: mobile mode diverges from desktop assumptions

Mitigation:

- keep this design scoped to desktop sidebar behavior
- preserve the existing separate mobile / iPhone flow for now

## Testing Strategy

Testing should cover both behavior and visual structure.

### Behavioral checks

- module switching still lands on the correct page
- page switching still preserves expected routing/session behavior
- controls moved from sidebar still drive the same underlying queries and charts
- heavy pages still expose all needed controls after migration

### UI structure checks

- the sidebar always shows only the active module page list
- page-local filters render under the page title, not in the sidebar
- active module and active page states are always visually clear
- `更多筛选` visibility and state remain understandable

### Regression coverage

Prefer adding or updating tests around:

- navigation option stability
- helper functions that determine current module/page state
- page-level control defaults after relocation

Manual smoke testing should include at least:

- one light-filter page
- one standard analysis page
- one heavy workbench page
- mobile / iPhone mode sanity check to confirm no unintended spillover

## Success Criteria

This redesign succeeds when:

1. The first look at the desktop sidebar feels clearly navigational, not operational
2. Users can identify active module and active page immediately
3. Page-local filters feel closer to the analysis they control
4. Frequent cross-page movement remains fast
5. Deep analytical pages still support expert workflows without crowding the global shell
