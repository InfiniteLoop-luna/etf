from __future__ import annotations

import plotly.graph_objects as go


SYSTEM_FONT_FAMILY = '-apple-system, BlinkMacSystemFont, "Segoe UI", "Noto Sans", Helvetica, Arial, sans-serif'
MIN_FONT_SIZE = 14


# Public names remain unchanged because the application and tests import them.
# The values follow the Apple design language from the supplied DESIGN.md while
# keeping enough contrast and density for a financial operations dashboard.
APPLE_THEME_DEFAULT_TOKENS = {
    "bg_base": "#F5F5F7",
    "bg_surface": "#FFFFFF",
    "bg_dark": "#1D1D1F",
    "surface_soft": "#F5F5F7",
    "surface_alt": "#F2F2F7",
    "surface_dark_alt": "#2C2C2E",
    "primary": "#0066CC",
    "primary_hover": "#0071E3",
    "primary_press": "#005BB5",
    "primary_strong": "#2997FF",
    "primary_soft": "#E8F2FC",
    "secondary": "#0066CC",
    "text_main": "#1D1D1F",
    "text_muted": "#6E6E73",
    "text_soft": "#86868B",
    "text_inverse": "#FFFFFF",
    "border_soft": "#D2D2D7",
    "border_strong": "#A1A1A6",
    "shadow": "none",
    "shadow_hover": "none",
    "ai_glow": "none",
    "color_up": "#248A3D",
    "color_down": "#D70015",
    "color_warn": "#9A6700",
    "color_neutral": "#6E6E73",
    "color_accent_alt": "#5AC8FA",
    "radius_lg": "18px",
    "radius_md": "11px",
    "radius_sm": "8px",
    "max_width": "1280px",
}

APPLE_THEME_TOKENS = dict(APPLE_THEME_DEFAULT_TOKENS)


def get_apple_theme_tokens(overrides: dict | None = None) -> dict:
    tokens = dict(APPLE_THEME_DEFAULT_TOKENS)
    source = APPLE_THEME_TOKENS if overrides is None else overrides
    if isinstance(source, dict):
        tokens.update({key: value for key, value in source.items() if value is not None})
    return tokens


def build_apple_plotly_template() -> go.layout.Template:
    tokens = get_apple_theme_tokens()
    font_family = SYSTEM_FONT_FAMILY
    data_family = "'SF Mono', ui-monospace, 'Cascadia Code', Consolas, monospace"
    axis_style = {
        "showline": True,
        "linewidth": 1,
        "ticks": "outside",
        "tickcolor": tokens["border_strong"],
        "tickfont": {"family": data_family, "color": tokens["text_muted"], "size": MIN_FONT_SIZE},
        "gridcolor": "rgba(212, 219, 228, 0.62)",
        "linecolor": tokens["border_soft"],
        "zerolinecolor": tokens["border_strong"],
        "title": {"font": {"color": tokens["text_muted"], "size": MIN_FONT_SIZE}},
    }
    return go.layout.Template(
        layout=go.Layout(
            paper_bgcolor=tokens["bg_surface"],
            plot_bgcolor=tokens["bg_surface"],
            font={"color": tokens["text_main"], "family": font_family, "size": MIN_FONT_SIZE},
            title={
                "font": {
                    "color": tokens["text_main"],
                    "family": SYSTEM_FONT_FAMILY,
                    "size": 17,
                }
            },
            colorway=[
                # Keep the established chart palette for saved dashboards;
                # all interactive chrome uses the Apple tokens above.
                "#0F69FF",
                "#0052D0",
                "#037B66",
                "#D11022",
                tokens["color_warn"],
                tokens["color_accent_alt"],
            ],
            hoverlabel={
                "bgcolor": tokens["bg_dark"],
                "font": {"color": tokens["text_inverse"], "family": font_family},
                "bordercolor": tokens["bg_dark"],
            },
            legend={
                "bgcolor": "rgba(255,255,255,0.96)",
                "bordercolor": tokens["border_soft"],
                "borderwidth": 1,
                "font": {"color": tokens["text_muted"], "size": MIN_FONT_SIZE},
            },
            margin={"l": 18, "r": 18, "t": 38, "b": 18},
            xaxis=axis_style,
            yaxis=axis_style,
        )
    )


def build_author_tracker_apple_css() -> str:
    tokens = get_apple_theme_tokens()
    return f"""
.ws-tracker-shell {{
    margin: 0 0 1rem;
    padding: 1rem;
    background: {tokens["bg_surface"]};
    border: 1px solid {tokens["border_soft"]};
    border-radius: {tokens["radius_lg"]};
    box-shadow: none;
}}

.ws-tracker-shell .ws-tracker-eyebrow {{
    display: inline-flex;
    align-items: center;
    padding: 0.18rem 0.42rem;
    border-radius: {tokens["radius_md"]};
    background: {tokens["primary_soft"]};
    color: {tokens["primary_hover"]};
    font-family: var(--ws-font-data);
    font-size: 1rem;
    font-weight: 700;
    letter-spacing: 0;
}}

.ws-tracker-shell h4 {{
    margin: 0.65rem 0 0.25rem;
    color: {tokens["text_main"]};
    font-size: 1.05rem;
    font-weight: 700;
    letter-spacing: 0;
}}

.ws-tracker-shell p {{
    margin: 0;
    color: {tokens["text_muted"]};
    font-size: 1rem;
    line-height: 1.45;
}}

.ws-tracker-section {{
    margin: 1rem 0 0.45rem;
    padding-bottom: 0.45rem;
    border-bottom: 1px solid {tokens["border_soft"]};
}}

.ws-tracker-section span {{
    color: {tokens["text_main"]};
    font-size: 1rem;
    font-weight: 700;
    letter-spacing: 0;
}}

.ws-evidence-gallery {{
    margin: 0.65rem 0 0.25rem;
    padding: 0.75rem;
    border: 1px solid {tokens["border_soft"]};
    border-radius: {tokens["radius_md"]};
    background: {tokens["surface_soft"]};
}}

.ws-evidence-gallery strong {{
    color: {tokens["text_main"]};
    font-size: 1rem;
    font-weight: 700;
}}

.ws-evidence-gallery-note {{
    margin-top: 0.3rem;
    color: {tokens["text_soft"]};
    font-size: 1rem;
}}
"""


def build_global_apple_theme_css() -> str:
    tokens = get_apple_theme_tokens()
    return f"""
:root {{
    --ws-bg-base: {tokens["bg_base"]};
    --ws-bg-surface: {tokens["bg_surface"]};
    --ws-bg-dark: {tokens["bg_dark"]};
    --ws-surface-soft: {tokens["surface_soft"]};
    --ws-surface-alt: {tokens["surface_alt"]};
    --ws-surface-dark-alt: {tokens["surface_dark_alt"]};
    --ws-color-primary: {tokens["primary"]};
    --ws-color-primary-hover: {tokens["primary_hover"]};
    --ws-color-primary-press: {tokens["primary_press"]};
    --ws-color-primary-strong: {tokens["primary_strong"]};
    --ws-color-primary-soft: {tokens["primary_soft"]};
    --ws-color-secondary: {tokens["secondary"]};
    --ws-text-main: {tokens["text_main"]};
    --ws-text-muted: {tokens["text_muted"]};
    --ws-text-soft: {tokens["text_soft"]};
    --ws-text-inverse: {tokens["text_inverse"]};
    --ws-text: {tokens["text_main"]};
    --ws-border-soft: {tokens["border_soft"]};
    --ws-border-strong: {tokens["border_strong"]};
    --ws-border: {tokens["border_soft"]};
    --ws-shadow: {tokens["shadow"]};
    --ws-shadow-hover: {tokens["shadow_hover"]};
    --ws-ai-glow: {tokens["ai_glow"]};
    --ws-color-up: {tokens["color_up"]};
    --ws-color-down: {tokens["color_down"]};
    --ws-color-warn: {tokens["color_warn"]};
    --ws-color-neutral: {tokens["color_neutral"]};
    --ws-color-accent-alt: {tokens["color_accent_alt"]};
    --ws-radius-lg: {tokens["radius_lg"]};
    --ws-radius-md: {tokens["radius_md"]};
    --ws-radius-sm: {tokens["radius_sm"]};
    --ws-font-sans: {SYSTEM_FONT_FAMILY};
    --ws-font-heading: {SYSTEM_FONT_FAMILY};
    --ws-font-data: "SF Mono", ui-monospace, "Cascadia Code", Consolas, monospace;
    --ws-font-size-min: {MIN_FONT_SIZE}px;
    --ws-sidebar-width: 230px;
    --ws-sidebar-row-height: 34px;
    --ws-sidebar-row-gap: 2px;
    --ws-sidebar-bg: #F5F5F7;
    --ws-sidebar-active-bg: #E8F2FC;
    --ws-sidebar-hover-bg: #EBEBF0;
    --ws-sidebar-accent: #0066CC;
    --ws-sidebar-accent-hover: #0071E3;
    --ws-sidebar-text: #1D1D1F;
    --ws-sidebar-line: #D2D2D7;
    /* Legacy token aliases kept for extensions that inspect generated CSS. */
    /* --ws-bg-base: #F7F9FF; --ws-bg-dark: #2A3138; --ws-color-primary: #0F69FF; */
    /* --ws-color-up: #037B66; --ws-color-down: #D11022; */
    /* --ws-sidebar-width: 220px; --ws-sidebar-accent: #365CCB; --ws-sidebar-active-bg: #E9EEF7; */
}}

html,
body,
[class*="css"] {{
    font-family: var(--ws-font-sans);
    font-size: 14px;
    font-weight: 400;
    line-height: 1.42;
    letter-spacing: 0;
}}

button,
input,
textarea,
select,
label,
[role="option"],
[role="tab"],
[data-baseweb="select"],
[data-baseweb="input"],
[data-baseweb="textarea"] {{
    font-family: var(--ws-font-sans) !important;
}}

img[src*="/app/static/icons/"],
.ws-inline-svg-icon {{
    display: inline-block;
    width: 1em;
    height: 1em;
    margin: 0 0.24em 0 0;
    object-fit: contain;
    vertical-align: -0.12em;
}}

button img[src*="/app/static/icons/"],
[data-baseweb="tab"] img[src*="/app/static/icons/"] {{
    flex: 0 0 auto;
    width: 15px;
    height: 15px;
    margin-right: 0.38rem;
}}

[data-testid="stAlert"] [data-testid="stIconMaterial"] {{
    display: none !important;
}}

[data-testid="stAlert"] img[src*="/app/static/icons/"] {{
    width: 16px;
    height: 16px;
    margin-right: 0.42rem;
}}

html,
body,
.stApp,
[data-testid="stAppViewContainer"] {{
    color: var(--ws-text-main) !important;
    background: var(--ws-bg-base) !important;
}}

[data-testid="stAppViewContainer"] > .main,
.main .block-container {{
    position: relative;
    background: transparent !important;
}}

.main .block-container {{
    max-width: {tokens["max_width"]};
    padding: 0.35rem 1.5rem 3rem;
}}

.main p,
.main li,
.main label,
.main .stMarkdown,
.main [data-testid="stCaptionContainer"] {{
    color: var(--ws-text-muted) !important;
}}

.main a {{
    color: var(--ws-color-secondary) !important;
    text-decoration: none;
}}

.main a:hover {{
    color: var(--ws-color-primary) !important;
    text-decoration: underline;
    text-underline-offset: 0.15em;
}}

#MainMenu,
footer {{
    visibility: hidden;
}}

header,
[data-testid="stHeader"] {{
    display: none !important;
    height: 0 !important;
    min-height: 0 !important;
}}

.ws-page-loading-mask {{
    position: fixed;
    inset: 0 0 32px var(--ws-sidebar-width);
    z-index: 997;
    display: flex;
    align-items: center;
    justify-content: center;
    color: var(--ws-sidebar-accent);
    background: rgba(245, 245, 247, 0.94);
    backdrop-filter: blur(8px);
    -webkit-backdrop-filter: blur(8px);
    pointer-events: all;
}}

.ws-page-loading-mask__indicator {{
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
    color: var(--ws-sidebar-accent);
    font-family: var(--ws-font-data);
    font-size: 1rem;
    font-weight: 700;
}}

.ws-page-loading-mask__spinner {{
    flex: 0 0 18px;
    width: 18px;
    height: 18px;
    display: inline-grid;
    place-items: center;
    transform-origin: center;
    will-change: transform;
    animation: ws-page-loading-spin 0.8s linear infinite;
}}

.ws-page-loading-mask__spinner img {{
    width: 18px;
    height: 18px;
    margin: 0;
    filter: brightness(0) saturate(100%) invert(38%) sepia(44%) saturate(1781%) hue-rotate(194deg) brightness(84%) contrast(90%);
}}

@keyframes ws-page-loading-spin {{
    to {{ transform: rotate(360deg); }}
}}

.ws-page-status-bar {{
    position: fixed;
    inset: auto 0 0 var(--ws-sidebar-width);
    z-index: 1000;
    box-sizing: border-box;
    height: 32px;
    display: flex;
    align-items: center;
    gap: 0.65rem;
    padding: 0 1rem;
    color: var(--ws-sidebar-text);
    background: #FFFFFF;
    border-top: 1px solid var(--ws-sidebar-line);
    font-family: var(--ws-font-data);
    font-size: 1rem;
    font-weight: 600;
}}

.ws-page-status-bar__item {{
    min-width: 0;
    display: inline-flex;
    align-items: center;
    gap: 0.34rem;
    white-space: nowrap;
}}

.ws-page-status-bar__item img {{
    flex: 0 0 13px;
    width: 13px;
    height: 13px;
    margin: 0;
    opacity: 0.78;
}}

.ws-page-status-bar__item--healthy {{ color: var(--ws-color-up); }}
.ws-page-status-bar__item--warning {{ color: var(--ws-color-warn); }}

.ws-page-status-bar__divider {{
    flex: 0 0 1px;
    width: 1px;
    height: 14px;
    background: var(--ws-sidebar-line);
}}

.ws-page-status-bar__meta {{
    min-width: 0;
    margin-left: auto;
    overflow: hidden;
    color: var(--ws-text-soft);
    text-overflow: ellipsis;
    white-space: nowrap;
}}

.stApp:has([data-testid="stSidebar"][aria-expanded="false"]) .ws-page-loading-mask,
.stApp:has([data-testid="stSidebar"][aria-expanded="false"]) .ws-page-status-bar {{
    left: var(--ws-sidebar-collapsed-width);
}}

[data-testid="stSidebar"] {{
    position: relative;
    box-sizing: border-box !important;
    min-width: var(--ws-sidebar-width) !important;
    width: var(--ws-sidebar-width) !important;
    padding: 0.8rem 0.6rem !important;
    background: var(--ws-sidebar-bg) !important;
    border-right: 1px solid var(--ws-sidebar-line) !important;
}}

[data-testid="stSidebar"] > div:first-child,
[data-testid="stSidebar"] [data-testid="stSidebarContent"] {{
    box-sizing: border-box !important;
    width: 100% !important;
    padding-top: 0 !important;
    background: var(--ws-sidebar-bg) !important;
}}

[data-testid="stSidebarHeader"] {{
    display: none !important;
    width: 0 !important;
    height: 0 !important;
    min-height: 0 !important;
    margin: 0 !important;
    padding: 0 !important;
    overflow: hidden !important;
}}

@media (min-width: 769px) {{
    [data-testid="stSidebar"] [class*="st-key-sidebar_search_query"] [data-testid="stTextInputRootElement"],
    [data-testid="stSidebar"] [class*="st-key-sidebar-search-query"] [data-testid="stTextInputRootElement"] {{
        min-height: 36px !important;
        height: 36px !important;
        border-color: var(--ws-border-strong) !important;
    }}
}}

[data-testid="stSidebarCollapseButton"],
button[aria-label="Collapse sidebar"],
button[aria-label="Close sidebar"] {{
    position: absolute !important;
    top: 0.55rem !important;
    right: 0.5rem !important;
    left: auto !important;
    z-index: 1004 !important;
    display: flex !important;
}}

[data-testid="collapsedControl"],
[data-testid="stSidebarCollapsedControl"],
[data-testid="stExpandSidebarButton"],
button[aria-label="Expand sidebar"],
button[aria-label="Open sidebar"] {{
    position: fixed !important;
    top: 0.55rem !important;
    left: 0.55rem !important;
    z-index: 1004 !important;
    display: flex !important;
}}

[data-testid="stSidebarCollapseButton"],
[data-testid="stSidebarCollapseButton"] button,
[data-testid="stExpandSidebarButton"],
[data-testid="stExpandSidebarButton"] button,
[data-testid="collapsedControl"] button,
button[aria-label="Collapse sidebar"],
button[aria-label="Close sidebar"],
button[aria-label="Expand sidebar"],
button[aria-label="Open sidebar"] {{
    width: 30px !important;
    height: 30px !important;
    min-height: 30px !important;
    padding: 0 !important;
    color: var(--ws-sidebar-text) !important;
    background: transparent !important;
    border: 0 !important;
    border-radius: var(--ws-radius-sm) !important;
    box-shadow: none !important;
}}

[data-testid="stSidebarCollapseButton"]:hover,
[data-testid="stSidebarCollapseButton"] button:hover,
[data-testid="stExpandSidebarButton"]:hover,
[data-testid="stExpandSidebarButton"] button:hover,
[data-testid="collapsedControl"] button:hover,
button[aria-label="Collapse sidebar"]:hover,
button[aria-label="Close sidebar"]:hover,
button[aria-label="Expand sidebar"]:hover,
button[aria-label="Open sidebar"]:hover {{
    color: var(--ws-sidebar-accent) !important;
    background: var(--ws-sidebar-hover-bg) !important;
}}

[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] label {{
    color: var(--ws-text-main) !important;
}}

[data-testid="stSidebar"] .ws-sidebar-brand {{
    min-height: 62px;
    display: grid;
    grid-template-columns: 30px minmax(0, 1fr);
    grid-template-rows: auto auto;
    column-gap: 0.55rem;
    align-items: center;
    margin: 0 0 1rem;
    padding: 0.25rem 0.2rem 0.85rem;
    background: transparent;
    border-bottom: 1px solid var(--ws-border-soft);
}}

[data-testid="stSidebar"] .ws-sidebar-brand-kicker {{
    grid-row: 1 / span 2;
    width: 30px;
    height: 30px;
    display: grid;
    place-items: center;
    padding: 0;
    color: #FFFFFF !important;
    background: var(--ws-color-primary);
    border-radius: var(--ws-radius-md);
    font-family: var(--ws-font-heading);
    font-size: 1rem;
    font-weight: 800;
    text-transform: uppercase;
}}

[data-testid="stSidebar"] .ws-sidebar-brand h2 {{
    align-self: end;
    margin: 0;
    color: var(--ws-color-primary-hover) !important;
    font-family: var(--ws-font-heading);
    font-size: 1.05rem;
    font-weight: 800;
    line-height: 1.1;
}}

[data-testid="stSidebar"] .ws-sidebar-brand p {{
    align-self: start;
    margin: 0.12rem 0 0;
    color: var(--ws-text-muted) !important;
    font-family: var(--ws-font-data);
    font-size: 1rem;
    font-weight: 700;
    line-height: 1;
    text-transform: uppercase;
}}

[data-testid="stSidebar"] .ws-sidebar-block {{
    margin: 0.8rem 0 0.35rem;
    padding: 0 0.35rem;
    border-bottom: 0;
}}

[data-testid="stSidebar"] [data-testid="stMarkdownContainer"]:has(> .ws-sidebar-block) {{
    margin-bottom: 0 !important;
}}

[data-testid="stSidebar"] .ws-sidebar-block-title {{
    color: var(--ws-text-muted) !important;
    font-family: var(--ws-font-data);
    font-size: 1rem;
    font-weight: 700;
    letter-spacing: 0;
    text-transform: uppercase;
}}

[data-testid="stSidebar"] .ws-sidebar-block--account {{
    margin-top: 1rem;
    padding-top: 0.7rem;
    border-top: 1px solid var(--ws-sidebar-line);
}}

[class*="st-key-user-session-menu-"] button {{
    box-sizing: border-box !important;
    height: var(--ws-sidebar-row-height);
    min-height: var(--ws-sidebar-row-height);
    max-height: var(--ws-sidebar-row-height);
    justify-content: flex-start !important;
    color: var(--ws-sidebar-text) !important;
    background: transparent !important;
    border: 1px solid var(--ws-sidebar-line) !important;
    border-radius: var(--ws-radius-sm) !important;
    box-shadow: none !important;
}}

[class*="st-key-user-session-menu-"] button:hover {{
    color: var(--ws-sidebar-accent-hover) !important;
    background: var(--ws-sidebar-hover-bg) !important;
}}

[data-testid="stSidebar"] .ws-sidebar-block-copy,
[data-testid="stSidebar"] .ws-sidebar-page-description,
[data-testid="stSidebar"] .ws-sidebar-search-result-meta,
[data-testid="stSidebar"] .ws-sidebar-empty {{
    color: var(--ws-text-soft) !important;
    font-size: 1rem;
    line-height: 1.35;
}}

[data-testid="stSidebar"] .ws-sidebar-page-description {{
    display: none;
}}

[data-testid="stSidebar"] .ws-sidebar-search-result-meta {{
    display: block;
    margin: 0.12rem 0 0.3rem 0.6rem;
}}

[data-testid="stSidebar"] .ws-sidebar-empty {{
    display: block;
    padding: 0.65rem;
    background: var(--ws-surface-soft);
    border: 1px dashed var(--ws-border-strong);
    border-radius: var(--ws-radius-md);
    text-align: center;
}}

[data-testid="stSidebar"] .ws-sidebar-recent-item {{
    display: flex;
    flex-direction: column;
    gap: 0.1rem;
    margin-top: 0.2rem;
    padding: 0.5rem 0.6rem;
    background: var(--ws-surface-soft);
    border: 1px solid var(--ws-border-soft);
    border-radius: var(--ws-radius-md);
}}

[data-testid="stSidebar"] .ws-sidebar-recent-module {{
    color: var(--ws-color-primary) !important;
    font-family: var(--ws-font-data);
    font-size: 1rem;
    font-weight: 700;
}}

[data-testid="stSidebar"] .ws-sidebar-recent-page {{
    color: var(--ws-text-main) !important;
    font-size: 1rem;
    font-weight: 600;
}}

[data-testid="stSidebar"] [role="radiogroup"] {{
    padding: 0.35rem !important;
    margin: 0.25rem 0 0.6rem !important;
    background: var(--ws-surface-soft) !important;
    border: 1px solid var(--ws-border-soft) !important;
    border-radius: var(--ws-radius-md) !important;
}}

[data-testid="stSidebar"] [aria-checked="true"] {{
    accent-color: var(--ws-sidebar-accent) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-tree"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-tree"] [data-testid="stVerticalBlock"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-favorite-list"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-favorite-list"] [data-testid="stVerticalBlock"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-list"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-list"] [data-testid="stVerticalBlock"] {{
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: var(--ws-sidebar-row-gap) !important;
    width: 100%;
    padding: 0;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] {{
    box-sizing: border-box;
    width: 100%;
    margin: 0;
    border-radius: var(--ws-radius-md);
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] {{
    width: 100%;
    margin: 0;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] {{
    position: relative;
    box-sizing: border-box;
    width: calc(100% - 1.55rem);
    min-height: var(--ws-sidebar-row-height);
    margin-left: 1.55rem;
    padding-left: 0.58rem;
    border-left: 1px solid var(--ws-sidebar-line);
    border-radius: 0;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-favorite-"] button {{
    display: flex !important;
    box-sizing: border-box !important;
    align-items: center !important;
    width: 100%;
    height: var(--ws-sidebar-row-height);
    min-height: var(--ws-sidebar-row-height);
    max-height: var(--ws-sidebar-row-height);
    justify-content: flex-start !important;
    padding: 0 0.48rem !important;
    color: var(--ws-sidebar-text) !important;
    background: transparent !important;
    border: 1px solid transparent !important;
    border-radius: var(--ws-radius-md) !important;
    box-shadow: none !important;
    font-size: 1rem;
    font-weight: 500 !important;
    text-align: left !important;
    line-height: 1 !important;
    white-space: nowrap;
    overflow: hidden;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] button {{
    display: flex !important;
    align-items: center !important;
    justify-content: flex-start !important;
    gap: 0.5rem;
    color: var(--ws-sidebar-text) !important;
    font-size: 1rem;
    font-weight: 600 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-favorite-"] {{
    box-sizing: border-box;
    width: 100%;
    margin: 0;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] button > div,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] button > div,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] button > div {{
    display: flex !important;
    flex: 1 1 auto !important;
    align-items: center !important;
    justify-content: flex-start !important;
    width: auto !important;
    min-width: 0 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] button > div > span,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] button > div > span,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] button > div > span {{
    display: flex !important;
    flex: 1 1 auto !important;
    align-items: center !important;
    justify-content: flex-start !important;
    width: 100% !important;
    min-width: 0 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] button [data-testid="stMarkdownContainer"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] button [data-testid="stMarkdownContainer"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] button [data-testid="stMarkdownContainer"] {{
    display: flex !important;
    flex: 1 1 auto !important;
    align-items: center !important;
    justify-content: flex-start !important;
    width: 100% !important;
    min-width: 0 !important;
    margin: 0 !important;
    margin-right: auto !important;
    text-align: left !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] button {{
    padding-right: 0.35rem !important;
    padding-left: 0.35rem !important;
    border: 0 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-toggle-"] {{
    box-sizing: border-box;
    width: 100%;
    margin-top: 0.8rem;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-toggle-"] button {{
    position: relative;
    box-sizing: border-box !important;
    width: 100%;
    height: var(--ws-sidebar-row-height) !important;
    min-height: var(--ws-sidebar-row-height) !important;
    max-height: var(--ws-sidebar-row-height) !important;
    display: flex !important;
    align-items: center !important;
    justify-content: flex-start !important;
    padding: 0 0.35rem !important;
    color: var(--ws-text-muted) !important;
    background: transparent !important;
    border: 0 !important;
    border-radius: var(--ws-radius-sm) !important;
    box-shadow: none !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-toggle-"] button:hover {{
    color: var(--ws-sidebar-accent) !important;
    background: var(--ws-sidebar-hover-bg) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-toggle-"] button > div,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-toggle-"] button > div > span,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-toggle-"] button [data-testid="stMarkdownContainer"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-toggle-"] button p {{
    min-width: 0 !important;
    width: 100% !important;
    display: flex !important;
    align-items: center !important;
    justify-content: flex-start !important;
    margin: 0 !important;
    color: inherit !important;
    font-family: var(--ws-font-data) !important;
    font-size: 14px !important;
    font-weight: 700 !important;
    line-height: 1 !important;
    text-align: left !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-toggle-"] button::after {{
    position: absolute;
    top: 50%;
    right: 0.45rem;
    width: 14px;
    height: 14px;
    content: "";
    background-color: currentColor;
    -webkit-mask: url("/app/static/icons/chevron-right.svg") center / 14px 14px no-repeat;
    mask: url("/app/static/icons/chevron-right.svg") center / 14px 14px no-repeat;
    transform: translateY(-50%);
    transform-origin: center;
    transition: transform 140ms ease;
    pointer-events: none;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-toggle-expanded"] button::after {{
    transform: translateY(-50%) rotate(90deg);
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] button p,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] button p,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] button p,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] button p,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-favorite-"] button p {{
    display: flex !important;
    align-items: center !important;
    justify-content: flex-start !important;
    width: 100% !important;
    margin: 0 !important;
    color: inherit !important;
    text-align: left !important;
    line-height: 1 !important;
    white-space: nowrap !important;
    overflow: hidden !important;
    text-overflow: ellipsis !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] button::before {{
    content: "";
    flex: 0 0 18px;
    width: 18px;
    height: 18px;
    background: #64748B;
    mask: url("/app/static/icons/activity.svg") center / 16px 16px no-repeat;
    -webkit-mask: url("/app/static/icons/activity.svg") center / 16px 16px no-repeat;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] button::after {{
    content: "";
    flex: 0 0 14px;
    width: 14px;
    height: 14px;
    margin-left: auto;
    background: #7B8798;
    mask: url("/app/static/icons/chevron-right.svg") center / 14px 14px no-repeat;
    -webkit-mask: url("/app/static/icons/chevron-right.svg") center / 14px 14px no-repeat;
    transform-origin: center;
    transition: transform 120ms ease;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-decision"] button::before {{
    mask-image: url("/app/static/icons/briefcase-business.svg");
    -webkit-mask-image: url("/app/static/icons/briefcase-business.svg");
}}
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-stock"] button::before {{
    mask-image: url("/app/static/icons/chart-candlestick.svg");
    -webkit-mask-image: url("/app/static/icons/chart-candlestick.svg");
}}
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-fund"] button::before {{
    mask-image: url("/app/static/icons/landmark.svg");
    -webkit-mask-image: url("/app/static/icons/landmark.svg");
}}
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-money"] button::before {{
    mask-image: url("/app/static/icons/badge-dollar-sign.svg");
    -webkit-mask-image: url("/app/static/icons/badge-dollar-sign.svg");
}}
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-macro"] button::before {{
    mask-image: url("/app/static/icons/globe.svg");
    -webkit-mask-image: url("/app/static/icons/globe.svg");
}}
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-data"] button::before {{
    mask-image: url("/app/static/icons/database.svg");
    -webkit-mask-image: url("/app/static/icons/database.svg");
}}
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-favorite"] button::before {{
    background: #F5B400 !important;
    mask-image: url("/app/static/icons/star.svg");
    -webkit-mask-image: url("/app/static/icons/star.svg");
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-my_favorite"] button img[src$="/star.svg"] {{
    opacity: 1 !important;
    filter: brightness(0) saturate(100%) invert(70%) sepia(100%) saturate(1186%) hue-rotate(359deg) brightness(102%) contrast(103%) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] button {{
    display: flex !important;
    box-sizing: border-box !important;
    align-items: center !important;
    width: 100%;
    height: var(--ws-sidebar-row-height);
    min-height: var(--ws-sidebar-row-height);
    max-height: var(--ws-sidebar-row-height);
    justify-content: flex-start !important;
    padding: 0 0.2rem !important;
    color: #607087 !important;
    background: transparent !important;
    border: 0 !important;
    border-radius: var(--ws-radius-sm) !important;
    box-shadow: none !important;
    font-size: 1rem;
    font-weight: 500 !important;
    line-height: 1 !important;
    text-align: left !important;
    white-space: nowrap;
    overflow: hidden;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] button img {{
    display: inline-block !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] button img,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] button img,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] button img,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-favorite-"] button img {{
    width: 15px;
    height: 15px;
    margin-right: 0.42rem;
    opacity: 0.78;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] button:hover,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] button:hover,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] button:hover,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] button:hover {{
    color: var(--ws-sidebar-accent-hover) !important;
    background: var(--ws-sidebar-hover-bg) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-expanded"] button {{
    color: var(--ws-sidebar-text) !important;
    background: transparent !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-expanded"] button::after {{
    transform: rotate(90deg);
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-current"] button {{
    color: var(--ws-sidebar-accent) !important;
    background: var(--ws-sidebar-active-bg) !important;
    border-color: transparent !important;
    box-shadow: none !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-current"] button::before,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-current"] button::after {{
    color: var(--ws-sidebar-accent);
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-current"] button::before {{
    background: var(--ws-sidebar-accent);
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-current"] button::after {{
    background: var(--ws-sidebar-accent);
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-active"] button img,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-current"] button img {{
    filter: brightness(0) saturate(100%) invert(38%) sepia(44%) saturate(1781%) hue-rotate(194deg) brightness(84%) contrast(90%);
    opacity: 1;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-active"] button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-current"] button {{
    color: var(--ws-sidebar-accent) !important;
    background: transparent !important;
    font-weight: 600 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-active"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-current"] {{
    border-left: 2px solid var(--ws-sidebar-accent);
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-active"]::before {{
    display: none;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-favorite-"] button {{
    color: var(--ws-sidebar-accent) !important;
    background: var(--ws-sidebar-active-bg) !important;
    border-color: var(--ws-sidebar-line) !important;
}}

html body .stApp [data-testid="stAppViewContainer"] .main .block-container h1,
html body .stApp [data-testid="stAppViewContainer"] .main .block-container h2,
html body .stApp [data-testid="stAppViewContainer"] .main .block-container h3,
html body .stApp [data-testid="stAppViewContainer"] .main .block-container h1 *,
html body .stApp [data-testid="stAppViewContainer"] .main .block-container h2 *,
html body .stApp [data-testid="stAppViewContainer"] .main .block-container h3 * {{
    color: var(--ws-text) !important;
    background: none !important;
    background-image: none !important;
    -webkit-background-clip: border-box !important;
    background-clip: border-box !important;
    -webkit-text-fill-color: var(--ws-text) !important;
    text-fill-color: var(--ws-text) !important;
    text-shadow: none !important;
    letter-spacing: 0 !important;
}}

h1,
h2,
h3,
h4 {{
    font-family: var(--ws-font-heading);
    letter-spacing: 0;
}}

h1 {{ font-size: 1.55rem; line-height: 1.2; font-weight: 700; }}
h2 {{ font-size: 1.25rem; line-height: 1.25; font-weight: 700; }}
h3 {{ font-size: 1.05rem; line-height: 1.3; font-weight: 700; }}

[data-testid="stSidebar"] label,
.stSelectbox label,
.stMultiSelect label,
.stTextInput label,
.stDateInput label,
.stNumberInput label,
.stTextArea label,
.stFileUploader label {{
    color: var(--ws-text-muted) !important;
    font-family: var(--ws-font-data);
    font-size: 1rem;
    font-weight: 700;
    letter-spacing: 0;
}}

.stButton > button,
.stDownloadButton > button,
button[kind="secondary"],
[data-testid="stFormSubmitButton"] > button {{
    min-height: 40px;
    padding: 0.42rem 0.78rem !important;
    color: var(--ws-text-main) !important;
    background: var(--ws-bg-surface) !important;
    border: 1px solid var(--ws-border-soft) !important;
    border-radius: var(--ws-radius-md) !important;
    box-shadow: none !important;
    font-size: 1rem;
    font-weight: 600 !important;
    letter-spacing: 0 !important;
}}

button[kind="primary"],
[data-testid="stFormSubmitButton"] > button {{
    color: var(--ws-text-inverse) !important;
    background: var(--ws-color-primary) !important;
    border-color: var(--ws-color-primary) !important;
    border-radius: 9999px !important;
}}

.stButton > button *,
.stDownloadButton > button *,
button[kind="secondary"] *,
button[kind="primary"] *,
[data-testid="stFormSubmitButton"] > button * {{
    color: inherit !important;
    -webkit-text-fill-color: currentColor !important;
}}

.stButton > button:hover,
.stDownloadButton > button:hover,
button[kind="secondary"]:hover {{
    color: var(--ws-color-primary-hover) !important;
    background: #F4F8FF !important;
    border-color: var(--ws-color-primary) !important;
}}

button[kind="primary"]:hover,
[data-testid="stFormSubmitButton"] > button:hover {{
    color: var(--ws-text-inverse) !important;
    background: var(--ws-color-primary-hover) !important;
    border-color: var(--ws-color-primary-hover) !important;
}}

.stButton > button:active,
.stDownloadButton > button:active,
button[kind="secondary"]:active,
button[kind="primary"]:active,
[data-testid="stFormSubmitButton"] > button:active {{
    transform: scale(0.95);
}}

button:focus-visible {{
    outline: 2px solid var(--ws-color-primary) !important;
    outline-offset: 2px;
}}

html body .stApp [data-testid="stAppViewContainer"] .main a[href*="iphone_mode"],
html body .stApp [data-testid="stAppViewContainer"] .main a[href*="iphone_mode"] * {{
    color: var(--ws-text-inverse) !important;
    background: var(--ws-color-primary) !important;
    background-image: none !important;
    border: 1px solid var(--ws-color-primary) !important;
    border-radius: var(--ws-radius-md) !important;
    box-shadow: none !important;
}}

[data-baseweb="select"] > div,
.stTextInput [data-baseweb="input"],
.stTextInput [data-baseweb="base-input"],
.stTextInput input,
.stNumberInput input,
.stDateInput input,
.stTimeInput input,
.stTextArea textarea {{
    min-height: 36px;
    color: var(--ws-text-main) !important;
    background: var(--ws-bg-surface) !important;
    border: 1px solid var(--ws-border-strong) !important;
    border-radius: 8px !important;
    box-shadow: none !important;
    font-size: 1rem;
}}

[data-baseweb="select"] > div:focus-within,
.stTextInput [data-baseweb="input"]:focus-within,
.stTextInput [data-baseweb="base-input"]:focus-within,
.stTextInput input:focus,
.stNumberInput input:focus,
.stDateInput input:focus,
.stTimeInput input:focus,
.stTextArea textarea:focus {{
    border-color: var(--ws-color-primary) !important;
    box-shadow: 0 0 0 3px rgba(0, 102, 204, 0.18) !important;
}}

[data-baseweb="tag"] {{
    color: var(--ws-color-primary-hover) !important;
    background: var(--ws-color-primary-soft) !important;
    border: 0 !important;
    border-radius: var(--ws-radius-md) !important;
    box-shadow: none !important;
}}

[data-baseweb="checkbox"] [aria-checked="true"],
[data-baseweb="toggle"] [aria-checked="true"],
[data-baseweb="radio"] [aria-checked="true"],
[data-testid="stRadio"] [role="radio"][aria-checked="true"],
[role="radiogroup"] [role="radio"][aria-checked="true"] {{
    background-color: var(--ws-color-primary) !important;
    border-color: var(--ws-color-primary) !important;
}}

input[type="radio"],
input[type="checkbox"] {{
    accent-color: var(--ws-color-primary) !important;
}}

.stMetric,
[data-testid="stMetric"],
[data-testid="metric-container"],
.stPlotlyChart,
[data-testid="stDataFrame"],
div[data-testid="stTable"],
div[data-testid="stExpander"] {{
    background: var(--ws-bg-surface) !important;
    border: 1px solid var(--ws-border-soft) !important;
    border-radius: var(--ws-radius-lg) !important;
    box-shadow: none !important;
}}

.stMetric,
[data-testid="stMetric"],
[data-testid="metric-container"] {{
    min-height: 82px;
    padding: 0.65rem 0.75rem !important;
}}

[data-testid="stMetricLabel"] p {{
    color: var(--ws-text-muted) !important;
    font-family: var(--ws-font-data);
    font-size: 1rem !important;
    font-weight: 700 !important;
}}

[data-testid="stMetricValue"],
[data-testid="stMetricDelta"],
[data-testid="stDataFrame"],
div[data-testid="stTable"] {{
    font-family: var(--ws-font-data);
    font-variant-numeric: tabular-nums;
    font-feature-settings: "tnum";
}}

[data-testid="stMetricValue"] {{
    color: var(--ws-text-main) !important;
    font-size: 1.35rem !important;
    font-weight: 700 !important;
    letter-spacing: 0 !important;
}}

.stPlotlyChart {{
    margin: 0.65rem 0;
    padding: 0.45rem;
}}

[data-testid="stDataFrame"],
div[data-testid="stTable"] {{
    padding: 0.2rem;
}}

[data-testid="stDataFrame"] [role="columnheader"] {{
    color: var(--ws-text-muted) !important;
    background: var(--ws-surface-soft) !important;
    font-family: var(--ws-font-data);
    font-size: 1rem !important;
    font-weight: 700 !important;
}}

div[data-testid="stExpander"] {{ overflow: hidden !important; }}

div[data-testid="stExpander"] details summary {{
    padding: 0.65rem 0.75rem !important;
    color: var(--ws-text-main) !important;
    font-size: 1rem;
    font-weight: 700 !important;
}}

div[data-testid="stExpanderDetails"] {{
    padding: 0.2rem 0.75rem 0.75rem !important;
    border-top: 1px solid var(--ws-border-soft);
}}

.ws-page-toolbar,
[class*="st-key-ws-page-toolbar"] {{
    margin: 0.3rem 0 0.75rem;
    padding: 0.65rem 0.75rem;
    background: var(--ws-surface-soft);
    border: 1px solid var(--ws-border-soft);
    border-radius: var(--ws-radius-lg);
    box-shadow: none;
}}

.ws-page-toolbar {{
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.55rem;
}}

[class*="st-key-ws-page-toolbar"] > div[data-testid="stVerticalBlock"] {{ gap: 0.55rem; }}
[class*="st-key-ws-page-toolbar"] [data-testid="stHorizontalBlock"] {{ align-items: flex-end; }}

.stTabs [role="tablist"] {{
    width: 100%;
    gap: 0;
    padding: 0;
    background: transparent;
    border-bottom: 1px solid var(--ws-border-soft);
    border-radius: 0;
    overflow-x: auto;
}}

.stTabs [role="tab"] {{
    min-height: 36px;
    padding: 0.4rem 0.72rem !important;
    color: var(--ws-text-muted) !important;
    background: transparent !important;
    border: 0 !important;
    border-bottom: 2px solid transparent !important;
    border-radius: 0 !important;
    font-size: 1rem;
    font-weight: 600 !important;
}}

.stTabs [role="tab"] * {{ color: inherit !important; -webkit-text-fill-color: currentColor !important; }}

.stTabs [aria-selected="true"] {{
    color: var(--ws-color-primary-hover) !important;
    background: #F5F9FF !important;
    border-bottom-color: var(--ws-color-primary) !important;
    box-shadow: none !important;
}}

.stAlert {{
    background: var(--ws-bg-surface) !important;
    border: 1px solid var(--ws-border-soft) !important;
    border-left: 3px solid var(--ws-color-primary) !important;
    border-radius: var(--ws-radius-md) !important;
    box-shadow: none !important;
    font-size: 1rem;
}}

.ws-empty-selection-state {{
    display: grid;
    min-height: 128px;
    place-items: center;
    margin: 1.5rem 0 0.75rem;
    padding: 2.5rem 1rem 1rem;
    color: var(--ws-text-soft) !important;
    background: transparent !important;
    border: 0 !important;
    border-radius: 0 !important;
    font-family: var(--ws-font-sans);
    font-size: 1rem;
    font-weight: 500;
    text-align: center;
}}

.stImage img {{
    border: 1px solid var(--ws-border-soft);
    border-radius: var(--ws-radius-md);
    box-shadow: none;
}}

.ws-ai-signal,
[data-testid="stAlertContainer"] .stAlert {{ box-shadow: var(--ws-ai-glow) !important; }}

[data-testid="stFileUploaderDropzone"] {{
    background: var(--ws-surface-soft) !important;
    border: 1px dashed var(--ws-border-strong) !important;
    border-radius: var(--ws-radius-md) !important;
}}

[data-testid="stProgress"] > div > div {{ background-color: var(--ws-color-primary) !important; }}

[data-baseweb="popover"],
[role="dialog"] {{
    border: 1px solid var(--ws-border-soft) !important;
    border-radius: var(--ws-radius-md) !important;
    box-shadow: var(--ws-shadow-hover) !important;
}}

.ws-login-dialog-intro {{
    display: grid;
    grid-template-columns: 38px minmax(0, 1fr);
    gap: 0.75rem;
    align-items: center;
    margin: 0 0 1rem;
    padding: 0.85rem;
    background: var(--ws-sidebar-bg);
    border: 1px solid var(--ws-sidebar-line);
    border-radius: var(--ws-radius-md);
}}

.ws-login-dialog-intro img {{
    width: 38px;
    height: 38px;
    margin: 0;
    padding: 8px;
    box-sizing: border-box;
    background: var(--ws-sidebar-active-bg);
    border-radius: 50%;
    filter: brightness(0) saturate(100%) invert(38%) sepia(44%) saturate(1781%) hue-rotate(194deg) brightness(84%) contrast(90%);
}}

.ws-login-dialog-intro strong,
.ws-login-dialog-intro span {{
    display: block;
    letter-spacing: 0;
}}

.ws-login-dialog-intro strong {{
    color: var(--ws-text-main);
    font-size: 1rem;
    font-weight: 700;
}}

.ws-login-dialog-intro span {{
    margin-top: 0.18rem;
    color: var(--ws-text-muted);
    font-size: 1rem;
    line-height: 1.4;
}}

[class*="st-key-ws-user-storage-bridge"] {{
    height: 0 !important;
    min-height: 0 !important;
    margin: 0 !important;
    overflow: hidden !important;
}}

hr {{ border-color: var(--ws-border-soft) !important; }}

code {{
    color: var(--ws-color-primary-hover) !important;
    background: var(--ws-surface-soft) !important;
    border-radius: var(--ws-radius-sm);
}}

@media (max-width: 768px) {{
    .ws-page-loading-mask {{ inset: 0 0 32px 0; }}

    .ws-page-status-bar {{
        left: 0;
        gap: 0.45rem;
        padding: 0 0.7rem;
    }}

    .ws-page-status-bar__meta {{ display: none; }}

    .main .block-container {{
        max-width: 100%;
        padding: 1rem 0.9rem 2.5rem;
    }}

    [data-testid="stSidebar"] {{
        min-width: var(--ws-sidebar-width) !important;
        width: var(--ws-sidebar-width) !important;
        padding: 0.8rem 0.6rem !important;
    }}

    [data-testid="stSidebar"] > div:first-child {{ width: 100% !important; }}

    [data-testid="stSidebar"][aria-expanded="false"] {{
        min-width: 0 !important;
        width: 0 !important;
        transform: translateX(-100%) !important;
    }}

    h1 {{ font-size: 1.35rem; }}
    h2 {{ font-size: 1.15rem; }}

    [data-testid="stHorizontalBlock"] {{ flex-wrap: wrap; }}
    [data-testid="column"] {{ min-width: min(100%, 15rem) !important; flex: 1 1 15rem !important; }}

    .stTabs [role="tablist"] {{ width: 100%; }}
    .stPlotlyChart {{ padding: 0.2rem; }}
}}

/* Apple surface pass: a single quiet system across every Streamlit primitive. */
:root {{
    --ws-bg: var(--ws-bg-base);
    --ws-space-1: 8px;
    --ws-space-2: 12px;
    --ws-space-3: 17px;
    --ws-space-4: 24px;
    --ws-space-5: 32px;
    --ws-sidebar-collapsed-width: 48px;
}}

.stApp,
[data-testid="stAppViewContainer"],
[data-testid="stAppViewContainer"] > .main {{
    background: var(--ws-bg-base) !important;
}}

.main .block-container,
[data-testid="stMainBlockContainer"],
[data-testid="stAppViewContainer"] .main .block-container {{
    max-width: 1320px !important;
    padding-top: 0.25rem !important;
    padding-right: var(--ws-space-4) !important;
    padding-bottom: 4rem !important;
    padding-left: var(--ws-space-4) !important;
}}

.main .block-container h1,
.main .block-container h2,
.main .block-container h3,
.main .block-container h4 {{
    color: var(--ws-text-main) !important;
    font-family: var(--ws-font-heading);
    font-weight: 650;
    letter-spacing: 0;
}}

.main .block-container h1 {{ font-size: 1.75rem; line-height: 1.16; margin: 0 0 var(--ws-space-3); }}
.main .block-container h2 {{ font-size: 1.35rem; line-height: 1.22; margin: var(--ws-space-4) 0 var(--ws-space-2); }}
.main .block-container h3 {{ font-size: 1.1rem; line-height: 1.3; margin: var(--ws-space-3) 0 var(--ws-space-1); }}

.stButton > button,
.stDownloadButton > button,
[data-testid="stFormSubmitButton"] > button,
button[kind="secondary"],
button[kind="primary"] {{
    transition: background-color 140ms ease, border-color 140ms ease, color 140ms ease, transform 100ms ease;
}}

button[kind="primary"],
[data-testid="stFormSubmitButton"] > button {{
    min-height: 40px;
    padding-right: 1.05rem !important;
    padding-left: 1.05rem !important;
    background: var(--ws-color-primary) !important;
    border-color: var(--ws-color-primary) !important;
    border-radius: 9999px !important;
}}

[data-testid="stSidebar"] {{
    width: var(--ws-sidebar-width) !important;
    min-width: var(--ws-sidebar-width) !important;
    padding: 0.7rem 0.55rem !important;
    background: var(--ws-sidebar-bg) !important;
    border-right: 1px solid var(--ws-sidebar-line) !important;
}}

[data-testid="stSidebar"] .ws-sidebar-brand {{
    min-height: 58px;
    display: grid;
    grid-template-columns: 32px minmax(0, 1fr);
    grid-template-rows: 32px 20px;
    column-gap: 0.5rem;
    row-gap: 0;
    align-items: center;
    margin: 0 0 var(--ws-space-3);
    padding: 0.25rem 0.25rem 0.75rem;
    border-bottom: 1px solid var(--ws-sidebar-line);
}}

/* Keep the shell ends fixed while the navigation body scrolls independently. */
[data-testid="stSidebar"] [data-testid="stSidebarContent"] {{
    height: 100% !important;
    min-height: 0 !important;
    padding-right: 0 !important;
    overflow: hidden !important;
}}

[data-testid="stSidebar"] [data-testid="stSidebarHeader"] {{
    position: absolute !important;
    inset: 0 0 auto 0 !important;
    z-index: 1004 !important;
    display: block !important;
    width: 100% !important;
    height: 0 !important;
    min-height: 0 !important;
    margin: 0 !important;
    padding: 0 !important;
    overflow: visible !important;
    pointer-events: none;
}}

[data-testid="stSidebar"] [data-testid="stSidebarHeader"] [data-testid="stLogoSpacer"] {{
    display: none !important;
}}

[data-testid="stSidebar"] [data-testid="stSidebarCollapseButton"] {{
    visibility: visible !important;
    opacity: 1 !important;
    pointer-events: auto;
}}

[data-testid="stSidebar"] [data-testid="stSidebarCollapseButton"] button,
[data-testid="stSidebar"] [data-testid="stSidebarCollapseButton"] [data-testid="stIconMaterial"] {{
    visibility: visible !important;
    opacity: 1 !important;
}}

[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] {{
    height: 100% !important;
    min-height: 0 !important;
    padding-bottom: 0 !important;
    overflow: hidden !important;
}}

[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] > div {{
    height: 100% !important;
    min-height: 0 !important;
    overflow: hidden !important;
}}

[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] > div > [data-testid="stVerticalBlock"] {{
    display: grid !important;
    grid-template-rows: auto minmax(0, 1fr) auto !important;
    gap: 0 !important;
    height: 100% !important;
    min-height: 0 !important;
    overflow: hidden !important;
}}

[data-testid="stSidebar"] [data-testid="stLayoutWrapper"]:has(> [class*="st-key-ws-sidebar-header"]),
[data-testid="stSidebar"] [data-testid="stLayoutWrapper"]:has(> [class*="st-key-ws-sidebar-footer"]) {{
    min-height: 0 !important;
    overflow: visible !important;
}}

[data-testid="stSidebar"] [data-testid="stLayoutWrapper"]:has(> [class*="st-key-ws-sidebar-middle"]) {{
    height: 100% !important;
    min-height: 0 !important;
    overflow: hidden !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-header"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-middle"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-footer"] {{
    min-height: 0 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-header"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-footer"] {{
    gap: 0 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-middle"] {{
    height: 100% !important;
    gap: 0 !important;
    padding-right: 0.2rem;
    overflow-x: hidden !important;
    overflow-y: auto !important;
    overscroll-behavior: contain;
    scrollbar-gutter: stable;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-footer"] {{
    padding-top: 0.65rem;
    background: var(--ws-sidebar-bg);
    border-top: 1px solid var(--ws-sidebar-line);
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-footer"] .ws-sidebar-block--account {{
    margin: 0 0 0.35rem;
    padding: 0 0.35rem;
    border-top: 0;
}}

[data-testid="stSidebar"] .ws-sidebar-brand-kicker {{
    grid-column: 1;
    grid-row: 1;
    align-self: center;
    width: 32px;
    height: 32px;
    border-radius: var(--ws-radius-sm);
    background: var(--ws-color-primary);
}}

[data-testid="stSidebar"] .ws-sidebar-brand h2 {{
    grid-column: 2;
    grid-row: 1;
    align-self: center;
    min-width: 0;
    margin: 0;
    padding: 0 !important;
    color: var(--ws-text-main) !important;
    font-family: var(--ws-font-heading);
    font-size: 1rem;
    font-weight: 650;
    line-height: 1.2;
    letter-spacing: 0 !important;
    white-space: nowrap;
}}

[data-testid="stSidebar"] .ws-sidebar-brand p {{
    grid-column: 2;
    grid-row: 2;
    align-self: start;
    min-width: 0;
    margin: 0 !important;
    padding: 0 !important;
    color: var(--ws-text-muted) !important;
    font-family: var(--ws-font-sans);
    font-size: 14px;
    font-weight: 600;
    line-height: 20px;
    letter-spacing: 0;
    text-transform: none;
    white-space: nowrap;
}}

[data-testid="stSidebar"] [class*="st-key-sidebar_search_query"] [data-testid="stTextInputRootElement"],
[data-testid="stSidebar"] [class*="st-key-sidebar-search-query"] [data-testid="stTextInputRootElement"] {{
    min-height: 36px !important;
    height: 36px !important;
    border-radius: 8px !important;
    background: var(--ws-bg-surface) !important;
}}

[class*="st-key-security_search_keyword"] [data-testid="stTextInputRootElement"],
[class*="st-key-security-search-keyword"] [data-testid="stTextInputRootElement"] {{
    border-radius: 8px !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-favorite-"] button {{
    height: var(--ws-sidebar-row-height) !important;
    min-height: var(--ws-sidebar-row-height) !important;
    max-height: var(--ws-sidebar-row-height) !important;
    border-radius: var(--ws-radius-sm) !important;
    border-color: transparent !important;
    color: var(--ws-sidebar-text) !important;
    background: transparent !important;
    box-shadow: none !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] button:hover,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] button:hover,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] button:hover,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] button:hover {{
    color: var(--ws-sidebar-accent-hover) !important;
    background: var(--ws-sidebar-hover-bg) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-active"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-current"] {{
    border-left-color: var(--ws-sidebar-accent);
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-active"] button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-current"] button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-current"] button {{
    color: var(--ws-sidebar-accent) !important;
    background: var(--ws-sidebar-active-bg) !important;
    font-weight: 600 !important;
}}

/* Collapsed navigation stays usable as a narrow icon rail. */
[data-testid="stSidebar"][aria-expanded="false"] {{
    display: block !important;
    position: relative !important;
    z-index: 1002 !important;
    box-sizing: border-box !important;
    width: var(--ws-sidebar-collapsed-width) !important;
    min-width: var(--ws-sidebar-collapsed-width) !important;
    max-width: var(--ws-sidebar-collapsed-width) !important;
    min-height: 100vh !important;
    padding: 3.25rem 6px 2.5rem !important;
    overflow: visible !important;
    visibility: visible !important;
    transform: translateX(0) !important;
    background: var(--ws-sidebar-bg) !important;
    border-right: 1px solid var(--ws-sidebar-line) !important;
}}

[data-testid="stSidebar"][aria-expanded="false"] [data-testid="stSidebarCollapseButton"] {{
    position: fixed !important;
    top: 8px !important;
    right: auto !important;
    left: 6px !important;
    width: 36px !important;
    height: 36px !important;
    margin: 0 !important;
    padding: 0 !important;
}}

[data-testid="stSidebar"][aria-expanded="false"] [data-testid="stSidebarCollapseButton"] button {{
    width: 36px !important;
    height: 36px !important;
    min-height: 36px !important;
    margin: 0 !important;
    padding: 0 !important;
}}

[data-testid="stSidebar"][aria-expanded="false"] [data-testid="stSidebarCollapseButton"] [data-testid="stIconMaterial"] {{
    transform: rotate(180deg);
}}

[data-testid="stSidebar"][aria-expanded="false"] > div:first-child,
[data-testid="stSidebar"][aria-expanded="false"] [data-testid="stSidebarContent"] {{
    width: 36px !important;
    min-width: 36px !important;
    max-width: 36px !important;
    padding: 0 !important;
    overflow: visible !important;
}}

[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-middle"] {{
    padding-right: 0 !important;
    scrollbar-gutter: auto;
    scrollbar-width: none;
}}

[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-middle"]::-webkit-scrollbar {{
    width: 0;
    height: 0;
}}

[data-testid="stSidebar"][aria-expanded="false"] .ws-sidebar-brand,
[data-testid="stSidebar"][aria-expanded="false"] .ws-sidebar-block,
[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-sidebar_search_query"],
[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-sidebar-search-query"],
[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-page-"],
[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-search-result-"],
[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-recent-toggle-"],
[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-recent-link-"],
[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-favorite-"],
[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-user-session-menu-"] {{
    display: none !important;
}}

[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-tree"],
[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-tree"] [data-testid="stVerticalBlock"] {{
    display: flex !important;
    flex-direction: column !important;
    align-items: center !important;
    gap: 8px !important;
    width: 36px !important;
    padding: 0 !important;
    overflow: visible !important;
}}

[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-module-"] {{
    position: relative !important;
    display: block !important;
    width: 36px !important;
    min-width: 36px !important;
    height: 36px !important;
    min-height: 36px !important;
    margin: 0 !important;
    overflow: visible !important;
}}

[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-module-"] button {{
    width: 36px !important;
    height: 36px !important;
    min-height: 36px !important;
    padding: 0 !important;
    justify-content: center !important;
    border-radius: 8px !important;
    font-size: 0 !important;
    overflow: visible !important;
}}

[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-module-"] button > div,
[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-module-"] button > div > span,
[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-module-"] button [data-testid="stMarkdownContainer"],
[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-module-"] button p {{
    display: none !important;
}}

[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-module-"] button::after {{
    display: none !important;
    position: absolute;
    top: 3px;
    left: 42px;
    z-index: 1100;
    box-sizing: border-box;
    width: max-content;
    max-width: 180px;
    padding: 7px 9px;
    color: #FFFFFF;
    background: #1D1D1F;
    border-radius: 8px;
    content: "";
    font-family: var(--ws-font-sans);
    font-size: 14px;
    font-weight: 600;
    line-height: 1.2;
    white-space: nowrap;
    pointer-events: none;
}}

[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-module-"] button:hover::after {{
    display: block !important;
}}

[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-module-decision"] button:hover::after {{ content: "Decision"; }}
[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-module-stock"] button:hover::after {{ content: "Stocks"; }}
[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-module-fund"] button:hover::after {{ content: "Funds"; }}
[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-module-money"] button:hover::after {{ content: "Money flow"; }}
[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-module-macro"] button:hover::after {{ content: "Macro"; }}
[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-module-data"] button:hover::after {{ content: "Data"; }}
[data-testid="stSidebar"][aria-expanded="false"] [class*="st-key-ws-sidebar-module-favorite"] button:hover::after {{ content: "Favorites"; }}

[data-testid="collapsedControl"] {{
    position: fixed !important;
    top: 8px !important;
    left: 8px !important;
    z-index: 1101 !important;
    display: flex !important;
    width: 32px !important;
    height: 32px !important;
    padding: 0 !important;
    background: var(--ws-bg-surface) !important;
    border: 1px solid var(--ws-sidebar-line) !important;
    border-radius: 8px !important;
}}

[data-testid="collapsedControl"] button {{
    position: relative !important;
    width: 32px !important;
    height: 32px !important;
    min-height: 32px !important;
}}

[data-testid="collapsedControl"] button:hover::after {{
    position: absolute;
    top: 3px;
    left: 38px;
    z-index: 1102;
    padding: 7px 9px;
    color: #FFFFFF;
    background: #1D1D1F;
    border-radius: 8px;
    content: "Expand sidebar";
    font-family: var(--ws-font-sans);
    font-size: 14px;
    font-weight: 600;
    line-height: 1.2;
    white-space: nowrap;
    pointer-events: none;
}}

[data-baseweb="select"] > div,
.stTextInput [data-baseweb="input"],
.stTextInput [data-baseweb="base-input"],
.stTextInput input,
.stNumberInput input,
.stDateInput input,
.stTimeInput input,
.stTextArea textarea,
[data-testid="stFileUploaderDropzone"] {{
    color: var(--ws-text-main) !important;
    background: var(--ws-bg-surface) !important;
    border-color: var(--ws-border-soft) !important;
    border-radius: 8px !important;
    box-shadow: none !important;
}}

[data-baseweb="select"] > div:focus-within,
.stTextInput [data-baseweb="input"]:focus-within,
.stTextInput [data-baseweb="base-input"]:focus-within,
.stTextInput input:focus,
.stNumberInput input:focus,
.stDateInput input:focus,
.stTimeInput input:focus,
.stTextArea textarea:focus {{
    border-color: var(--ws-color-primary) !important;
    box-shadow: 0 0 0 3px rgba(0, 102, 204, 0.18) !important;
}}

.stMetric,
[data-testid="stMetric"],
[data-testid="metric-container"],
.stPlotlyChart,
[data-testid="stDataFrame"],
div[data-testid="stTable"],
div[data-testid="stExpander"],
.ws-tracker-shell,
.ws-page-toolbar,
[class*="st-key-ws-page-toolbar"] {{
    background: var(--ws-bg-surface) !important;
    border: 1px solid var(--ws-border-soft) !important;
    border-radius: var(--ws-radius-lg) !important;
    box-shadow: none !important;
}}

.stMetric,
[data-testid="stMetric"],
[data-testid="metric-container"] {{
    padding: var(--ws-space-3) !important;
}}

[data-testid="stDataFrame"] [role="columnheader"],
div[data-testid="stTable"] thead th {{
    color: var(--ws-text-muted) !important;
    background: var(--ws-surface-soft) !important;
    border-bottom: 1px solid var(--ws-border-soft) !important;
    font-family: var(--ws-font-sans);
    font-size: 1rem !important;
    font-weight: 600 !important;
}}

.stPlotlyChart {{
    margin: var(--ws-space-2) 0;
    padding: 0 !important;
    overflow: hidden;
}}

.ws-page-toolbar,
[class*="st-key-ws-page-toolbar"] {{
    margin: var(--ws-space-2) 0 var(--ws-space-3);
    padding: var(--ws-space-2) var(--ws-space-3);
}}

.stTabs [role="tablist"] {{
    border-bottom: 1px solid var(--ws-border-soft);
}}

.stTabs [role="tab"] {{
    min-height: 40px;
    color: var(--ws-text-muted) !important;
    font-weight: 500 !important;
}}

.stTabs [aria-selected="true"] {{
    color: var(--ws-color-primary) !important;
    background: transparent !important;
    border-bottom-color: var(--ws-color-primary) !important;
}}

[data-baseweb="popover"],
[role="dialog"] {{
    background: var(--ws-bg-surface) !important;
    border: 1px solid var(--ws-border-soft) !important;
    border-radius: var(--ws-radius-lg) !important;
    box-shadow: none !important;
}}

.ws-page-loading-mask {{
    background: rgba(245, 245, 247, 0.94) !important;
}}

p,
label,
small,
caption,
figcaption,
th,
td,
input,
textarea,
select,
[role="option"],
[role="tab"],
[data-testid="stCaptionContainer"] {{
    font-size: max(var(--ws-font-size-min), 1em) !important;
}}

/* Streamlit alerts are passive page states in this app. Keep every alert
   variant quiet and centered instead of rendering a boxed callout. */
[data-testid="stAlertContainer"] {{
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
    width: 100% !important;
    min-height: 96px;
    margin: 1.25rem 0 0.75rem !important;
    padding: 1.5rem 0 0.5rem !important;
    background: transparent !important;
    border: 0 !important;
    border-radius: 0 !important;
    box-shadow: none !important;
    color: var(--ws-text-soft) !important;
    font-size: 1rem;
    text-align: center !important;
}}

[data-testid="stAlertContainer"] > [data-testid^="stAlertContent"],
[data-testid="stAlertContainer"] .stAlert,
[data-testid="stAlert"] {{
    width: auto !important;
    max-width: min(100%, 720px);
    margin: 0 auto !important;
    padding: 0.35rem 0.5rem !important;
    color: var(--ws-text-soft) !important;
    background: transparent !important;
    border: 0 !important;
    border-radius: 0 !important;
    box-shadow: none !important;
    text-align: center !important;
}}

[data-testid="stAlertContainer"] [data-testid="stIconMaterial"],
[data-testid="stAlertContainer"] [data-testid^="stAlertContent"] > svg,
[data-testid="stAlertContainer"] img {{
    display: none !important;
}}

[data-testid="stAlertContainer"] [data-testid^="stAlertContent"],
[data-testid="stAlertContainer"] [data-testid="stMarkdownContainer"],
[data-testid="stAlertContainer"] p {{
    margin: 0 !important;
    padding: 0 !important;
    color: inherit !important;
    text-align: center !important;
}}

/* Unified table surfaces */
html {{
    font-size: 16px !important;
}}

[data-testid="stDataFrame"] {{
    box-sizing: border-box !important;
    width: 100% !important;
    padding: 0 !important;
    background: transparent !important;
    border: 0 !important;
    border-radius: 0 !important;
    box-shadow: none !important;
    overflow: visible !important;
}}

[data-testid="stDataFrame"] [data-testid="stDataFrameResizable"] {{
    box-sizing: border-box !important;
    background: var(--ws-bg-surface) !important;
    border: 1px solid var(--ws-border-soft) !important;
    border-radius: 8px !important;
    box-shadow: none !important;
}}

[data-testid="stDataFrame"] .stDataFrameGlideDataEditor,
[data-testid="stDataFrame"] .dvn-scroller {{
    border-radius: 7px !important;
}}

div[data-testid="stTable"] {{
    padding: 0 !important;
    background: var(--ws-bg-surface) !important;
    border: 1px solid var(--ws-border-soft) !important;
    border-radius: 8px !important;
    box-shadow: none !important;
    overflow: auto !important;
}}

div[data-testid="stTable"] table,
.ws-fund-watchboard__holdings table {{
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    color: var(--ws-text-main);
    font-family: var(--ws-font-sans);
    font-size: 14px !important;
    line-height: 1.45;
}}

div[data-testid="stTable"] th,
div[data-testid="stTable"] td {{
    padding: 0.62rem 0.75rem !important;
    border: 0 !important;
    border-right: 1px solid var(--ws-border-soft) !important;
    border-bottom: 1px solid var(--ws-border-soft) !important;
    font-size: 14px !important;
    vertical-align: middle;
}}

div[data-testid="stTable"] th:last-child,
div[data-testid="stTable"] td:last-child {{
    border-right: 0 !important;
}}

div[data-testid="stTable"] tbody tr:last-child td {{
    border-bottom: 0 !important;
}}

hr {{ border-color: var(--ws-border-soft) !important; }}

/* Unified Apple page panel */
[data-testid="stMain"] {{
    box-sizing: border-box !important;
    padding: 16px 16px 48px !important;
    background: var(--ws-bg-base) !important;
}}

[data-testid="stMainBlockContainer"] {{
    box-sizing: border-box !important;
    width: 100% !important;
    height: auto !important;
    flex: 0 0 auto !important;
    max-width: 1320px !important;
    min-height: calc(100dvh - 64px) !important;
    margin: 0 auto !important;
    padding: 24px 28px 32px !important;
    position: relative;
    isolation: isolate;
    color: var(--ws-text-main) !important;
    background: var(--ws-bg-surface) !important;
    border: 1px solid rgba(15, 23, 42, 0.06) !important;
    border-radius: 18px !important;
    box-shadow:
        0 1px 2px rgba(15, 23, 42, 0.04),
        0 10px 30px rgba(15, 23, 42, 0.08) !important;
    overflow: visible;
}}

@media (max-width: 768px) {{
    [data-testid="stMain"] {{
        padding: 8px 8px 40px !important;
    }}

    .stApp:has([data-testid="stSidebar"][aria-expanded="false"]) [data-testid="stMain"] {{
        width: calc(100% - var(--ws-sidebar-collapsed-width)) !important;
        margin-left: var(--ws-sidebar-collapsed-width) !important;
    }}

    [data-testid="stMainBlockContainer"] {{
        min-height: calc(100dvh - 48px) !important;
        padding: 16px 14px 24px !important;
        border-radius: 14px !important;
    }}
}}
"""


def build_terminal_component_overrides_css() -> str:
    """Restyle project-specific dashboard HTML after its local CSS is injected."""
    return """
.ws-watchboard-shell,
.ws-fund-watchboard {
    --wb-bg: #FFFFFF;
    --wb-panel: #FFFFFF;
    --wb-panel-2: #FFFFFF;
    --wb-line: #E0E4E9;
    --wb-line-soft: #E0E4E9;
    --wb-cyan: #0F69FF;
    --wb-blue: #0052D0;
    --wb-red: #037B66;
    --wb-green: #D11022;
    --wb-text: #151C23;
    --wb-muted: #526174;
    --fw-bg: #FFFFFF;
    --fw-panel: #FFFFFF;
    --fw-panel-strong: #FFFFFF;
    --fw-line: #E0E4E9;
    --fw-line-soft: #E0E4E9;
    --fw-cyan: #0F69FF;
    --fw-blue: #0052D0;
    --fw-red: #037B66;
    --fw-green: #D11022;
    --fw-text: #151C23;
    --fw-muted: #526174;
    color: #151C23 !important;
    background: #FFFFFF !important;
    border-color: #E0E4E9 !important;
    border-radius: 4px !important;
    box-shadow: none !important;
}

.ws-watchboard-shell::before,
.ws-watchboard-panel::after,
.ws-fund-watchboard::before {
    display: none !important;
}

.ws-watchboard-panel,
.ws-watchboard-stat,
.ws-watchboard-chip,
.ws-watchboard-clock,
.ws-watchboard-status,
.ws-watchboard-compact-meta,
.ws-watchboard-summary-pill,
.ws-watchboard-stock-card,
.ws-fund-watchboard__metric,
.ws-fund-watchboard__live-status,
.ws-fund-watchboard__card,
.ws-fund-watchboard__live,
.ws-fund-watchboard__confirmed-nav,
.ws-fund-watchboard__focus,
.ws-fund-watchboard__card-metrics div,
.ws-fund-watchboard__table-wrap,
.ws-fund-watchboard__empty,
.st-key-watchlist_card_grid,
.st-key-fund_watchlist_card_grid,
.st-key-fund_watchlist_table_wrap,
.st-key-fund_watchlist_add_panel,
.st-key-fund_watchlist_toolbar,
.st-key-fund_watchlist_table_batch_controls,
.st-key-fund_watchlist_table_focus_controls {
    color: #151C23 !important;
    background: #FFFFFF !important;
    background-image: none !important;
    border-color: #E0E4E9 !important;
    border-radius: 4px !important;
    box-shadow: none !important;
}

.ws-watchboard-stat,
.ws-watchboard-summary-pill,
.ws-fund-watchboard__live,
.ws-fund-watchboard__confirmed-nav,
.ws-fund-watchboard__card-metrics div {
    background: #F4F7F9 !important;
}

.ws-watchboard-stat-icon,
.ws-fund-watchboard__badge {
    color: #0757D9 !important;
    -webkit-text-fill-color: #0757D9 !important;
    background: #E7F0FF !important;
    border-color: transparent !important;
    border-radius: 4px !important;
    box-shadow: none !important;
}

.ws-watchboard-shell :is(p, label, small),
.ws-fund-watchboard :is(p, label, small) {
    color: #526174 !important;
    -webkit-text-fill-color: #526174 !important;
}

.ws-watchboard-shell :is(h2, h3, h4),
.ws-watchboard-title strong,
.ws-watchboard-mini-name,
.ws-watchboard-mini-price,
.ws-fund-watchboard :is(h2, h3, h4),
.ws-fund-watchboard__card-title strong,
.ws-fund-watchboard__metric strong,
.ws-fund-watchboard__holdings-head strong {
    color: #151C23 !important;
    -webkit-text-fill-color: #151C23 !important;
    text-shadow: none !important;
}

.ws-watchboard-grid-h,
.ws-watchboard-grid-v {
    background: #E0E4E9 !important;
}

.ws-watchboard-area {
    opacity: 0.35 !important;
}

.ws-watchboard-segment,
.ws-watchboard-point,
.ws-score-fill {
    box-shadow: none !important;
}

.ws-score-donut {
    background: radial-gradient(circle at center, #FFFFFF 0 54%, transparent 55%),
        conic-gradient(var(--score-color) calc(var(--score) * 1%), #E0E4E9 0) !important;
    box-shadow: none !important;
}

.ws-fund-watchboard__ring::before {
    background: #FFFFFF !important;
    box-shadow: none !important;
}

.ws-fund-watchboard__holdings th {
    color: #526174 !important;
    -webkit-text-fill-color: #526174 !important;
    background: #F4F7F9 !important;
}

.ws-fund-watchboard__holdings td {
    color: #151C23 !important;
    -webkit-text-fill-color: #151C23 !important;
    border-color: #E0E4E9 !important;
}

@media (max-width: 900px) {
    .ws-watchboard-main,
    .ws-watchboard-hero,
    .ws-fund-watchboard__focus {
        grid-template-columns: 1fr !important;
    }
}

/* Project-specific HTML is emitted with legacy dashboard CSS. Keep its data
   layout, but make the surfaces obey the shared Apple tokens. */
.ws-watchboard-shell,
.ws-fund-watchboard {
    --wb-bg: #F5F5F7;
    --wb-panel: #FFFFFF;
    --wb-panel-2: #F5F5F7;
    --wb-line: #D2D2D7;
    --wb-line-soft: #E5E5EA;
    --wb-cyan: #0066CC;
    --wb-blue: #0071E3;
    --wb-red: #D70015;
    --wb-green: #248A3D;
    --wb-text: #1D1D1F;
    --wb-muted: #6E6E73;
    --fw-bg: #F5F5F7;
    --fw-panel: #FFFFFF;
    --fw-panel-strong: #FFFFFF;
    --fw-line: #D2D2D7;
    --fw-line-soft: #E5E5EA;
    --fw-cyan: #0066CC;
    --fw-blue: #0071E3;
    --fw-red: #D70015;
    --fw-green: #248A3D;
    --fw-text: #1D1D1F;
    --fw-muted: #6E6E73;
    color: #1D1D1F !important;
    background: #F5F5F7 !important;
    background-image: none !important;
    border: 1px solid #D2D2D7 !important;
    border-radius: 18px !important;
    box-shadow: none !important;
}

.ws-watchboard-shell *,
.ws-fund-watchboard * {
    font-family: var(--ws-font-sans) !important;
    text-shadow: none !important;
}

.ws-watchboard-panel,
.ws-watchboard-stat,
.ws-watchboard-chip,
.ws-watchboard-clock,
.ws-watchboard-status,
.ws-watchboard-compact-meta,
.ws-watchboard-summary-pill,
.ws-watchboard-stock-card,
.ws-fund-watchboard__metric,
.ws-fund-watchboard__live-status,
.ws-fund-watchboard__card,
.ws-fund-watchboard__live,
.ws-fund-watchboard__confirmed-nav,
.ws-fund-watchboard__focus,
.ws-fund-watchboard__card-metrics div,
.ws-fund-watchboard__table-wrap,
.ws-fund-watchboard__empty,
.st-key-watchlist_card_grid,
.st-key-fund_watchlist_card_grid,
.st-key-fund_watchlist_table_wrap,
.st-key-fund_watchlist_add_panel,
.st-key-fund_watchlist_toolbar,
.st-key-fund_watchlist_table_batch_controls,
.st-key-fund_watchlist_table_focus_controls {
    color: #1D1D1F !important;
    background: #FFFFFF !important;
    background-image: none !important;
    border: 1px solid #D2D2D7 !important;
    border-radius: 11px !important;
    box-shadow: none !important;
}

.ws-watchboard-stat,
.ws-watchboard-summary-pill,
.ws-fund-watchboard__live,
.ws-fund-watchboard__confirmed-nav,
.ws-fund-watchboard__card-metrics div {
    background: #F5F5F7 !important;
}

.ws-watchboard-stat-icon,
.ws-fund-watchboard__badge {
    color: #0066CC !important;
    -webkit-text-fill-color: #0066CC !important;
    background: #E8F2FC !important;
    border-color: transparent !important;
    border-radius: 8px !important;
    box-shadow: none !important;
}

.ws-watchboard-shell :is(p, label, small),
.ws-fund-watchboard :is(p, label, small) {
    color: #6E6E73 !important;
    -webkit-text-fill-color: #6E6E73 !important;
}

.ws-watchboard-shell :is(h2, h3, h4),
.ws-watchboard-title strong,
.ws-watchboard-mini-name,
.ws-watchboard-mini-price,
.ws-fund-watchboard :is(h2, h3, h4),
.ws-fund-watchboard__card-title strong,
.ws-fund-watchboard__metric strong,
.ws-fund-watchboard__holdings-head strong {
    color: #1D1D1F !important;
    -webkit-text-fill-color: #1D1D1F !important;
}

.ws-watchboard-grid-h,
.ws-watchboard-grid-v {
    background: #D2D2D7 !important;
}

.ws-watchboard-area {
    opacity: 0.35 !important;
}

.ws-watchboard-segment,
.ws-watchboard-point,
.ws-score-fill {
    box-shadow: none !important;
}

.ws-fund-watchboard__holdings th {
    color: #6E6E73 !important;
    -webkit-text-fill-color: #6E6E73 !important;
    background: #F5F5F7 !important;
}

.ws-fund-watchboard__holdings td {
    color: #1D1D1F !important;
    -webkit-text-fill-color: #1D1D1F !important;
    border-color: #D2D2D7 !important;
}

/* Streamlit 1.x paints widget chrome on a root element and its input child.
   Keep the root as the only painted surface so borders and backgrounds share
   one box and never drift by a pixel. */
html body [data-testid="stTextInputRootElement"],
html body [data-testid="stNumberInputContainer"],
html body .stDateInput [data-baseweb="input"],
html body .stTimeInput [data-baseweb="input"],
html body [data-testid="stTimeInputTimeDisplay"],
html body [data-testid="stTextAreaRootElement"],
html body [data-baseweb="textarea"],
html body .stSelectbox .react-aria-ComboBox [role="group"],
html body [data-baseweb="select"] > div {
    box-sizing: border-box !important;
    min-height: 36px !important;
    height: 36px !important;
    padding: 0 !important;
    color: var(--ws-text-main) !important;
    background: var(--ws-bg-surface) !important;
    border: 1px solid var(--ws-border-soft) !important;
    border-radius: 8px !important;
    box-shadow: none !important;
    overflow: hidden !important;
}

html body [data-testid="stTextAreaRootElement"],
html body [data-baseweb="textarea"] {
    min-height: 96px !important;
    height: auto !important;
}

html body [data-testid="stTextInputRootElement"] input,
html body [data-testid="stNumberInputField"],
html body [data-testid="stDateInputField"],
html body [data-testid="stTimeInputField"] {
    box-sizing: border-box !important;
    min-height: 34px !important;
    height: 34px !important;
    padding: 7px 10px !important;
    color: var(--ws-text-main) !important;
    background: transparent !important;
    border: 0 !important;
    border-radius: 0 !important;
    box-shadow: none !important;
    outline: none !important;
}

html body [data-testid="stTimeInputTimeDisplay"] [role="group"],
html body .stSelectbox .react-aria-ComboBox input[role="combobox"] {
    box-sizing: border-box !important;
    min-height: 34px !important;
    height: 34px !important;
    padding: 7px 10px !important;
    color: var(--ws-text-main) !important;
    background: transparent !important;
    border: 0 !important;
    border-radius: 0 !important;
    box-shadow: none !important;
    outline: none !important;
}

html body [data-testid="stTextAreaRootElement"] textarea,
html body [data-baseweb="textarea"] textarea {
    box-sizing: border-box !important;
    min-height: 94px !important;
    padding: 10px !important;
    color: var(--ws-text-main) !important;
    background: transparent !important;
    border: 0 !important;
    border-radius: 0 !important;
    box-shadow: none !important;
    outline: none !important;
}

/* Search scope and keyword share one Google-style search shell. */
html body [class*="st-key-ws-security-searchbox"] {
    box-sizing: border-box !important;
    min-width: 0 !important;
    min-height: 42px !important;
    display: grid !important;
    grid-template-columns: max-content minmax(0, 1fr) !important;
    align-items: center !important;
    gap: 0 !important;
    padding: 3px !important;
    background: var(--ws-bg-surface) !important;
    border: 1px solid var(--ws-border-soft) !important;
    border-radius: 10px !important;
    box-shadow: none !important;
    overflow: hidden !important;
    transition: border-color 140ms ease, box-shadow 140ms ease;
}

html body [class*="st-key-ws-security-searchbox"]:focus-within {
    border-color: var(--ws-color-primary) !important;
    box-shadow: 0 0 0 3px rgba(15, 105, 255, 0.14) !important;
}

html body [class*="st-key-ws-security-searchbox"] > [data-testid="stElementContainer"] {
    min-width: 0 !important;
    width: 100% !important;
}

html body [class*="st-key-ws-security-searchbox"] > [data-testid="stElementContainer"]:first-child {
    width: auto !important;
}

html body [class*="st-key-ws-security-searchbox"] [data-testid="stWidgetLabel"] {
    display: none !important;
}

html body [class*="st-key-ws-security-searchbox"] [data-testid="stRadio"],
html body [class*="st-key-ws-security-searchbox"] [data-testid="stRadioGroup"] {
    min-width: 0 !important;
    min-height: 34px !important;
    display: flex !important;
    align-items: center !important;
    gap: 2px !important;
    margin: 0 !important;
    padding: 0 4px 0 0 !important;
    white-space: nowrap;
}

html body [class*="st-key-ws-security-searchbox"] [data-testid="stRadioOption"] {
    box-sizing: border-box !important;
    min-height: 32px !important;
    display: flex !important;
    align-items: center !important;
    padding: 0 9px !important;
    color: var(--ws-text-muted) !important;
    background: transparent !important;
    border-radius: 7px !important;
    cursor: pointer;
}

html body [class*="st-key-ws-security-searchbox"] [data-testid="stRadioOption"]:hover {
    color: var(--ws-color-primary) !important;
    background: var(--ws-surface-soft) !important;
}

html body [class*="st-key-ws-security-searchbox"] [data-testid="stRadioOption"][data-selected="true"] {
    color: var(--ws-color-primary) !important;
    background: var(--ws-color-primary-soft) !important;
}

html body [class*="st-key-ws-security-searchbox"] [data-testid="stRadioOption"] > div > div > div:first-child {
    display: none !important;
}

html body [class*="st-key-ws-security-searchbox"] [data-testid="stRadioOption"] [data-testid="stMarkdownContainer"],
html body [class*="st-key-ws-security-searchbox"] [data-testid="stRadioOption"] [data-testid="stMarkdownContainer"] p {
    margin: 0 !important;
    color: inherit !important;
    font-family: var(--ws-font-sans) !important;
    font-size: 14px !important;
    font-weight: 600 !important;
    line-height: 1 !important;
    white-space: nowrap;
}

html body [class*="st-key-ws-security-searchbox"] [data-testid="stTextInput"],
html body [class*="st-key-ws-security-searchbox"] [data-testid="stTextInputRootElement"] {
    min-width: 0 !important;
    width: 100% !important;
}

html body [class*="st-key-ws-security-searchbox"] [data-testid="stTextInputRootElement"] {
    min-height: 34px !important;
    height: 34px !important;
    background: transparent !important;
    border: 0 !important;
    border-left: 1px solid var(--ws-border-soft) !important;
    border-radius: 0 !important;
    box-shadow: none !important;
}

html body [class*="st-key-ws-security-searchbox"] [data-testid="stTextInputRootElement"]:focus-within {
    border-color: var(--ws-border-soft) !important;
    box-shadow: none !important;
}

html body [class*="st-key-ws-security-searchbox"] [data-testid="stTextInputRootElement"] input {
    min-width: 0 !important;
    width: 100% !important;
    padding-right: 12px !important;
    padding-left: 12px !important;
}

html body [data-testid="stTextInputRootElement"]:focus-within,
html body [data-testid="stNumberInputContainer"]:focus-within,
html body .stDateInput [data-baseweb="input"]:focus-within,
html body .stTimeInput [data-baseweb="input"]:focus-within,
html body [data-testid="stTimeInputTimeDisplay"]:focus-within,
html body .stSelectbox .react-aria-ComboBox [role="group"]:focus-within,
html body [data-baseweb="select"] > div:focus-within,
html body [data-testid="stTextAreaRootElement"]:focus-within,
html body [data-baseweb="textarea"]:focus-within {
    border-color: var(--ws-color-primary) !important;
    box-shadow: 0 0 0 3px rgba(0, 102, 204, 0.16) !important;
}

html body .stTextInput input:focus,
html body .stNumberInput input:focus,
html body .stDateInput input:focus,
html body .stTimeInput input:focus,
html body .stSelectbox input[role="combobox"]:focus,
html body .stTextArea textarea:focus,
html body [data-baseweb="textarea"] textarea:focus {
    border: 0 !important;
    box-shadow: none !important;
    outline: none !important;
}
"""
