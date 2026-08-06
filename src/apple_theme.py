from __future__ import annotations

import plotly.graph_objects as go


# Public names remain unchanged because the application and tests import them.
# The values follow the supplied Modern Directory Navigator design system.
APPLE_THEME_DEFAULT_TOKENS = {
    "bg_base": "#F7F9FF",
    "bg_surface": "#FFFFFF",
    "bg_dark": "#2A3138",
    "surface_soft": "#F4F7F9",
    "surface_alt": "#EDF4FE",
    "surface_dark_alt": "#3A424A",
    "primary": "#6001D2",
    "primary_hover": "#44009A",
    "primary_press": "#25005A",
    "primary_strong": "#732EE5",
    "primary_soft": "#EADDFF",
    "secondary": "#0052D0",
    "text_main": "#151C23",
    "text_muted": "#4A4455",
    "text_soft": "#6B6675",
    "text_inverse": "#FFFFFF",
    "border_soft": "#E0E4E9",
    "border_strong": "#B8C0CA",
    "shadow": "none",
    "shadow_hover": "0 4px 14px rgba(21, 28, 35, 0.08)",
    "ai_glow": "none",
    "color_up": "#037B66",
    "color_down": "#D11022",
    "color_warn": "#8A5A00",
    "color_neutral": "#6B6675",
    "color_purple": "#732EE5",
    "radius_lg": "4px",
    "radius_md": "4px",
    "radius_sm": "2px",
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
    font_family = "Inter, 'Segoe UI Variable', 'PingFang SC', 'Microsoft YaHei', sans-serif"
    data_family = "'Archivo Narrow', Inter, 'Segoe UI Variable', sans-serif"
    axis_style = {
        "showline": True,
        "linewidth": 1,
        "ticks": "outside",
        "tickcolor": tokens["border_strong"],
        "tickfont": {"family": data_family, "color": tokens["text_muted"], "size": 12},
        "gridcolor": "rgba(212, 219, 228, 0.62)",
        "linecolor": tokens["border_soft"],
        "zerolinecolor": tokens["border_strong"],
        "title": {"font": {"color": tokens["text_muted"], "size": 12}},
    }
    return go.layout.Template(
        layout=go.Layout(
            paper_bgcolor=tokens["bg_surface"],
            plot_bgcolor=tokens["bg_surface"],
            font={"color": tokens["text_main"], "family": font_family, "size": 12},
            title={
                "font": {
                    "color": tokens["text_main"],
                    "family": "'Hanken Grotesk', Inter, sans-serif",
                    "size": 17,
                }
            },
            colorway=[
                tokens["primary"],
                tokens["secondary"],
                tokens["color_up"],
                tokens["color_down"],
                tokens["color_warn"],
                tokens["color_purple"],
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
                "font": {"color": tokens["text_muted"], "size": 12},
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
    font-family: 'Archivo Narrow', Inter, sans-serif;
    font-size: 0.72rem;
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
    font-size: 0.84rem;
    line-height: 1.45;
}}

.ws-tracker-section {{
    margin: 1rem 0 0.45rem;
    padding-bottom: 0.45rem;
    border-bottom: 1px solid {tokens["border_soft"]};
}}

.ws-tracker-section span {{
    color: {tokens["text_main"]};
    font-size: 0.92rem;
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
    font-size: 0.84rem;
    font-weight: 700;
}}

.ws-evidence-gallery-note {{
    margin-top: 0.3rem;
    color: {tokens["text_soft"]};
    font-size: 0.76rem;
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
    --ws-color-purple: {tokens["color_purple"]};
    --ws-radius-lg: {tokens["radius_lg"]};
    --ws-radius-md: {tokens["radius_md"]};
    --ws-radius-sm: {tokens["radius_sm"]};
    --ws-font-sans: Inter, "Segoe UI Variable", "PingFang SC", "Microsoft YaHei", sans-serif;
    --ws-font-heading: "Hanken Grotesk", Inter, "Segoe UI Variable", "PingFang SC", sans-serif;
    --ws-font-data: "Archivo Narrow", Inter, "Segoe UI Variable", sans-serif;
    --ws-sidebar-row-height: 38px;
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
    padding: 4.55rem 1.5rem 3rem;
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
footer,
header {{
    visibility: hidden;
}}

.ws-terminal-header {{
    position: fixed;
    inset: 0 0 auto 232px;
    z-index: 999;
    height: 50px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
    padding: 0 1.4rem;
    color: var(--ws-text-main);
    background: rgba(255, 255, 255, 0.98);
    border-bottom: 1px solid var(--ws-border-soft);
}}

.ws-terminal-header__title {{
    min-width: 220px;
    font-family: var(--ws-font-heading);
    font-size: 0.98rem;
    font-weight: 700;
    letter-spacing: 0;
}}

.ws-terminal-header__meta {{
    display: inline-flex;
    align-items: center;
    gap: 0.45rem;
    color: var(--ws-text-muted);
    font-family: var(--ws-font-data);
    font-size: 0.75rem;
    font-weight: 700;
    white-space: nowrap;
}}

.ws-terminal-header__pulse {{
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--ws-color-up);
}}

.ws-page-intro {{
    margin: 0 0 0.75rem;
    padding: 0 0 0.7rem;
    border-bottom: 1px solid var(--ws-border-soft);
}}

.ws-page-intro__eyebrow {{
    display: block;
    margin-bottom: 0.18rem;
    color: var(--ws-color-primary);
    font-family: var(--ws-font-data);
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0;
    text-transform: uppercase;
}}

.ws-page-intro h1 {{
    margin: 0;
    color: var(--ws-text-main);
    font-size: 1.55rem;
    font-weight: 700;
}}

.ws-page-intro p {{
    margin: 0.18rem 0 0;
    color: var(--ws-text-muted) !important;
    font-size: 0.84rem;
}}

[data-testid="stSidebar"] {{
    min-width: 232px !important;
    width: 232px !important;
    padding: 0.9rem 0.75rem !important;
    background: var(--ws-bg-surface) !important;
    border-right: 1px solid var(--ws-border-soft) !important;
}}

[data-testid="stSidebar"] > div:first-child {{
    width: 232px !important;
    padding-top: 0 !important;
    background: var(--ws-bg-surface) !important;
}}

@media (min-width: 769px) {{
    [data-testid="stSidebar"][aria-expanded="false"] {{
        min-width: 232px !important;
        width: 232px !important;
        margin-left: 0 !important;
        transform: none !important;
    }}

    [data-testid="stSidebar"] [class*="st-key-sidebar_search_query"],
    [data-testid="stSidebar"] [class*="st-key-sidebar-search-query"] {{
        position: fixed !important;
        top: 7px;
        left: 475px;
        z-index: 1002;
        width: min(340px, calc(100vw - 690px));
        min-width: 220px;
        margin: 0 !important;
    }}

    [data-testid="stSidebar"] [class*="st-key-sidebar_search_query"] input,
    [data-testid="stSidebar"] [class*="st-key-sidebar-search-query"] input {{
        min-height: 36px !important;
        height: 36px !important;
        padding-left: 0.75rem !important;
        border-color: #AEB6C0 !important;
    }}
}}

[data-testid="collapsedControl"],
button[aria-label="Open sidebar"],
button[aria-label="Close sidebar"] {{
    position: fixed !important;
    top: 7px !important;
    left: 0.7rem !important;
    z-index: 1004 !important;
    width: 36px !important;
    height: 36px !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
    color: var(--ws-text-main) !important;
    background: var(--ws-bg-surface) !important;
    border: 1px solid var(--ws-border-soft) !important;
    border-radius: var(--ws-radius-md) !important;
    box-shadow: none !important;
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
    font-size: 0.9rem;
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
    font-size: 0.58rem;
    font-weight: 700;
    line-height: 1;
    text-transform: uppercase;
}}

[data-testid="stSidebar"] .ws-sidebar-block {{
    margin: 0.65rem 0 0.25rem;
    padding: 0 0.35rem 0.35rem;
    border-bottom: 0;
}}

[data-testid="stSidebar"] .ws-sidebar-block-title {{
    color: var(--ws-text-muted) !important;
    font-family: var(--ws-font-data);
    font-size: 0.68rem;
    font-weight: 700;
    letter-spacing: 0;
    text-transform: uppercase;
}}

[data-testid="stSidebar"] .ws-sidebar-block-copy,
[data-testid="stSidebar"] .ws-sidebar-page-description,
[data-testid="stSidebar"] .ws-sidebar-search-result-meta,
[data-testid="stSidebar"] .ws-sidebar-empty {{
    color: var(--ws-text-soft) !important;
    font-size: 0.72rem;
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
    font-size: 0.66rem;
    font-weight: 700;
}}

[data-testid="stSidebar"] .ws-sidebar-recent-page {{
    color: var(--ws-text-main) !important;
    font-size: 0.78rem;
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
    accent-color: var(--ws-color-primary) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-tree"] > div {{
    display: flex;
    flex-direction: column;
    gap: 0;
    padding: 0 0.05rem;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] {{
    border-radius: var(--ws-radius-md);
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] {{
    margin: 1px 0;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] {{
    position: relative;
    width: calc(100% - 1rem);
    margin-left: auto;
    padding-left: 0.55rem;
    border-left: 1px solid var(--ws-border-soft);
    border-radius: 0;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] > div button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] > div button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] > div button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-favorite-"] > div button {{
    width: 100%;
    min-height: var(--ws-sidebar-row-height);
    justify-content: flex-start;
    padding: 0.45rem 0.55rem !important;
    color: var(--ws-text-muted) !important;
    background: transparent !important;
    border: 1px solid transparent !important;
    border-radius: var(--ws-radius-md) !important;
    box-shadow: none !important;
    font-size: 0.8rem;
    font-weight: 500 !important;
    text-align: left;
    white-space: normal;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] > div button {{
    display: flex;
    align-items: center;
    gap: 0.55rem;
    color: var(--ws-text-muted) !important;
    font-size: 0.82rem;
    font-weight: 600 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] > div button::before {{
    content: "";
    flex: 0 0 18px;
    width: 18px;
    height: 18px;
    background: #716B7D;
    mask: url("/app/static/icons/activity.svg") center / 16px 16px no-repeat;
    -webkit-mask: url("/app/static/icons/activity.svg") center / 16px 16px no-repeat;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] > div button::after {{
    content: "";
    flex: 0 0 14px;
    width: 14px;
    height: 14px;
    margin-left: auto;
    background: #8A8493;
    mask: url("/app/static/icons/chevron-right.svg") center / 14px 14px no-repeat;
    -webkit-mask: url("/app/static/icons/chevron-right.svg") center / 14px 14px no-repeat;
    transform-origin: center;
    transition: transform 120ms ease;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-decision"] > div button::before {{
    mask-image: url("/app/static/icons/briefcase-business.svg");
    -webkit-mask-image: url("/app/static/icons/briefcase-business.svg");
}}
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-stock"] > div button::before {{
    mask-image: url("/app/static/icons/chart-candlestick.svg");
    -webkit-mask-image: url("/app/static/icons/chart-candlestick.svg");
}}
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-fund"] > div button::before {{
    mask-image: url("/app/static/icons/landmark.svg");
    -webkit-mask-image: url("/app/static/icons/landmark.svg");
}}
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-money"] > div button::before {{
    mask-image: url("/app/static/icons/badge-dollar-sign.svg");
    -webkit-mask-image: url("/app/static/icons/badge-dollar-sign.svg");
}}
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-macro"] > div button::before {{
    mask-image: url("/app/static/icons/globe.svg");
    -webkit-mask-image: url("/app/static/icons/globe.svg");
}}
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-data"] > div button::before {{
    mask-image: url("/app/static/icons/database.svg");
    -webkit-mask-image: url("/app/static/icons/database.svg");
}}
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-favorite"] > div button::before {{
    mask-image: url("/app/static/icons/star.svg");
    -webkit-mask-image: url("/app/static/icons/star.svg");
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] > div button {{
    width: 100%;
    min-height: 31px;
    justify-content: flex-start;
    padding: 0.28rem 0.45rem 0.28rem 0.62rem !important;
    color: var(--ws-text-soft) !important;
    background: transparent !important;
    border: 0 !important;
    border-radius: var(--ws-radius-sm) !important;
    box-shadow: none !important;
    font-size: 0.74rem;
    font-weight: 500 !important;
    line-height: 1.25;
    text-align: left;
    white-space: normal;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] > div button img,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] > div button img,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] > div button img,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-favorite-"] > div button img {{
    width: 15px;
    height: 15px;
    margin-right: 0.42rem;
    opacity: 0.78;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] > div button:hover,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] > div button:hover,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] > div button:hover,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] > div button:hover {{
    color: var(--ws-color-primary-hover) !important;
    background: var(--ws-surface-soft) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-expanded"] > div button {{
    color: var(--ws-text-main) !important;
    background: var(--ws-surface-soft) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-expanded"] > div button::after {{
    transform: rotate(90deg);
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-current"] > div button {{
    color: var(--ws-color-primary-hover) !important;
    background: #F0E9FF !important;
    border-color: transparent !important;
    box-shadow: inset 3px 0 0 var(--ws-color-primary) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-current"] > div button::before,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-current"] > div button::after {{
    color: var(--ws-color-primary-hover);
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-current"] > div button::before {{
    background: var(--ws-color-primary-hover);
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-current"] > div button::after {{
    background: var(--ws-color-primary-hover);
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-active"] > div button img,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-current"] > div button img {{
    filter: brightness(0) saturate(100%) invert(13%) sepia(99%) saturate(5048%) hue-rotate(275deg) brightness(71%) contrast(121%);
    opacity: 1;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-active"] > div button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-current"] > div button {{
    color: var(--ws-color-primary-hover) !important;
    background: #F8F4FF !important;
    font-weight: 700 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-active"]::before {{
    content: "";
    position: absolute;
    top: 50%;
    left: -3px;
    width: 5px;
    height: 5px;
    border-radius: 50%;
    background: var(--ws-color-primary);
    transform: translateY(-50%);
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-favorite-"] > div button {{
    color: var(--ws-color-primary-hover) !important;
    background: #F8F4FF !important;
    border-color: #E7D9FF !important;
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
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0;
}}

.stButton > button,
.stDownloadButton > button,
button[kind="secondary"],
[data-testid="stFormSubmitButton"] > button {{
    min-height: 36px;
    padding: 0.42rem 0.78rem !important;
    color: var(--ws-text-main) !important;
    background: var(--ws-bg-surface) !important;
    border: 1px solid #7B828A !important;
    border-radius: var(--ws-radius-md) !important;
    box-shadow: none !important;
    font-size: 0.8rem;
    font-weight: 700 !important;
    letter-spacing: 0 !important;
}}

button[kind="primary"],
[data-testid="stFormSubmitButton"] > button {{
    color: var(--ws-text-inverse) !important;
    background: var(--ws-color-primary) !important;
    border-color: var(--ws-color-primary) !important;
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
    background: #F7F2FF !important;
    border-color: var(--ws-color-primary) !important;
}}

button[kind="primary"]:hover,
[data-testid="stFormSubmitButton"] > button:hover {{
    color: var(--ws-text-inverse) !important;
    background: var(--ws-color-primary-hover) !important;
    border-color: var(--ws-color-primary-hover) !important;
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
.stTextInput input,
.stNumberInput input,
.stDateInput input,
.stTimeInput input,
.stTextArea textarea {{
    min-height: 36px;
    color: var(--ws-text-main) !important;
    background: var(--ws-bg-surface) !important;
    border: 1px solid var(--ws-border-strong) !important;
    border-radius: var(--ws-radius-md) !important;
    box-shadow: none !important;
    font-size: 0.82rem;
}}

[data-baseweb="select"] > div:focus-within,
.stTextInput input:focus,
.stNumberInput input:focus,
.stDateInput input:focus,
.stTimeInput input:focus,
.stTextArea textarea:focus {{
    border-color: var(--ws-color-primary) !important;
    box-shadow: 0 0 0 2px rgba(96, 1, 210, 0.18) !important;
}}

[data-baseweb="tag"] {{
    color: var(--ws-color-primary-hover) !important;
    background: var(--ws-color-primary-soft) !important;
    border: 0 !important;
    border-radius: var(--ws-radius-md) !important;
    box-shadow: none !important;
}}

[data-baseweb="checkbox"] [aria-checked="true"],
[data-baseweb="toggle"] [aria-checked="true"] {{
    background-color: var(--ws-color-primary) !important;
    border-color: var(--ws-color-primary) !important;
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
    border-radius: var(--ws-radius-md) !important;
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
    font-size: 0.72rem !important;
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
    font-size: 0.72rem !important;
    font-weight: 700 !important;
}}

div[data-testid="stExpander"] {{ overflow: hidden !important; }}

div[data-testid="stExpander"] details summary {{
    padding: 0.65rem 0.75rem !important;
    color: var(--ws-text-main) !important;
    font-size: 0.82rem;
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
    border-radius: var(--ws-radius-md);
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
    font-size: 0.8rem;
    font-weight: 600 !important;
}}

.stTabs [role="tab"] * {{ color: inherit !important; -webkit-text-fill-color: currentColor !important; }}

.stTabs [aria-selected="true"] {{
    color: var(--ws-color-primary-hover) !important;
    background: #F8F4FF !important;
    border-bottom-color: var(--ws-color-primary) !important;
    box-shadow: none !important;
}}

.stAlert {{
    background: var(--ws-bg-surface) !important;
    border: 1px solid var(--ws-border-soft) !important;
    border-left: 3px solid var(--ws-color-primary) !important;
    border-radius: var(--ws-radius-md) !important;
    box-shadow: none !important;
    font-size: 0.8rem;
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

hr {{ border-color: var(--ws-border-soft) !important; }}

code {{
    color: var(--ws-color-primary-hover) !important;
    background: var(--ws-surface-soft) !important;
    border-radius: var(--ws-radius-sm);
}}

@media (max-width: 1050px) {{
    .ws-terminal-header__meta {{ display: none; }}
}}

@media (max-width: 768px) {{
    .ws-terminal-header {{ display: none; }}

    .main .block-container {{
        max-width: 100%;
        padding: 1rem 0.9rem 2.5rem;
    }}

    [data-testid="stSidebar"] {{
        min-width: 280px !important;
        width: 280px !important;
        padding: 0.8rem 0.7rem !important;
    }}

    [data-testid="stSidebar"] > div:first-child {{ width: 280px !important; }}

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
    --wb-cyan: #6001D2;
    --wb-blue: #0052D0;
    --wb-red: #037B66;
    --wb-green: #D11022;
    --wb-text: #151C23;
    --wb-muted: #4A4455;
    --fw-bg: #FFFFFF;
    --fw-panel: #FFFFFF;
    --fw-panel-strong: #FFFFFF;
    --fw-line: #E0E4E9;
    --fw-line-soft: #E0E4E9;
    --fw-cyan: #6001D2;
    --fw-blue: #0052D0;
    --fw-red: #037B66;
    --fw-green: #D11022;
    --fw-text: #151C23;
    --fw-muted: #4A4455;
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
    color: #44009A !important;
    -webkit-text-fill-color: #44009A !important;
    background: #EADDFF !important;
    border-color: transparent !important;
    border-radius: 4px !important;
    box-shadow: none !important;
}

.ws-watchboard-shell :is(p, label, small),
.ws-fund-watchboard :is(p, label, small) {
    color: #4A4455 !important;
    -webkit-text-fill-color: #4A4455 !important;
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
    color: #4A4455 !important;
    -webkit-text-fill-color: #4A4455 !important;
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
"""
