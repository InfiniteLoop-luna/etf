from __future__ import annotations

import plotly.graph_objects as go


APPLE_THEME_DEFAULT_TOKENS = {
    "bg_base": "#F4F7F6",
    "bg_surface": "#FFFFFF",
    "bg_dark": "#1B263B",
    "surface_soft": "#EEF2F0",
    "surface_alt": "#F8FAF8",
    "surface_dark_alt": "#22314A",
    "primary": "#D4AF37",
    "primary_hover": "#E5C158",
    "primary_strong": "#FFD700",
    "primary_soft": "rgba(212, 175, 55, 0.16)",
    "text_main": "#1B263B",
    "text_muted": "#6E7C8C",
    "text_soft": "#8A97A6",
    "text_inverse": "#F8FAFC",
    "border_soft": "rgba(27, 38, 59, 0.05)",
    "border_strong": "rgba(27, 38, 59, 0.12)",
    "shadow": "0 4px 20px rgba(27, 38, 59, 0.04)",
    "shadow_hover": "0 10px 28px rgba(27, 38, 59, 0.08)",
    "ai_glow": "0 0 0 1px rgba(212, 175, 55, 0.24), 0 12px 30px rgba(212, 175, 55, 0.12)",
    "color_up": "#E63946",
    "color_down": "#2A9D8F",
    "color_warn": "#F4A261",
    "color_neutral": "#607086",
    "color_purple": "#7D6B91",
    "radius_lg": "22px",
    "radius_md": "16px",
    "radius_sm": "12px",
    "max_width": "1480px",
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
    return go.layout.Template(
        layout=go.Layout(
            paper_bgcolor=tokens["bg_base"],
            plot_bgcolor=tokens["bg_surface"],
            font={"color": tokens["text_main"], "family": "-apple-system, BlinkMacSystemFont, 'SF Pro Text', 'PingFang SC', sans-serif"},
            title={"font": {"color": tokens["text_main"], "size": 20}},
            colorway=[
                tokens["bg_dark"],
                tokens["primary"],
                "#4F6785",
                "#5B8E7D",
                "#C28C4E",
                "#7D6B91",
            ],
            hoverlabel={
                "bgcolor": tokens["bg_dark"],
                "font": {"color": tokens["text_inverse"]},
                "bordercolor": tokens["border_strong"],
            },
            legend={
                "bgcolor": "rgba(255,255,255,0.94)",
                "bordercolor": tokens["border_soft"],
                "borderwidth": 1,
                "font": {"color": tokens["text_muted"]},
            },
            margin={"l": 20, "r": 20, "t": 36, "b": 20},
            xaxis={
                "showline": True,
                "linewidth": 1,
                "ticks": "outside",
                "tickcolor": "rgba(27, 38, 59, 0.16)",
                "gridcolor": "rgba(27, 38, 59, 0.08)",
                "linecolor": "rgba(27, 38, 59, 0.12)",
                "zerolinecolor": "rgba(27, 38, 59, 0.08)",
                "title": {"font": {"color": tokens["text_muted"]}},
            },
            yaxis={
                "showline": True,
                "linewidth": 1,
                "ticks": "outside",
                "tickcolor": "rgba(27, 38, 59, 0.16)",
                "gridcolor": "rgba(27, 38, 59, 0.08)",
                "linecolor": "rgba(27, 38, 59, 0.12)",
                "zerolinecolor": "rgba(27, 38, 59, 0.08)",
                "title": {"font": {"color": tokens["text_muted"]}},
            },
        )
    )


def build_author_tracker_apple_css() -> str:
    tokens = get_apple_theme_tokens()
    return f"""
.ws-tracker-shell {{
    background: linear-gradient(180deg, {tokens["bg_surface"]} 0%, {tokens["surface_alt"]} 100%);
    border: 1px solid {tokens["border_soft"]};
    border-radius: {tokens["radius_lg"]};
    padding: 1.2rem 1.3rem;
    box-shadow: {tokens["shadow"]};
    margin: 0.35rem 0 1rem 0;
}}

.ws-tracker-shell .ws-tracker-eyebrow {{
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    padding: 0.3rem 0.7rem;
    border-radius: 999px;
    background: {tokens["primary_soft"]};
    color: {tokens["primary"]};
    font-size: 0.76rem;
    font-weight: 700;
    letter-spacing: 0.02em;
}}

.ws-tracker-shell h4 {{
    margin: 0.9rem 0 0.35rem 0;
    color: {tokens["text_main"]};
    font-size: 1.25rem;
    font-weight: 700;
    letter-spacing: -0.02em;
}}

.ws-tracker-shell p {{
    margin: 0;
    color: {tokens["text_muted"]};
    font-size: 0.95rem;
    line-height: 1.6;
}}

.ws-tracker-section {{
    margin: 1.15rem 0 0.55rem 0;
    padding: 0.15rem 0 0.55rem 0;
    border-bottom: 1px solid {tokens["border_soft"]};
}}

.ws-tracker-section span {{
    color: {tokens["text_main"]};
    font-size: 1rem;
    font-weight: 650;
    letter-spacing: -0.01em;
}}

.ws-evidence-gallery {{
    margin: 0.8rem 0 0.25rem 0;
    padding: 0.9rem 1rem;
    border-radius: {tokens["radius_md"]};
    background: linear-gradient(180deg, rgba(255,255,255,0.98) 0%, rgba(244, 247, 246, 0.96) 100%);
    border: 1px solid {tokens["border_soft"]};
    box-shadow: {tokens["shadow"]};
}}

.ws-evidence-gallery strong {{
    color: {tokens["text_main"]};
    font-size: 0.93rem;
}}

.ws-evidence-gallery-note {{
    margin-top: 0.35rem;
    color: {tokens["text_soft"]};
    font-size: 0.82rem;
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
    --ws-color-primary-strong: {tokens["primary_strong"]};
    --ws-color-primary-soft: {tokens["primary_soft"]};
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
}}

html, body, [class*="css"] {{
    font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
}}

html, body, .stApp, [data-testid="stAppViewContainer"] {{
    background: var(--ws-bg-base) !important;
    color: var(--ws-text-main) !important;
}}

[data-testid="stAppViewContainer"]::before {{
    content: "";
    position: fixed;
    inset: 0;
    pointer-events: none;
    background:
        radial-gradient(circle at 16% 0%, rgba(212, 175, 55, 0.10), transparent 24%),
        radial-gradient(circle at 84% 8%, rgba(27, 38, 59, 0.08), transparent 26%),
        linear-gradient(180deg, rgba(255,255,255,0.28) 0%, rgba(244, 247, 246, 0.00) 44%);
    z-index: 0;
}}

[data-testid="stAppViewContainer"] > .main,
.main .block-container {{
    background: transparent !important;
    position: relative;
    z-index: 1;
}}

.main .block-container {{
    max-width: {tokens["max_width"]};
    padding: 1.6rem 2.2rem 3rem 2.2rem;
    margin-top: 0.55rem;
    background: linear-gradient(180deg, var(--ws-bg-surface) 0%, rgba(248, 250, 248, 0.98) 100%) !important;
    border: 1px solid var(--ws-border-soft);
    border-radius: 32px;
    box-shadow: 0 4px 20px rgba(27, 38, 59, 0.04);
}}

.main .block-container::before {{
    content: "";
    position: absolute;
    inset: 0 auto auto 0;
    width: 100%;
    height: 7rem;
    pointer-events: none;
    border-radius: 32px 32px 0 0;
    background: linear-gradient(180deg, rgba(212, 175, 55, 0.06) 0%, rgba(212, 175, 55, 0.00) 100%);
}}

.main p,
.main li,
.main label,
.main span,
.main .stMarkdown,
.main [data-testid="stCaptionContainer"] {{
    color: var(--ws-text-muted) !important;
}}

#MainMenu,
footer,
header {{
    visibility: hidden;
}}

[data-testid="stSidebar"] {{
    background: var(--ws-bg-dark) !important;
    border-right: 1px solid rgba(255,255,255,0.06) !important;
    padding: 1.5rem 1rem !important;
    min-width: 300px !important;
}}

[data-testid="stSidebar"][aria-expanded="false"] {{
    min-width: 300px !important;
    width: 300px !important;
    transform: none !important;
    margin-left: 0 !important;
}}

[data-testid="stSidebar"] > div:first-child {{
    width: 300px !important;
    background: linear-gradient(180deg, var(--ws-bg-dark) 0%, var(--ws-surface-dark-alt) 100%) !important;
    box-shadow: inset -1px 0 0 rgba(255,255,255,0.04);
}}

[data-testid="collapsedControl"],
button[aria-label="Open sidebar"],
button[aria-label="Close sidebar"] {{
    position: fixed !important;
    top: 0.85rem !important;
    left: 0.85rem !important;
    width: 2.8rem !important;
    height: 2.8rem !important;
    border-radius: 999px !important;
    border: 1px solid rgba(212, 175, 55, 0.30) !important;
    background: linear-gradient(135deg, var(--ws-bg-dark) 0%, #24344F 100%) !important;
    color: var(--ws-text-inverse) !important;
    box-shadow: 0 12px 28px rgba(27, 38, 59, 0.20) !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
    z-index: 1000 !important;
    transition: transform 0.2s ease, box-shadow 0.2s ease, background 0.2s ease !important;
}}

[data-testid="collapsedControl"]:hover,
button[aria-label="Open sidebar"]:hover,
button[aria-label="Close sidebar"]:hover {{
    transform: translateY(-1px);
    box-shadow: 0 16px 30px rgba(27, 38, 59, 0.24) !important;
    background: linear-gradient(135deg, var(--ws-bg-dark) 0%, #30415E 100%) !important;
}}

[data-testid="stSidebar"] *,
.main * {{
    color: inherit;
}}

[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] label {{
    color: var(--ws-text-inverse) !important;
}}

[data-testid="stSidebar"] [role="radiogroup"] {{
    padding: 0.75rem 0.85rem !important;
    margin: 0.4rem 0 0.85rem 0 !important;
    background: rgba(255,255,255,0.04) !important;
    border: 1px solid rgba(255,255,255,0.06) !important;
    border-radius: 20px !important;
}}

[data-testid="stSidebar"] [aria-checked="true"] {{
    accent-color: var(--ws-color-primary) !important;
}}

[data-testid="stSidebar"] [aria-checked="true"] + div,
[data-testid="stSidebar"] [aria-checked="true"] ~ div {{
    color: var(--ws-color-primary) !important;
}}

[data-testid="stSidebar"] .ws-sidebar-brand {{
    margin: 0 0 1rem 0;
    padding: 1rem 1.05rem;
    border-radius: 22px;
    background: linear-gradient(180deg, rgba(212, 175, 55, 0.18) 0%, rgba(255, 255, 255, 0.04) 100%);
    border: 1px solid rgba(212, 175, 55, 0.22);
    box-shadow: 0 18px 34px rgba(9, 15, 25, 0.18);
}}

[data-testid="stSidebar"] .ws-sidebar-brand h2 {{
    margin: 0.35rem 0 0.2rem 0;
    font-size: 1.02rem;
    color: var(--ws-text-inverse) !important;
}}

[data-testid="stSidebar"] .ws-sidebar-brand p {{
    margin: 0;
    color: rgba(248, 250, 252, 0.76) !important;
    font-size: 0.82rem;
    line-height: 1.55;
}}

[data-testid="stSidebar"] .ws-sidebar-brand-kicker {{
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    padding: 0.22rem 0.68rem;
    border-radius: 999px;
    background: rgba(255, 255, 255, 0.08);
    color: var(--ws-color-primary) !important;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.05em;
    text-transform: uppercase;
}}

[data-testid="stSidebar"] .ws-sidebar-block {{
    margin: 0.85rem 0 0.3rem 0;
    padding: 0.75rem 0.9rem;
    border-radius: 18px;
    background: rgba(255, 255, 255, 0.04);
    border: 1px solid rgba(255, 255, 255, 0.07);
}}

[data-testid="stSidebar"] .ws-sidebar-block-title {{
    color: var(--ws-text-inverse) !important;
    font-size: 0.82rem;
    font-weight: 700;
    letter-spacing: 0.02em;
}}

[data-testid="stSidebar"] .ws-sidebar-block-copy {{
    margin: 0.35rem 0 0 0;
    color: rgba(248, 250, 252, 0.72) !important;
    font-size: 0.78rem;
    line-height: 1.5;
}}

[data-testid="stSidebar"] .ws-sidebar-recent-item {{
    display: flex;
    flex-direction: column;
    gap: 0.12rem;
    padding: 0.72rem 0.85rem;
    margin: 0.28rem 0 0 0;
    border-radius: 16px;
    background: rgba(255, 255, 255, 0.035);
    border: 1px solid rgba(255, 255, 255, 0.05);
}}

[data-testid="stSidebar"] .ws-sidebar-recent-module {{
    color: var(--ws-color-primary) !important;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.04em;
}}

[data-testid="stSidebar"] .ws-sidebar-recent-page {{
    color: var(--ws-text-inverse) !important;
    font-size: 0.84rem;
    font-weight: 600;
    line-height: 1.45;
}}

[data-testid="stSidebar"] .ws-sidebar-tree {{
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
    margin: 0.9rem 0 0 0;
}}

[data-testid="stSidebar"] .ws-sidebar-page-description,
[data-testid="stSidebar"] .ws-sidebar-search-result-meta,
[data-testid="stSidebar"] .ws-sidebar-empty {{
    display: block;
    color: rgba(248, 250, 252, 0.66) !important;
    font-size: 0.76rem;
    line-height: 1.45;
}}

[data-testid="stSidebar"] .ws-sidebar-page-description {{
    margin-top: 0.18rem;
}}

[data-testid="stSidebar"] .ws-sidebar-search-result-meta {{
    margin-top: 0.22rem;
    color: rgba(248, 250, 252, 0.6) !important;
}}

[data-testid="stSidebar"] .ws-sidebar-empty {{
    padding: 0.95rem 1rem;
    border-radius: 16px;
    background: rgba(255, 255, 255, 0.04);
    border: 1px dashed rgba(255, 255, 255, 0.12);
    text-align: center;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] {{
    border-radius: 18px;
    transition: transform 0.18s ease, background 0.18s ease, border-color 0.18s ease, box-shadow 0.18s ease;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] {{
    margin: 0.15rem 0 0 0;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] > div button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] > div button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] > div button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] > div button {{
    width: 100%;
    min-height: auto;
    justify-content: flex-start;
    white-space: normal;
    text-align: left;
    border-radius: 18px !important;
    box-shadow: none !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] > div button {{
    padding: 0.7rem 0.85rem !important;
    background: linear-gradient(180deg, rgba(212, 175, 55, 0.14) 0%, rgba(255, 255, 255, 0.04) 100%) !important;
    border: 1px solid rgba(212, 175, 55, 0.2) !important;
    box-shadow: 0 14px 28px rgba(9, 15, 25, 0.14) !important;
    color: var(--ws-text-inverse) !important;
    font-weight: 650 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-expanded"] > div button {{
    background: linear-gradient(180deg, rgba(212, 175, 55, 0.22) 0%, rgba(255, 255, 255, 0.08) 100%) !important;
    border-color: rgba(212, 175, 55, 0.32) !important;
    box-shadow: inset 3px 0 0 var(--ws-color-primary), 0 16px 30px rgba(9, 15, 25, 0.18) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] {{
    margin: 0;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] > div button {{
    padding: 0.58rem 0.72rem 0.62rem 1rem !important;
    background: rgba(255, 255, 255, 0.03) !important;
    border: 1px solid transparent !important;
    color: var(--ws-text-inverse) !important;
    font-weight: 600 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] > div button:hover {{
    transform: translateX(2px);
    background: rgba(255, 255, 255, 0.06) !important;
    border-color: rgba(255, 255, 255, 0.1) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-active"] > div button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-current"] > div button {{
    background: linear-gradient(135deg, rgba(212, 175, 55, 0.22) 0%, rgba(212, 175, 55, 0.12) 100%) !important;
    border-color: rgba(212, 175, 55, 0.3) !important;
    box-shadow: inset 3px 0 0 var(--ws-color-primary), 0 12px 24px rgba(9, 15, 25, 0.16) !important;
    color: var(--ws-text-inverse) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] {{
    margin: 0.2rem 0 0 0;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] > div button {{
    padding: 0.8rem 0.9rem !important;
    background: linear-gradient(180deg, rgba(255, 255, 255, 0.08) 0%, rgba(255, 255, 255, 0.04) 100%) !important;
    border: 1px solid rgba(255, 255, 255, 0.1) !important;
    box-shadow: 0 16px 28px rgba(9, 15, 25, 0.14) !important;
    color: var(--ws-text-inverse) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] > div button:hover {{
    transform: translateY(-1px);
    border-color: rgba(212, 175, 55, 0.22) !important;
    box-shadow: 0 18px 32px rgba(9, 15, 25, 0.18) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] {{
    margin: 0;
    opacity: 0.82;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] > div button {{
    padding: 0.45rem 0.72rem 0.48rem 1.25rem !important;
    background: transparent !important;
    border: 1px solid transparent !important;
    color: rgba(248, 250, 252, 0.8) !important;
    font-weight: 500 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] > div button:hover {{
    opacity: 1;
    background: rgba(255, 255, 255, 0.04) !important;
}}

html body .stApp [data-testid="stAppViewContainer"] .main .block-container h1,
html body .stApp [data-testid="stAppViewContainer"] .main .block-container h2,
html body .stApp [data-testid="stAppViewContainer"] .main .block-container h3,
html body .stApp [data-testid="stAppViewContainer"] .main .block-container h1 *,
html body .stApp [data-testid="stAppViewContainer"] .main .block-container h2 *,
html body .stApp [data-testid="stAppViewContainer"] .main .block-container h3 * {{
    background: none !important;
    background-image: none !important;
    -webkit-background-clip: border-box !important;
    background-clip: border-box !important;
    color: var(--ws-text) !important;
    -webkit-text-fill-color: var(--ws-text) !important;
    text-fill-color: var(--ws-text) !important;
    text-shadow: none !important;
}}

h1 {{
    font-size: clamp(2rem, 3vw, 2.7rem);
    font-weight: 720;
    margin-bottom: 0.45rem;
}}

h2, h3 {{
    font-weight: 650;
}}

[data-testid="stSidebar"] label,
.stSelectbox label,
.stMultiSelect label,
.stTextInput label,
.stDateInput label,
.stNumberInput label,
.stTextArea label {{
    color: var(--ws-text-muted) !important;
    font-size: 0.84rem;
    font-weight: 600;
    letter-spacing: 0.01em;
}}

.stButton > button,
button[kind="primary"],
button[kind="secondary"] {{
    min-height: 2.75rem;
    border-radius: 999px !important;
    border: 1px solid rgba(212, 175, 55, 0.22) !important;
    background: linear-gradient(135deg, var(--ws-color-primary) 0%, var(--ws-color-primary-strong) 100%) !important;
    color: var(--ws-bg-dark) !important;
    box-shadow: var(--ws-shadow) !important;
    font-weight: 700 !important;
    transition: transform 0.18s ease, box-shadow 0.18s ease, border-color 0.18s ease, background 0.18s ease !important;
}}

.stButton > button:hover,
button[kind="primary"]:hover,
button[kind="secondary"]:hover {{
    transform: translateY(-1px);
    box-shadow: var(--ws-shadow-hover) !important;
    background: linear-gradient(135deg, var(--ws-color-primary-hover) 0%, var(--ws-color-primary-strong) 100%) !important;
}}

button[kind="secondary"] {{
    background: var(--ws-bg-surface) !important;
    color: var(--ws-color-primary) !important;
    border-color: rgba(212, 175, 55, 0.38) !important;
}}

html body .stApp [data-testid="stAppViewContainer"] .main a[href*="iphone_mode"],
html body .stApp [data-testid="stAppViewContainer"] .main a[href*="iphone_mode"] * {{
    background: linear-gradient(135deg, var(--ws-color-primary) 0%, var(--ws-color-primary-strong) 100%) !important;
    background-image: none !important;
    color: var(--ws-bg-dark) !important;
    border: 1px solid rgba(212, 175, 55, 0.30) !important;
    box-shadow: var(--ws-shadow) !important;
}}

[data-baseweb="select"] > div,
.stTextInput input,
.stNumberInput input,
.stDateInput input,
.stTextArea textarea {{
    border-radius: var(--ws-radius-sm) !important;
    border: 1px solid var(--ws-border-soft) !important;
    background: var(--ws-bg-surface) !important;
    color: var(--ws-text-main) !important;
    box-shadow: none !important;
}}

[data-baseweb="select"] > div:focus-within,
.stTextInput input:focus,
.stNumberInput input:focus,
.stDateInput input:focus,
.stTextArea textarea:focus {{
    border-color: rgba(212, 175, 55, 0.45) !important;
    box-shadow: 0 0 0 4px rgba(212, 175, 55, 0.10) !important;
}}

[data-baseweb="tag"] {{
    background: rgba(212, 175, 55, 0.12) !important;
    border: 1px solid rgba(212, 175, 55, 0.18) !important;
    border-radius: 999px !important;
    box-shadow: none !important;
}}

[data-baseweb="tag"] span {{
    color: var(--ws-color-primary) !important;
    font-weight: 600;
}}

.stMetric,
[data-testid="metric-container"],
[data-testid="stMetric"],
.stPlotlyChart,
[data-testid="stDataFrame"],
div[data-testid="stTable"],
div[data-testid="stExpander"] {{
    background: var(--ws-bg-surface) !important;
    border: 1px solid var(--ws-border-soft) !important;
    border-radius: var(--ws-radius-md) !important;
    box-shadow: var(--ws-shadow) !important;
}}

.stMetric,
[data-testid="metric-container"],
[data-testid="stMetric"] {{
    padding: 0.95rem 1rem !important;
}}

[data-testid="stMetricLabel"] p,
[data-testid="stMetricValue"] {{
    color: var(--ws-text-main) !important;
}}

.stPlotlyChart {{
    padding: 0.95rem 1rem;
    margin: 0.85rem 0;
    transition: transform 0.2s ease, box-shadow 0.2s ease;
}}

.stPlotlyChart:hover {{
    transform: translateY(-1px);
    box-shadow: var(--ws-shadow-hover) !important;
}}

[data-testid="stDataFrame"],
div[data-testid="stTable"] {{
    padding: 0.65rem;
}}

div[data-testid="stExpander"] {{
    overflow: hidden !important;
}}

div[data-testid="stExpander"] details summary {{
    padding: 0.9rem 1rem !important;
    color: var(--ws-text-main) !important;
    font-weight: 620 !important;
}}

.ws-page-toolbar,
[class*="st-key-ws-page-toolbar"] {{
    margin: 0.4rem 0 1.1rem 0;
    padding: 0.95rem 1.05rem;
    background: linear-gradient(180deg, rgba(255, 255, 255, 0.98) 0%, rgba(248, 250, 248, 0.92) 100%);
    border: 1px solid var(--ws-border-soft);
    border-radius: var(--ws-radius-md);
    box-shadow: var(--ws-shadow);
}}

.ws-page-toolbar {{
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.85rem;
}}

.ws-page-toolbar > * {{
    flex: 0 1 auto;
}}

[class*="st-key-ws-page-toolbar"] > div[data-testid="stVerticalBlock"] {{
    gap: 0.85rem;
}}

[class*="st-key-ws-page-toolbar"] [data-testid="stHorizontalBlock"] {{
    align-items: flex-end;
}}

div[data-testid="stExpanderDetails"] {{
    padding: 0.2rem 1rem 1rem 1rem !important;
}}

.stTabs [role="tablist"] {{
    gap: 0.5rem;
}}

.stTabs [role="tab"] {{
    border-radius: 999px !important;
    background: rgba(255,255,255,0.94) !important;
    border: 1px solid var(--ws-border-soft) !important;
    color: var(--ws-text-muted) !important;
    padding: 0.45rem 0.9rem !important;
}}

.stTabs [aria-selected="true"] {{
    color: var(--ws-bg-dark) !important;
    background: rgba(212, 175, 55, 0.14) !important;
    border-color: rgba(212, 175, 55, 0.34) !important;
    box-shadow: 0 8px 18px rgba(212, 175, 55, 0.10) !important;
}}

.stAlert {{
    border-radius: var(--ws-radius-md) !important;
    border: 1px solid var(--ws-border-soft) !important;
    border-left: 1px solid var(--ws-border-soft) !important;
    background: var(--ws-bg-surface) !important;
    box-shadow: var(--ws-shadow) !important;
}}

.stImage img {{
    border-radius: 18px;
    border: 1px solid var(--ws-border-soft);
    box-shadow: var(--ws-shadow);
}}

.ws-ai-signal,
[data-testid="stAlertContainer"] .stAlert {{
    box-shadow: var(--ws-ai-glow) !important;
}}

@media (max-width: 768px) {{
    .main .block-container {{
        padding: 1rem 0.95rem 2rem 0.95rem;
        border-radius: 24px;
    }}

    [data-testid="stSidebar"] {{
        padding: 1rem 0.75rem !important;
    }}

    .stPlotlyChart,
    [data-testid="stDataFrame"],
    div[data-testid="stTable"] {{
        padding: 0.55rem;
    }}

    .ws-tracker-shell {{
        padding: 1rem;
    }}
}}
"""
