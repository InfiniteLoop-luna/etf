from __future__ import annotations

import plotly.graph_objects as go


# The public names are retained for compatibility with existing imports. The
# implementation now follows the Stripi-inspired design system in DESIGN.md.
APPLE_THEME_DEFAULT_TOKENS = {
    "bg_base": "#F6F9FC",
    "bg_surface": "#FFFFFF",
    "bg_dark": "#1C1E54",
    "surface_soft": "#F6F9FC",
    "surface_alt": "#F5E9D4",
    "surface_dark_alt": "#252761",
    "primary": "#533AFD",
    "primary_hover": "#4434D4",
    "primary_press": "#2E2B8C",
    "primary_strong": "#665EFD",
    "primary_soft": "rgba(83, 58, 253, 0.12)",
    "text_main": "#0D253D",
    "text_muted": "#64748D",
    "text_soft": "#61718A",
    "text_inverse": "#FFFFFF",
    "border_soft": "#E3E8EE",
    "border_strong": "#A8C3DE",
    "shadow": "0 1px 3px rgba(0, 55, 112, 0.08)",
    "shadow_hover": "0 8px 24px rgba(0, 55, 112, 0.08), 0 2px 6px rgba(0, 55, 112, 0.04)",
    "ai_glow": "0 0 0 1px rgba(83, 58, 253, 0.20), 0 10px 28px rgba(83, 58, 253, 0.10)",
    "color_up": "#EA2261",
    "color_down": "#2A9D8F",
    "color_warn": "#9B6829",
    "color_neutral": "#64748D",
    "color_purple": "#665EFD",
    "radius_lg": "12px",
    "radius_md": "8px",
    "radius_sm": "6px",
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
    font_family = "Inter, 'Segoe UI Variable', 'PingFang SC', 'Microsoft YaHei', sans-serif"
    return go.layout.Template(
        layout=go.Layout(
            paper_bgcolor=tokens["bg_surface"],
            plot_bgcolor=tokens["bg_surface"],
            font={"color": tokens["text_main"], "family": font_family, "size": 13},
            title={"font": {"color": tokens["text_main"], "size": 18}},
            colorway=[
                tokens["primary"],
                tokens["color_up"],
                tokens["bg_dark"],
                "#F96BEE",
                tokens["color_down"],
                tokens["primary_strong"],
            ],
            hoverlabel={
                "bgcolor": tokens["bg_dark"],
                "font": {"color": tokens["text_inverse"], "family": font_family},
                "bordercolor": tokens["primary_strong"],
            },
            legend={
                "bgcolor": "rgba(255,255,255,0.96)",
                "bordercolor": tokens["border_soft"],
                "borderwidth": 1,
                "font": {"color": tokens["text_muted"]},
            },
            margin={"l": 20, "r": 20, "t": 38, "b": 20},
            xaxis={
                "showline": True,
                "linewidth": 1,
                "ticks": "outside",
                "tickcolor": tokens["border_strong"],
                "gridcolor": "rgba(227, 232, 238, 0.78)",
                "linecolor": tokens["border_soft"],
                "zerolinecolor": tokens["border_soft"],
                "title": {"font": {"color": tokens["text_muted"]}},
            },
            yaxis={
                "showline": True,
                "linewidth": 1,
                "ticks": "outside",
                "tickcolor": tokens["border_strong"],
                "gridcolor": "rgba(227, 232, 238, 0.78)",
                "linecolor": tokens["border_soft"],
                "zerolinecolor": tokens["border_soft"],
                "title": {"font": {"color": tokens["text_muted"]}},
            },
        )
    )


def build_author_tracker_apple_css() -> str:
    tokens = get_apple_theme_tokens()
    return f"""
.ws-tracker-shell {{
    background: {tokens["bg_surface"]};
    border: 1px solid {tokens["border_soft"]};
    border-radius: {tokens["radius_lg"]};
    padding: 1.25rem 1.4rem;
    box-shadow: {tokens["shadow"]};
    margin: 0.35rem 0 1rem 0;
}}

.ws-tracker-shell .ws-tracker-eyebrow {{
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    padding: 0.25rem 0.55rem;
    border-radius: 9999px;
    background: {tokens["primary_soft"]};
    color: {tokens["primary"]};
    font-size: 0.68rem;
    font-weight: 400;
    letter-spacing: 0;
}}

.ws-tracker-shell h4 {{
    margin: 0.85rem 0 0.35rem 0;
    color: {tokens["text_main"]};
    font-size: 1.25rem;
    font-weight: 300;
    letter-spacing: 0;
}}

.ws-tracker-shell p {{
    margin: 0;
    color: {tokens["text_muted"]};
    font-size: 0.94rem;
    line-height: 1.5;
}}

.ws-tracker-section {{
    margin: 1.15rem 0 0.55rem 0;
    padding: 0.15rem 0 0.55rem 0;
    border-bottom: 1px solid {tokens["border_soft"]};
}}

.ws-tracker-section span {{
    color: {tokens["text_main"]};
    font-size: 1rem;
    font-weight: 400;
    letter-spacing: 0;
}}

.ws-evidence-gallery {{
    margin: 0.8rem 0 0.25rem 0;
    padding: 0.9rem 1rem;
    border-radius: {tokens["radius_md"]};
    background: {tokens["surface_soft"]};
    border: 1px solid {tokens["border_soft"]};
}}

.ws-evidence-gallery strong {{
    color: {tokens["text_main"]};
    font-size: 0.93rem;
    font-weight: 400;
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
    --ws-color-primary-press: {tokens["primary_press"]};
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
    --ws-font-sans: Inter, "Segoe UI Variable", "SF Pro Display", "PingFang SC", "Microsoft YaHei", sans-serif;
}}

html,
body,
[class*="css"] {{
    font-family: var(--ws-font-sans);
    font-feature-settings: "ss01";
    font-size: 15px;
    font-weight: 300;
    line-height: 1.4;
    letter-spacing: 0;
}}

html,
body,
.stApp,
[data-testid="stAppViewContainer"] {{
    background: var(--ws-bg-surface) !important;
    color: var(--ws-text-main) !important;
}}

[data-testid="stAppViewContainer"]::before {{
    content: "";
    position: fixed;
    inset: 0 0 auto 0;
    height: 19rem;
    pointer-events: none;
    background:
        radial-gradient(ellipse 42% 82% at 2% 18%, rgba(245, 233, 212, 0.96) 0%, rgba(245, 233, 212, 0) 73%),
        radial-gradient(ellipse 38% 94% at 29% 0%, rgba(241, 170, 116, 0.62) 0%, rgba(241, 170, 116, 0) 70%),
        radial-gradient(ellipse 46% 96% at 55% 7%, rgba(185, 185, 249, 0.88) 0%, rgba(185, 185, 249, 0) 72%),
        radial-gradient(ellipse 37% 98% at 78% 0%, rgba(83, 58, 253, 0.78) 0%, rgba(83, 58, 253, 0) 73%),
        radial-gradient(ellipse 32% 88% at 101% 5%, rgba(234, 34, 97, 0.68) 0%, rgba(249, 107, 238, 0) 74%),
        linear-gradient(180deg, rgba(246, 249, 252, 0.16) 0%, rgba(255, 255, 255, 0.98) 100%);
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
    padding: 2rem 2.25rem 4rem;
    margin-top: 0;
}}

.main p,
.main li,
.main label,
.main .stMarkdown,
.main [data-testid="stCaptionContainer"] {{
    color: var(--ws-text-muted) !important;
}}

.main a {{
    color: var(--ws-color-primary) !important;
    text-decoration: none;
}}

.main a:hover {{
    color: var(--ws-color-primary-hover) !important;
    text-decoration: underline;
    text-underline-offset: 0.18em;
}}

#MainMenu,
footer,
header {{
    visibility: hidden;
}}

[data-testid="stSidebar"] {{
    background: var(--ws-bg-dark) !important;
    border-right: 1px solid rgba(255, 255, 255, 0.08) !important;
    padding: 1.25rem 0.9rem !important;
    min-width: 288px !important;
}}

[data-testid="stSidebar"] > div:first-child {{
    width: 288px !important;
    background: var(--ws-bg-dark) !important;
    box-shadow: inset -1px 0 0 rgba(255, 255, 255, 0.05);
}}

@media (min-width: 769px) {{
    [data-testid="stSidebar"][aria-expanded="false"] {{
        min-width: 288px !important;
        width: 288px !important;
        transform: none !important;
        margin-left: 0 !important;
    }}
}}

[data-testid="collapsedControl"],
button[aria-label="Open sidebar"],
button[aria-label="Close sidebar"] {{
    position: fixed !important;
    top: 0.75rem !important;
    left: 0.75rem !important;
    width: 2.5rem !important;
    height: 2.5rem !important;
    border-radius: 9999px !important;
    border: 1px solid rgba(255, 255, 255, 0.18) !important;
    background: var(--ws-bg-dark) !important;
    color: var(--ws-text-inverse) !important;
    box-shadow: 0 8px 24px rgba(28, 30, 84, 0.20) !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
    z-index: 1000 !important;
    transition: background 0.18s ease, border-color 0.18s ease !important;
}}

[data-testid="collapsedControl"]:hover,
button[aria-label="Open sidebar"]:hover,
button[aria-label="Close sidebar"]:hover {{
    border-color: rgba(255, 255, 255, 0.34) !important;
    background: var(--ws-surface-dark-alt) !important;
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
    padding: 0.55rem 0.6rem !important;
    margin: 0.35rem 0 0.75rem !important;
    background: rgba(255, 255, 255, 0.04) !important;
    border: 1px solid rgba(255, 255, 255, 0.08) !important;
    border-radius: var(--ws-radius-md) !important;
}}

[data-testid="stSidebar"] [aria-checked="true"] {{
    accent-color: var(--ws-color-primary-strong) !important;
}}

[data-testid="stSidebar"] [aria-checked="true"] + div,
[data-testid="stSidebar"] [aria-checked="true"] ~ div {{
    color: #B9B9F9 !important;
}}

[data-testid="stSidebar"] .ws-sidebar-brand {{
    margin: 0 0 1rem;
    padding: 1rem 1.05rem 1.1rem;
    border-radius: var(--ws-radius-md);
    background:
        radial-gradient(ellipse 80% 130% at 0% 0%, rgba(245, 233, 212, 0.25) 0%, transparent 70%),
        radial-gradient(ellipse 75% 120% at 72% 0%, rgba(83, 58, 253, 0.55) 0%, transparent 72%),
        radial-gradient(ellipse 60% 110% at 100% 100%, rgba(234, 34, 97, 0.30) 0%, transparent 72%),
        rgba(255, 255, 255, 0.05);
    border: 1px solid rgba(255, 255, 255, 0.12);
    box-shadow: 0 8px 24px rgba(0, 0, 0, 0.12);
}}

[data-testid="stSidebar"] .ws-sidebar-brand h2 {{
    margin: 0.55rem 0 0.25rem;
    font-size: 1.15rem;
    font-weight: 300;
    letter-spacing: 0;
}}

[data-testid="stSidebar"] .ws-sidebar-brand p {{
    margin: 0;
    color: rgba(255, 255, 255, 0.72) !important;
    font-size: 0.8rem;
    line-height: 1.5;
}}

[data-testid="stSidebar"] .ws-sidebar-brand-kicker {{
    display: inline-flex;
    align-items: center;
    padding: 0.22rem 0.55rem;
    border-radius: 9999px;
    background: rgba(255, 255, 255, 0.12);
    color: #FFFFFF !important;
    font-size: 0.68rem;
    font-weight: 400;
    letter-spacing: 0;
    text-transform: uppercase;
}}

[data-testid="stSidebar"] .ws-sidebar-block {{
    margin: 0.9rem 0 0.35rem;
    padding: 0.15rem 0.35rem 0.6rem;
    border-bottom: 1px solid rgba(255, 255, 255, 0.10);
}}

[data-testid="stSidebar"] .ws-sidebar-block-title {{
    color: var(--ws-text-inverse) !important;
    font-size: 0.79rem;
    font-weight: 400;
    letter-spacing: 0;
}}

[data-testid="stSidebar"] .ws-sidebar-block-copy {{
    margin: 0.28rem 0 0;
    color: rgba(255, 255, 255, 0.58) !important;
    font-size: 0.74rem;
    line-height: 1.45;
}}

[data-testid="stSidebar"] .ws-sidebar-recent-item {{
    display: flex;
    flex-direction: column;
    gap: 0.12rem;
    padding: 0.6rem 0.72rem;
    margin: 0.25rem 0 0;
    border-radius: var(--ws-radius-sm);
    background: rgba(255, 255, 255, 0.04);
    border: 1px solid rgba(255, 255, 255, 0.07);
}}

[data-testid="stSidebar"] .ws-sidebar-recent-module {{
    color: #B9B9F9 !important;
    font-size: 0.69rem;
    font-weight: 400;
    letter-spacing: 0;
}}

[data-testid="stSidebar"] .ws-sidebar-recent-page {{
    color: var(--ws-text-inverse) !important;
    font-size: 0.82rem;
    font-weight: 400;
    line-height: 1.4;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-tree"] > div {{
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
    margin: 0.75rem 0 0;
}}

[data-testid="stSidebar"] .ws-sidebar-page-description,
[data-testid="stSidebar"] .ws-sidebar-search-result-meta,
[data-testid="stSidebar"] .ws-sidebar-empty {{
    display: block;
    color: rgba(255, 255, 255, 0.58) !important;
    font-size: 0.73rem;
    line-height: 1.4;
}}

[data-testid="stSidebar"] .ws-sidebar-page-description {{
    margin: 0.18rem 0 0.35rem 0.75rem;
}}

[data-testid="stSidebar"] .ws-sidebar-search-result-meta {{
    margin: 0.16rem 0 0.3rem 0.65rem;
}}

[data-testid="stSidebar"] .ws-sidebar-empty {{
    padding: 0.8rem;
    border-radius: var(--ws-radius-sm);
    background: rgba(255, 255, 255, 0.04);
    border: 1px dashed rgba(255, 255, 255, 0.14);
    text-align: center;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] {{
    border-radius: var(--ws-radius-sm);
    transition: background 0.16s ease, border-color 0.16s ease;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] > div button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] > div button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] > div button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] > div button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-favorite-"] > div button {{
    width: 100%;
    min-height: auto;
    justify-content: flex-start;
    white-space: normal;
    text-align: left;
    border-radius: var(--ws-radius-sm) !important;
    box-shadow: none !important;
    color: var(--ws-text-inverse) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] > div button {{
    padding: 0.62rem 0.72rem !important;
    background: rgba(255, 255, 255, 0.05) !important;
    border: 1px solid rgba(255, 255, 255, 0.08) !important;
    font-weight: 400 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-expanded"] > div button {{
    background: rgba(83, 58, 253, 0.24) !important;
    border-color: rgba(185, 185, 249, 0.32) !important;
    box-shadow: inset 2px 0 0 #B9B9F9 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] > div button {{
    padding: 0.52rem 0.65rem 0.55rem 0.88rem !important;
    background: transparent !important;
    border: 1px solid transparent !important;
    font-weight: 300 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] > div button:hover {{
    background: rgba(255, 255, 255, 0.06) !important;
    border-color: rgba(255, 255, 255, 0.08) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-active"] > div button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-current"] > div button {{
    background: rgba(83, 58, 253, 0.32) !important;
    border-color: rgba(185, 185, 249, 0.30) !important;
    box-shadow: inset 2px 0 0 #B9B9F9 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-favorite-"] > div button {{
    padding: 0.62rem 0.75rem !important;
    background: rgba(83, 58, 253, 0.25) !important;
    border: 1px solid rgba(185, 185, 249, 0.28) !important;
    font-weight: 400 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] > div button {{
    padding: 0.65rem 0.72rem !important;
    background: rgba(255, 255, 255, 0.07) !important;
    border: 1px solid rgba(255, 255, 255, 0.10) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] {{
    opacity: 0.78;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] > div button {{
    padding: 0.42rem 0.65rem 0.45rem 1rem !important;
    background: transparent !important;
    border: 1px solid transparent !important;
    font-weight: 300 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] > div button:hover {{
    opacity: 1;
    background: rgba(255, 255, 255, 0.05) !important;
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
    letter-spacing: 0 !important;
}}

h1 {{
    font-size: 2rem;
    line-height: 1.1;
    font-weight: 300;
    margin-bottom: 0.45rem;
}}

h2 {{
    font-size: 1.625rem;
    line-height: 1.12;
    font-weight: 300;
}}

h3 {{
    font-size: 1.25rem;
    line-height: 1.35;
    font-weight: 300;
}}

[data-testid="stSidebar"] label,
.stSelectbox label,
.stMultiSelect label,
.stTextInput label,
.stDateInput label,
.stNumberInput label,
.stTextArea label,
.stFileUploader label {{
    color: var(--ws-text-muted) !important;
    font-size: 0.78rem;
    font-weight: 400;
    letter-spacing: 0;
}}

.stButton > button,
.stDownloadButton > button,
button[kind="secondary"] {{
    min-height: 2.5rem;
    padding: 0.5rem 1rem !important;
    border-radius: 9999px !important;
    border: 1px solid var(--ws-color-primary) !important;
    background: var(--ws-bg-surface) !important;
    color: var(--ws-color-primary) !important;
    box-shadow: none !important;
    font-weight: 400 !important;
    letter-spacing: 0 !important;
    transition: color 0.16s ease, background 0.16s ease, border-color 0.16s ease !important;
}}

button[kind="primary"] {{
    min-height: 2.5rem;
    padding: 0.5rem 1rem !important;
    border-radius: 9999px !important;
    border: 1px solid var(--ws-color-primary) !important;
    background: var(--ws-color-primary) !important;
    color: var(--ws-text-inverse) !important;
    box-shadow: none !important;
    font-weight: 400 !important;
}}

.stButton > button *,
.stDownloadButton > button *,
button[kind="secondary"] * {{
    color: inherit !important;
    -webkit-text-fill-color: currentColor !important;
}}

button[kind="primary"] * {{
    color: var(--ws-text-inverse) !important;
    -webkit-text-fill-color: var(--ws-text-inverse) !important;
}}

.stButton > button:hover,
.stDownloadButton > button:hover,
button[kind="secondary"]:hover {{
    color: var(--ws-color-primary-hover) !important;
    background: rgba(83, 58, 253, 0.06) !important;
    border-color: var(--ws-color-primary-hover) !important;
}}

button[kind="primary"]:hover {{
    background: var(--ws-color-primary-hover) !important;
    border-color: var(--ws-color-primary-hover) !important;
}}

button[kind="primary"]:active {{
    background: var(--ws-color-primary-press) !important;
    border-color: var(--ws-color-primary-press) !important;
}}

.stButton > button:focus-visible,
.stDownloadButton > button:focus-visible,
button:focus-visible {{
    outline: 3px solid rgba(83, 58, 253, 0.20) !important;
    outline-offset: 2px;
}}

html body .stApp [data-testid="stAppViewContainer"] .main a[href*="iphone_mode"],
html body .stApp [data-testid="stAppViewContainer"] .main a[href*="iphone_mode"] * {{
    background: var(--ws-color-primary) !important;
    background-image: none !important;
    color: var(--ws-text-inverse) !important;
    border: 1px solid var(--ws-color-primary) !important;
    box-shadow: none !important;
}}

[data-baseweb="select"] > div,
.stTextInput input,
.stNumberInput input,
.stDateInput input,
.stTimeInput input,
.stTextArea textarea {{
    min-height: 2.5rem;
    border-radius: var(--ws-radius-sm) !important;
    border: 1px solid var(--ws-border-strong) !important;
    background: var(--ws-bg-surface) !important;
    color: var(--ws-text-main) !important;
    box-shadow: none !important;
}}

[data-baseweb="select"] > div:focus-within,
.stTextInput input:focus,
.stNumberInput input:focus,
.stDateInput input:focus,
.stTimeInput input:focus,
.stTextArea textarea:focus {{
    border-color: var(--ws-color-primary) !important;
    box-shadow: 0 0 0 3px rgba(83, 58, 253, 0.12) !important;
}}

[data-baseweb="tag"] {{
    background: var(--ws-color-primary-soft) !important;
    border: 1px solid rgba(83, 58, 253, 0.16) !important;
    border-radius: 9999px !important;
    box-shadow: none !important;
}}

[data-baseweb="tag"] span {{
    color: var(--ws-color-primary-hover) !important;
    font-weight: 400;
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
    box-shadow: var(--ws-shadow) !important;
}}

.stMetric,
[data-testid="stMetric"],
[data-testid="metric-container"] {{
    padding: 0.85rem 0.95rem !important;
}}

[data-testid="stMetricLabel"] p {{
    color: var(--ws-text-muted) !important;
    font-size: 0.78rem !important;
    font-weight: 400 !important;
}}

[data-testid="stMetricValue"],
[data-testid="stMetricDelta"],
[data-testid="stDataFrame"],
div[data-testid="stTable"] {{
    font-variant-numeric: tabular-nums;
    font-feature-settings: "tnum";
}}

[data-testid="stMetricValue"] {{
    color: var(--ws-text-main) !important;
    font-size: 1.55rem !important;
    font-weight: 300 !important;
    letter-spacing: 0 !important;
}}

.stPlotlyChart {{
    padding: 0.85rem 0.95rem;
    margin: 0.75rem 0;
    border-radius: var(--ws-radius-lg) !important;
}}

[data-testid="stDataFrame"],
div[data-testid="stTable"] {{
    padding: 0.55rem;
    border-radius: var(--ws-radius-lg) !important;
}}

div[data-testid="stExpander"] {{
    overflow: hidden !important;
}}

div[data-testid="stExpander"] details summary {{
    padding: 0.8rem 0.9rem !important;
    color: var(--ws-text-main) !important;
    font-weight: 400 !important;
}}

div[data-testid="stExpanderDetails"] {{
    padding: 0.15rem 0.9rem 0.9rem !important;
    border-top: 1px solid var(--ws-border-soft);
}}

.ws-page-toolbar,
[class*="st-key-ws-page-toolbar"] {{
    margin: 0.35rem 0 1rem;
    padding: 0.85rem 0.95rem;
    background: rgba(246, 249, 252, 0.92);
    border-top: 1px solid var(--ws-border-soft);
    border-bottom: 1px solid var(--ws-border-soft);
    border-radius: 0;
    box-shadow: none;
}}

.ws-page-toolbar {{
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.75rem;
}}

.ws-page-toolbar > * {{
    flex: 0 1 auto;
}}

[class*="st-key-ws-page-toolbar"] > div[data-testid="stVerticalBlock"] {{
    gap: 0.75rem;
}}

[class*="st-key-ws-page-toolbar"] [data-testid="stHorizontalBlock"] {{
    align-items: flex-end;
}}

.stTabs [role="tablist"] {{
    width: fit-content;
    max-width: 100%;
    gap: 0.2rem;
    padding: 0.2rem;
    border-radius: 9999px;
    background: var(--ws-surface-soft);
    border: 1px solid var(--ws-border-soft);
    overflow-x: auto;
}}

.stTabs [role="tab"] {{
    min-height: 2.25rem;
    border-radius: 9999px !important;
    background: transparent !important;
    border: 1px solid transparent !important;
    color: var(--ws-text-muted) !important;
    padding: 0.4rem 0.8rem !important;
    font-weight: 400 !important;
}}

.stTabs [role="tab"] * {{
    color: inherit !important;
    -webkit-text-fill-color: currentColor !important;
}}

.stTabs [aria-selected="true"] {{
    color: var(--ws-color-primary-hover) !important;
    background: var(--ws-bg-surface) !important;
    border-color: rgba(83, 58, 253, 0.22) !important;
    box-shadow: var(--ws-shadow) !important;
}}

.stAlert {{
    border-radius: var(--ws-radius-md) !important;
    border: 1px solid var(--ws-border-soft) !important;
    border-left: 3px solid var(--ws-color-primary) !important;
    background: var(--ws-bg-surface) !important;
    box-shadow: none !important;
}}

.stImage img {{
    border-radius: var(--ws-radius-md);
    border: 1px solid var(--ws-border-soft);
    box-shadow: var(--ws-shadow);
}}

.ws-ai-signal,
[data-testid="stAlertContainer"] .stAlert {{
    box-shadow: var(--ws-ai-glow) !important;
}}

[data-testid="stFileUploaderDropzone"] {{
    background: var(--ws-surface-soft) !important;
    border: 1px dashed var(--ws-border-strong) !important;
    border-radius: var(--ws-radius-md) !important;
}}

[data-testid="stProgress"] > div > div {{
    background-color: var(--ws-color-primary) !important;
}}

[data-baseweb="popover"],
[role="dialog"] {{
    border-radius: var(--ws-radius-md) !important;
    border-color: var(--ws-border-soft) !important;
    box-shadow: var(--ws-shadow-hover) !important;
}}

hr {{
    border-color: var(--ws-border-soft) !important;
}}

code {{
    color: var(--ws-color-primary-hover) !important;
    background: var(--ws-surface-soft) !important;
    border-radius: 4px;
}}

@media (max-width: 768px) {{
    [data-testid="stAppViewContainer"]::before {{
        height: 12rem;
        background:
            radial-gradient(ellipse 85% 90% at 0% 0%, rgba(245, 233, 212, 0.94) 0%, rgba(245, 233, 212, 0) 72%),
            radial-gradient(ellipse 82% 95% at 48% 0%, rgba(185, 185, 249, 0.82) 0%, rgba(185, 185, 249, 0) 73%),
            radial-gradient(ellipse 72% 90% at 100% 0%, rgba(234, 34, 97, 0.54) 0%, rgba(83, 58, 253, 0) 75%),
            linear-gradient(180deg, rgba(246, 249, 252, 0.12) 0%, rgba(255, 255, 255, 0.98) 100%);
    }}

    .main .block-container {{
        padding: 1.25rem 0.95rem 2.5rem;
    }}

    [data-testid="stSidebar"] {{
        min-width: 280px !important;
        width: 280px !important;
        padding: 1rem 0.75rem !important;
    }}

    [data-testid="stSidebar"] > div:first-child {{
        width: 280px !important;
    }}

    [data-testid="stSidebar"][aria-expanded="false"] {{
        min-width: 0 !important;
        width: 0 !important;
        transform: translateX(-100%) !important;
    }}

    h1 {{
        font-size: 1.625rem;
    }}

    h2 {{
        font-size: 1.375rem;
    }}

    .stButton > button,
    .stDownloadButton > button,
    button[kind="primary"],
    button[kind="secondary"] {{
        min-height: 2.75rem;
    }}

    .stPlotlyChart,
    [data-testid="stDataFrame"],
    div[data-testid="stTable"] {{
        padding: 0.45rem;
    }}

    .ws-tracker-shell {{
        padding: 1rem;
    }}
}}
"""
