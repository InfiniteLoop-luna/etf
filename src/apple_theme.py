from __future__ import annotations

import plotly.graph_objects as go


APPLE_THEME_TOKENS = {
    "bg": "#F5F5F7",
    "surface": "#FFFFFF",
    "surface_muted": "#FBFBFD",
    "surface_subtle": "#F0F2F5",
    "text": "#1D1D1F",
    "text_muted": "#6E6E73",
    "text_soft": "#8E8E93",
    "border": "rgba(29, 29, 31, 0.10)",
    "border_strong": "rgba(29, 29, 31, 0.16)",
    "accent": "#0071E3",
    "accent_hover": "#0058B5",
    "accent_soft": "rgba(0, 113, 227, 0.10)",
    "success": "#1D8F6A",
    "warning": "#B7791F",
    "danger": "#C2413B",
    "shadow": "0 10px 30px rgba(15, 23, 42, 0.06)",
    "shadow_hover": "0 18px 40px rgba(15, 23, 42, 0.10)",
    "radius_lg": "22px",
    "radius_md": "16px",
    "radius_sm": "12px",
    "max_width": "1480px",
}


def build_apple_plotly_template() -> go.layout.Template:
    return go.layout.Template(
        layout=go.Layout(
            paper_bgcolor="#F5F5F7",
            plot_bgcolor="#FFFFFF",
            font={"color": APPLE_THEME_TOKENS["text"], "family": "-apple-system, BlinkMacSystemFont, 'SF Pro Text', 'PingFang SC', sans-serif"},
            title={"font": {"color": APPLE_THEME_TOKENS["text"], "size": 20}},
            colorway=["#0071E3", "#34AADC", "#30D158", "#FF9F0A", "#FF453A", "#8E8E93"],
            hoverlabel={
                "bgcolor": "rgba(29, 29, 31, 0.94)",
                "font": {"color": "#FFFFFF"},
                "bordercolor": "rgba(255,255,255,0.10)",
            },
            legend={
                "bgcolor": "rgba(255,255,255,0.88)",
                "bordercolor": "rgba(29, 29, 31, 0.08)",
                "borderwidth": 1,
                "font": {"color": APPLE_THEME_TOKENS["text_muted"]},
            },
            margin={"l": 20, "r": 20, "t": 36, "b": 20},
            xaxis={
                "showline": True,
                "linewidth": 1,
                "ticks": "outside",
                "tickcolor": "rgba(110, 110, 115, 0.28)",
                "gridcolor": "rgba(110, 110, 115, 0.12)",
                "linecolor": "rgba(110, 110, 115, 0.20)",
                "zerolinecolor": "rgba(110, 110, 115, 0.08)",
                "title": {"font": {"color": APPLE_THEME_TOKENS["text_muted"]}},
            },
            yaxis={
                "showline": True,
                "linewidth": 1,
                "ticks": "outside",
                "tickcolor": "rgba(110, 110, 115, 0.28)",
                "gridcolor": "rgba(110, 110, 115, 0.12)",
                "linecolor": "rgba(110, 110, 115, 0.20)",
                "zerolinecolor": "rgba(110, 110, 115, 0.08)",
                "title": {"font": {"color": APPLE_THEME_TOKENS["text_muted"]}},
            },
        )
    )


def build_author_tracker_apple_css() -> str:
    return f"""
.ws-tracker-shell {{
    background: linear-gradient(180deg, rgba(255,255,255,0.94) 0%, rgba(251,251,253,0.96) 100%);
    border: 1px solid {APPLE_THEME_TOKENS["border"]};
    border-radius: {APPLE_THEME_TOKENS["radius_lg"]};
    padding: 1.2rem 1.3rem;
    box-shadow: {APPLE_THEME_TOKENS["shadow"]};
    margin: 0.35rem 0 1rem 0;
}}

.ws-tracker-shell .ws-tracker-eyebrow {{
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    padding: 0.3rem 0.7rem;
    border-radius: 999px;
    background: {APPLE_THEME_TOKENS["accent_soft"]};
    color: {APPLE_THEME_TOKENS["accent"]};
    font-size: 0.76rem;
    font-weight: 600;
    letter-spacing: 0.02em;
}}

.ws-tracker-shell h4 {{
    margin: 0.9rem 0 0.35rem 0;
    color: {APPLE_THEME_TOKENS["text"]};
    font-size: 1.25rem;
    font-weight: 700;
    letter-spacing: -0.02em;
}}

.ws-tracker-shell p {{
    margin: 0;
    color: {APPLE_THEME_TOKENS["text_muted"]};
    font-size: 0.95rem;
    line-height: 1.6;
}}

.ws-tracker-section {{
    margin: 1.15rem 0 0.55rem 0;
    padding: 0.15rem 0 0.55rem 0;
    border-bottom: 1px solid rgba(29, 29, 31, 0.08);
}}

.ws-tracker-section span {{
    color: {APPLE_THEME_TOKENS["text"]};
    font-size: 1rem;
    font-weight: 650;
    letter-spacing: -0.01em;
}}

.ws-evidence-gallery {{
    margin: 0.8rem 0 0.25rem 0;
    padding: 0.9rem 1rem;
    border-radius: {APPLE_THEME_TOKENS["radius_md"]};
    background: linear-gradient(180deg, rgba(245,245,247,0.9) 0%, rgba(255,255,255,0.96) 100%);
    border: 1px solid rgba(29, 29, 31, 0.06);
}}

.ws-evidence-gallery strong {{
    color: {APPLE_THEME_TOKENS["text"]};
    font-size: 0.93rem;
}}

.ws-evidence-gallery-note {{
    margin-top: 0.35rem;
    color: {APPLE_THEME_TOKENS["text_soft"]};
    font-size: 0.82rem;
}}
"""


def build_global_apple_theme_css() -> str:
    return f"""
:root {{
    --ws-bg: {APPLE_THEME_TOKENS["bg"]};
    --ws-surface: {APPLE_THEME_TOKENS["surface"]};
    --ws-surface-muted: {APPLE_THEME_TOKENS["surface_muted"]};
    --ws-surface-subtle: {APPLE_THEME_TOKENS["surface_subtle"]};
    --ws-text: {APPLE_THEME_TOKENS["text"]};
    --ws-text-muted: {APPLE_THEME_TOKENS["text_muted"]};
    --ws-text-soft: {APPLE_THEME_TOKENS["text_soft"]};
    --ws-border: {APPLE_THEME_TOKENS["border"]};
    --ws-border-strong: {APPLE_THEME_TOKENS["border_strong"]};
    --ws-accent: {APPLE_THEME_TOKENS["accent"]};
    --ws-accent-hover: {APPLE_THEME_TOKENS["accent_hover"]};
    --ws-accent-soft: {APPLE_THEME_TOKENS["accent_soft"]};
    --ws-shadow: {APPLE_THEME_TOKENS["shadow"]};
    --ws-shadow-hover: {APPLE_THEME_TOKENS["shadow_hover"]};
    --ws-radius-lg: {APPLE_THEME_TOKENS["radius_lg"]};
    --ws-radius-md: {APPLE_THEME_TOKENS["radius_md"]};
    --ws-radius-sm: {APPLE_THEME_TOKENS["radius_sm"]};
}}

html, body, [class*="css"] {{
    font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
}}

html, body, .stApp, [data-testid="stAppViewContainer"] {{
    background:
        radial-gradient(circle at top left, rgba(255, 255, 255, 0.92), transparent 26%),
        linear-gradient(180deg, #FCFCFD 0%, var(--ws-bg) 38%, #EFF1F4 100%) !important;
    color: var(--ws-text) !important;
}}

[data-testid="stAppViewContainer"] > .main,
.main .block-container {{
    background: transparent !important;
}}

.main .block-container {{
    max-width: {APPLE_THEME_TOKENS["max_width"]};
    padding: 1.6rem 2.2rem 3rem 2.2rem;
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
    background: linear-gradient(180deg, rgba(251, 251, 253, 0.96) 0%, rgba(245, 245, 247, 0.96) 100%) !important;
    border-right: 1px solid var(--ws-border) !important;
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
    border: 1px solid rgba(29, 29, 31, 0.08) !important;
    background: rgba(255, 255, 255, 0.86) !important;
    color: var(--ws-text) !important;
    backdrop-filter: blur(18px);
    box-shadow: 0 12px 28px rgba(15, 23, 42, 0.12) !important;
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
    box-shadow: 0 16px 30px rgba(15, 23, 42, 0.16) !important;
    background: rgba(255, 255, 255, 0.96) !important;
}}

[data-testid="stSidebar"] *,
.main * {{
    color: inherit;
}}

[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3,
h1,
h2,
h3 {{
    color: var(--ws-text) !important;
    letter-spacing: -0.03em;
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
    border: 1px solid rgba(29, 29, 31, 0.10) !important;
    background: rgba(255, 255, 255, 0.9) !important;
    color: var(--ws-text) !important;
    box-shadow: 0 4px 16px rgba(15, 23, 42, 0.06) !important;
    font-weight: 600 !important;
    transition: transform 0.18s ease, box-shadow 0.18s ease, border-color 0.18s ease, background 0.18s ease !important;
}}

.stButton > button:hover,
button[kind="primary"]:hover,
button[kind="secondary"]:hover {{
    transform: translateY(-1px);
    box-shadow: 0 10px 24px rgba(15, 23, 42, 0.10) !important;
    border-color: rgba(0, 113, 227, 0.22) !important;
}}

button[kind="primary"] {{
    background: linear-gradient(180deg, #0A84FF 0%, var(--ws-accent) 100%) !important;
    color: #FFFFFF !important;
    border-color: rgba(0, 113, 227, 0.28) !important;
}}

[data-baseweb="select"] > div,
.stTextInput input,
.stNumberInput input,
.stDateInput input,
.stTextArea textarea {{
    border-radius: var(--ws-radius-sm) !important;
    border: 1px solid var(--ws-border) !important;
    background: rgba(255, 255, 255, 0.92) !important;
    color: var(--ws-text) !important;
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.6);
}}

[data-baseweb="select"] > div:focus-within,
.stTextInput input:focus,
.stNumberInput input:focus,
.stDateInput input:focus,
.stTextArea textarea:focus {{
    border-color: rgba(0, 113, 227, 0.34) !important;
    box-shadow: 0 0 0 4px rgba(0, 113, 227, 0.12) !important;
}}

[data-baseweb="tag"] {{
    background: rgba(255, 255, 255, 0.88) !important;
    border: 1px solid rgba(0, 113, 227, 0.14) !important;
    border-radius: 999px !important;
    box-shadow: none !important;
}}

[data-baseweb="tag"] span {{
    color: var(--ws-accent) !important;
    font-weight: 600;
}}

.stMetric,
[data-testid="metric-container"],
[data-testid="stMetric"] {{
    background: linear-gradient(180deg, rgba(255,255,255,0.96) 0%, rgba(251,251,253,0.96) 100%) !important;
    border: 1px solid var(--ws-border) !important;
    border-radius: var(--ws-radius-md) !important;
    padding: 0.95rem 1rem !important;
    box-shadow: var(--ws-shadow) !important;
}}

[data-testid="stMetricLabel"] p,
[data-testid="stMetricValue"] {{
    color: var(--ws-text) !important;
}}

.stPlotlyChart {{
    background: linear-gradient(180deg, rgba(255,255,255,0.96) 0%, rgba(251,251,253,0.96) 100%);
    border: 1px solid var(--ws-border);
    border-radius: var(--ws-radius-lg);
    padding: 0.95rem 1rem;
    box-shadow: var(--ws-shadow);
    margin: 0.85rem 0;
    transition: transform 0.2s ease, box-shadow 0.2s ease;
}}

.stPlotlyChart:hover {{
    transform: translateY(-1px);
    box-shadow: var(--ws-shadow-hover);
}}

[data-testid="stDataFrame"],
div[data-testid="stTable"] {{
    background: linear-gradient(180deg, rgba(255,255,255,0.98) 0%, rgba(251,251,253,0.98) 100%);
    border: 1px solid var(--ws-border);
    border-radius: var(--ws-radius-lg);
    padding: 0.65rem;
    box-shadow: var(--ws-shadow);
}}

div[data-testid="stExpander"] {{
    border: 1px solid var(--ws-border) !important;
    border-radius: var(--ws-radius-md) !important;
    background: rgba(255,255,255,0.90) !important;
    overflow: hidden !important;
    box-shadow: var(--ws-shadow) !important;
}}

div[data-testid="stExpander"] details summary {{
    padding: 0.9rem 1rem !important;
    color: var(--ws-text) !important;
    font-weight: 620 !important;
}}

div[data-testid="stExpanderDetails"] {{
    padding: 0.2rem 1rem 1rem 1rem !important;
}}

.stTabs [role="tablist"] {{
    gap: 0.5rem;
}}

.stTabs [role="tab"] {{
    border-radius: 999px !important;
    background: rgba(255,255,255,0.72) !important;
    border: 1px solid rgba(29,29,31,0.06) !important;
    color: var(--ws-text-muted) !important;
    padding: 0.45rem 0.9rem !important;
}}

.stTabs [aria-selected="true"] {{
    color: var(--ws-text) !important;
    background: #FFFFFF !important;
    box-shadow: 0 8px 20px rgba(15, 23, 42, 0.06) !important;
}}

.stAlert {{
    border-radius: var(--ws-radius-md) !important;
    border: 1px solid var(--ws-border) !important;
}}

.stImage img {{
    border-radius: 18px;
    border: 1px solid rgba(29, 29, 31, 0.08);
    box-shadow: 0 10px 24px rgba(15, 23, 42, 0.08);
}}

@media (max-width: 768px) {{
    .main .block-container {{
        padding: 1rem 0.95rem 2rem 0.95rem;
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
