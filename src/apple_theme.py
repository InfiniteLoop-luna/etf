from __future__ import annotations

import plotly.graph_objects as go


APPLE_THEME_TOKENS = {
    "bg": "#CDD8E4",
    "bg_deep": "#8598AE",
    "surface": "#F9FAFB",
    "surface_muted": "#F2F4F7",
    "surface_subtle": "#DEE6EF",
    "surface_tint": "rgba(211, 224, 240, 0.80)",
    "surface_tint_warm": "rgba(231, 237, 245, 0.28)",
    "surface_titanium": "rgba(142, 161, 184, 0.48)",
    "surface_shell": "rgba(102, 120, 143, 0.60)",
    "surface_glass": "rgba(247, 250, 255, 0.62)",
    "surface_glass_strong": "rgba(252, 254, 255, 0.82)",
    "shell_highlight": "rgba(255, 255, 255, 0.34)",
    "shell_shadow_edge": "rgba(45, 61, 84, 0.24)",
    "text": "#1D1D1F",
    "text_muted": "#6E6E73",
    "text_soft": "#8E8E93",
    "border": "rgba(49, 63, 82, 0.18)",
    "border_strong": "rgba(49, 63, 82, 0.30)",
    "accent": "#3B9CFF",
    "accent_hover": "#147FEA",
    "accent_soft": "rgba(59, 156, 255, 0.18)",
    "success": "#1D8F6A",
    "warning": "#B7791F",
    "danger": "#C2413B",
    "shadow": "0 22px 48px rgba(34, 46, 72, 0.18)",
    "shadow_hover": "0 32px 60px rgba(34, 46, 72, 0.24)",
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
            colorway=["#3B9CFF", "#6A7BFF", "#58B8D8", "#8AB4F8", "#34C759", "#FF9F0A"],
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
    background: linear-gradient(180deg, {APPLE_THEME_TOKENS["surface_glass_strong"]} 0%, rgba(245, 247, 250, 0.72) 100%);
    border: 1px solid {APPLE_THEME_TOKENS["border"]};
    border-radius: {APPLE_THEME_TOKENS["radius_lg"]};
    padding: 1.2rem 1.3rem;
    box-shadow: {APPLE_THEME_TOKENS["shadow"]};
    margin: 0.35rem 0 1rem 0;
    backdrop-filter: blur(28px);
    -webkit-backdrop-filter: blur(28px);
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
    background: linear-gradient(180deg, rgba(255,255,255,0.62) 0%, rgba(239,243,247,0.82) 100%);
    border: 1px solid rgba(29, 29, 31, 0.06);
    backdrop-filter: blur(28px);
    -webkit-backdrop-filter: blur(28px);
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
    --ws-bg-deep: {APPLE_THEME_TOKENS["bg_deep"]};
    --ws-surface: {APPLE_THEME_TOKENS["surface"]};
    --ws-surface-muted: {APPLE_THEME_TOKENS["surface_muted"]};
    --ws-surface-subtle: {APPLE_THEME_TOKENS["surface_subtle"]};
    --ws-surface-tint: {APPLE_THEME_TOKENS["surface_tint"]};
    --ws-surface-tint-warm: {APPLE_THEME_TOKENS["surface_tint_warm"]};
    --ws-surface-titanium: {APPLE_THEME_TOKENS["surface_titanium"]};
    --ws-surface-shell: {APPLE_THEME_TOKENS["surface_shell"]};
    --ws-surface-glass: {APPLE_THEME_TOKENS["surface_glass"]};
    --ws-surface-glass-strong: {APPLE_THEME_TOKENS["surface_glass_strong"]};
    --ws-shell-highlight: {APPLE_THEME_TOKENS["shell_highlight"]};
    --ws-shell-shadow-edge: {APPLE_THEME_TOKENS["shell_shadow_edge"]};
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
        radial-gradient(circle at 14% 0%, rgba(255, 255, 255, 0.58), transparent 24%),
        radial-gradient(circle at 82% 10%, rgba(88, 120, 166, 0.24), transparent 24%),
        radial-gradient(circle at 16% 76%, rgba(96, 124, 162, 0.18), transparent 19%),
        linear-gradient(180deg, #DCE4EE 0%, var(--ws-bg) 34%, var(--ws-bg-deep) 100%) !important;
    color: var(--ws-text) !important;
}}

[data-testid="stAppViewContainer"]::before {{
    content: "";
    position: fixed;
    inset: 0;
    pointer-events: none;
    background:
        linear-gradient(180deg, rgba(255,255,255,0.14) 0%, rgba(255,255,255,0.02) 18%, rgba(44, 58, 80, 0.18) 100%),
        radial-gradient(circle at 22% 16%, rgba(255,255,255,0.12), transparent 16%),
        radial-gradient(circle at 78% 14%, rgba(76, 117, 186, 0.08), transparent 14%);
    z-index: 0;
}}

[data-testid="stAppViewContainer"] > .main,
.main .block-container {{
    background: transparent !important;
    position: relative;
    z-index: 1;
}}

.main .block-container {{
    max-width: {APPLE_THEME_TOKENS["max_width"]};
    padding: 1.6rem 2.2rem 3rem 2.2rem;
    margin-top: 0.55rem;
    overflow: hidden;
    background:
        linear-gradient(145deg, var(--ws-surface-glass-strong) 0%, var(--ws-surface-glass) 16%, var(--ws-surface-tint) 34%, var(--ws-surface-titanium) 68%, var(--ws-surface-shell) 100%) !important;
    border: 1px solid rgba(255,255,255,0.20);
    border-radius: 32px;
    backdrop-filter: blur(36px);
    -webkit-backdrop-filter: blur(36px);
    box-shadow:
        inset 0 1px 0 var(--ws-shell-highlight),
        inset 0 -1px 0 rgba(255,255,255,0.08),
        0 34px 78px rgba(28, 38, 52, 0.22);
}}

.main .block-container::before {{
    content: "";
    position: absolute;
    inset: 1px 1px 44% 1px;
    border-radius: 31px 31px 24px 24px;
    pointer-events: none;
    background: linear-gradient(180deg, var(--ws-shell-highlight) 0%, rgba(255,255,255,0.10) 54%, rgba(255,255,255,0.00) 100%);
}}

.main .block-container::after {{
    content: "";
    position: absolute;
    left: 2rem;
    right: 2rem;
    bottom: 1rem;
    height: 1.35rem;
    border-radius: 999px;
    pointer-events: none;
    background: linear-gradient(180deg, rgba(95, 121, 156, 0.20) 0%, rgba(64, 82, 106, 0.02) 100%);
    box-shadow: 0 18px 32px var(--ws-shell-shadow-edge);
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
    background: linear-gradient(180deg, rgba(201, 214, 228, 0.88) 0%, rgba(142, 160, 183, 0.96) 100%) !important;
    border-right: 1px solid var(--ws-border) !important;
    padding: 1.5rem 1rem !important;
    min-width: 300px !important;
    box-shadow: inset -1px 0 0 rgba(255, 255, 255, 0.16);
}}

[data-testid="stSidebar"][aria-expanded="false"] {{
    min-width: 300px !important;
    width: 300px !important;
    transform: none !important;
    margin-left: 0 !important;
}}

[data-testid="stSidebar"] > div:first-child {{
    width: 300px !important;
    position: relative;
    overflow: hidden;
    background: linear-gradient(180deg, rgba(252, 254, 255, 0.82) 0%, var(--ws-surface-tint) 44%, var(--ws-surface-titanium) 72%, var(--ws-surface-shell) 100%) !important;
    backdrop-filter: blur(28px);
    -webkit-backdrop-filter: blur(28px);
    box-shadow:
        inset 0 1px 0 var(--ws-shell-highlight),
        inset -1px 0 0 rgba(255,255,255,0.08);
}}

[data-testid="stSidebar"] > div:first-child::before {{
    content: "";
    position: absolute;
    inset: 0 0 auto 0;
    height: 7rem;
    pointer-events: none;
    background: linear-gradient(180deg, rgba(255,255,255,0.26) 0%, rgba(255,255,255,0.00) 100%);
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
    background: linear-gradient(180deg, rgba(251, 253, 255, 0.84) 0%, rgba(205, 220, 239, 0.94) 100%) !important;
    color: var(--ws-text) !important;
    backdrop-filter: blur(28px);
    -webkit-backdrop-filter: blur(28px);
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
    background: linear-gradient(180deg, rgba(255, 255, 255, 0.92) 0%, rgba(219, 232, 248, 0.98) 100%) !important;
}}

[data-testid="stSidebar"] *,
.main * {{
    color: inherit;
}}

[data-testid="stSidebar"] [role="radiogroup"] {{
    padding: 0.75rem 0.85rem !important;
    margin: 0.4rem 0 0.85rem 0 !important;
    background: linear-gradient(180deg, rgba(251,253,255,0.86) 0%, rgba(204, 219, 239, 0.84) 72%, rgba(154, 173, 196, 0.72) 100%) !important;
    border: 1px solid rgba(49, 63, 82, 0.14) !important;
    border-radius: 20px !important;
    box-shadow: 0 12px 24px rgba(25, 37, 56, 0.10) !important;
    backdrop-filter: blur(28px);
    -webkit-backdrop-filter: blur(28px);
}}

[data-testid="stSidebar"] [role="radiogroup"] p,
[data-testid="stSidebar"] [role="radiogroup"] label,
[data-testid="stSidebar"] [role="radiogroup"] span {{
    color: var(--ws-text) !important;
    font-weight: 560 !important;
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
    text-shadow: 0 1px 0 rgba(255,255,255,0.22);
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
    background: linear-gradient(180deg, rgba(251, 253, 255, 0.80) 0%, rgba(208, 222, 240, 0.94) 100%) !important;
    color: var(--ws-text) !important;
    box-shadow: inset 0 1px 0 var(--ws-shell-highlight), 0 4px 16px rgba(15, 23, 42, 0.06) !important;
    font-weight: 600 !important;
    transition: transform 0.18s ease, box-shadow 0.18s ease, border-color 0.18s ease, background 0.18s ease !important;
    backdrop-filter: blur(28px);
    -webkit-backdrop-filter: blur(28px);
}}

html body .stApp [data-testid="stAppViewContainer"] .main a[href*="iphone_mode"],
html body .stApp [data-testid="stAppViewContainer"] .main a[href*="iphone_mode"] * {{
    background: linear-gradient(180deg, rgba(251, 253, 255, 0.80) 0%, rgba(205, 223, 244, 0.92) 100%) !important;
    background-image: none !important;
    color: var(--ws-accent) !important;
    border: 1px solid rgba(0, 113, 227, 0.16) !important;
    box-shadow: 0 10px 26px rgba(15, 23, 42, 0.08) !important;
}}

.stButton > button:hover,
button[kind="primary"]:hover,
button[kind="secondary"]:hover {{
    transform: translateY(-1px);
    box-shadow: 0 14px 28px rgba(41, 78, 133, 0.14) !important;
    border-color: rgba(10, 132, 255, 0.26) !important;
}}

button[kind="primary"] {{
    background: linear-gradient(180deg, #71B7FF 0%, var(--ws-accent) 52%, #0D6FD1 100%) !important;
    color: #FFFFFF !important;
    border-color: rgba(10, 132, 255, 0.34) !important;
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.24), 0 16px 34px rgba(10, 132, 255, 0.22) !important;
}}

[data-baseweb="select"] > div,
.stTextInput input,
.stNumberInput input,
.stDateInput input,
.stTextArea textarea {{
    border-radius: var(--ws-radius-sm) !important;
    border: 1px solid var(--ws-border) !important;
    background: linear-gradient(180deg, rgba(250, 252, 255, 0.76) 0%, rgba(208, 221, 239, 0.88) 100%) !important;
    color: var(--ws-text) !important;
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.6);
    backdrop-filter: blur(28px);
    -webkit-backdrop-filter: blur(28px);
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
    background: linear-gradient(180deg, rgba(251, 253, 255, 0.82) 0%, rgba(205, 222, 243, 0.94) 100%) !important;
    border: 1px solid rgba(10, 132, 255, 0.18) !important;
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
    background: linear-gradient(180deg, rgba(251,253,255,0.88) 0%, rgba(201, 217, 238, 0.86) 76%, rgba(154, 173, 196, 0.54) 100%) !important;
    border: 1px solid var(--ws-border-strong) !important;
    border-radius: var(--ws-radius-md) !important;
    padding: 0.95rem 1rem !important;
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.32), var(--ws-shadow) !important;
    backdrop-filter: blur(28px);
    -webkit-backdrop-filter: blur(28px);
}}

[data-testid="stMetricLabel"] p,
[data-testid="stMetricValue"] {{
    color: var(--ws-text) !important;
}}

.stPlotlyChart {{
    background: linear-gradient(180deg, rgba(251,253,255,0.90) 0%, rgba(199, 215, 234, 0.88) 74%, rgba(149, 168, 191, 0.56) 100%);
    border: 1px solid var(--ws-border-strong);
    border-radius: var(--ws-radius-lg);
    padding: 0.95rem 1rem;
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.30), var(--ws-shadow);
    margin: 0.85rem 0;
    transition: transform 0.2s ease, box-shadow 0.2s ease;
    backdrop-filter: blur(28px);
    -webkit-backdrop-filter: blur(28px);
}}

.stPlotlyChart:hover {{
    transform: translateY(-1px);
    box-shadow: var(--ws-shadow-hover);
}}

[data-testid="stDataFrame"],
div[data-testid="stTable"] {{
    background: linear-gradient(180deg, rgba(251,253,255,0.90) 0%, rgba(201, 216, 235, 0.92) 76%, rgba(150, 169, 193, 0.58) 100%);
    border: 1px solid var(--ws-border-strong);
    border-radius: var(--ws-radius-lg);
    padding: 0.65rem;
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.30), var(--ws-shadow);
    backdrop-filter: blur(28px);
    -webkit-backdrop-filter: blur(28px);
}}

div[data-testid="stExpander"] {{
    border: 1px solid var(--ws-border-strong) !important;
    border-radius: var(--ws-radius-md) !important;
    background: linear-gradient(180deg, rgba(251,253,255,0.88) 0%, rgba(200, 216, 236, 0.88) 76%, rgba(150, 169, 193, 0.58) 100%) !important;
    overflow: hidden !important;
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.30), var(--ws-shadow) !important;
    backdrop-filter: blur(28px);
    -webkit-backdrop-filter: blur(28px);
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
    background: linear-gradient(180deg, rgba(250,252,255,0.72) 0%, rgba(203, 218, 236, 0.82) 100%) !important;
    border: 1px solid rgba(29,29,31,0.06) !important;
    color: var(--ws-text-muted) !important;
    padding: 0.45rem 0.9rem !important;
    backdrop-filter: blur(28px);
    -webkit-backdrop-filter: blur(28px);
}}

.stTabs [aria-selected="true"] {{
    color: var(--ws-text) !important;
    background: linear-gradient(180deg, rgba(255,255,255,0.94) 0%, rgba(195, 214, 238, 0.98) 100%) !important;
    border-color: rgba(10, 132, 255, 0.20) !important;
    box-shadow: 0 12px 24px rgba(49, 88, 145, 0.12) !important;
}}

.stAlert {{
    border-radius: var(--ws-radius-md) !important;
    border: 1px solid var(--ws-border) !important;
    border-left: 1px solid var(--ws-border) !important;
    background: linear-gradient(180deg, rgba(250,252,255,0.82) 0%, rgba(198, 216, 237, 0.90) 100%) !important;
    box-shadow: var(--ws-shadow) !important;
    backdrop-filter: blur(28px) !important;
    -webkit-backdrop-filter: blur(28px) !important;
}}

.stImage img {{
    border-radius: 18px;
    border: 1px solid rgba(29, 29, 31, 0.08);
    box-shadow: 0 10px 24px rgba(15, 23, 42, 0.08);
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
