from __future__ import annotations


# Palette sampled from the supplied Doraemon dashboard reference:
# airy ice-blue canvas, vivid character blue, nose red, and bell yellow.
DORAEMON_THEME_TOKENS: dict[str, str] = {
    "bg_base": "#E5F3FE",
    "bg_surface": "#FFFFFF",
    "bg_dark": "#176A9E",
    "surface_soft": "#F0F8FF",
    "surface_alt": "#E8F5FE",
    "surface_dark_alt": "#D7EEFC",
    "primary": "#11A9EE",
    "primary_hover": "#0799DC",
    "primary_press": "#0787C2",
    "primary_strong": "#57B2F6",
    "primary_soft": "#E3F4FE",
    "secondary": "#F46968",
    "text_main": "#29465B",
    "text_muted": "#6F8798",
    "text_soft": "#98AAB7",
    "text_inverse": "#FFFFFF",
    "border_soft": "#CFE7F6",
    "border_strong": "#9FD2EF",
    "shadow": "0 8px 24px rgba(42, 136, 192, 0.10)",
    "shadow_hover": "0 12px 30px rgba(42, 136, 192, 0.16)",
    "ai_glow": "0 0 0 3px rgba(17, 169, 238, 0.10)",
    "color_up": "#2FA36B",
    "color_down": "#F46968",
    "color_warn": "#FCCD3D",
    "color_neutral": "#6F8798",
    "color_accent_alt": "#FCCD3D",
    "radius_lg": "22px",
    "radius_md": "15px",
    "radius_sm": "10px",
    "max_width": "1280px",
}

DORAEMON_SIDEBAR_TOKENS: dict[str, str] = {
    "sidebar_bg": "#EAF6FE",
    "sidebar_active_bg": "#FFFFFF",
    "sidebar_hover_bg": "#DDF1FD",
    "sidebar_accent": "#11A9EE",
    "sidebar_accent_hover": "#0787C2",
    "sidebar_text": "#35566D",
    "sidebar_line": "#C9E5F5",
}


def build_doraemon_extra_css() -> str:
    """Return the Doraemon-specific visual layer appended after the base theme."""
    s = DORAEMON_SIDEBAR_TOKENS
    return f"""
/* ── Doraemon reference theme ────────────────────────────────── */
:root {{
    --ws-sidebar-bg: {s["sidebar_bg"]};
    --ws-sidebar-active-bg: {s["sidebar_active_bg"]};
    --ws-sidebar-hover-bg: {s["sidebar_hover_bg"]};
    --ws-sidebar-accent: {s["sidebar_accent"]};
    --ws-sidebar-accent-hover: {s["sidebar_accent_hover"]};
    --ws-sidebar-text: {s["sidebar_text"]};
    --ws-sidebar-line: {s["sidebar_line"]};
    --dora-blue: #11A9EE;
    --dora-blue-soft: #E5F3FE;
    --dora-red: #F46968;
    --dora-yellow: #FCCD3D;
    --dora-ink: #29465B;
}}

/* Airy canvas from the reference instead of the previous dark/strong gradient. */
html,
body,
.stApp,
[data-testid="stAppViewContainer"],
[data-testid="stAppViewContainer"] > .main,
[data-testid="stMain"] {{
    color: var(--dora-ink) !important;
    background: #E5F3FE !important;
}}

[data-testid="stMain"] {{
    background:
        radial-gradient(circle at 91% 7%, rgba(255,255,255,.72) 0 52px, transparent 53px),
        radial-gradient(circle at 84% 13%, rgba(252,205,61,.20) 0 20px, transparent 21px),
        #E5F3FE !important;
}}

[data-testid="stMainBlockContainer"] {{
    background: rgba(255,255,255,.97) !important;
    border: 1px solid rgba(159,210,239,.72) !important;
    border-radius: 24px !important;
    box-shadow: 0 12px 34px rgba(42,136,192,.11) !important;
}}

/* Light navigation rail. Blue is structure/accent, not a full-height color wall. */
[data-testid="stSidebar"] {{
    background: {s["sidebar_bg"]} !important;
    border-right: 1px solid {s["sidebar_line"]} !important;
}}

[data-testid="stSidebar"] > div:first-child,
[data-testid="stSidebar"] [data-testid="stSidebarContent"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-footer"] {{
    background: transparent !important;
}}

[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] label {{
    color: {s["sidebar_text"]} !important;
}}

[data-testid="stSidebar"] .ws-sidebar-brand {{
    position: relative;
    min-height: 68px;
    padding-bottom: .9rem;
    border-bottom-color: {s["sidebar_line"]} !important;
}}

/* CSS-built Doraemon face: recognisable without adding a heavy decorative image. */
[data-testid="stSidebar"] .ws-sidebar-brand-kicker {{
    position: relative;
    overflow: visible;
    flex-basis: 38px;
    width: 38px;
    height: 38px;
    color: transparent !important;
    background:
        radial-gradient(ellipse at 50% 66%, #FFFFFF 0 45%, transparent 46%),
        #11A9EE !important;
    border: 2px solid #0787C2;
    border-radius: 50% !important;
    box-shadow: inset 0 -2px 0 rgba(7,135,194,.16);
}}

[data-testid="stSidebar"] .ws-sidebar-brand-kicker::before {{
    position: absolute;
    top: 14px;
    left: 50%;
    width: 8px;
    height: 8px;
    background: #F46968;
    border: 1px solid #D84F54;
    border-radius: 50%;
    box-shadow: 0 1px 0 rgba(255,255,255,.8) inset;
    content: "";
    transform: translateX(-50%);
}}

[data-testid="stSidebar"] .ws-sidebar-brand-kicker::after {{
    position: absolute;
    top: 5px;
    left: 50%;
    width: 12px;
    height: 10px;
    background:
        radial-gradient(circle at 31% 55%, #29465B 0 1px, transparent 1.5px),
        radial-gradient(circle at 69% 55%, #29465B 0 1px, transparent 1.5px),
        #FFFFFF;
    border: 1px solid #9FD2EF;
    border-radius: 50%;
    content: "";
    transform: translateX(-50%);
}}

[data-testid="stSidebar"] .ws-sidebar-brand h2 {{
    color: #176A9E !important;
    font-weight: 750 !important;
}}

[data-testid="stSidebar"] .ws-sidebar-brand p {{
    color: #6F8798 !important;
    font-style: normal !important;
}}

[data-testid="stSidebar"] .ws-sidebar-brand p::before {{
    display: inline-block;
    width: 10px;
    height: 10px;
    margin-right: 6px;
    background: #FCCD3D;
    border: 1px solid #D4A91E;
    border-radius: 50%;
    box-shadow: inset 0 -2px 0 rgba(212,169,30,.24);
    content: "";
}}

[data-testid="stSidebar"] .ws-sidebar-block-title {{
    color: #7894A6 !important;
    font-weight: 700 !important;
}}

[data-testid="stSidebar"] [class*="st-key-sidebar_search_query"] [data-testid="stTextInputRootElement"],
[data-testid="stSidebar"] [class*="st-key-sidebar-search-query"] [data-testid="stTextInputRootElement"] {{
    background: rgba(255,255,255,.92) !important;
    border: 1px solid #C9E5F5 !important;
    box-shadow: 0 4px 12px rgba(42,136,192,.06) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-favorite-"] button {{
    color: {s["sidebar_text"]} !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] button:hover,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] button:hover,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] button:hover,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] button:hover,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-favorite-"] button:hover {{
    color: #0787C2 !important;
    background: #DDF1FD !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-active"] button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-current"] button,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-current"] button {{
    color: #0787C2 !important;
    background: #FFFFFF !important;
    border-color: #C9E5F5 !important;
    border-radius: 12px !important;
    box-shadow: 0 5px 14px rgba(42,136,192,.10) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-active"],
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-current"] {{
    border-left-color: #F46968 !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-toggle-"] button {{
    color: #7894A6 !important;
}}

[class*="st-key-user-session-menu-"] button {{
    color: {s["sidebar_text"]} !important;
    background: rgba(255,255,255,.74) !important;
    border-color: {s["sidebar_line"]} !important;
}}

/* Reference-like white cards: larger radius, blue-gray line, soft floating shadow. */
.stMetric,
[data-testid="stMetric"],
[data-testid="metric-container"],
.stPlotlyChart,
div[data-testid="stExpander"],
.ws-tracker-shell,
.ws-page-toolbar,
[class*="st-key-ws-page-toolbar"],
[data-testid="stDataFrame"] [data-testid="stDataFrameResizable"],
div[data-testid="stTable"],
[class*="st-key-theme-center-card-"] {{
    background: #FFFFFF !important;
    border: 1px solid #CFE7F6 !important;
    border-radius: 20px !important;
    box-shadow: 0 8px 24px rgba(42,136,192,.09) !important;
}}

.stMetric:hover,
[data-testid="stMetric"]:hover,
.stPlotlyChart:hover,
div[data-testid="stExpander"]:hover {{
    border-color: #9FD2EF !important;
    box-shadow: 0 12px 28px rgba(42,136,192,.13) !important;
}}

[data-testid="stMetricLabel"] p,
[data-testid="stCaptionContainer"],
.main p,
.main label {{
    color: #6F8798 !important;
}}

[data-testid="stMetricValue"] {{
    color: #176A9E !important;
}}

.main .block-container h1,
.main .block-container h2,
.main .block-container h3,
.main .block-container h4 {{
    color: #29465B !important;
}}

/* A small red nose marker gives page titles the missing character accent. */
.main .block-container h3:first-of-type::before {{
    display: inline-block;
    width: 10px;
    height: 10px;
    margin: 0 9px 1px 0;
    background: #F46968;
    border-radius: 50%;
    box-shadow: 0 0 0 4px rgba(244,105,104,.12);
    content: "";
}}

.stButton > button,
.stDownloadButton > button,
[data-testid="stFormSubmitButton"] > button {{
    border-radius: 14px !important;
}}

button[kind="primary"],
[data-testid="stFormSubmitButton"] > button {{
    color: #FFFFFF !important;
    background: #11A9EE !important;
    border-color: #11A9EE !important;
    box-shadow: 0 7px 16px rgba(17,169,238,.20) !important;
}}

button[kind="primary"]:hover,
[data-testid="stFormSubmitButton"] > button:hover {{
    background: #0799DC !important;
    border-color: #0799DC !important;
}}

[data-baseweb="select"] > div,
.stTextInput [data-baseweb="input"],
.stTextInput [data-baseweb="base-input"],
.stTextInput input,
.stNumberInput input,
.stDateInput input,
.stTextArea textarea,
[data-testid="stFileUploaderDropzone"] {{
    background: #FFFFFF !important;
    border-color: #CFE7F6 !important;
    border-radius: 13px !important;
}}

[data-baseweb="select"] > div:focus-within,
.stTextInput [data-baseweb="input"]:focus-within,
.stTextInput [data-baseweb="base-input"]:focus-within {{
    border-color: #11A9EE !important;
    box-shadow: 0 0 0 3px rgba(17,169,238,.14) !important;
}}

.stTabs [role="tablist"] {{
    padding: 4px !important;
    background: #EFF8FE !important;
    border: 1px solid #CFE7F6 !important;
    border-radius: 14px !important;
}}

.stTabs [role="tab"] {{
    border: 0 !important;
    border-radius: 10px !important;
}}

.stTabs [aria-selected="true"] {{
    color: #0787C2 !important;
    background: #FFFFFF !important;
    border: 0 !important;
    box-shadow: 0 4px 12px rgba(42,136,192,.10) !important;
}}

[data-testid="stDataFrame"] [role="columnheader"],
div[data-testid="stTable"] thead th {{
    color: #176A9E !important;
    background: #EAF6FE !important;
}}

[data-testid="stAlertContainer"] {{
    min-height: 74px !important;
    margin-top: .75rem !important;
    padding: 1rem !important;
    background: #F5FBFF !important;
    border: 1px dashed #9FD2EF !important;
    border-radius: 18px !important;
}}

[data-testid="stAlertContainer"] p {{
    color: #6F8798 !important;
}}

.ws-page-status-bar {{
    color: #5D7C91 !important;
    background: rgba(255,255,255,.96) !important;
    border-top-color: #CFE7F6 !important;
}}

.ws-page-status-bar__item--healthy {{ color: #2FA36B !important; }}
.ws-page-status-bar__item--warning {{ color: #D4A91E !important; }}

.ws-page-loading-mask {{
    color: #11A9EE !important;
    background: rgba(229,243,254,.95) !important;
}}

/* Bell-yellow is reserved for small status accents, matching the reference. */
[class*="st-key-theme-center-card-doraemon"] {{
    position: relative;
    border-top: 5px solid #11A9EE !important;
}}

[class*="st-key-theme-center-card-doraemon"]::after {{
    position: absolute;
    top: 14px;
    right: 16px;
    width: 18px;
    height: 18px;
    background: #FCCD3D;
    border: 2px solid #D4A91E;
    border-radius: 50%;
    box-shadow: inset 0 -3px 0 rgba(212,169,30,.22);
    content: "";
}}

@media (max-width: 768px) {{
    [data-testid="stMainBlockContainer"] {{
        border-radius: 18px !important;
    }}
}}
"""
