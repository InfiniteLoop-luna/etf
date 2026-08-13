from __future__ import annotations


DORAEMON_THEME_TOKENS: dict[str, str] = {
    "bg_base": "#F5FAFF",           # 浅蓝页底
    "bg_surface": "#FFFFFF",         # 纯白背景
    "bg_dark": "#1D2D44",            # 深蓝容器
    "surface_soft": "#F0F7FF",       # 柔蓝表面
    "surface_alt": "#E8F4FF",        # 替代表面
    "surface_dark_alt": "#253A54",   # 深蓝替代
    "primary": "#4DB7FF",            # 机器猫蓝
    "primary_hover": "#3BA8F0",      # 蓝色悬停
    "primary_press": "#2E99E0",      # 蓝色按下
    "primary_strong": "#4DB7FF",     # 强调蓝
    "primary_soft": "#E0F2FF",       # 淡蓝底
    "secondary": "#FF6B6B",          # 暖心红（辅助色）
    "text_main": "#1D2D44",          # 主要文字
    "text_muted": "#6B7C93",         # 次要文字
    "text_soft": "#A5B3C2",          # 柔和文字
    "text_inverse": "#FFFFFF",       # 反色文字
    "border_soft": "#D0E3F5",        # 柔蓝边框
    "border_strong": "#A0BFD8",      # 强蓝边框
    "shadow": "0 2px 12px rgba(77, 183, 255, 0.08)",
    "shadow_hover": "0 4px 20px rgba(77, 183, 255, 0.15)",
    "ai_glow": "0 0 20px rgba(77, 183, 255, 0.12)",
    "color_up": "#248A3D",           # 涨（绿）
    "color_down": "#D70015",         # 跌（红）
    "color_warn": "#E6A817",         # 铃铛黄
    "color_neutral": "#6B7C93",      # 中性灰蓝
    "color_accent_alt": "#E7F6FF",   # 清新柔和蓝
    "radius_lg": "18px",
    "radius_md": "12px",
    "radius_sm": "8px",
    "max_width": "1280px",
}

DORAEMON_SIDEBAR_TOKENS: dict[str, str] = {
    "sidebar_bg": "linear-gradient(180deg, #4DB7FF 0%, #7ECBFF 50%, #B8E2FF 100%)",
    "sidebar_active_bg": "rgba(255, 255, 255, 0.35)",
    "sidebar_hover_bg": "rgba(255, 255, 255, 0.22)",
    "sidebar_accent": "#FFFFFF",
    "sidebar_accent_hover": "#FFFFFF",
    "sidebar_text": "#FFFFFF",
    "sidebar_line": "rgba(255, 255, 255, 0.25)",
}


def build_doraemon_extra_css() -> str:
    """Return CSS overrides specific to the Doraemon theme.

    This CSS is appended **after** the base global theme CSS, so it can
    override sidebar colors, the brand area, and other theme-specific
    elements without touching the shared layout rules.
    """
    s = DORAEMON_SIDEBAR_TOKENS
    return f"""
/* ── Doraemon Theme Overrides ────────────────────────────────── */

/* Root variable overrides for sidebar */
:root {{
    --ws-sidebar-bg: {s["sidebar_bg"]};
    --ws-sidebar-active-bg: {s["sidebar_active_bg"]};
    --ws-sidebar-hover-bg: {s["sidebar_hover_bg"]};
    --ws-sidebar-accent: {s["sidebar_accent"]};
    --ws-sidebar-accent-hover: {s["sidebar_accent_hover"]};
    --ws-sidebar-text: {s["sidebar_text"]};
    --ws-sidebar-line: {s["sidebar_line"]};
}}

/* Sidebar background gradient */
[data-testid="stSidebar"] {{
    background: {s["sidebar_bg"]} !important;
}}

[data-testid="stSidebar"] > div:first-child,
[data-testid="stSidebar"] [data-testid="stSidebarContent"] {{
    background: transparent !important;
}}

/* Sidebar text white on gradient */
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] label {{
    color: {s["sidebar_text"]} !important;
}}

/* Brand kicker — white icon badge on blue */
[data-testid="stSidebar"] .ws-sidebar-brand-kicker {{
    color: #4DB7FF !important;
    background: rgba(255, 255, 255, 0.92) !important;
}}

/* Brand name — white */
[data-testid="stSidebar"] .ws-sidebar-brand h2 {{
    color: {s["sidebar_text"]} !important;
}}

/* Brand subtitle — translucent white */
[data-testid="stSidebar"] .ws-sidebar-brand p {{
    color: rgba(255, 255, 255, 0.78) !important;
}}

/* Brand border uses translucent white */
[data-testid="stSidebar"] .ws-sidebar-brand {{
    border-bottom-color: {s["sidebar_line"]} !important;
}}

/* Block titles — translucent white */
[data-testid="stSidebar"] .ws-sidebar-block-title {{
    color: rgba(255, 255, 255, 0.78) !important;
}}

/* Account section border */
[data-testid="stSidebar"] .ws-sidebar-block--account {{
    border-top-color: {s["sidebar_line"]} !important;
}}

/* Module buttons — white text */
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] button {{
    color: {s["sidebar_text"]} !important;
}}

/* Page buttons — translucent white text */
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] button {{
    color: rgba(255, 255, 255, 0.88) !important;
    border-left-color: {s["sidebar_line"]} !important;
}}

/* Active page — stronger white */
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-active-"] button {{
    color: {s["sidebar_text"]} !important;
    background: {s["sidebar_active_bg"]} !important;
    border-color: {s["sidebar_active_bg"]} !important;
    border-radius: var(--ws-radius-md) !important;
}}

/* Active module — slightly highlighted */
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-current-"] button {{
    background: {s["sidebar_active_bg"]} !important;
    border-radius: var(--ws-radius-md) !important;
}}

/* Hover states on all sidebar buttons */
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] button:hover,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] button:hover,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] button:hover,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] button:hover,
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-favorite-"] button:hover {{
    color: {s["sidebar_text"]} !important;
    background: {s["sidebar_hover_bg"]} !important;
}}

/* Recent toggle — translucent white text */
[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-toggle-"] button {{
    color: rgba(255, 255, 255, 0.78) !important;
}}

[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-toggle-"] button:hover {{
    color: {s["sidebar_text"]} !important;
    background: {s["sidebar_hover_bg"]} !important;
}}

/* Recent visits labels */
[data-testid="stSidebar"] .ws-sidebar-recent-module {{
    color: rgba(255, 255, 255, 0.88) !important;
}}

[data-testid="stSidebar"] .ws-sidebar-recent-page {{
    color: {s["sidebar_text"]} !important;
}}

/* Search input — translucent white background */
[data-testid="stSidebar"] [data-testid="stTextInputRootElement"] {{
    background: rgba(255, 255, 255, 0.18) !important;
    border-color: rgba(255, 255, 255, 0.3) !important;
}}

[data-testid="stSidebar"] [data-testid="stTextInputRootElement"]:focus-within {{
    background: rgba(255, 255, 255, 0.28) !important;
    border-color: rgba(255, 255, 255, 0.5) !important;
}}

[data-testid="stSidebar"] [data-testid="stTextInputRootElement"] input {{
    color: {s["sidebar_text"]} !important;
}}

[data-testid="stSidebar"] [data-testid="stTextInputRootElement"] input::placeholder {{
    color: rgba(255, 255, 255, 0.55) !important;
}}

/* Search result meta */
[data-testid="stSidebar"] .ws-sidebar-search-result-meta {{
    color: rgba(255, 255, 255, 0.65) !important;
}}

/* Empty state */
[data-testid="stSidebar"] .ws-sidebar-empty {{
    color: rgba(255, 255, 255, 0.7) !important;
    background: rgba(255, 255, 255, 0.1) !important;
    border-color: rgba(255, 255, 255, 0.25) !important;
}}

/* Sidebar recent items */
[data-testid="stSidebar"] .ws-sidebar-recent-item {{
    background: rgba(255, 255, 255, 0.12) !important;
    border-color: rgba(255, 255, 255, 0.2) !important;
}}

/* User session menu buttons */
[class*="st-key-user-session-menu-"] button {{
    color: {s["sidebar_text"]} !important;
    border-color: {s["sidebar_line"]} !important;
}}

[class*="st-key-user-session-menu-"] button:hover {{
    color: {s["sidebar_text"]} !important;
    background: {s["sidebar_hover_bg"]} !important;
}}

/* Collapse/expand sidebar button */
[data-testid="stSidebarCollapseButton"],
[data-testid="stSidebarCollapseButton"] button,
button[aria-label="Collapse sidebar"],
button[aria-label="Close sidebar"] {{
    color: {s["sidebar_text"]} !important;
}}

[data-testid="stSidebarCollapseButton"]:hover,
[data-testid="stSidebarCollapseButton"] button:hover,
button[aria-label="Collapse sidebar"]:hover,
button[aria-label="Close sidebar"]:hover {{
    color: {s["sidebar_text"]} !important;
    background: {s["sidebar_hover_bg"]} !important;
}}

/* Radio group in sidebar */
[data-testid="stSidebar"] [role="radiogroup"] {{
    background: rgba(255, 255, 255, 0.15) !important;
    border-color: rgba(255, 255, 255, 0.25) !important;
}}

/* Status bar — light blue tint */
.ws-page-status-bar {{
    background: #F5FAFF !important;
    border-top-color: #D0E3F5 !important;
}}

/* Loading mask — match page background */
.ws-page-loading-mask {{
    background: rgba(245, 250, 255, 0.94) !important;
    color: #4DB7FF !important;
}}

"""
