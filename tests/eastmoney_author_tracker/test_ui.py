import unittest
import re
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, unquote, urlparse
from pathlib import Path

from streamlit import config as streamlit_config

from src.apple_theme import (
    APPLE_THEME_TOKENS,
    MIN_FONT_SIZE,
    SYSTEM_FONT_FAMILY,
    build_apple_plotly_template,
    build_author_tracker_apple_css,
    build_global_apple_theme_css,
    build_terminal_component_overrides_css,
    get_apple_theme_tokens,
)
from src.eastmoney_author_tracker.ui import (
    DIRECTION_COLORS,
    _format_metadata_caption,
    _format_cycle_option,
    _render_evidence_images,
    _to_cycle_display_df,
    build_cycle_detail_payload,
    build_dashboard_payload,
    build_summary_trend_df,
    render_author_tracking_tab,
)


NAME_ZX = "\u4e2d\u4fe1\u8bc1\u5238"
NAME_MY = "\u660e\u9633\u7535\u6c14"
LABEL_BUY = "\u770b\u591a"
LABEL_EXIT = "\u51fa\u8d27"
LABEL_CLOSED = "\u5df2\u5173\u95ed"
LABEL_EXITED = "\u5df2\u51fa\u8d27"
LABEL_EFFECTIVE = "\u6709\u6548"
LABEL_WATCH = "\u5f85\u89c2\u5bdf"


class TrackerUiPayloadTests(unittest.TestCase):
    def test_get_apple_theme_tokens_backfills_missing_required_keys(self):
        with patch("src.apple_theme.APPLE_THEME_TOKENS", {"bg_base": "#000000", "text_main": "#FFFFFF"}):
            tokens = get_apple_theme_tokens()

        self.assertEqual(tokens["bg_base"], "#000000")
        self.assertEqual(tokens["text_main"], "#FFFFFF")
        self.assertEqual(tokens["primary"], APPLE_THEME_TOKENS["primary"])
        self.assertEqual(tokens["primary_hover"], APPLE_THEME_TOKENS["primary_hover"])
        self.assertEqual(tokens["primary_strong"], APPLE_THEME_TOKENS["primary_strong"])

    def test_build_global_apple_theme_css_contains_terminal_core_tokens(self):
        css = build_global_apple_theme_css()

        self.assertIn("--ws-bg-base: #F7F9FF", css)
        self.assertIn("--ws-bg-surface: #FFFFFF", css)
        self.assertIn("--ws-bg-dark: #2A3138", css)
        self.assertIn("--ws-color-primary: #0F69FF", css)
        self.assertIn("--ws-color-up: #037B66", css)
        self.assertIn("--ws-color-down: #D11022", css)
        self.assertIn('[data-testid="stSidebar"]', css)
        self.assertIn('[data-testid="stDataFrame"]', css)
        self.assertIn(".stMetric", css)

    def test_global_theme_uses_system_font_stack(self):
        css = build_global_apple_theme_css()
        expected = '"Microsoft YaHei", sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", "Noto Sans", Helvetica, Arial, sans-serif'

        self.assertEqual(SYSTEM_FONT_FAMILY, expected)
        self.assertIn(f"--ws-font-sans: {expected}", css)
        self.assertIn(f"--ws-font-heading: {expected}", css)
        self.assertIn('input,\ntextarea,\nselect,', css)
        self.assertIn('font-family: var(--ws-font-sans) !important', css)

        standalone_renderers = (
            Path("src/lhb_board_component/index.html"),
            Path("src/stock_analysis_template_report.py"),
            Path("src/stock_research_html_renderer.py"),
        )
        for path in standalone_renderers:
            self.assertIn(expected, path.read_text(encoding="utf-8", errors="ignore"))

    def test_text_font_sizes_do_not_fall_below_minimum(self):
        self.assertEqual(MIN_FONT_SIZE, 14)
        css = build_global_apple_theme_css()
        self.assertIn("--ws-font-size-min: 14px", css)
        self.assertIn("font-size: max(var(--ws-font-size-min), 1em) !important", css)
        sources = [Path("app.py")]
        sources.extend(Path("src").rglob("*.py"))
        sources.extend(Path("src").rglob("*.html"))

        undersized = []
        fixed_size_pattern = re.compile(r"font-size\s*:\s*(\d*\.?\d+)\s*(px|rem)", re.IGNORECASE)
        clamp_pattern = re.compile(r"font-size\s*:\s*clamp\(\s*(\d*\.?\d+)\s*rem", re.IGNORECASE)
        plotly_font_pattern = re.compile(r"font=dict\([^\r\n]*?\bsize=(\d+)")
        for path in sources:
            source = path.read_text(encoding="utf-8", errors="ignore")
            for match in fixed_size_pattern.finditer(source):
                value = float(match.group(1))
                pixels = value if match.group(2).lower() == "px" else value * MIN_FONT_SIZE
                if 0 < pixels < MIN_FONT_SIZE:
                    undersized.append(f"{path}:{match.group(0)}")
            for match in clamp_pattern.finditer(source):
                if 0 < float(match.group(1)) < 1:
                    undersized.append(f"{path}:{match.group(0)}")
            for match in plotly_font_pattern.finditer(source):
                if 0 < int(match.group(1)) < MIN_FONT_SIZE:
                    undersized.append(f"{path}:{match.group(0)}")

        self.assertEqual(undersized, [])

    def test_all_table_surfaces_use_single_borders_and_readable_type(self):
        css = build_global_apple_theme_css()
        final_table_rules = css[css.rfind('/* Unified table surfaces */') :]

        self.assertEqual(streamlit_config.get_option("theme.baseFontSize"), 16)
        self.assertEqual(streamlit_config.get_option("theme.dataframeBorderColor"), "#D2D2D7")
        self.assertEqual(streamlit_config.get_option("theme.dataframeHeaderBackgroundColor"), "#F8F9FB")
        self.assertIn("html {", final_table_rules)
        self.assertIn("font-size: 16px !important", final_table_rules)
        self.assertIn('[data-testid="stDataFrame"] {', final_table_rules)
        self.assertIn("border: 0 !important", final_table_rules)
        self.assertIn("padding: 0 !important", final_table_rules)
        self.assertIn('[data-testid="stDataFrameResizable"] {', final_table_rules)
        self.assertIn("border: 1px solid var(--ws-border-soft) !important", final_table_rules)
        self.assertIn("border-radius: 8px !important", final_table_rules)
        self.assertIn('div[data-testid="stTable"] table', final_table_rules)
        self.assertIn("font-size: 14px !important", final_table_rules)
        self.assertIn("border-collapse: separate", final_table_rules)
        self.assertIn(".ws-fund-watchboard__holdings table", final_table_rules)

    def test_build_global_apple_theme_css_includes_primary_interaction_selectors(self):
        css = build_global_apple_theme_css()

        self.assertIn(".stButton", css)
        self.assertIn('[data-baseweb="select"]', css)
        self.assertIn(".stPlotlyChart", css)
        self.assertIn(".ws-sidebar-block", css)
        self.assertIn(".ws-sidebar-brand", css)
        self.assertIn(".ws-sidebar-recent-item", css)
        self.assertIn(".ws-page-toolbar", css)
        self.assertIn("st-key-ws-page-toolbar", css)

    def test_sidebar_tree_uses_compact_directory_hierarchy(self):
        css = build_global_apple_theme_css()

        self.assertIn("--ws-sidebar-width: 230px", css)
        self.assertIn("--ws-sidebar-row-height: 34px", css)
        self.assertIn("--ws-sidebar-row-gap: 2px", css)
        self.assertIn("--ws-sidebar-accent: #365CCB", css)
        self.assertIn("--ws-sidebar-active-bg: #E9EEF7", css)
        self.assertIn('[class*="st-key-ws-sidebar-module-"] button::before', css)
        self.assertIn('[class*="st-key-ws-sidebar-module-"] button::after', css)
        self.assertIn("width: calc(100% - 1.55rem)", css)
        self.assertIn("border-left: 1px solid var(--ws-sidebar-line)", css)
        self.assertIn(".ws-sidebar-page-description {", css)
        self.assertIn("display: none", css)
        self.assertIn('[class*="-expanded"] button', css)
        self.assertIn('[class*="-active"] button', css)

    def test_sidebar_keeps_header_and_footer_fixed_while_middle_scrolls(self):
        css = build_global_apple_theme_css()
        app_source = Path("app.py").read_text(encoding="utf-8", errors="ignore")

        self.assertIn('key="ws-sidebar-header"', app_source)
        self.assertIn('key="ws-sidebar-middle"', app_source)
        self.assertIn('key="ws-sidebar-footer"', app_source)
        self.assertIn("grid-template-rows: auto minmax(0, 1fr) auto !important", css)
        self.assertIn('[class*="st-key-ws-sidebar-middle"]', css)
        self.assertIn("overflow-y: auto !important", css)
        self.assertIn('[class*="st-key-ws-sidebar-footer"]', css)
        self.assertIn('[data-testid="stSidebar"] [data-testid="stSidebarHeader"]', css)
        self.assertIn("position: absolute !important", css)
        self.assertIn("pointer-events: auto", css)
        sidebar_content_rule = css[css.rfind('[data-testid="stSidebar"] [data-testid="stSidebarContent"] {') :]
        self.assertIn("padding-right: 0 !important", sidebar_content_rule)

    def test_sidebar_brand_uses_clean_two_row_lockup(self):
        css = build_global_apple_theme_css()
        brand_rule = css[css.rfind('[data-testid="stSidebar"] .ws-sidebar-brand {') :]

        self.assertIn("grid-template-rows: 32px 20px", brand_rule)
        self.assertIn("grid-row: 1", brand_rule)
        self.assertIn("grid-row: 2", brand_rule)
        self.assertIn("grid-column: 1 / -1", brand_rule)
        self.assertIn("padding: 0 !important", brand_rule)
        self.assertIn("white-space: nowrap", brand_rule)
        self.assertIn("text-transform: none", brand_rule)
        self.assertIn('[data-testid="stSidebar"]:not([aria-expanded="false"]) [data-testid="stSidebarCollapseButton"]', brand_rule)
        self.assertIn("right: 2px !important", brand_rule)

    def test_sidebar_favorite_stars_use_high_visibility_yellow(self):
        css = build_global_apple_theme_css()
        favorite_rule = css[css.rfind('[class*="st-key-ws-sidebar-module-favorite"] button::before') :]

        self.assertIn("background: #F5B400 !important", favorite_rule)
        self.assertIn('[class*="st-key-ws-sidebar-page-my_favorite"] button img[src$="/star.svg"]', css)
        self.assertIn("opacity: 1 !important", favorite_rule)
        self.assertIn("filter:", favorite_rule)
        self.assertIn("contrast(103%) !important", favorite_rule)

    def test_collapsed_sidebar_controls_share_one_centered_rail_grid(self):
        css = build_global_apple_theme_css()
        collapsed_rule = css[css.rfind('[data-testid="stSidebar"][aria-expanded="false"] {') :]

        self.assertIn('[data-testid="stSidebarCollapseButton"] button', collapsed_rule)
        self.assertIn("left: 6px !important", collapsed_rule)
        self.assertIn("width: 36px !important", collapsed_rule)
        self.assertIn("margin: 0 !important", collapsed_rule)
        self.assertIn("scrollbar-gutter: auto", collapsed_rule)

    def test_recent_sidebar_group_is_collapsible_and_left_aligned(self):
        css = build_global_apple_theme_css()
        app_source = Path("app.py").read_text(encoding="utf-8", errors="ignore")

        self.assertIn('"sidebar_recent_expanded"', app_source)
        self.assertIn('"ws-sidebar-recent-toggle"', app_source)
        self.assertIn('[class*="st-key-ws-sidebar-recent-toggle-"] button::after', css)
        self.assertIn('url("/app/static/icons/chevron-right.svg")', css)
        self.assertIn('[class*="st-key-ws-sidebar-recent-toggle-expanded"] button::after', css)
        self.assertIn('[class*="st-key-ws-sidebar-recent-link-"] button > div', css)
        self.assertIn("justify-content: flex-start !important", css)

    def test_sidebar_section_labels_clear_the_following_control(self):
        css = build_global_apple_theme_css()

        selector = '[data-testid="stMarkdownContainer"]:has(> .ws-sidebar-block)'
        section_label_override = css[css.index(selector) :]
        self.assertIn(selector, css)
        self.assertIn("margin-bottom: 0 !important", section_label_override)

    def test_build_global_apple_theme_css_includes_strong_legacy_overrides(self):
        css = build_global_apple_theme_css()

        self.assertIn("-webkit-text-fill-color: var(--ws-text) !important", css)
        self.assertIn("background-image: none !important", css)
        self.assertIn("-webkit-background-clip: border-box !important", css)
        self.assertIn('[data-testid="stSidebar"] [role="radiogroup"]', css)
        self.assertIn('.block-container h1 *', css)

    def test_build_global_apple_theme_css_uses_terminal_shell_structure(self):
        css = build_global_apple_theme_css()

        self.assertIn("background: var(--ws-bg-surface) !important", css)
        self.assertIn("background: var(--ws-bg-base) !important", css)
        self.assertIn("var(--ws-bg-surface)", css)
        self.assertIn("var(--ws-color-primary)", css)
        self.assertNotIn(".ws-terminal-header", css)
        self.assertNotIn(".ws-page-intro", css)
        self.assertNotIn("radial-gradient(ellipse", css)

    def test_build_global_apple_theme_css_adds_shell_finish_details(self):
        css = build_global_apple_theme_css()

        self.assertIn("--ws-ai-glow", css)
        self.assertIn(".ws-ai-signal", css)
        self.assertIn(".main .block-container", css)
        self.assertIn('[data-testid="stSidebar"] [aria-checked="true"]', css)
        self.assertIn("background: var(--ws-color-primary) !important", css)

    def test_right_side_pages_use_one_responsive_apple_panel(self):
        css = build_global_apple_theme_css()
        page_shell = css[css.rfind("/* Unified Apple page panel */") :]

        self.assertIn('[data-testid="stMain"] {', page_shell)
        self.assertIn('[data-testid="stMainBlockContainer"] {', page_shell)
        self.assertIn("max-width: 1800px !important", page_shell)
        self.assertNotIn("max-width: 1320px !important", css)
        self.assertIn("background: var(--ws-bg-base) !important", page_shell)
        self.assertIn("background: var(--ws-bg-surface) !important", page_shell)
        self.assertIn("border: 1px solid rgba(15, 23, 42, 0.06) !important", page_shell)
        self.assertIn("border-radius: 18px !important", page_shell)
        self.assertIn("height: auto !important", page_shell)
        self.assertIn("flex: 0 0 auto !important", page_shell)
        self.assertIn("box-shadow:", page_shell)
        self.assertIn("min-height: calc(100dvh - 64px) !important", page_shell)
        self.assertIn("@media (max-width: 768px)", page_shell)
        self.assertIn('.stApp:has([data-testid="stSidebar"][aria-expanded="false"]) [data-testid="stMain"]', page_shell)
        self.assertIn("margin-left: var(--ws-sidebar-collapsed-width) !important", page_shell)
        self.assertIn("width: calc(100% - var(--ws-sidebar-collapsed-width)) !important", page_shell)
        self.assertIn("border-radius: 14px !important", page_shell)

    def test_global_alerts_are_borderless_centered_page_states(self):
        css = build_global_apple_theme_css()
        final_alert_override = css[css.rfind('[data-testid="stAlertContainer"] {') :]

        self.assertIn('justify-content: center !important', final_alert_override)
        self.assertIn('background: transparent !important', final_alert_override)
        self.assertIn('border: 0 !important', final_alert_override)
        self.assertIn('box-shadow: none !important', final_alert_override)
        self.assertIn('> [data-testid^="stAlertContent"]', final_alert_override)
        self.assertIn('[data-testid="stIconMaterial"]', final_alert_override)
        self.assertIn('display: none !important', final_alert_override)
        self.assertIn('text-align: center !important', final_alert_override)

    def test_build_apple_plotly_template_uses_terminal_palette(self):
        template = build_apple_plotly_template()

        self.assertEqual(template.layout.font.family, SYSTEM_FONT_FAMILY)
        self.assertEqual(template.layout.title.font.family, SYSTEM_FONT_FAMILY)
        self.assertEqual(template.layout.font.size, MIN_FONT_SIZE)
        self.assertEqual(template.layout.xaxis.tickfont.size, MIN_FONT_SIZE)
        self.assertEqual(template.layout.paper_bgcolor, "#FFFFFF")
        self.assertEqual(template.layout.plot_bgcolor, "#FFFFFF")
        self.assertEqual(template.layout.colorway[0], "#0F69FF")
        self.assertEqual(template.layout.colorway[1], "#0052D0")
        self.assertEqual(template.layout.colorway[2], "#037B66")

    def test_terminal_component_overrides_cover_custom_watchboards(self):
        css = build_terminal_component_overrides_css()

        self.assertIn(".ws-watchboard-shell", css)
        self.assertIn(".ws-fund-watchboard", css)
        self.assertIn('[data-testid="stTextInputRootElement"]', css)
        self.assertIn('[data-testid="stTextAreaRootElement"]', css)
        self.assertIn('[data-testid="stTimeInputTimeDisplay"]', css)
        self.assertIn('.react-aria-ComboBox', css)
        self.assertIn('min-height: 96px !important', css)
        self.assertIn('background: transparent !important', css)
        self.assertIn('box-sizing: border-box !important', css)
        self.assertIn("background: #FFFFFF !important", css)
        self.assertIn("border-radius: 4px !important", css)

    def test_security_search_embeds_scope_radio_inside_keyword_shell(self):
        css = build_terminal_component_overrides_css()
        app_source = Path("app.py").read_text(encoding="utf-8", errors="ignore")
        start = app_source.index("def render_security_search_tab():")
        end = app_source.index("\ndef ", start + 1)
        search_source = app_source[start:end]

        self.assertIn('key="ws-security-searchbox"', search_source)
        self.assertIn('st.columns([2.4, 2.6]', search_source)
        self.assertGreaterEqual(search_source.count('label_visibility="collapsed"'), 2)
        self.assertIn('[class*="st-key-ws-security-searchbox"]', css)
        self.assertIn("grid-template-columns: max-content minmax(0, 1fr) !important", css)
        self.assertIn('[data-testid="stRadioOption"][data-selected="true"]', css)
        self.assertIn("border-left: 1px solid var(--ws-border-soft) !important", css)

    def test_app_py_no_longer_uses_legacy_cold_blue_theme_literals(self):
        app_source = Path("app.py").read_text(encoding="utf-8", errors="ignore")

        self.assertNotIn("Inter, PingFang SC, sans-serif", app_source)
        self.assertNotIn("rgba(248, 250, 252, 0.92)", app_source)
        self.assertNotIn("rgba(241, 245, 249, 0.58)", app_source)
        self.assertNotIn("rgba(236, 241, 247, 0.84)", app_source)
        self.assertNotIn("linear-gradient(180deg, #F8FAFC 0%, #EEF4FF 48%, #E2E8F0 100%)", app_source)

    def test_app_does_not_render_current_location_breadcrumb(self):
        app_source = Path("app.py").read_text(encoding="utf-8", errors="ignore")

        self.assertNotIn("当前位置：", app_source)

    def test_build_author_tracker_apple_css_contains_tracker_hooks(self):
        css = build_author_tracker_apple_css()

        self.assertIn(".ws-tracker-shell", css)
        self.assertIn(".ws-tracker-section", css)
        self.assertIn(".ws-evidence-gallery", css)

    def test_tracker_direction_colors_follow_terminal_semantics(self):
        self.assertEqual(DIRECTION_COLORS["bullish"], "#037B66")
        self.assertEqual(DIRECTION_COLORS["exit_signal"], "#D11022")
        self.assertEqual(DIRECTION_COLORS["neutral"], "#718096")

    def test_build_dashboard_payload_splits_cycles_and_keeps_metadata(self):
        rows = [
            {"cycle_id": "c1", "cycle_status": "active", "ts_code": "301139.SZ", "security_name": NAME_MY},
            {"cycle_id": "c2", "cycle_status": "closed", "ts_code": "600030.SH", "security_name": NAME_ZX, "total_return": 0.08},
        ]
        metadata = {"post_count": 8, "mention_count": 15, "last_mention_time": "2026-05-12 20:08:17"}

        payload = build_dashboard_payload(rows, metadata=metadata)

        self.assertEqual(len(payload["active_cycles"]), 1)
        self.assertEqual(len(payload["closed_cycles"]), 1)
        self.assertEqual(payload["metadata"]["post_count"], 8)

    def test_build_summary_trend_df_sorts_snapshots_for_charting(self):
        snapshots = [
            {"snapshot_date": "2026-05-13", "win_rate": 0.5, "avg_return": 0.08, "cycle_count": 4, "closed_count": 3},
            {"snapshot_date": "2026-05-11", "win_rate": 0.25, "avg_return": 0.03, "cycle_count": 2, "closed_count": 1},
            {"snapshot_date": "2026-05-12", "win_rate": 1 / 3, "avg_return": 0.04, "cycle_count": 3, "closed_count": 2},
        ]

        df = build_summary_trend_df(snapshots)

        self.assertEqual(df["日期"].astype(str).tolist(), ["2026-05-11", "2026-05-12", "2026-05-13"])
        self.assertAlmostEqual(float(df.iloc[-1]["胜率%"]), 50.0)
        self.assertAlmostEqual(float(df.iloc[-1]["平均收益%"]), 8.0)

    def test_format_metadata_caption_includes_ocr_status_summary(self):
        caption = _format_metadata_caption(
            {
                "last_mention_time": "2026-05-12 20:08:17",
                "last_post_time": "2026-05-12 18:00:00",
                "post_count": 8,
                "mention_count": 15,
                "pending_image_count": 3,
                "last_ocr_update_time": "2026-05-13 09:30:00",
            }
        )

        self.assertIn("待OCR：3", caption)
        self.assertIn("最近OCR更新：2026-05-13 09:30:00", caption)

    @patch("src.eastmoney_author_tracker.ui.list_author_score_snapshots", return_value=[])
    @patch(
        "src.eastmoney_author_tracker.ui.get_author_tracking_metadata",
        return_value={
            "post_count": 8,
            "mention_count": 0,
            "pending_image_count": 3,
            "ocr_processed_image_count": 5,
            "last_ocr_update_time": "2026-05-13 09:30:00",
        },
    )
    @patch("src.eastmoney_author_tracker.ui.list_cycles_with_scores", return_value=[])
    @patch("src.eastmoney_author_tracker.ui.st")
    def test_render_author_tracking_tab_keeps_ocr_status_visible_without_cycles(
        self,
        mock_st,
        _mock_cycles,
        _mock_metadata,
        _mock_snapshots,
    ):
        mock_st.columns.return_value = [MagicMock(), MagicMock(), MagicMock()]

        render_author_tracking_tab(engine=object())

        caption_values = [str(call.args[0]) for call in mock_st.caption.call_args_list if call.args]
        self.assertTrue(any("待OCR：3" in value for value in caption_values))
        self.assertTrue(any("最近OCR更新：2026-05-13 09:30:00" in value for value in caption_values))
        self.assertTrue(mock_st.info.called)

    def test_cycle_display_df_includes_clickable_name_link_and_security_name(self):
        rows = [
            {
                "cycle_id": "c1",
                "cycle_status": "active",
                "ts_code": "600030.SH",
                "security_name": NAME_ZX,
                "latest_direction": "bullish",
                "latest_reason_text": "\u7ee7\u7eed\u770b\u597d",
                "origin_post_id": 1001,
                "origin_post_guba_code": "600030",
            }
        ]

        df = _to_cycle_display_df(rows)

        security_link = str(df.iloc[0]["股票名称"])
        original_post_link = str(df.iloc[0]["作者原帖"])

        row_values = list(df.iloc[0].astype(str).values)
        self.assertIn(f"#{NAME_ZX}", security_link)
        query = parse_qs(urlparse(security_link).query).get("security_query", [""])[0]
        self.assertEqual(unquote(query), "600030.SH")
        self.assertEqual(urlparse(original_post_link)._replace(fragment="", query="").geturl(), "https://guba.eastmoney.com/news,600030,1001.html")
        self.assertIn("#原帖", original_post_link)
        self.assertIn("600030.SH", row_values)

    def test_format_cycle_option_prefers_security_name(self):
        label = _format_cycle_option(
            {
                "ts_code": "600030.SH",
                "security_name": NAME_ZX,
                "cycle_status": "active",
                "cycle_open_time": "2026-05-08 14:57:43",
            }
        )

        self.assertIn(NAME_ZX, label)
        self.assertIn("600030.SH", label)

    def test_build_cycle_detail_payload_builds_timeline_and_markers(self):
        cycle_row = {
            "cycle_id": "600030-20260508145743-1",
            "ts_code": "600030.SH",
            "cycle_status": "closed",
            "cycle_open_time": "2026-05-08 14:57:43",
            "cycle_close_time": "2026-05-12 10:00:00",
            "close_reason": "explicit_exit",
            "total_return": 0.12,
            "benchmark_return": 0.05,
            "excess_return": 0.07,
            "max_drawdown": -0.03,
            "hold_days": 2,
            "event_count": 3,
            "latest_mention_time": "2026-05-12 10:00:00",
            "latest_direction": "exit_signal",
            "latest_source_type": "author_reply",
            "latest_reason_text": "\u4eca\u5929\u5148\u51fa\u8d27\u3002",
            "exit_quality_2d": True,
            "exit_quality_5d": True,
            "exit_quality_10d": False,
            "exit_quality_20d": True,
        }
        event_rows = [
            {
                "event_sequence": 1,
                "mention_time": "2026-05-08 14:57:43",
                "source_type": "stockbar",
                "direction": "bullish",
                "confidence_score": 0.99,
                "reason_text": "\u9996\u6b21\u63d0\u53ca",
                "target_text": "12.5",
                "post_title": "\u770b\u597d 600030",
                "post_content": "\u5148\u770b\u4e00\u6ce2\u3002",
                "reply_text": None,
            },
            {
                "event_sequence": 2,
                "mention_time": "2026-05-09 09:35:00",
                "source_type": "author_reply",
                "direction": "trim_signal",
                "confidence_score": 0.88,
                "reason_text": "\u5148\u51cf\u4e00\u70b9",
                "target_text": None,
                "post_title": "\u770b\u597d 600030",
                "post_content": "\u5148\u770b\u4e00\u6ce2\u3002",
                "reply_text": "\u5148\u51cf\u4e00\u70b9",
            },
            {
                "event_sequence": 3,
                "mention_time": "2026-05-12 10:00:00",
                "source_type": "author_reply",
                "direction": "exit_signal",
                "confidence_score": 0.91,
                "reason_text": "\u4eca\u5929\u5148\u51fa\u8d27\u3002",
                "target_text": None,
                "post_title": "\u770b\u597d 600030",
                "post_content": "\u5148\u770b\u4e00\u6ce2\u3002",
                "reply_text": "\u4eca\u5929\u5148\u51fa\u8d27\u3002",
            },
        ]
        price_rows = [
            {"trade_date": "2026-05-08", "close": 10.0},
            {"trade_date": "2026-05-09", "close": 10.8},
            {"trade_date": "2026-05-12", "close": 11.2},
        ]

        payload = build_cycle_detail_payload(cycle_row, event_rows, price_rows)

        self.assertEqual(payload["overview"]["status_label"], LABEL_CLOSED)
        self.assertEqual(payload["overview"]["latest_stance_label"], LABEL_EXITED)
        self.assertEqual(payload["overview"]["event_count"], 3)
        self.assertEqual(payload["overview"]["exit_quality_5d_label"], LABEL_EFFECTIVE)
        self.assertEqual(payload["overview"]["exit_quality_10d_label"], LABEL_WATCH)
        self.assertAlmostEqual(payload["overview"]["benchmark_return_pct"], 5.0)
        self.assertAlmostEqual(payload["overview"]["excess_return_pct"], 7.0)
        self.assertIn(LABEL_BUY, payload["event_df"].iloc[0].astype(str).tolist())
        self.assertIn(LABEL_EXIT, payload["event_df"].iloc[-1].astype(str).tolist())
        self.assertIn(LABEL_EXIT, payload["marker_df"].iloc[-1].astype(str).tolist())
        self.assertIn("2026-05-12", payload["marker_df"].iloc[-1].astype(str).tolist())

    def test_build_cycle_detail_payload_keeps_override_state_for_manual_review(self):
        cycle_row = {
            "cycle_id": "c1",
            "ts_code": "600030.SH",
            "cycle_status": "active",
            "cycle_open_time": "2026-05-08 14:57:43",
            "cycle_close_time": None,
            "latest_direction": "bullish",
        }
        event_rows = [
            {
                "mention_id": "m1",
                "event_sequence": 1,
                "mention_time": "2026-05-08 14:57:43",
                "source_type": "stockbar",
                "direction": "bullish",
                "confidence_score": 0.99,
                "reason_text": "\u9996\u6b21\u63d0\u53ca",
                "target_text": None,
                "post_title": "\u770b\u597d 600030",
                "post_content": "\u5148\u770b\u4e00\u6ce2\u3002",
                "reply_text": None,
                "override_ts_code": "000001.SZ",
                "override_direction": "bearish",
                "is_excluded": 0,
                "force_new_cycle": 1,
                "override_note": "Manual override note",
            }
        ]

        payload = build_cycle_detail_payload(cycle_row, event_rows, [])

        self.assertEqual(payload["evidence_items"][0]["mention_id"], "m1")
        self.assertEqual(payload["evidence_items"][0]["override_ts_code"], "000001.SZ")
        self.assertEqual(payload["evidence_items"][0]["override_direction"], "bearish")
        self.assertEqual(payload["evidence_items"][0]["override_note"], "Manual override note")
        self.assertIs(payload["evidence_items"][0]["force_new_cycle"], True)

    def test_build_cycle_detail_payload_includes_post_images_for_ocr_evidence(self):
        cycle_row = {
            "cycle_id": "c1",
            "ts_code": "600030.SH",
            "cycle_status": "active",
            "cycle_open_time": "2026-05-08 14:57:43",
            "cycle_close_time": None,
            "latest_direction": "bullish",
        }
        event_rows = [
            {
                "mention_id": "m1",
                "event_sequence": 1,
                "mention_time": "2026-05-08 14:57:43",
                "source_type": "image_ocr",
                "direction": "bullish",
                "confidence_score": 0.7,
                "reason_text": "继续看好 600030",
                "target_text": None,
                "post_title": "图片观点",
                "post_content": "正文见图",
                "reply_text": None,
                "post_pic_url_json": '["https://example.com/0.png", "https://example.com/1.png"]',
                "evidence_payload_json": '{"image_index": 1}',
            }
        ]

        payload = build_cycle_detail_payload(cycle_row, event_rows, [])

        self.assertEqual(payload["evidence_items"][0]["image_urls"], ["https://example.com/0.png", "https://example.com/1.png"])
        self.assertEqual(payload["evidence_items"][0]["image_index"], 1)
        self.assertEqual(payload["evidence_items"][0]["primary_image_url"], "https://example.com/1.png")

    @patch("src.eastmoney_author_tracker.ui.st")
    def test_render_evidence_images_uses_streamlit_image(self, mock_st):
        _render_evidence_images(
            {
                "image_urls": ["https://example.com/0.png", "https://example.com/1.png"],
                "primary_image_url": "https://example.com/1.png",
                "source_label": "图片OCR",
            }
        )

        self.assertEqual(len(mock_st.image.call_args_list), 2)
        self.assertEqual(mock_st.image.call_args_list[0].args[0], "https://example.com/0.png")
        self.assertEqual(mock_st.image.call_args_list[1].args[0], "https://example.com/1.png")


if __name__ == "__main__":
    unittest.main()
