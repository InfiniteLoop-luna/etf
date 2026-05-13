import unittest
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, unquote, urlparse
from pathlib import Path

from src.apple_theme import (
    build_apple_plotly_template,
    build_author_tracker_apple_css,
    build_global_apple_theme_css,
)
from src.eastmoney_author_tracker.ui import (
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
    def test_build_global_apple_theme_css_contains_core_shell_tokens(self):
        css = build_global_apple_theme_css()

        self.assertIn("--ws-bg", css)
        self.assertIn('[data-testid="stSidebar"]', css)
        self.assertIn('[data-testid="stDataFrame"]', css)
        self.assertIn(".stMetric", css)

    def test_build_global_apple_theme_css_includes_primary_interaction_selectors(self):
        css = build_global_apple_theme_css()

        self.assertIn(".stButton", css)
        self.assertIn('[data-baseweb="select"]', css)
        self.assertIn(".stPlotlyChart", css)

    def test_build_global_apple_theme_css_includes_strong_legacy_overrides(self):
        css = build_global_apple_theme_css()

        self.assertIn("-webkit-text-fill-color: var(--ws-text) !important", css)
        self.assertIn("background-image: none !important", css)
        self.assertIn("-webkit-background-clip: border-box !important", css)
        self.assertIn('[data-testid="stSidebar"] [role="radiogroup"]', css)
        self.assertIn('.block-container h1 *', css)

    def test_build_global_apple_theme_css_uses_layered_glass_surfaces(self):
        css = build_global_apple_theme_css()

        self.assertIn("--ws-bg-deep", css)
        self.assertIn("--ws-surface-glass", css)
        self.assertIn("--ws-surface-tint", css)
        self.assertIn("--ws-surface-titanium", css)
        self.assertIn("--ws-surface-shell", css)
        self.assertIn("backdrop-filter: blur(28px)", css)
        self.assertIn("var(--ws-surface-glass)", css)

    def test_build_global_apple_theme_css_adds_shell_finish_details(self):
        css = build_global_apple_theme_css()

        self.assertIn("--ws-shell-highlight", css)
        self.assertIn("--ws-shell-shadow-edge", css)
        self.assertIn(".main .block-container::before", css)
        self.assertIn(".main .block-container::after", css)
        self.assertIn('[data-testid="stSidebar"] > div:first-child::before', css)

    def test_build_apple_plotly_template_uses_light_backgrounds(self):
        template = build_apple_plotly_template()

        self.assertEqual(template.layout.paper_bgcolor, "#F5F5F7")
        self.assertEqual(template.layout.plot_bgcolor, "#FFFFFF")

    def test_app_py_no_longer_uses_legacy_plot_background_literals(self):
        app_source = Path("app.py").read_text(encoding="utf-8", errors="ignore")

        self.assertNotIn("rgba(248, 250, 252, 0.92)", app_source)
        self.assertNotIn("rgba(241, 245, 249, 0.58)", app_source)

    def test_build_author_tracker_apple_css_contains_tracker_hooks(self):
        css = build_author_tracker_apple_css()

        self.assertIn(".ws-tracker-shell", css)
        self.assertIn(".ws-tracker-section", css)
        self.assertIn(".ws-evidence-gallery", css)

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
            }
        ]

        df = _to_cycle_display_df(rows)

        row_values = list(df.iloc[0].astype(str).values)
        link = next(value for value in row_values if value.startswith("?security_query="))
        self.assertIn(f"#{NAME_ZX}", link)
        query = parse_qs(urlparse(link).query).get("security_query", [""])[0]
        self.assertEqual(unquote(query), "600030.SH")
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
