import unittest
from urllib.parse import unquote, urlparse, parse_qs

from src.eastmoney_author_tracker.ui import build_cycle_detail_payload, build_dashboard_payload, _to_cycle_display_df, _format_cycle_option


class TrackerUiPayloadTests(unittest.TestCase):
    def test_build_dashboard_payload_splits_cycles_and_keeps_metadata(self):
        rows = [
            {"cycle_id": "c1", "cycle_status": "active", "ts_code": "301139.SZ", "security_name": "明阳电气"},
            {"cycle_id": "c2", "cycle_status": "closed", "ts_code": "600030.SH", "security_name": "中信证券", "total_return": 0.08},
        ]
        metadata = {"post_count": 8, "mention_count": 15, "last_mention_time": "2026-05-12 20:08:17"}

        payload = build_dashboard_payload(rows, metadata=metadata)

        self.assertEqual(len(payload["active_cycles"]), 1)
        self.assertEqual(len(payload["closed_cycles"]), 1)
        self.assertEqual(payload["metadata"]["post_count"], 8)

    def test_cycle_display_df_includes_clickable_name_link_and_security_name(self):
        rows = [
            {
                "cycle_id": "c1",
                "cycle_status": "active",
                "ts_code": "600030.SH",
                "security_name": "中信证券",
                "latest_direction": "bullish",
                "latest_reason_text": "继续看好",
            }
        ]

        df = _to_cycle_display_df(rows)

        self.assertEqual(df.iloc[0]["代码"], "600030.SH")
        link = df.iloc[0]["股票名称"]
        self.assertIn("#中信证券", link)
        query = parse_qs(urlparse(link).query).get("security_query", [""])[0]
        self.assertEqual(unquote(query), "600030.SH")

    def test_format_cycle_option_prefers_security_name(self):
        label = _format_cycle_option(
            {
                "ts_code": "600030.SH",
                "security_name": "中信证券",
                "cycle_status": "active",
                "cycle_open_time": "2026-05-08 14:57:43",
            }
        )

        self.assertIn("中信证券（600030.SH）", label)

    def test_build_cycle_detail_payload_builds_timeline_and_markers(self):
        cycle_row = {
            "cycle_id": "600030-20260508145743-1",
            "ts_code": "600030.SH",
            "cycle_status": "closed",
            "cycle_open_time": "2026-05-08 14:57:43",
            "cycle_close_time": "2026-05-12 10:00:00",
            "close_reason": "explicit_exit",
            "total_return": 0.12,
            "max_drawdown": -0.03,
            "hold_days": 2,
            "event_count": 3,
            "latest_mention_time": "2026-05-12 10:00:00",
            "latest_direction": "exit_signal",
            "latest_source_type": "author_reply",
            "latest_reason_text": "今天先出货。",
            "exit_quality_2d": True,
        }
        event_rows = [
            {
                "event_sequence": 1,
                "mention_time": "2026-05-08 14:57:43",
                "source_type": "stockbar",
                "direction": "bullish",
                "confidence_score": 0.99,
                "reason_text": "首次提及",
                "target_text": "12.5",
                "post_title": "看好 600030",
                "post_content": "先看一波。",
                "reply_text": None,
            },
            {
                "event_sequence": 2,
                "mention_time": "2026-05-09 09:35:00",
                "source_type": "author_reply",
                "direction": "trim_signal",
                "confidence_score": 0.88,
                "reason_text": "先减一点",
                "target_text": None,
                "post_title": "看好 600030",
                "post_content": "先看一波。",
                "reply_text": "先减一点",
            },
            {
                "event_sequence": 3,
                "mention_time": "2026-05-12 10:00:00",
                "source_type": "author_reply",
                "direction": "exit_signal",
                "confidence_score": 0.91,
                "reason_text": "今天先出货。",
                "target_text": None,
                "post_title": "看好 600030",
                "post_content": "先看一波。",
                "reply_text": "今天先出货。",
            },
        ]
        price_rows = [
            {"trade_date": "2026-05-08", "close": 10.0},
            {"trade_date": "2026-05-09", "close": 10.8},
            {"trade_date": "2026-05-12", "close": 11.2},
        ]

        payload = build_cycle_detail_payload(cycle_row, event_rows, price_rows)

        self.assertEqual(payload["overview"]["status_label"], "已关闭")
        self.assertEqual(payload["overview"]["latest_stance_label"], "已出货")
        self.assertEqual(payload["overview"]["event_count"], 3)
        self.assertEqual(payload["event_df"].iloc[0]["动作"], "看多")
        self.assertEqual(payload["event_df"].iloc[-1]["动作"], "出货")
        self.assertEqual(payload["marker_df"].iloc[-1]["动作"], "出货")
        self.assertEqual(payload["marker_df"].iloc[-1]["日期"], "2026-05-12")


if __name__ == "__main__":
    unittest.main()
