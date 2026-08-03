import unittest
from unittest.mock import patch

import pandas as pd

from app import _render_manual_refresh_status


class ManualRefreshStatusRenderTests(unittest.TestCase):
    def test_render_manual_refresh_status_shows_failed_rows(self):
        captured_frames = []
        metric_calls = []
        warning_calls = []
        info_calls = []
        success_calls = []
        caption_calls = []
        markdown_calls = []
        expander_calls = []

        class FakeColumn:
            def metric(self, label, value):
                metric_calls.append((label, value))

        payload = {
            "status": "partial_failed",
            "selected_keys": ["moneyflow", "lhb"],
            "completed_keys": ["moneyflow"],
            "recovered_keys": ["moneyflow"],
            "remaining_stale_keys": ["lhb"],
            "failed_keys": [{"key": "lhb", "error": "rate limit"}],
            "current_key": None,
            "started_at": "2026-08-03T11:00:00",
            "finished_at": "2026-08-03T11:05:00",
            "message": "manual refresh finished",
        }
        registry = {
            "moneyflow": {"label": "资金流向"},
            "lhb": {"label": "龙虎榜"},
        }

        class FakeContext:
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc, tb):
                return False

        with patch("app.st.columns", side_effect=[[FakeColumn(), FakeColumn(), FakeColumn(), FakeColumn(), FakeColumn()], [FakeContext(), FakeContext()]]):
            with patch("app.st.dataframe", side_effect=lambda df, **kwargs: captured_frames.append(df.copy())):
                with patch("app.st.warning", side_effect=lambda msg: warning_calls.append(msg)):
                    with patch("app.st.success", side_effect=lambda msg: success_calls.append(msg)):
                        with patch("app.st.info", side_effect=lambda msg: info_calls.append(msg)):
                            with patch("app.st.caption", side_effect=lambda msg: caption_calls.append(msg)):
                                with patch("app.st.markdown", side_effect=lambda msg: markdown_calls.append(msg)):
                                    with patch("app.st.expander", side_effect=lambda *args, **kwargs: expander_calls.append((args, kwargs)) or FakeContext()):
                                        _render_manual_refresh_status(payload, registry)

        self.assertEqual(metric_calls[0], ("状态", "partial_failed"))
        self.assertEqual(metric_calls[3], ("恢复项数", "1"))
        self.assertEqual(metric_calls[4], ("剩余滞后项", "1"))
        self.assertEqual(len(captured_frames), 4)
        self.assertIsInstance(captured_frames[0], pd.DataFrame)
        self.assertIn("项目", captured_frames[0].columns)
        self.assertIn("值", captured_frames[0].columns)
        self.assertIn("项目", captured_frames[1].columns)
        self.assertIn("本次恢复", captured_frames[1]["项目"].tolist())
        self.assertIn("失败链路", captured_frames[2].columns)
        self.assertIn("错误摘要", captured_frames[2].columns)
        self.assertEqual(captured_frames[2].iloc[0]["错误摘要"], "rate limit")
        self.assertIn("完整错误", captured_frames[3].columns)
        self.assertEqual(captured_frames[3].iloc[0]["完整错误"], "rate limit")
        self.assertTrue(warning_calls)
        self.assertEqual(success_calls, [])
        self.assertEqual(info_calls, [])
        self.assertEqual(caption_calls, [])
        self.assertTrue(markdown_calls)
        self.assertTrue(expander_calls)


if __name__ == "__main__":
    unittest.main()
