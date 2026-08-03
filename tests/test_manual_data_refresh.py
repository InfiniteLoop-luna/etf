import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.manual_data_refresh import _expand_selected_keys, run_manual_refresh


class ManualDataRefreshTests(unittest.TestCase):
    def test_expand_selected_keys_adds_dependencies_first(self):
        registry = {
            "etf_share_size": {"depends_on": []},
            "etf_category_agg": {"depends_on": ["etf_share_size"]},
            "moneyflow": {"depends_on": []},
        }

        result = _expand_selected_keys(["etf_category_agg", "moneyflow"], registry)

        self.assertEqual(result, ["etf_share_size", "etf_category_agg", "moneyflow"])

    def test_run_manual_refresh_marks_success_and_writes_status(self):
        calls = []
        registry = {
            "moneyflow": {"label": "资金流向", "runner": lambda: calls.append("moneyflow")},
            "lhb": {"label": "龙虎榜", "runner": lambda: calls.append("lhb")},
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            status_path = Path(tmpdir) / "manual_refresh_status.json"

            before_summary = {"items": [{"key": "moneyflow", "ok": False}, {"key": "lhb", "ok": False}]}
            after_summary = {"items": [{"key": "lhb", "ok": False}]}

            with patch("src.manual_data_refresh.get_refresh_registry", return_value=registry):
                with patch("src.manual_data_refresh.STATUS_PATH", status_path):
                    with patch("src.manual_data_refresh.DATA_DIR", Path(tmpdir)):
                        with patch("src.manual_data_refresh._refresh_summary_files", side_effect=lambda: calls.append("summary")):
                            with patch("src.manual_data_refresh._load_funding_freshness_summary_file", side_effect=[before_summary, after_summary]):
                                result = run_manual_refresh(["moneyflow", "lhb"])

            self.assertEqual(calls, ["moneyflow", "lhb", "summary"])
            self.assertEqual(result["status"], "success")
            self.assertEqual(result["completed_keys"], ["moneyflow", "lhb"])
            self.assertEqual(result["recovered_keys"], ["moneyflow"])
            self.assertEqual(result["remaining_stale_keys"], ["lhb"])
            payload = json.loads(status_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "success")
            self.assertEqual(payload["completed_keys"], ["moneyflow", "lhb"])
            self.assertEqual(payload["recovered_keys"], ["moneyflow"])


if __name__ == "__main__":
    unittest.main()
