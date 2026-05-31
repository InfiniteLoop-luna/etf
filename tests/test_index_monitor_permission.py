import unittest
from unittest.mock import patch

import pandas as pd

import app


class IndexMonitorPermissionTests(unittest.TestCase):
    def test_index_monitor_edit_password_falls_back_to_etf_edit_password(self):
        with patch.dict(app.os.environ, {"ETF_EDIT_PASSWORD": "spark2006"}, clear=False):
            with patch.object(app.st, "secrets", {}, create=True):
                self.assertEqual(app.get_index_monitor_edit_password(), "spark2006")

    def test_grant_index_monitor_edit_permission_sets_session_flag(self):
        session_state = {}
        with patch.dict(app.os.environ, {"ETF_EDIT_PASSWORD": "spark2006"}, clear=False):
            with patch.object(app.st, "secrets", {}, create=True):
                with patch.object(app.st, "session_state", session_state, create=True):
                    self.assertTrue(app.grant_index_monitor_edit_permission("spark2006"))
                    self.assertTrue(session_state["index_monitor_edit_authorized"])
                    self.assertTrue(app.has_index_monitor_edit_permission())

    def test_clear_index_monitor_edit_permission_resets_open_flags(self):
        session_state = {
            "index_monitor_edit_authorized": True,
            "index_manual_month_open": True,
            "index_single_edit_open": True,
            "index_import_open": True,
        }
        with patch.object(app.st, "session_state", session_state, create=True):
            app.clear_index_monitor_edit_permission()

        self.assertFalse(session_state["index_monitor_edit_authorized"])
        self.assertFalse(session_state["index_manual_month_open"])
        self.assertFalse(session_state["index_single_edit_open"])
        self.assertFalse(session_state["index_import_open"])

    def test_collect_index_batch_rows_skips_blank_index_names(self):
        editor_df = pd.DataFrame([
            {"index_name": "沪深300", "close_price": 10, "change_pct": None},
            {"index_name": "", "close_price": 20, "change_pct": 1.2},
        ])

        rows = app._collect_index_batch_rows(editor_df, "2026-05")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["index_name"], "沪深300")
        self.assertEqual(rows[0]["month"], "2026-05")


if __name__ == "__main__":
    unittest.main()
