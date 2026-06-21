import os
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from src.sync_tushare_security_data import (
    DATASET_TABLES,
    NORMALIZED_VIEW_SPECS,
    build_stock_basic_change_summary,
    fetch_stock_holdertrade,
    prepare_records,
    purge_delisted_stock_history,
    record_stock_change_log,
)


class FakeResult:
    def __init__(self, rows=None, scalar_value=None, rowcount=0):
        self._rows = rows or []
        self._scalar_value = scalar_value
        self.rowcount = rowcount

    def fetchall(self):
        return self._rows

    def scalar(self):
        return self._scalar_value


class FakeConnection:
    def __init__(self, scripted_results):
        self.scripted_results = list(scripted_results)
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((str(sql), params))
        if not self.scripted_results:
            raise AssertionError("Unexpected execute call with no scripted result left")
        return self.scripted_results.pop(0)


class FakeBeginContext:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeEngine:
    def __init__(self, conn):
        self.conn = conn

    def begin(self):
        return FakeBeginContext(self.conn)


class StockSyncChangeLogTests(unittest.TestCase):
    def test_stock_holdertrade_dataset_is_registered_for_landing_and_view(self):
        self.assertEqual(DATASET_TABLES["stock_holdertrade"], "ts_stock_holdertrade")
        view_spec = NORMALIZED_VIEW_SPECS["stock_holdertrade"]
        self.assertEqual(view_spec["view_name"], "vw_ts_stock_holdertrade")
        self.assertIn(("numeric", "change_vol"), view_spec["columns"])
        self.assertIn(("numeric", "after_ratio"), view_spec["columns"])

    def test_stock_holdertrade_records_use_stable_business_key(self):
        df = pd.DataFrame(
            [
                {
                    "ts_code": "000733.SZ",
                    "ann_date": "20250424",
                    "holder_name": "中国振华电子集团有限公司",
                    "holder_type": "C",
                    "in_de": "IN",
                    "change_vol": 400400.0,
                    "after_share": 171837944.0,
                    "after_ratio": 31.0307,
                }
            ]
        )

        prepared = prepare_records("stock_holdertrade", df)

        self.assertEqual(len(prepared), 1)
        self.assertEqual(prepared.iloc[0]["dataset_name"], "stock_holdertrade")
        self.assertIn("stock_holdertrade|000733.SZ|20250424", prepared.iloc[0]["business_key"])
        self.assertEqual(str(prepared.iloc[0]["ann_date"]), "2025-04-24")

    def test_fetch_stock_holdertrade_splits_by_trade_type(self):
        class FakePro:
            def __init__(self):
                self.calls = []

            def stk_holdertrade(self, **kwargs):
                self.calls.append(kwargs)
                return pd.DataFrame(
                    [
                        {
                            "ts_code": "000733.SZ",
                            "ann_date": kwargs["start_date"],
                            "holder_name": f"holder-{kwargs['trade_type']}",
                            "holder_type": "C",
                            "in_de": kwargs["trade_type"],
                            "change_vol": 100.0 if kwargs["trade_type"] == "IN" else -50.0,
                            "after_share": 1000.0,
                            "after_ratio": 1.0,
                        }
                    ]
                )

        fake_pro = FakePro()

        result = fetch_stock_holdertrade(fake_pro, "20250101", "20250110")

        self.assertEqual([call["trade_type"] for call in fake_pro.calls], ["IN", "DE"])
        self.assertEqual(len(result), 2)
        self.assertEqual(set(result["in_de"]), {"IN", "DE"})

    @patch("src.sync_tushare_security_data.load_existing_stock_basic_snapshot")
    def test_build_stock_basic_change_summary_detects_add_and_delist(self, mock_load_snapshot):
        mock_load_snapshot.return_value = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "平安银行",
                    "list_status": "L",
                    "list_date": "19910403",
                    "delist_date": None,
                    "payload": {"ts_code": "000001.SZ"},
                },
                {
                    "ts_code": "000002.SZ",
                    "symbol": "000002",
                    "name": "万科A",
                    "list_status": "L",
                    "list_date": "19910129",
                    "delist_date": None,
                    "payload": {"ts_code": "000002.SZ"},
                },
            ]
        )

        latest_df = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "平安银行",
                    "list_status": "L",
                    "list_date": "19910403",
                    "delist_date": None,
                },
                {
                    "ts_code": "000002.SZ",
                    "symbol": "000002",
                    "name": "万科A",
                    "list_status": "D",
                    "list_date": "19910129",
                    "delist_date": "20260510",
                },
                {
                    "ts_code": "920999.BJ",
                    "symbol": "920999",
                    "name": "新上市样本",
                    "list_status": "L",
                    "list_date": "20260510",
                    "delist_date": None,
                },
            ]
        )

        summary = build_stock_basic_change_summary(engine=object(), latest_df=latest_df)

        self.assertEqual([item["ts_code"] for item in summary["added"]], ["920999.BJ"])
        self.assertEqual([item["ts_code"] for item in summary["delisted"]], ["000002.SZ"])
        self.assertEqual(str(summary["added"][0]["list_date"]), "2026-05-10")
        self.assertEqual(str(summary["delisted"][0]["delist_date"]), "2026-05-10")

    @patch("src.sync_tushare_security_data.load_existing_stock_basic_snapshot")
    def test_build_stock_basic_change_summary_bootstrap_skips_initial_full_snapshot(self, mock_load_snapshot):
        mock_load_snapshot.return_value = pd.DataFrame(
            columns=["ts_code", "symbol", "name", "list_status", "list_date", "delist_date", "payload"]
        )

        latest_df = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "平安银行",
                    "list_status": "L",
                    "list_date": "19910403",
                    "delist_date": None,
                },
                {
                    "ts_code": "000003.SZ",
                    "symbol": "000003",
                    "name": "PT金田A(退)",
                    "list_status": "D",
                    "list_date": "19910703",
                    "delist_date": "20020614",
                },
            ]
        )

        summary = build_stock_basic_change_summary(engine=object(), latest_df=latest_df)

        self.assertTrue(summary["bootstrap"])
        self.assertEqual(summary["previous_count"], 0)
        self.assertEqual(summary["latest_count"], 2)
        self.assertEqual(summary["added"], [])
        self.assertEqual(summary["delisted"], [])

    @patch("src.sync_tushare_security_data.load_existing_stock_basic_snapshot")
    def test_build_stock_basic_change_summary_ignores_existing_delisted_stock(self, mock_load_snapshot):
        mock_load_snapshot.return_value = pd.DataFrame(
            [
                {
                    "ts_code": "000002.SZ",
                    "symbol": "000002",
                    "name": "万科A",
                    "list_status": "D",
                    "list_date": "19910129",
                    "delist_date": "20260501",
                    "payload": {"ts_code": "000002.SZ"},
                }
            ]
        )

        latest_df = pd.DataFrame(
            [
                {
                    "ts_code": "000002.SZ",
                    "symbol": "000002",
                    "name": "万科A",
                    "list_status": "D",
                    "list_date": "19910129",
                    "delist_date": "20260510",
                }
            ]
        )

        summary = build_stock_basic_change_summary(engine=object(), latest_df=latest_df)

        self.assertFalse(summary["bootstrap"])
        self.assertEqual(summary["added"], [])
        self.assertEqual(summary["delisted"], [])

    @patch("src.sync_tushare_security_data.table_exists", return_value=True)
    @patch("src.sync_tushare_security_data.get_delisted_stock_basic_df")
    def test_purge_delisted_stock_history_returns_code_table_results(self, mock_get_delisted_df, _mock_table_exists):
        mock_get_delisted_df.return_value = pd.DataFrame(
            [
                {"ts_code": "000002.SZ"},
                {"ts_code": "000004.SZ"},
            ]
        )
        scripted_results = [
            FakeResult(rows=[("000002.SZ", 2), ("000004.SZ", 1)]),
            FakeResult(scalar_value=3),
            FakeResult(rowcount=3),
        ]
        conn = FakeConnection(scripted_results)
        engine = FakeEngine(conn)

        with patch("src.sync_tushare_security_data.DELISTED_PURGE_TARGETS", [
            {"table_name": "ts_stock_daily", "join_expr": "t.ts_code = d.ts_code"}
        ]):
            result = purge_delisted_stock_history(engine, dry_run=False, target_codes=["000002.SZ", "000004.SZ"])

        self.assertEqual(result["delisted_count"], 2)
        self.assertEqual(result["ts_codes"], ["000002.SZ", "000004.SZ"])
        self.assertEqual(
            result["code_table_results"]["000002.SZ"],
            [{"table_name": "ts_stock_daily", "matched_rows": 2, "deleted_rows": 2}],
        )
        self.assertEqual(
            result["code_table_results"]["000004.SZ"],
            [{"table_name": "ts_stock_daily", "matched_rows": 1, "deleted_rows": 1}],
        )
        self.assertEqual(
            result["table_results"],
            [{"table_name": "ts_stock_daily", "matched_rows": 3, "deleted_rows": 3}],
        )
        self.assertEqual(len(conn.calls), 3)

    def test_record_stock_change_log_builds_jsonb_payload_rows(self):
        conn = FakeConnection([FakeResult()])
        engine = FakeEngine(conn)

        record_stock_change_log(
            engine,
            run_id="20260510103000",
            action="delist",
            stock_rows=[
                {
                    "ts_code": "000002.SZ",
                    "symbol": "000002",
                    "name": "万科A",
                    "list_status": "D",
                    "list_date": "19910129",
                    "delist_date": "20260510",
                    "raw_payload": {"ts_code": "000002.SZ", "name": "万科A"},
                }
            ],
            deleted_tables_map={
                "000002.SZ": [
                    {"table_name": "ts_stock_daily", "matched_rows": 8, "deleted_rows": 8}
                ]
            },
        )

        self.assertEqual(len(conn.calls), 1)
        sql_text, params = conn.calls[0]
        self.assertIn("CAST(:raw_payload AS JSONB)", sql_text)
        self.assertIn("CAST(:deleted_tables AS JSONB)", sql_text)
        self.assertEqual(len(params), 1)
        row = params[0]
        self.assertEqual(row["run_id"], "20260510103000")
        self.assertEqual(row["action"], "delist")
        self.assertEqual(row["ts_code"], "000002.SZ")
        self.assertEqual(row["raw_payload"], '{"name": "万科A", "ts_code": "000002.SZ"}')
        self.assertEqual(
            row["deleted_tables"],
            '[{"deleted_rows": 8, "matched_rows": 8, "table_name": "ts_stock_daily"}]',
        )


class RunSyncOnceTests(unittest.TestCase):
    def _make_args(self):
        return SimpleNamespace(
            end_date="20260510",
            datasets=["stock_basic"],
            backfill_missing_history=False,
            daily_start="20250101",
            financial_start="20240101",
            index_codes=[],
            schedule=False,
            interval_minutes=60,
            daily_lookback_days=1,
            financial_lookback_days=30,
            daily_min_coverage_ratio=0.9,
            daily_publish_cutoff_hour=20,
            index_publish_cutoff_hour=20,
        )

    @patch("src.sync_tushare_security_data.logger")
    @patch("src.sync_tushare_security_data.record_stock_change_log")
    @patch("src.sync_tushare_security_data.purge_delisted_stock_history")
    @patch("src.sync_tushare_security_data.sync_dataset")
    def test_run_sync_once_bootstrap_purges_all_delisted_but_skips_event_logs(
        self,
        mock_sync_dataset,
        mock_purge,
        mock_record_log,
        _mock_logger,
    ):
        mock_sync_dataset.return_value = (
            10,
            {
                "added": [],
                "delisted": [],
                "bootstrap": True,
                "previous_count": 0,
                "latest_count": 10,
            },
        )
        mock_purge.return_value = {
            "delisted_count": 325,
            "table_results": [{"table_name": "ts_stock_daily", "deleted_rows": 8}],
            "code_table_results": {},
        }

        from src.sync_tushare_security_data import run_sync_once

        engine = object()
        total = run_sync_once(engine=engine, pro=object(), args=self._make_args())

        self.assertEqual(total, 10)
        mock_purge.assert_called_once_with(engine, dry_run=False, target_codes=None)
        mock_record_log.assert_not_called()

    @patch("src.sync_tushare_security_data.logger")
    @patch("src.sync_tushare_security_data.record_stock_change_log")
    @patch("src.sync_tushare_security_data.purge_delisted_stock_history")
    @patch("src.sync_tushare_security_data.sync_dataset")
    def test_run_sync_once_skips_purge_when_no_new_delist(
        self,
        mock_sync_dataset,
        mock_purge,
        mock_record_log,
        _mock_logger,
    ):
        mock_sync_dataset.return_value = (
            5,
            {
                "added": [{"ts_code": "920999.BJ"}],
                "delisted": [],
                "bootstrap": False,
                "previous_count": 10,
                "latest_count": 11,
            },
        )

        from src.sync_tushare_security_data import run_sync_once

        total = run_sync_once(engine=object(), pro=object(), args=self._make_args())

        self.assertEqual(total, 5)
        mock_purge.assert_not_called()
        self.assertEqual(mock_record_log.call_count, 2)


class FakeSecrets(dict):
    def get(self, key, default=None):
        if key in {"ETF_PG_PASSWORD", "PGPASSWORD", "database"}:
            os.environ["ETF_PG_HOST"] = "67.216.207.73"
            os.environ["ETF_PG_PORT"] = "5432"
        return super().get(key, default)


class BuildDbUrlTests(unittest.TestCase):
    def _write_env_file(self, directory: str) -> None:
        with open(os.path.join(directory, ".env"), "w", encoding="utf-8") as handle:
            handle.write(
                "ETF_PG_HOST=127.0.0.1\n"
                "ETF_PG_PORT=5432\n"
                "ETF_PG_DATABASE=postgres\n"
                "ETF_PG_USER=postgres\n"
                "ETF_PG_SSLMODE=disable\n"
            )

    def test_build_db_url_prefers_repo_env_host_over_process_env(self):
        from src.sync_tushare_security_data import build_db_url

        with tempfile.TemporaryDirectory() as temp_dir:
            self._write_env_file(temp_dir)
            with patch("src.sync_tushare_security_data.PROJECT_ROOT", temp_dir), patch.dict(
                os.environ,
                {
                    "ETF_PG_HOST": "67.216.207.73",
                    "ETF_PG_PASSWORD": "unit-test-secret",
                },
                clear=True,
            ):
                url = build_db_url()
                self.assertEqual(url.host, "127.0.0.1")
                self.assertEqual(url.username, "postgres")
                self.assertEqual(url.database, "postgres")
                self.assertEqual(url.query.get("sslmode"), "disable")

    def test_build_db_url_uses_streamlit_secret_password_without_overriding_repo_env(self):
        from src.sync_tushare_security_data import build_db_url

        fake_streamlit = SimpleNamespace(
            secrets=FakeSecrets(
                {
                    "ETF_PG_PASSWORD": "secret-from-streamlit",
                    "database": {"password": "secret-from-database"},
                }
            )
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            self._write_env_file(temp_dir)
            with patch("src.sync_tushare_security_data.PROJECT_ROOT", temp_dir), patch.dict(
                os.environ,
                {"ETF_PG_HOST": "67.216.207.73"},
                clear=True,
            ), patch.dict(sys.modules, {"streamlit": fake_streamlit}):
                url = build_db_url()
                password_env = os.environ.get("ETF_PG_PASSWORD")

        self.assertEqual(url.host, "127.0.0.1")
        self.assertEqual(str(url.password), "secret-from-streamlit")
        self.assertEqual(password_env, "secret-from-streamlit")


if __name__ == "__main__":
    unittest.main()
