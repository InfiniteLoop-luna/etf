import unittest
from unittest.mock import patch

import pandas as pd

from src.sync_tushare_security_data import (
    build_stock_basic_change_summary,
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


if __name__ == "__main__":
    unittest.main()
