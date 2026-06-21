from io import BytesIO
import unittest

import pandas as pd
from openpyxl import Workbook

from src.watchlist_excel_importer import (
    import_watchlist_rows,
    normalize_stock_ts_code,
    parse_watchlist_import_workbook,
)


def _build_watchlist_workbook(rows):
    wb = Workbook()
    ws = wb.active
    ws.append(["代码", "名称"])
    for row in rows:
        ws.append(row)

    stream = BytesIO()
    wb.save(stream)
    stream.seek(0)
    return stream


class WatchlistExcelImporterTests(unittest.TestCase):
    def test_normalize_stock_ts_code_infers_a_share_exchange_suffix(self):
        self.assertEqual(normalize_stock_ts_code("688808"), "688808.SH")
        self.assertEqual(normalize_stock_ts_code("300887"), "300887.SZ")
        self.assertEqual(normalize_stock_ts_code("920167"), "920167.BJ")
        self.assertEqual(normalize_stock_ts_code("000001.SZ"), "000001.SZ")
        self.assertEqual(normalize_stock_ts_code("SZ000001"), "000001.SZ")
        self.assertEqual(normalize_stock_ts_code("not-a-code"), "")

    def test_parse_watchlist_import_workbook_reads_code_name_sheet_and_deduplicates(self):
        workbook = _build_watchlist_workbook(
            [
                ["688808", "联讯仪器"],
                ["300887", "谱尼测试"],
                ["300887.SZ", "谱尼测试重复"],
                ["", "空白代码"],
                ["not-a-code", "非法代码"],
            ]
        )

        rows = parse_watchlist_import_workbook(workbook)

        self.assertEqual(
            rows,
            [
                {
                    "ts_code": "688808.SH",
                    "security_name": "联讯仪器",
                    "security_type": "stock",
                    "source_row": 2,
                },
                {
                    "ts_code": "300887.SZ",
                    "security_name": "谱尼测试",
                    "security_type": "stock",
                    "source_row": 3,
                },
            ],
        )

    def test_import_watchlist_rows_requires_username_and_adds_only_missing_codes(self):
        rows = [
            {"ts_code": "688808.SH", "security_name": "联讯仪器", "security_type": "stock"},
            {"ts_code": "300887.SZ", "security_name": "谱尼测试", "security_type": "stock"},
            {"ts_code": "", "security_name": "空白", "security_type": "stock"},
        ]
        existing_df = pd.DataFrame(
            [{"ts_code": "300887.SZ", "security_type": "stock", "security_name": "谱尼测试"}]
        )
        add_calls = []

        with self.assertRaises(ValueError):
            import_watchlist_rows("", rows, add_item=lambda **kwargs: None)

        summary = import_watchlist_rows(
            " alice ",
            rows,
            existing_watchlist_df=existing_df,
            add_item=lambda username, ts_code, security_name="", security_type="stock": add_calls.append(
                (username, ts_code, security_name, security_type)
            ),
        )

        self.assertEqual(summary["added"], 1)
        self.assertEqual(summary["added_codes"], ["688808.SH"])
        self.assertEqual(summary["skipped_existing"], 1)
        self.assertEqual(summary["skipped_invalid"], 1)
        self.assertEqual(summary["failed"], 0)
        self.assertEqual(add_calls, [("alice", "688808.SH", "联讯仪器", "stock")])


if __name__ == "__main__":
    unittest.main()
