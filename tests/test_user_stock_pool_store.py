import unittest

from src.user_stock_pool_store import import_stock_pool_rows


class UserStockPoolStoreTests(unittest.TestCase):
    def test_import_stock_pool_rows_accepts_company_screener_columns(self):
        upsert_calls = []

        def fake_upsert_item(username, ts_code, **kwargs):
            upsert_calls.append((username, ts_code, kwargs))
            return "inserted"

        summary = import_stock_pool_rows(
            "Alice",
            [
                {
                    "代码": "688808.SH",
                    "简称": "科创样本",
                    "行业": "电子",
                }
            ],
            source_file="公司主营与产品筛选",
            upsert_item=fake_upsert_item,
        )

        self.assertEqual(summary["added"], 1)
        self.assertEqual(upsert_calls[0][0], "Alice")
        self.assertEqual(upsert_calls[0][1], "688808.SH")
        self.assertEqual(upsert_calls[0][2]["security_name"], "科创样本")
        self.assertEqual(upsert_calls[0][2]["industry"], "电子")
        self.assertEqual(upsert_calls[0][2]["source_file"], "公司主营与产品筛选")


if __name__ == "__main__":
    unittest.main()
