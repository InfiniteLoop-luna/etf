import unittest
from unittest.mock import Mock, patch

import pandas as pd

from src.etf_stats import (
    get_stock_profile,
    update_stock_custom_info_batch,
    validate_stock_custom_info_inputs,
)


class StockInfoEditTests(unittest.TestCase):
    def test_get_stock_profile_uses_basic_alias_inside_cte(self):
        captured = {}

        def fake_read_sql(query, engine, params=None):
            captured["sql"] = str(query)
            captured["params"] = params
            return pd.DataFrame([
                {"ts_code": "000001.SZ", "name": "平安银行"}
            ])

        with patch("src.etf_stats.pd.read_sql", side_effect=fake_read_sql):
            df = get_stock_profile("000001.SZ", engine=object())

        self.assertEqual(df.iloc[0]["ts_code"], "000001.SZ")
        self.assertIn(f"SELECT * FROM {{STOCK_BASIC_VIEW}} AS basic".format(STOCK_BASIC_VIEW='vw_ts_stock_basic'), captured["sql"])
        self.assertIn("WHERE basic.ts_code = :ts_code", captured["sql"])
        self.assertEqual(captured["params"]["ts_code"], "000001.SZ")

    def test_validate_stock_custom_info_inputs_returns_clear_for_blank_values(self):
        result = validate_stock_custom_info_inputs("   ", "\n")

        self.assertEqual(result["action"], "clear")
        self.assertEqual(result["main_business"], "")
        self.assertEqual(result["product"], "")

    def test_validate_stock_custom_info_inputs_rejects_too_short_values(self):
        result = validate_stock_custom_info_inputs("a", "b")

        self.assertEqual(result["action"], "invalid")
        self.assertEqual(result["message"], "too_short")

    def test_validate_stock_custom_info_inputs_returns_trimmed_save_payload(self):
        result = validate_stock_custom_info_inputs(" 半导体设计 ", " 芯片、IP授权 ")

        self.assertEqual(result["action"], "save")
        self.assertEqual(result["main_business"], "半导体设计")
        self.assertEqual(result["product"], "芯片、IP授权")

    def test_update_stock_custom_info_batch_updates_each_code_once(self):
        update_func = Mock()

        updated_count = update_stock_custom_info_batch(
            ["000001.SZ", "000002.SZ", "000001.SZ", "", None],
            "银行业务",
            "零售金融",
            update_func=update_func,
        )

        self.assertEqual(updated_count, 2)
        self.assertEqual(update_func.call_count, 2)
        update_func.assert_any_call("000001.SZ", "银行业务", "零售金融", engine=None)
        update_func.assert_any_call("000002.SZ", "银行业务", "零售金融", engine=None)


if __name__ == "__main__":
    unittest.main()
