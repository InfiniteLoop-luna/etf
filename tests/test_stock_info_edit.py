import unittest
from unittest.mock import Mock, patch

import pandas as pd

from app import build_hotmoney_stock_preference_display_df, format_security_option
from src.etf_stats import (
    get_stock_profile,
    search_companies,
    search_security,
    search_stocks_by_technical_signals,
    update_stock_custom_info_batch,
    validate_stock_custom_info_inputs,
)


class StockInfoEditTests(unittest.TestCase):
    def test_build_hotmoney_stock_preference_display_df_turns_stock_name_into_jump_link(self):
        source_df = pd.DataFrame([
            {
                "ts_name": "平安银行",
                "ts_code": "000001.SZ",
                "hit_count": 5,
                "hm_count": 3,
                "total_net_amount_yi": 1.23,
            }
        ])

        with patch("app.st.session_state", {}):
            display_df = build_hotmoney_stock_preference_display_df(source_df)

        self.assertEqual(list(display_df.columns), ["股票名称", "代码", "上榜次数", "游资数", "净买卖(亿)"])
        self.assertEqual(display_df.iloc[0]["代码"], "000001.SZ")
        self.assertEqual(display_df.iloc[0]["净买卖(亿)"], "1.23")
        self.assertEqual(
            display_df.iloc[0]["股票名称"],
            "?security_query=000001.SZ&security_type=stock&open_tab=security&jump_nonce=1_000001.SZ#平安银行",
        )

    def test_search_companies_exposes_historical_st_flag(self):
        captured = {}

        def fake_read_sql(query, engine, params=None):
            captured["sql"] = str(query)
            captured["params"] = params
            return pd.DataFrame([
                {
                    "ts_code": "600230.SH",
                    "name": "沧州大化",
                    "industry": "基础化工",
                    "has_ever_st": True,
                    "main_business": "TDI",
                    "product": "化工产品",
                }
            ])

        with patch("src.etf_stats.pd.read_sql", side_effect=fake_read_sql):
            df = search_companies(["基础化工"], product_kw="TDI", business_kw="化工", engine=object())

        self.assertTrue(bool(df.iloc[0]["has_ever_st"]))
        self.assertIn("AS has_ever_st", captured["sql"])
        self.assertIn("FROM vw_ts_stock_namechange nc", captured["sql"])

    def test_search_stocks_by_technical_signals_exposes_historical_st_flag(self):
        captured = {}

        def fake_read_sql(query, engine, params=None):
            captured["sql"] = str(query)
            captured["params"] = params
            return pd.DataFrame([
                {
                    "ts_code": "600230.SH",
                    "name": "沧州大化",
                    "industry": "基础化工",
                    "has_ever_st": True,
                    "trade_date": "2026-05-09",
                    "w_ema5": 10.0,
                    "w_ema30": 11.0,
                    "m_ema5": 9.0,
                    "m_ema30": 12.0,
                    "main_business": "TDI",
                }
            ])

        with patch("src.etf_stats.pd.read_sql", side_effect=fake_read_sql):
            df = search_stocks_by_technical_signals(True, False, engine=object())

        self.assertTrue(bool(df.iloc[0]["has_ever_st"]))
        self.assertIn("AS has_ever_st", captured["sql"])
        self.assertIn("FROM vw_ts_stock_namechange nc", captured["sql"])

    def test_format_security_option_appends_historical_st_tag(self):
        row = pd.Series({
            "security_type": "stock",
            "name": "沧州大化",
            "ts_code": "600230.SH",
            "symbol": "600230",
            "industry": "化工原料",
            "market": "主板",
            "has_ever_st": True,
        })

        label = format_security_option(row)

        self.assertIn("曾经ST", label)
        self.assertIn("沧州大化", label)

    def test_search_security_exposes_historical_st_flag(self):
        captured = {}

        def fake_read_sql(query, engine, params=None):
            captured["sql"] = str(query)
            captured["params"] = params
            return pd.DataFrame([
                {
                    "security_type": "stock",
                    "ts_code": "600230.SH",
                    "symbol": "600230",
                    "name": "沧州大化",
                    "industry": "基础化工",
                    "market": "主板",
                    "latest_date": None,
                    "has_ever_st": True,
                }
            ])

        with patch("src.etf_stats.pd.read_sql", side_effect=fake_read_sql):
            df = search_security("沧州大化", "stock", limit=20, engine=object())

        self.assertTrue(bool(df.iloc[0]["has_ever_st"]))
        self.assertIn("AS has_ever_st", captured["sql"])
        self.assertIn("FROM vw_ts_stock_namechange nc", captured["sql"])

    def test_search_security_filters_current_st_name_not_historical_namechange(self):
        captured = {}

        def fake_read_sql(query, engine, params=None):
            captured["sql"] = str(query)
            captured["params"] = params
            return pd.DataFrame([
                {
                    "security_type": "stock",
                    "ts_code": "600230.SH",
                    "symbol": "600230",
                    "name": "沧州大化",
                    "industry": "基础化工",
                    "market": "主板",
                    "latest_date": None,
                    "has_ever_st": True,
                }
            ])

        with patch("src.etf_stats.pd.read_sql", side_effect=fake_read_sql):
            df = search_security("沧州大化", "stock", limit=20, engine=object())

        self.assertEqual(df.iloc[0]["ts_code"], "600230.SH")
        self.assertIn("COALESCE(b.name, '') NOT LIKE '*ST%'", captured["sql"])
        self.assertNotIn("vw_ts_stock_namechange WHERE name LIKE '%ST%'", captured["sql"])

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
