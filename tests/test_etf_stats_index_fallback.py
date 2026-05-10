import unittest
from unittest.mock import patch

import pandas as pd

from src.etf_stats import get_index_profile, search_security


class EtfStatsIndexFallbackTests(unittest.TestCase):
    def test_search_security_uses_fallback_index_names_when_payload_name_missing(self):
        captured = {}

        def fake_read_sql(query, engine, params=None):
            captured["sql"] = str(query)
            captured["params"] = params
            return pd.DataFrame(
                [
                    {
                        "security_type": "index",
                        "ts_code": "000905.SH",
                        "symbol": "000905.SH",
                        "name": "中证500指数",
                        "industry": None,
                        "market": None,
                        "latest_date": "2026-05-08",
                    }
                ]
            )

        with patch("src.etf_stats.pd.read_sql", side_effect=fake_read_sql):
            df = search_security("中证", "index", limit=20, engine=object())

        self.assertEqual(df.iloc[0]["name"], "中证500指数")
        self.assertIn("WHEN ts_code = '000905.SH' THEN '中证500指数'", captured["sql"])
        self.assertEqual(captured["params"]["like_kw"], "%中证%")

    def test_get_index_profile_uses_fallback_index_name_mapping(self):
        captured = {}

        def fake_read_sql(query, engine, params=None):
            captured["sql"] = str(query)
            captured["params"] = params
            return pd.DataFrame(
                [
                    {
                        "ts_code": "399006.SZ",
                        "name": "创业板指",
                        "latest_trade_date": "2026-05-08",
                    }
                ]
            )

        with patch("src.etf_stats.pd.read_sql", side_effect=fake_read_sql):
            df = get_index_profile("399006.SZ", engine=object())

        self.assertEqual(df.iloc[0]["name"], "创业板指")
        self.assertIn("WHEN ts_code = '399006.SZ' THEN '创业板指'", captured["sql"])
        self.assertEqual(captured["params"]["ts_code"], "399006.SZ")


if __name__ == "__main__":
    unittest.main()
