import unittest
from unittest.mock import patch

import pandas as pd

from src.etf_stats import search_companies


class CompanyScreenerSearchTests(unittest.TestCase):
    def test_search_companies_keeps_date_filter_optional(self):
        captured = {}

        def fake_read_sql(sql, engine, params=None):
            captured["sql"] = str(sql)
            captured["params"] = params or {}
            return pd.DataFrame()

        with patch("src.etf_stats.pd.read_sql", side_effect=fake_read_sql):
            search_companies(engine=object())

        self.assertIn("b.list_date,", captured["sql"])
        self.assertNotIn("CAST(:list_date_start AS date)", captured["sql"])
        self.assertNotIn("CAST(:list_date_end AS date)", captured["sql"])
        self.assertNotIn("list_date_start", captured["params"])
        self.assertNotIn("list_date_end", captured["params"])
        self.assertIn("ORDER BY b.ts_code", captured["sql"])

    def test_search_companies_applies_list_date_range(self):
        captured = {}

        def fake_read_sql(sql, engine, params=None):
            captured["sql"] = str(sql)
            captured["params"] = params or {}
            return pd.DataFrame()

        with patch("src.etf_stats.pd.read_sql", side_effect=fake_read_sql):
            search_companies(
                industries=["电子"],
                list_date_start="2026-03-21",
                list_date_end="2026-06-21",
                engine=object(),
            )

        self.assertIn("b.list_date >= CAST(:list_date_start AS date)", captured["sql"])
        self.assertIn("b.list_date <= CAST(:list_date_end AS date)", captured["sql"])
        self.assertIn("ORDER BY b.list_date DESC NULLS LAST, b.ts_code", captured["sql"])
        self.assertEqual(captured["params"]["list_date_start"], "2026-03-21")
        self.assertEqual(captured["params"]["list_date_end"], "2026-06-21")
        self.assertEqual(captured["params"]["ind_0"], "电子")


if __name__ == "__main__":
    unittest.main()
