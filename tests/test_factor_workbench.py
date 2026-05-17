import unittest

import pandas as pd

from src.factor_workbench import (
    apply_factor_filters,
    compute_factor_scores,
    get_factor_catalog,
    get_score_preset,
)


class FactorWorkbenchLogicTests(unittest.TestCase):
    def test_apply_factor_filters_respects_enabled_flags(self):
        df = pd.DataFrame(
            [
                {"ts_code": "A", "turnover_rate": 2.0, "roe": 8.0, "market": "主板"},
                {"ts_code": "B", "turnover_rate": 6.0, "roe": 12.0, "market": "创业板"},
            ]
        )
        filters = {
            "markets": ["主板", "创业板"],
            "exclude_historical_st": False,
            "require_is_hs": False,
            "min_turnover_rate_enabled": True,
            "min_turnover_rate": 5.0,
            "min_roe_enabled": False,
            "min_roe": 10.0,
        }

        filtered = apply_factor_filters(df, filters)

        self.assertEqual(filtered["ts_code"].tolist(), ["B"])

    def test_compute_factor_scores_honors_factor_direction(self):
        df = pd.DataFrame(
            [
                {"ts_code": "A", "roe": 15.0, "pb": 3.0},
                {"ts_code": "B", "roe": 10.0, "pb": 1.0},
                {"ts_code": "C", "roe": 6.0, "pb": 2.0},
            ]
        )

        scored = compute_factor_scores(df, factor_weights={"roe": 1.0, "pb": 1.0})

        self.assertIn("final_score", scored.columns)
        self.assertEqual(scored.iloc[0]["ts_code"], "B")

    def test_compute_factor_scores_fills_missing_values_neutrally(self):
        df = pd.DataFrame(
            [
                {"ts_code": "A", "roe": 10.0, "pb": 2.0},
                {"ts_code": "B", "roe": None, "pb": 1.0},
            ]
        )

        scored = compute_factor_scores(df, factor_weights={"roe": 1.0, "pb": 1.0})

        self.assertTrue(scored["final_score"].notna().all())
        self.assertTrue(scored["score_missing_count"].ge(0).all())

    def test_factor_catalog_and_presets_expose_v1_core_factors(self):
        catalog = get_factor_catalog()
        catalog_keys = {item["key"] for item in catalog}
        preset = get_score_preset("均衡打分")

        self.assertIn("alpha095_cv", catalog_keys)
        self.assertIn("net_mf_amount_rate", catalog_keys)
        self.assertIn("roe", catalog_keys)
        self.assertTrue(any(weight > 0 for weight in preset["factor_weights"].values()))


if __name__ == "__main__":
    unittest.main()
