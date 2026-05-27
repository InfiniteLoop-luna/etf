import json
import unittest
from datetime import date, datetime

import pandas as pd

from src.distribution_llm_analysis import make_json_safe


class DistributionLLMAnalysisTests(unittest.TestCase):
    def test_make_json_safe_converts_nested_non_json_values(self):
        payload = {
            "trade_date": date(2026, 5, 27),
            "generated_at": datetime(2026, 5, 27, 17, 30, 0),
            "nan_value": float("nan"),
            "inf_value": float("inf"),
            "nested": [
                pd.Timestamp("2026-05-27 09:30:00"),
                {"neg_inf": float("-inf")},
            ],
        }

        safe_payload = make_json_safe(payload)

        self.assertEqual(safe_payload["trade_date"], "2026-05-27")
        self.assertEqual(safe_payload["generated_at"], "2026-05-27T17:30:00")
        self.assertIsNone(safe_payload["nan_value"])
        self.assertIsNone(safe_payload["inf_value"])
        self.assertEqual(safe_payload["nested"][0], "2026-05-27T09:30:00")
        self.assertIsNone(safe_payload["nested"][1]["neg_inf"])

        json.dumps(safe_payload, ensure_ascii=False)


if __name__ == "__main__":
    unittest.main()
