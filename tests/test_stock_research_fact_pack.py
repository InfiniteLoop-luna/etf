import unittest
from unittest.mock import Mock, patch

import pandas as pd
from sqlalchemy import create_engine

from src.stock_research_fact_pack import build_stock_research_fact_pack


class StockResearchFactPackTests(unittest.TestCase):
    def test_fact_pack_v2_includes_supplemental_status(self):
        engine = create_engine("sqlite:///:memory:")
        supplemental_builder = Mock(
            return_value={
                "news": {
                    "name": "news",
                    "status": "ok",
                    "row_count": 1,
                    "items": [{"新闻标题": "订单恢复"}],
                }
            }
        )

        with patch(
            "src.stock_research_fact_pack.get_stock_profile",
            return_value=pd.DataFrame(
                [
                    {
                        "ts_code": "000733.SZ",
                        "name": "振华科技",
                        "industry": "半导体",
                        "market": "主板",
                        "pe_ttm": 22.5,
                        "pb": 2.1,
                    }
                ]
            ),
        ), patch(
            "src.stock_research_fact_pack.get_stock_timeseries",
            return_value=pd.DataFrame(
                [
                    {"trade_date": "2026-05-22", "close": 10.0, "open": 9.8, "high": 10.2, "low": 9.7},
                    {"trade_date": "2026-05-23", "close": 10.5, "open": 10.0, "high": 10.8, "low": 9.9},
                ]
            ),
        ), patch(
            "src.stock_research_fact_pack.get_stock_financial_timeseries",
            return_value=pd.DataFrame([{"end_date": "2026-03-31", "roe": 9.5}]),
        ), patch(
            "src.stock_research_fact_pack.get_stock_kline_timeseries",
            return_value=pd.DataFrame(),
        ):
            fact_pack = build_stock_research_fact_pack(
                "000733.SZ",
                "振华科技",
                engine=engine,
                asof_trade_date="2026-05-23",
                allow_live_fetch=True,
                supplemental_builder=supplemental_builder,
            )

        self.assertEqual(fact_pack["schema_version"], "stock-research-fact-pack-v2")
        self.assertEqual(fact_pack["supplemental"]["news"]["items"][0]["新闻标题"], "订单恢复")
        self.assertTrue(fact_pack["data_quality"]["supplemental_enabled"])
        self.assertEqual(fact_pack["data_quality"]["supplemental_status"]["news"], "ok")
        supplemental_builder.assert_called_once()


if __name__ == "__main__":
    unittest.main()
