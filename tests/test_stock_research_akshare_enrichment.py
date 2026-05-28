import unittest

import pandas as pd

from src.stock_research_akshare_enrichment import (
    StockResearchAkshareConfig,
    build_stock_research_supplemental,
)


class FakeAkshareClient:
    def __init__(self):
        self.calls = []

    def stock_zygc_em(self, symbol):
        self.calls.append(("stock_zygc_em", symbol))
        return pd.DataFrame(
            [
                {"报告日期": "2025-12-31", "分类": "功率半导体", "主营收入": 12.3, "收入比例": "60%"},
            ]
        )

    def stock_news_em(self, symbol):
        self.calls.append(("stock_news_em", symbol))
        return pd.DataFrame(
            [
                {"发布时间": "2026-05-20", "新闻标题": "订单持续恢复", "文章来源": "测试源"},
            ]
        )

    def stock_research_report_em(self, symbol):
        self.calls.append(("stock_research_report_em", symbol))
        return pd.DataFrame(
            [
                {"发布日期": "2026-05-18", "报告名称": "盈利修复跟踪", "机构": "测试证券", "评级": "增持"},
            ]
        )

    def stock_individual_fund_flow(self, stock, market):
        self.calls.append(("stock_individual_fund_flow", stock, market))
        return pd.DataFrame(
            [
                {"日期": "2026-05-23", "主力净流入-净额": 123456.0, "涨跌幅": 1.2},
            ]
        )

    def stock_lhb_stock_detail_date_em(self, symbol):
        self.calls.append(("stock_lhb_stock_detail_date_em", symbol))
        return pd.DataFrame([{"上榜日": "2026-05-21", "上榜原因": "日涨幅偏离值达7%"}])

    def stock_board_industry_cons_em(self, symbol):
        self.calls.append(("stock_board_industry_cons_em", symbol))
        return pd.DataFrame([{"代码": "000733", "名称": "振华科技", "涨跌幅": 2.3}])


class StockResearchAkshareEnrichmentTests(unittest.TestCase):
    def test_disabled_config_returns_disabled_blocks_without_fetching(self):
        fake = FakeAkshareClient()

        supplemental = build_stock_research_supplemental(
            "000733.SZ",
            enabled=True,
            config=StockResearchAkshareConfig(enabled=False),
            ak_client=fake,
        )

        self.assertEqual(supplemental["news"]["status"], "disabled")
        self.assertEqual(fake.calls, [])

    def test_enabled_config_fetches_and_normalizes_supplemental_blocks(self):
        fake = FakeAkshareClient()
        config = StockResearchAkshareConfig(
            enabled=True,
            business_limit=2,
            news_limit=2,
            research_report_limit=2,
            money_flow_limit=2,
            lhb_limit=2,
            industry_peer_limit=2,
        )

        supplemental = build_stock_research_supplemental(
            "000733.SZ",
            stock_name="振华科技",
            industry="半导体",
            enabled=True,
            config=config,
            ak_client=fake,
        )

        self.assertEqual(supplemental["business_composition"]["status"], "ok")
        self.assertEqual(supplemental["business_composition"]["items"][0]["分类"], "功率半导体")
        self.assertEqual(supplemental["news"]["items"][0]["新闻标题"], "订单持续恢复")
        self.assertIn(("stock_zygc_em", "SZ000733"), fake.calls)
        self.assertIn(("stock_individual_fund_flow", "000733", "sz"), fake.calls)
        self.assertIn(("stock_board_industry_cons_em", "半导体"), fake.calls)

    def test_block_failure_is_recorded_without_raising(self):
        class FailingAkshareClient(FakeAkshareClient):
            def stock_news_em(self, symbol):
                raise RuntimeError("network down")

        supplemental = build_stock_research_supplemental(
            "600000.SH",
            industry="银行",
            enabled=True,
            config=StockResearchAkshareConfig(enabled=True),
            ak_client=FailingAkshareClient(),
        )

        self.assertEqual(supplemental["news"]["status"], "failed")
        self.assertIn("network down", supplemental["news"]["error"])
        self.assertEqual(supplemental["business_composition"]["status"], "ok")


if __name__ == "__main__":
    unittest.main()
