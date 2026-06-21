import unittest
from unittest.mock import Mock

from sqlalchemy import create_engine

from src.stock_analysis_template_report import (
    generate_stock_analysis_template_report_bundle,
    render_stock_analysis_template_html,
)


class StockAnalysisTemplateReportTests(unittest.TestCase):
    def _sample_fact_pack(self):
        return {
            "ts_code": "000733.SZ",
            "stock_name": "振华科技",
            "asof_trade_date": "2026-05-23",
            "generated_at": "2026-05-23T18:00:00",
            "profile": {
                "industry": "半导体",
                "market": "主板",
                "has_ever_st": False,
                "main_business": "功率半导体、电子元器件",
                "business_scope": "电子元器件研发、生产和销售",
                "holder_num": 12345,
                "holder_end_date": "2026-03-31",
            },
            "price_metrics": {
                "latest_close": 10.7,
                "ret_20d_pct": 8.2,
                "drawdown_from_52w_high_pct": -11.4,
                "volume_ratio_20": 1.6,
            },
            "valuation_snapshot": {
                "pe_ttm": 22.3,
                "pb": 2.1,
                "total_mv_yi": 180.5,
                "circ_mv_yi": 160.2,
            },
            "financial_metrics": {
                "latest": {
                    "fina_end_date": "2026-03-31",
                    "roe": 9.7,
                    "gross_margin": 32.1,
                    "debt_to_assets": 41.2,
                    "total_revenue_yi": 18.6,
                    "net_profit_yi": 2.1,
                    "operating_cashflow_yi": 1.9,
                }
            },
            "price_tail": [
                {"trade_date": "2026-05-20", "open": 10.0, "close": 10.2, "low": 9.9, "high": 10.4, "vol": 900},
                {"trade_date": "2026-05-21", "open": 10.2, "close": 10.4, "low": 10.1, "high": 10.5, "vol": 950},
                {"trade_date": "2026-05-22", "open": 10.4, "close": 10.5, "low": 10.2, "high": 10.8, "vol": 1000},
                {"trade_date": "2026-05-23", "open": 10.5, "close": 10.7, "low": 10.4, "high": 11.0, "vol": 1200},
            ],
            "financial_tail": [{"end_date": "2026-03-31", "roe": 9.7}],
            "supplemental": {
                "business_composition": {
                    "status": "ok",
                    "items": [
                        {"主营构成": "电子元器件", "主营收入": 123456789, "收入比例": 68.5, "毛利率": 32.0}
                    ],
                },
                "news": {"status": "empty", "items": []},
            },
            "data_quality": {
                "profile_rows": 1,
                "daily_rows": 90,
                "kline_rows": 90,
                "financial_rows": 4,
                "supplemental_enabled": True,
            },
        }

    def _sample_llm_result(self):
        return {
            "verdict": "谨慎跟踪",
            "risk_level": "中",
            "confidence": 76,
            "summary": "盈利质量改善，但仍需观察订单兑现。",
            "investment_thesis": "产业景气度和现金流改善形成跟踪价值。",
            "valuation_view": "估值处于可跟踪区间，但需要业绩确认。",
            "timing_view": "短期位置偏高，适合等待回撤确认。",
            "quality_score": {"score": 72, "grade": "B", "drivers": ["ROE 改善"], "weaknesses": ["波动较大"]},
            "key_evidence": ["近20日走势强于均值"],
            "risk_factors": ["需求恢复不及预期"],
            "watch_items": ["后续财报现金流"],
            "step_analysis": {"step0": "锁定自选股跟踪"},
        }

    def test_render_template_report_contains_docx_template_sections_and_llm_advice(self):
        report = render_stock_analysis_template_html(
            self._sample_fact_pack(),
            self._sample_llm_result(),
        )

        expected_sections = [
            "<!doctype html>",
            "<html lang=\"zh-CN\">",
            "<title>振华科技公司分析报告</title>",
            "<h1>振华科技公司分析报告</h1>",
            "<h2>基本面分析</h2>",
            "<h3>振华科技公司（000733.SZ）基本信息</h3>",
            "<h3>振华科技公司所属行业、业务范围及产品</h3>",
            "<h3>振华科技公司产品应用领域</h3>",
            "<h2>振华科技公司财务分析</h2>",
            "<h3>振华科技公司各产品&amp;服务收入分析</h3>",
            "<h2>技术面分析</h2>",
            "<h3>日线技术分析</h3>",
            "<h3>周线技术分析</h3>",
            "<h3>月线技术分析</h3>",
            "<h3>30分钟线技术分析</h3>",
            "<h3>60分钟线技术分析</h3>",
            "<h2>股东数量分析</h2>",
            "<h3>所有报告期前十大股东分析</h3>",
            "<h3>所有报告期前十大流通股东分析</h3>",
            "<h2>投资建议</h2>",
        ]
        for section in expected_sections:
            self.assertIn(section, report)
        self.assertIn("数据&amp;信息来源——WealthSpark 决策看板 &amp; tushare", report)
        self.assertIn("产业景气度和现金流改善形成跟踪价值。", report)
        self.assertIn("MACD、MA（5、10、20、30、60、120、233、250）、EMA（5、30）、交易量、金叉&amp;死叉等指标分析", report)
        self.assertIn("</html>", report)

    def test_generate_template_report_builds_fact_pack_and_calls_llm_on_demand(self):
        engine = create_engine("sqlite:///:memory:")
        fact_pack_builder = Mock(return_value=self._sample_fact_pack())
        llm_analyzer = Mock(return_value=self._sample_llm_result())

        bundle = generate_stock_analysis_template_report_bundle(
            "000733.SZ",
            "振华科技",
            engine=engine,
            allow_live_fetch=False,
            fact_pack_builder=fact_pack_builder,
            llm_analyzer=llm_analyzer,
        )

        self.assertIn("<h2>投资建议</h2>", bundle["report_html"])
        self.assertNotIn("report_md", bundle)
        self.assertEqual(bundle["fact_pack"]["ts_code"], "000733.SZ")
        self.assertEqual(bundle["llm_result"]["verdict"], "谨慎跟踪")
        fact_pack_builder.assert_called_once()
        llm_analyzer.assert_called_once_with(bundle["fact_pack"])


if __name__ == "__main__":
    unittest.main()
