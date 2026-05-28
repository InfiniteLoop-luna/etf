import unittest

from src.stock_research_html_renderer import (
    STOCK_RESEARCH_HTML_MARKER,
    render_stock_research_html,
)


class StockResearchHtmlRendererTests(unittest.TestCase):
    def _sample_fact_pack(self):
        return {
            "ts_code": "000733.SZ",
            "stock_name": "<script>alert(1)</script>",
            "asof_trade_date": "2026-05-23",
            "generated_at": "2026-05-23T18:00:00",
            "profile": {
                "industry": "半导体",
                "market": "主板",
                "main_business": "功率半导体",
                "holder_num": 12345,
                "holder_end_date": "2026-03-31",
            },
            "price_metrics": {
                "latest_close": 10.5,
                "ret_20d_pct": 8.2,
                "drawdown_from_52w_high_pct": -11.4,
                "volume_ratio_20": 1.6,
            },
            "valuation_snapshot": {
                "pe_ttm": 22.3,
                "pb": 2.1,
                "ps_ttm": 3.8,
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
                {"trade_date": "2026-05-22", "open": 10, "close": 10.5, "low": 9.8, "high": 10.8, "vol": 1000},
                {"trade_date": "2026-05-23", "open": 10.5, "close": 10.7, "low": 10.4, "high": 11.0, "vol": 1200},
            ],
            "financial_tail": [{"end_date": "2026-03-31", "roe": 9.7}],
            "supplemental": {
                "news": {
                    "status": "ok",
                    "items": [{"发布时间": "2026-05-20", "新闻标题": "订单持续恢复", "文章来源": "测试源"}],
                },
                "money_flow": {
                    "status": "ok",
                    "items": [{"日期": "2026-05-23", "主力净流入-净额": 123456, "涨跌幅": 1.2}],
                },
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
            "verdict": "重点跟踪",
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
            "step_analysis": {"step0": "锁定自选股跟踪", "step3": "公司质量中上"},
        }

    def test_render_html_contains_visual_report_marker_and_chart_payload(self):
        html = render_stock_research_html(
            self._sample_fact_pack(),
            self._sample_llm_result(),
            report_md="# markdown source",
        )

        self.assertIn(STOCK_RESEARCH_HTML_MARKER, html)
        self.assertIn("echarts", html)
        self.assertIn("klineRows", html)
        self.assertIn("2026-05-23", html)
        self.assertIn("综合判断：重点跟踪", html)
        self.assertIn("补充证据层", html)
        self.assertIn("订单持续恢复", html)
        self.assertIn("Markdown 原文", html)

    def test_render_html_escapes_dynamic_text(self):
        html = render_stock_research_html(self._sample_fact_pack(), self._sample_llm_result())

        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", html)
        self.assertNotIn("<script>alert(1)</script>", html)


if __name__ == "__main__":
    unittest.main()
