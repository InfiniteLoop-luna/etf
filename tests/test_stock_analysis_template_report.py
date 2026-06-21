import unittest
from unittest.mock import Mock

from sqlalchemy import create_engine
from sqlalchemy import text

from src.stock_analysis_template_report import (
    generate_stock_analysis_template_report_bundle,
    load_stock_analysis_template_chart_data,
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
            "template_chart_data": self._sample_chart_data(),
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

    def _sample_chart_data(self):
        return {
            "source_rows": {
                "vw_ts_stock_income": 6,
                "vw_ts_stock_fina_indicator": 6,
                "vw_ts_stock_cashflow": 6,
                "vw_ts_stock_daily_basic": 3,
                "vw_ts_stock_holdernumber": 3,
                "tushare_top10_holders": 4,
                "tushare_top10_floatholders": 4,
            },
            "latest": {
                "trade_date": "2026-05-23",
                "pe": 18.2,
                "pe_ttm": 22.3,
                "dv_ttm": 1.4,
                "total_mv_yi": 180.5,
                "circ_mv_yi": 160.2,
                "total_share_yi": 16.9,
                "float_share_yi": 13.4,
            },
            "charts": [
                {
                    "id": "revenue_annual",
                    "title": "图1：收入（柱状图）及增长率（折线图）（年度）",
                    "kind": "bar_line",
                    "value_label": "营业收入",
                    "unit": "亿元",
                    "line_label": "增长率",
                    "line_unit": "%",
                    "source": "vw_ts_stock_income.total_revenue",
                    "rows": [
                        {"label": "2024", "value": 30.0, "growth_pct": None},
                        {"label": "2025", "value": 36.0, "growth_pct": 20.0},
                    ],
                },
                {
                    "id": "static_pe",
                    "title": "图8：静态市盈率",
                    "kind": "line",
                    "value_label": "静态市盈率",
                    "unit": "倍",
                    "line_label": "增长率",
                    "line_unit": "%",
                    "source": "vw_ts_stock_daily_basic.pe",
                    "rows": [
                        {"label": "2026-05-21", "value": 17.8},
                        {"label": "2026-05-22", "value": 18.0},
                        {"label": "2026-05-23", "value": 18.2},
                    ],
                },
            ],
            "holder_number_charts": [
                {
                    "id": "holder_number_change",
                    "title": "图15：股东数量（柱状）及变化率（折线）",
                    "kind": "bar_line",
                    "value_label": "股东数量",
                    "unit": "户",
                    "line_label": "变化率",
                    "line_unit": "%",
                    "source": "vw_ts_stock_holdernumber.holder_num",
                    "rows": [
                        {"label": "2025-12-31", "value": 15000, "line_value": None},
                        {"label": "2026-03-31", "value": 12345, "line_value": -17.7},
                    ],
                },
                {
                    "id": "holder_number_price",
                    "title": "图16：股东数量（柱状）与股价趋势（折线）",
                    "kind": "bar_line",
                    "value_label": "股东数量",
                    "unit": "户",
                    "line_label": "收盘价",
                    "line_unit": "元",
                    "source": "vw_ts_stock_holdernumber.holder_num + vw_ts_stock_daily_basic.close",
                    "rows": [
                        {"label": "2025-12-31", "value": 15000, "line_value": 9.5},
                        {"label": "2026-03-31", "value": 12345, "line_value": 10.7},
                    ],
                },
            ],
            "shareholder_charts": [
                {
                    "id": "top10_holders",
                    "title": "图17：前十大股东",
                    "kind": "horizontal_bar",
                    "value_label": "持股数量",
                    "unit": "万股",
                    "line_label": "持股比例",
                    "line_unit": "%",
                    "source": "Tushare top10_holders（个股查询同源接口）",
                    "period": "2026-03-31",
                    "rows": [
                        {"label": "第一大股东", "value": 12000.0, "ratio": 21.5},
                        {"label": "第二大股东", "value": 6000.0, "ratio": 10.7},
                    ],
                },
                {
                    "id": "top10_holders_change",
                    "title": "图18：前十大股东变化情况（持股数量及持股比例）",
                    "kind": "bar_line",
                    "value_label": "前十合计持股数量",
                    "unit": "万股",
                    "line_label": "前十合计持股比例",
                    "line_unit": "%",
                    "source": "Tushare top10_holders（个股查询同源接口）",
                    "rows": [
                        {"label": "2025-12-31", "value": 28000.0, "growth_pct": 48.0},
                        {"label": "2026-03-31", "value": 30000.0, "growth_pct": 51.0},
                    ],
                },
                {
                    "id": "top10_float_holders",
                    "title": "图19：前十大流通股东",
                    "kind": "horizontal_bar",
                    "value_label": "持股数量",
                    "unit": "万股",
                    "line_label": "持股比例",
                    "line_unit": "%",
                    "source": "Tushare top10_floatholders（个股查询同源接口）",
                    "period": "2026-03-31",
                    "rows": [
                        {"label": "流通股东A", "value": 10000.0, "ratio": 18.2},
                        {"label": "流通股东B", "value": 5500.0, "ratio": 9.5},
                    ],
                },
                {
                    "id": "top10_float_holders_change",
                    "title": "图20：前十大流通股东变化情况（持股数量及持股比例）",
                    "kind": "bar_line",
                    "value_label": "前十合计持股数量",
                    "unit": "万股",
                    "line_label": "前十合计持股比例",
                    "line_unit": "%",
                    "source": "Tushare top10_floatholders（个股查询同源接口）",
                    "rows": [
                        {"label": "2025-12-31", "value": 24000.0, "growth_pct": 43.0},
                        {"label": "2026-03-31", "value": 26000.0, "growth_pct": 46.0},
                    ],
                },
            ],
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
        self.assertIn("图1：收入（柱状图）及增长率（折线图）（年度）", report)
        self.assertIn("图8：静态市盈率", report)
        self.assertIn("chart-svg", report)
        self.assertIn("已查询数据源：vw_ts_stock_income=6 行", report)
        self.assertIn("图15：股东数量（柱状）及变化率（折线）", report)
        self.assertIn("图16：股东数量（柱状）与股价趋势（折线）", report)
        self.assertIn("vw_ts_stock_holdernumber.holder_num", report)
        self.assertIn("图17：前十大股东", report)
        self.assertIn("图18：前十大股东变化情况（持股数量及持股比例）", report)
        self.assertIn("图19：前十大流通股东", report)
        self.assertIn("图20：前十大流通股东变化情况（持股数量及持股比例）", report)
        self.assertIn("Tushare top10_floatholders（个股查询同源接口）", report)
        self.assertNotIn("当前底稿保留最新值，年度序列需补充后绘制", report)
        self.assertNotIn("历史序列需补充后绘制", report)
        self.assertNotIn("待接入股东结构数据", report)
        self.assertIn("产业景气度和现金流改善形成跟踪价值。", report)
        self.assertIn("MACD、MA（5、10、20、30、60、120、233、250）、EMA（5、30）、交易量、金叉&amp;死叉等指标分析", report)
        self.assertIn("</html>", report)

    def test_generate_template_report_builds_fact_pack_and_calls_llm_on_demand(self):
        engine = create_engine("sqlite:///:memory:")
        fact_pack_builder = Mock(return_value=self._sample_fact_pack())
        llm_analyzer = Mock(return_value=self._sample_llm_result())
        chart_data_loader = Mock(return_value=self._sample_chart_data())

        bundle = generate_stock_analysis_template_report_bundle(
            "000733.SZ",
            "振华科技",
            engine=engine,
            allow_live_fetch=False,
            report_date="2026-06-21",
            fact_pack_builder=fact_pack_builder,
            llm_analyzer=llm_analyzer,
            chart_data_loader=chart_data_loader,
        )

        self.assertIn("<h2>投资建议</h2>", bundle["report_html"])
        self.assertIn("图1：收入（柱状图）及增长率（折线图）（年度）", bundle["report_html"])
        self.assertNotIn("report_md", bundle)
        self.assertFalse(bundle["cache_hit"])
        self.assertEqual(bundle["fact_pack"]["ts_code"], "000733.SZ")
        self.assertEqual(bundle["llm_result"]["verdict"], "谨慎跟踪")
        fact_pack_builder.assert_called_once()
        llm_analyzer.assert_called_once_with(bundle["fact_pack"])
        chart_data_loader.assert_called_once()

    def test_generate_template_report_reuses_same_day_database_cache(self):
        engine = create_engine("sqlite:///:memory:")
        fact_pack_builder = Mock(return_value=self._sample_fact_pack())
        llm_analyzer = Mock(return_value=self._sample_llm_result())
        chart_data_loader = Mock(return_value=self._sample_chart_data())

        first = generate_stock_analysis_template_report_bundle(
            "000733.SZ",
            "振华科技",
            engine=engine,
            allow_live_fetch=False,
            report_date="2026-06-21",
            fact_pack_builder=fact_pack_builder,
            llm_analyzer=llm_analyzer,
            chart_data_loader=chart_data_loader,
        )
        fact_pack_builder.reset_mock(side_effect=True)
        llm_analyzer.reset_mock(side_effect=True)
        chart_data_loader.reset_mock(side_effect=True)
        fact_pack_builder.side_effect = AssertionError("fact pack should not be rebuilt")
        llm_analyzer.side_effect = AssertionError("llm should not be called")
        chart_data_loader.side_effect = AssertionError("chart data should not be reloaded")

        second = generate_stock_analysis_template_report_bundle(
            "000733.SZ",
            "振华科技",
            engine=engine,
            allow_live_fetch=False,
            report_date="2026-06-21",
            fact_pack_builder=fact_pack_builder,
            llm_analyzer=llm_analyzer,
            chart_data_loader=chart_data_loader,
        )

        self.assertFalse(first["cache_hit"])
        self.assertTrue(second["cache_hit"])
        self.assertEqual(second["report_html"], first["report_html"])
        fact_pack_builder.assert_not_called()
        llm_analyzer.assert_not_called()
        chart_data_loader.assert_not_called()

    def test_chart_data_loader_queries_database_series(self):
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE vw_ts_stock_income (
                    ts_code TEXT,
                    end_date TEXT,
                    ann_date TEXT,
                    total_revenue REAL,
                    n_income_attr_p REAL,
                    n_income REAL
                )
            """))
            conn.execute(text("""
                CREATE TABLE vw_ts_stock_fina_indicator (
                    ts_code TEXT,
                    end_date TEXT,
                    ann_date TEXT,
                    profit_dedt REAL
                )
            """))
            conn.execute(text("""
                CREATE TABLE vw_ts_stock_cashflow (
                    ts_code TEXT,
                    end_date TEXT,
                    ann_date TEXT,
                    n_cashflow_act REAL
                )
            """))
            conn.execute(text("""
                CREATE TABLE vw_ts_stock_daily_basic (
                    ts_code TEXT,
                    trade_date TEXT,
                    close REAL,
                    pe REAL,
                    pe_ttm REAL,
                    dv_ratio REAL,
                    dv_ttm REAL,
                    total_mv REAL,
                    circ_mv REAL,
                    total_share REAL,
                    float_share REAL
                )
            """))
            conn.execute(text("""
                CREATE TABLE vw_ts_stock_holdernumber (
                    ts_code TEXT,
                    end_date TEXT,
                    ann_date TEXT,
                    holder_num REAL
                )
            """))
            for end_date, revenue, net_profit in [
                ("2024-03-31", 10_000_000_000, 1_000_000_000),
                ("2024-06-30", 22_000_000_000, 2_500_000_000),
                ("2024-12-31", 50_000_000_000, 6_000_000_000),
                ("2025-03-31", 15_000_000_000, 1_500_000_000),
                ("2025-06-30", 33_000_000_000, 3_600_000_000),
                ("2025-12-31", 75_000_000_000, 9_000_000_000),
            ]:
                conn.execute(
                    text("""
                        INSERT INTO vw_ts_stock_income
                        VALUES ('000733.SZ', :end_date, :ann_date, :revenue, :profit, :profit)
                    """),
                    {"end_date": end_date, "ann_date": end_date, "revenue": revenue, "profit": net_profit},
                )
                conn.execute(
                    text("""
                        INSERT INTO vw_ts_stock_fina_indicator
                        VALUES ('000733.SZ', :end_date, :ann_date, :profit)
                    """),
                    {"end_date": end_date, "ann_date": end_date, "profit": net_profit * 0.9},
                )
                conn.execute(
                    text("""
                        INSERT INTO vw_ts_stock_cashflow
                        VALUES ('000733.SZ', :end_date, :ann_date, :cashflow)
                    """),
                    {"end_date": end_date, "ann_date": end_date, "cashflow": net_profit * 1.1},
                )
            for trade_date, close, pe, pe_ttm, total_mv in [
                ("2025-12-31", 9.5, 16.0, 18.0, 1_450_000),
                ("2026-03-31", 10.7, 17.0, 19.0, 1_520_000),
                ("2026-06-16", 11.2, 18.0, 20.0, 1_600_000),
                ("2026-06-17", 11.4, 18.5, 20.5, 1_650_000),
                ("2026-06-18", 11.6, 19.0, 21.0, 1_700_000),
            ]:
                conn.execute(
                    text("""
                        INSERT INTO vw_ts_stock_daily_basic
                        VALUES ('000733.SZ', :trade_date, :close, :pe, :pe_ttm, 1.1, 1.2, :total_mv, 1500000, 160000, 120000)
                    """),
                    {"trade_date": trade_date, "close": close, "pe": pe, "pe_ttm": pe_ttm, "total_mv": total_mv},
                )
            for end_date, holder_num in [
                ("2025-09-30", 18000),
                ("2025-12-31", 15000),
                ("2026-03-31", 12000),
            ]:
                conn.execute(
                    text("""
                        INSERT INTO vw_ts_stock_holdernumber
                        VALUES ('000733.SZ', :end_date, :ann_date, :holder_num)
                    """),
                    {"end_date": end_date, "ann_date": end_date, "holder_num": holder_num},
                )

        chart_data = load_stock_analysis_template_chart_data(
            "000733.SZ",
            engine=engine,
            asof_trade_date="2026-06-18",
            shareholder_loader=None,
        )
        charts = {chart["id"]: chart for chart in chart_data["charts"]}

        self.assertEqual(chart_data["source_rows"]["vw_ts_stock_income"], 6)
        self.assertEqual(charts["revenue_annual"]["rows"][-1]["label"], "2025")
        self.assertEqual(charts["revenue_annual"]["rows"][-1]["value"], 750.0)
        revenue_quarterly = charts["revenue_quarterly"]["rows"]
        self.assertEqual(revenue_quarterly[-1]["label"], "2025Q2")
        self.assertEqual(revenue_quarterly[-1]["value"], 180.0)
        self.assertAlmostEqual(revenue_quarterly[-1]["growth_pct"], 50.0)
        self.assertEqual(charts["deducted_profit_annual"]["rows"][-1]["value"], 81.0)
        self.assertEqual(charts["static_pe"]["rows"][-1]["value"], 19.0)
        self.assertEqual(chart_data["latest"]["total_mv_yi"], 170.0)
        holder_charts = {chart["id"]: chart for chart in chart_data["holder_number_charts"]}
        self.assertEqual(chart_data["source_rows"]["vw_ts_stock_holdernumber"], 3)
        self.assertEqual(holder_charts["holder_number_change"]["rows"][-1]["label"], "2026-03-31")
        self.assertEqual(holder_charts["holder_number_change"]["rows"][-1]["value"], 12000.0)
        self.assertAlmostEqual(holder_charts["holder_number_change"]["rows"][-1]["line_value"], -20.0)
        self.assertEqual(holder_charts["holder_number_price"]["rows"][-1]["line_value"], 10.7)


if __name__ == "__main__":
    unittest.main()
