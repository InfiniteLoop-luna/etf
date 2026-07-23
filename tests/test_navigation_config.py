import unittest
from unittest.mock import patch

import pandas as pd
import streamlit as st

from src.navigation_config import (
    ETF_INDUSTRY_PAGE_LABEL,
    ETF_FUND_WATCHLIST_PAGE_LABEL,
    ETF_PAGE_OPTIONS,
    FAVORITE_MY_FAVORITE_PAGE_LABEL,
    FAVORITE_PAGE_OPTIONS,
    MONEY_MARGIN_PAGE_LABEL,
    MONEY_PAGE_OPTIONS,
    STOCK_PAGE_OPTIONS,
)
from app import (
    HISTORICAL_ST_BADGE_TEXT,
    build_security_jump_links,
    build_security_jump_table_styler,
    render_tech_picker_jump_table,
    style_historical_st_badge_column,
)


class NavigationConfigTests(unittest.TestCase):
    def setUp(self):
        st.session_state.clear()

    def tearDown(self):
        st.session_state.clear()

    def test_build_security_jump_links_uses_code_and_updates_nonce(self):
        df = pd.DataFrame(
            [
                {"代码": "600230.SH", "简称": "沧州大化"},
                {"代码": "000001.SZ", "简称": "平安银行"},
            ]
        )

        links = build_security_jump_links(
            df,
            code_col="代码",
            fallback_col="简称",
            nonce_key="company_screener_jump_render_nonce",
        )

        self.assertEqual(len(links), 2)
        self.assertIn("security_query=600230.SH", links[0])
        self.assertIn("security_type=stock", links[0])
        self.assertIn("open_tab=security", links[0])
        self.assertEqual(st.session_state.get("company_screener_jump_render_nonce"), 1)

    def test_build_security_jump_links_falls_back_to_name_when_code_missing(self):
        df = pd.DataFrame([{"代码": "", "简称": "沧州大化"}])

        links = build_security_jump_links(
            df,
            code_col="代码",
            fallback_col="简称",
            nonce_key="company_screener_jump_render_nonce",
        )

        self.assertEqual(len(links), 1)
        self.assertIn("security_query=%E6%B2%A7%E5%B7%9E%E5%A4%A7%E5%8C%96", links[0])

    def test_style_historical_st_badge_column_uses_professional_gold_palette(self):
        styles = style_historical_st_badge_column(pd.Series([HISTORICAL_ST_BADGE_TEXT, "", None]))

        self.assertIn("background-color: #F6E7B8", styles[0])
        self.assertIn("color: #1B263B", styles[0])
        self.assertEqual(styles[1], "")
        self.assertEqual(styles[2], "")

    def test_build_security_jump_table_styler_keeps_badge_text(self):
        df = pd.DataFrame([{"查询": "?security_query=600230.SH", "标签": HISTORICAL_ST_BADGE_TEXT, "代码": "600230.SH"}])

        styler = build_security_jump_table_styler(df)

        self.assertEqual(styler.data.iloc[0]["标签"], HISTORICAL_ST_BADGE_TEXT)

    def test_render_tech_picker_jump_table_adds_historical_st_label_column(self):
        df = pd.DataFrame(
            [
                {
                    "ts_code": "600230.SH",
                    "name": "沧州大化",
                    "industry": "化工原料",
                    "trade_date": "2026-05-09",
                    "w_ema5": 10.1,
                    "w_ema30": 11.2,
                    "m_ema5": 9.8,
                    "m_ema30": 12.0,
                    "main_business": "TDI",
                    "has_ever_st": True,
                }
            ]
        )

        captured = {}

        def fake_render_security_jump_table(display_df, help_text, code_col="代码", fallback_col="简称", nonce_key="security_jump_render_nonce"):
            captured["display_df"] = display_df.copy()
            captured["help_text"] = help_text
            captured["code_col"] = code_col
            captured["fallback_col"] = fallback_col
            captured["nonce_key"] = nonce_key

        with patch("app.render_security_jump_table", side_effect=fake_render_security_jump_table):
            render_tech_picker_jump_table(df)

        self.assertIn("标签", captured["display_df"].columns)
        self.assertEqual(captured["display_df"].iloc[0]["标签"], HISTORICAL_ST_BADGE_TEXT)
        self.assertNotIn("has_ever_st", captured["display_df"].columns)
        self.assertEqual(captured["nonce_key"], "tech_picker_render_nonce")

    def test_stock_page_options_include_author_tracking(self):
        self.assertIn("🧭 观点跟踪", STOCK_PAGE_OPTIONS)

    def test_stock_page_options_include_factor_workbench(self):
        self.assertIn("🧠 因子选股工作台", STOCK_PAGE_OPTIONS)

    def test_stock_page_options_include_user_watchlist(self):
        self.assertIn("⭐ 自选管理", STOCK_PAGE_OPTIONS)

    def test_fund_page_options_include_standalone_watchlist(self):
        self.assertEqual(ETF_FUND_WATCHLIST_PAGE_LABEL, "⭐ 自选基金")
        self.assertIn("⭐ 自选基金", ETF_PAGE_OPTIONS)

    def test_fund_page_options_include_industry_etf(self):
        self.assertEqual(ETF_INDUSTRY_PAGE_LABEL, "🏭 行业ETF")
        self.assertIn("🏭 行业ETF", ETF_PAGE_OPTIONS)

    def test_favorite_page_options_include_my_favorite(self):
        self.assertEqual(FAVORITE_MY_FAVORITE_PAGE_LABEL, "⭐ My Favorite")
        self.assertIn("⭐ My Favorite", FAVORITE_PAGE_OPTIONS)

    def test_money_page_options_include_margin_page(self):
        self.assertEqual(MONEY_MARGIN_PAGE_LABEL, "🏦 两融数据")
        self.assertIn("🏦 两融数据", MONEY_PAGE_OPTIONS)

    def test_navigation_option_labels_remain_stable(self):
        self.assertEqual(
            ETF_PAGE_OPTIONS,
            ["📈 主要宽基ETF份额", "🥧 ETF分类占比", "📈 ETF分类趋势", "📊 宽基指数ETF", "🏭 行业ETF", "📈 基金监测", "⭐ 自选基金", "🧩 基金对象页"],
        )
        self.assertEqual(
            STOCK_PAGE_OPTIONS,
            ["🔎 个股/指数查询", "🧩 股票对象页", "🐉 龙虎榜", "⭐ 自选管理", "🗂 自选池", "🏢 公司筛选", "🎯 技术选股", "🧠 因子选股工作台", "🧭 观点跟踪"],
        )
        self.assertEqual(
            FAVORITE_PAGE_OPTIONS,
            ["⭐ My Favorite"],
        )
        self.assertEqual(
            MONEY_PAGE_OPTIONS,
            ["💹 资金流向", "🏦 两融数据", "📊 每日成交量", "🏦 公募持仓热股", "🔥 打板情绪", "🧾 游资名录"],
        )


if __name__ == "__main__":
    unittest.main()
