import unittest

import pandas as pd
import streamlit as st

from app import build_security_jump_links, render_tech_picker_jump_table


class NavigationConfigTests(unittest.TestCase):
    def setUp(self):
        st.session_state.clear()

    def tearDown(self):
        st.session_state.clear()

    def test_build_security_jump_links_uses_code_and_updates_nonce(self):
        df = pd.DataFrame([
            {"代码": "600230.SH", "简称": "沧州大化"},
            {"代码": "000001.SZ", "简称": "平安银行"},
        ])

        links = build_security_jump_links(df, code_col="代码", fallback_col="简称", nonce_key="company_screener_jump_render_nonce")

        self.assertEqual(len(links), 2)
        self.assertIn("security_query=600230.SH", links[0])
        self.assertIn("security_type=stock", links[0])
        self.assertIn("open_tab=security", links[0])
        self.assertEqual(st.session_state.get("company_screener_jump_render_nonce"), 1)

    def test_build_security_jump_links_falls_back_to_name_when_code_missing(self):
        df = pd.DataFrame([
            {"代码": "", "简称": "沧州大化"},
        ])

        links = build_security_jump_links(df, code_col="代码", fallback_col="简称", nonce_key="company_screener_jump_render_nonce")

        self.assertEqual(len(links), 1)
        self.assertIn("security_query=%E6%B2%A7%E5%B7%9E%E5%A4%A7%E5%8C%96", links[0])

    def test_render_tech_picker_jump_table_adds_historical_st_label_column(self):
        df = pd.DataFrame([
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
        ])

        captured = {}

        def fake_render_security_jump_table(display_df, help_text, code_col='代码', fallback_col='简称', nonce_key='security_jump_render_nonce'):
            captured['display_df'] = display_df.copy()
            captured['help_text'] = help_text
            captured['code_col'] = code_col
            captured['fallback_col'] = fallback_col
            captured['nonce_key'] = nonce_key

        from unittest.mock import patch
        with patch('app.render_security_jump_table', side_effect=fake_render_security_jump_table):
            render_tech_picker_jump_table(df)

        self.assertIn('标签', captured['display_df'].columns)
        self.assertEqual(captured['display_df'].iloc[0]['标签'], '曾经ST')
        self.assertNotIn('has_ever_st', captured['display_df'].columns)
        self.assertEqual(captured['nonce_key'], 'tech_picker_render_nonce')


if __name__ == "__main__":
    unittest.main()
