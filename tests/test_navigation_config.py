import unittest
from unittest.mock import patch

import pandas as pd
import streamlit as st

from app import build_security_jump_links


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


if __name__ == "__main__":
    unittest.main()
