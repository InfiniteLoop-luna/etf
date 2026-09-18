from pathlib import Path

import pandas as pd
from streamlit.testing.v1 import AppTest

from src.fund_watchlist_comparison import build_fund_comparison
from src.fund_watchlist_comparison_ui import build_comparison_display


def _items():
    return [
        {"fund_code": "A", "fund_name": "同名基金", "latest_end_date": "2026-06-30", "holdings": [
            {"symbol": "600001.SH", "stock_name": "甲股票", "industry": "科技", "weight": 8},
            {"symbol": "600002.SH", "stock_name": "乙股票", "industry": "银行", "weight": 4},
        ]},
        {"fund_code": "B", "fund_name": "同名基金", "latest_end_date": "2026-03-31", "holdings": [
            {"symbol": "600001.SH", "stock_name": "甲股票", "industry": "科技", "weight": None},
            {"symbol": "600003.SH", "stock_name": "丙股票", "industry": "银行", "weight": 0},
        ]},
        {"fund_code": "C", "fund_name": "无数据基金", "latest_end_date": None, "holdings": []},
    ]


def _app():
    script = '''
import streamlit as st
from src.fund_watchlist_comparison_ui import render_fund_watchlist_comparison
st.session_state.setdefault("sample_items", SAMPLE_ITEMS)
st.session_state.setdefault("sample_user", "user-one")
render_fund_watchlist_comparison(st.session_state["sample_items"], username=st.session_state["sample_user"])
'''.replace("SAMPLE_ITEMS", repr(_items()))
    return AppTest.from_string(script, default_timeout=10).run()


def test_display_keeps_three_missing_states_and_observed_zero_distinct():
    comparison = build_fund_comparison(_items())
    display, styles = build_comparison_display(comparison, "股票持仓")
    rows = display.set_index("股票")
    assert rows.loc["甲股票（600001.SH）", "同名基金（A）"] == "8.00%"
    assert rows.loc["甲股票（600001.SH）", "同名基金（B）"] == "权重缺失"
    assert rows.loc["乙股票（600002.SH）", "同名基金（B）"] == "未观察到"
    assert rows.loc["丙股票（600003.SH）", "同名基金（B）"] == "0.00%"
    assert (rows["无数据基金（C）"] == "暂无数据").all()
    assert (styles["无数据基金（C）"] == "").all()
    common, _ = build_comparison_display(comparison, "股票持仓", stock_scope="共同持仓")
    assert common["股票"].tolist() == ["甲股票（600001.SH）"]


def test_display_no_data_overlap_is_not_zero_shared_stocks():
    display, _ = build_comparison_display(build_fund_comparison(_items()), "基金重叠")
    rows = display.set_index("基金")
    assert rows.loc["同名基金（A）", "同名基金（B）"] == "1 只"
    assert rows.loc["同名基金（A）", "同名基金（A）"] == "2 只"
    assert (rows["无数据基金（C）"] == "暂无数据").all()


def test_app_switches_all_three_matrix_dimensions_without_loading_sources():
    at = _app()
    assert not at.exception
    assert at.multiselect[0].value == ["A", "B", "C"]
    assert "同名基金（A）" in at.multiselect[0].options
    assert "同名基金（B）" in at.multiselect[0].options
    assert any("报告期不同" in warning.value for warning in at.warning)
    at.radio[1].set_value("共同持仓").run()
    assert not at.exception
    assert len(at.dataframe[-1].value) == 1
    at.radio[0].set_value("行业分布").run()
    assert not at.exception
    assert at.dataframe[-1].value.columns[0] == "行业"
    at.radio[0].set_value("基金重叠").run()
    assert not at.exception
    assert at.dataframe[-1].value.shape == (3, 4)
    assert "样本权重可用" in at.dataframe[0].value["数据状态"].tolist()


def test_app_empty_and_single_fund_selection_are_explicit():
    at = _app()
    at.multiselect[0].set_value([]).run()
    assert not at.exception
    assert at.multiselect[0].value == []
    assert not at.dataframe
    assert any("至少两只" in message.value for message in at.info)
    at.multiselect[0].set_value(["A"]).run()
    assert not at.exception
    assert not at.dataframe
    at.multiselect[0].set_value(["A", "B"]).run()
    assert not at.exception
    assert at.dataframe[-1].value.shape[1] == 5


def test_watchlist_additions_removals_and_user_changes_keep_scoped_selection():
    at = _app()
    at.multiselect[0].set_value(["A", "B"]).run()
    extra = {"fund_code": "D", "fund_name": "新增基金", "holdings": []}
    at.session_state["sample_items"] = _items() + [extra]
    at.run()
    assert not at.exception
    assert at.multiselect[0].value == ["A", "B"]
    at.session_state["sample_items"] = [_items()[0], _items()[2], extra]
    at.run()
    assert not at.exception
    assert at.multiselect[0].value == ["A"]
    at.session_state["sample_user"] = "user-two"
    at.run()
    assert not at.exception
    assert at.multiselect[0].value == ["A", "C", "D"]
    at.multiselect[0].set_value(["C", "D"]).run()
    at.session_state["sample_user"] = "user-three"
    at.run()
    assert not at.exception
    assert at.multiselect[0].value == ["A", "C", "D"]


def test_all_selected_follows_new_fund_and_single_empty_fund_is_safe():
    at = _app()
    at.session_state["sample_items"] = _items() + [{"fund_code": "D", "fund_name": "新增基金", "holdings": []}]
    at.run()
    assert not at.exception
    assert at.multiselect[0].value == ["A", "B", "C", "D"]
    at.session_state["sample_items"] = [_items()[2]]
    at.run()
    assert not at.exception
    assert any("至少两只" in message.value for message in at.info)


def test_comparison_route_returns_before_single_fund_focus():
    import ast

    module = ast.parse(Path("app.py").read_text(encoding="utf-8"))
    function = next(node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == "render_fund_watchlist_live_dashboard")
    branch = next(node for node in function.body if isinstance(node, ast.If) and "对比矩阵" in ast.unparse(node.test))
    assert ast.unparse(branch.body[0]) == "render_fund_watchlist_comparison(sorted_items, username=current_username)"
    assert isinstance(branch.body[-1], ast.Return)
