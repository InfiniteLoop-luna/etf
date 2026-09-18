"""Financial semantics of the watchlist's multi-fund comparison."""

import copy

import pandas as pd
import pytest

from src.fund_watchlist_comparison import build_fund_comparison


def _fund(code, holdings, *, report_date="2026-06-30", name=None):
    return {
        "fund_code": code,
        "fund_name": name or code,
        "latest_end_date": report_date,
        "holdings": holdings,
    }


def _holding(symbol, weight, *, name=None, industry="科技"):
    return {"symbol": symbol, "weight": weight, "stock_name": name or symbol, "industry": industry}


def test_weight_matrices_preserve_units_and_overlap_uses_codes_not_names():
    result = build_fund_comparison([
        _fund("A", [_holding("600001.SH", 8, name="同名"), _holding("600002.SH", 4, name="旧称")]),
        _fund("B", [_holding("600003.SH", 9, name="同名"), _holding("600002.SH", 3, name="新称")]),
    ])
    assert result["holdings"].loc["600002.SH", "A"] == 4
    assert result["industries"].loc["科技", "A"] == 12
    assert result["overlap_counts"].loc["A", "B"] == 1
    assert result["overlap_counts"].loc["A", "A"] == 2
    assert result["stock_details"].loc["600002.SH", "observed_fund_count"] == 2
    assert result["funds"].loc["A", "observed_weight"] == 12
    assert result["funds"].index.name == "fund_code"


def test_no_data_absent_position_and_missing_weight_are_distinct():
    result = build_fund_comparison([
        _fund("A", [_holding("600001.SH", None)]),
        _fund("B", [_holding("600002.SH", 0, industry="银行")]),
        _fund("C", []),
    ])
    presence = result["holding_presence"]
    assert presence.loc["600001.SH", "A"]
    assert not presence.loc["600001.SH", "B"]
    assert pd.isna(presence.loc["600001.SH", "C"])
    assert result["holdings"].loc["600002.SH", "B"] == 0
    assert result["holdings"].loc["600001.SH"].isna().all()
    assert result["overlap_counts"].loc["A", "B"] == 0
    assert result["overlap_counts"].loc["C"].isna().all()
    assert result["overlap_counts"]["C"].isna().all()
    assert result["funds"].loc["A", "data_status"] == "partial"
    assert result["funds"].loc["C", "data_status"] == "no_data"
    assert pd.isna(result["funds"].loc["A", "observed_weight"])
    assert pd.isna(result["industries"].loc["科技", "A"])
    assert not result["industry_presence"].loc["科技", "B"]
    assert pd.isna(result["industry_presence"].loc["科技", "C"])


def test_duplicates_are_not_summed_and_conflicting_weights_are_unavailable():
    result = build_fund_comparison([
        _fund(" a ", [_holding("600001.sh", 8), _holding("600001.SH", 8)]),
        _fund("A", [_holding("600009.SH", 99)]),
        _fund("B", [_holding("600001.SH", 7), _holding("600001.SH", 9)]),
    ])
    assert list(result["funds"].index) == ["A", "B"]
    assert result["funds"].loc["A", "holding_count"] == 1
    assert result["holdings"].loc["600001.SH", "A"] == 8
    assert pd.isna(result["holdings"].loc["600001.SH", "B"])
    assert result["overlap_counts"].loc["A", "B"] == 1
    assert any("权重冲突" in warning for warning in result["warnings"])


@pytest.mark.parametrize("weight", [None, float("nan"), float("inf"), -1, 101, "bad", True])
def test_invalid_weight_keeps_observed_position_and_invalidates_industry_sum(weight):
    result = build_fund_comparison([
        _fund("A", [_holding("600001.SH", weight), _holding("600002.SH", 8)]),
        _fund("B", [_holding("600001.SH", 2)]),
    ])
    assert pd.isna(result["industries"].loc["科技", "A"])
    assert pd.isna(result["funds"].loc["A", "observed_weight"])
    assert result["holding_presence"].loc["600001.SH", "A"]
    assert result["overlap_counts"].loc["A", "B"] == 1


def test_invalid_security_codes_are_not_matched_and_incomplete_total_is_unavailable():
    result = build_fund_comparison([
        _fund("A", [_holding("-", 10), _holding(None, 10), _holding("600001.SH", 5)]),
        _fund("B", [_holding("-", 10), _holding("中文名称", 10)]),
    ])
    assert list(result["holdings"].index) == ["600001.SH"]
    assert result["funds"].loc["A", "data_status"] == "partial"
    assert pd.isna(result["funds"].loc["A", "observed_weight"])
    assert result["overlap_counts"].loc["B"].isna().all()


def test_unknown_industry_and_conflicting_classifications_have_explicit_buckets():
    result = build_fund_comparison([
        _fund("A", [_holding("600001.SH", 5, industry="软件"), _holding("600002.SH", 3, industry="未识别")]),
        _fund("B", [_holding("600001.SH", 6, industry="电子"), _holding("600003.SH", 4, industry=pd.NA)]),
    ])
    assert set(result["industries"].index) == {"分类不一致", "未识别行业"}
    assert result["industries"].loc["分类不一致", "A"] == 5
    assert result["industries"].loc["分类不一致", "B"] == 6
    assert result["stock_details"].loc["600001.SH", "industry"] == "分类不一致"
    assert any("行业分类不一致" in warning for warning in result["warnings"])


def test_sum_above_100_is_flagged_without_clipping_or_normalizing_positions():
    result = build_fund_comparison([
        _fund("A", [_holding("600001.SH", 70), _holding("600002.SH", 40)]),
    ])
    assert result["holdings"].loc["600001.SH", "A"] == 70
    assert pd.isna(result["funds"].loc["A", "observed_weight"])
    assert pd.isna(result["industries"].loc["科技", "A"])
    assert result["funds"].loc["A", "data_status"] == "partial"
    assert any("超过 100%" in warning for warning in result["warnings"])


def test_report_periods_and_missing_dates_are_disclosed():
    result = build_fund_comparison([
        _fund("A", [_holding("600001.SH", 3)], report_date="2026-06-30"),
        _fund("B", [_holding("600001.SH", 3)], report_date=20260331),
        _fund("C", [_holding("600001.SH", 3)], report_date=None),
    ])
    assert result["funds"].loc["B", "report_date"] == pd.Timestamp("2026-03-31")
    assert any("持仓报告期不同" in warning for warning in result["warnings"])
    assert any("缺少持仓报告期" in warning for warning in result["warnings"])


def test_empty_inputs_and_input_data_are_safe():
    result = build_fund_comparison([])
    assert result["funds"].empty
    assert result["holdings"].empty
    assert result["overlap_counts"].empty
    items = [_fund("A", [_holding("600001.SH", 3)])]
    original = copy.deepcopy(items)
    build_fund_comparison(items)
    assert items == original
