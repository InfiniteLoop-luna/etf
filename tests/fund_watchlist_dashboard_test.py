from datetime import date

import pandas as pd
import pytest

from src.fund_nav import (
    build_latest_fund_nav_snapshot,
    fetch_latest_fund_nav_snapshot,
    normalize_fund_code_for_nav,
)
from src.fund_watchlist_dashboard import (
    build_fund_watchlist_item,
    build_fund_watchlist_summary,
    build_fund_watchlist_table,
    sort_fund_watchlist_items,
)


def _watchlist_row():
    return pd.Series(
        {
            "ts_code": "001938.OF",
            "security_name": "中欧时代先锋",
            "created_at": "2026-06-20",
        }
    )


def _meta_df():
    return pd.DataFrame(
        [
            {
                "fund_code": "001938.OF",
                "name": "中欧时代先锋",
                "management": "中欧基金",
                "fund_type": "混合型",
                "issue_amount": 128.4,
                "latest_end_date": "2026-03-31",
            }
        ]
    )


def _holding_df():
    return pd.DataFrame(
        [
            {
                "end_date": "2026-03-31",
                "stock_name": "宁德时代",
                "symbol": "300750.SZ",
                "mkv": 1_820_000_000,
                "stk_mkv_ratio": 7.9,
                "holding_change_flag": "increase",
                "management": "中欧基金",
                "fund_type": "混合型",
            },
            {
                "end_date": "2026-03-31",
                "stock_name": "立讯精密",
                "symbol": "002475.SZ",
                "mkv": 1_450_000_000,
                "stk_mkv_ratio": 6.3,
                "holding_change_flag": "new",
                "management": "中欧基金",
                "fund_type": "混合型",
            },
            {
                "end_date": "2026-03-31",
                "stock_name": "美的集团",
                "symbol": "000333.SZ",
                "mkv": 1_080_000_000,
                "stk_mkv_ratio": 4.7,
                "holding_change_flag": "decrease",
                "management": "中欧基金",
                "fund_type": "混合型",
            },
        ]
    )


def test_build_item_normalizes_existing_fund_and_holding_data():
    item = build_fund_watchlist_item(
        _watchlist_row(),
        _meta_df(),
        _holding_df(),
        nav_snapshot={
            "nav_date": "2026-07-15",
            "unit_nav": 2.1604,
            "daily_change_pct": -0.53,
            "source": "东方财富 / AkShare",
        },
    )
    assert item["fund_code"] == "001938.OF"
    assert item["fund_name"] == "中欧时代先锋"
    assert item["management"] == "中欧基金"
    assert item["fund_type"] == "混合型"
    assert item["issue_amount"] == 128.4
    assert item["holding_market_value"] == 43.5
    assert item["top10_ratio"] == 18.9
    assert item["holding_count"] == 3
    assert item["new_count"] == 1
    assert item["increase_count"] == 1
    assert item["decrease_count"] == 1
    assert item["latest_end_date"] == pd.Timestamp("2026-03-31")
    assert item["added_at"] == pd.Timestamp("2026-06-20")
    assert item["nav_date"] == pd.Timestamp("2026-07-15")
    assert item["unit_nav"] == 2.1604
    assert item["daily_change_pct"] == -0.53
    assert item["holdings"][0]["stock_name"] == "宁德时代"


def test_build_latest_snapshot_uses_latest_confirmed_row_and_source_change():
    snapshot = build_latest_fund_nav_snapshot(
        pd.DataFrame(
            [
                {"净值日期": "2026-07-14", "单位净值": 2.1720, "日增长率": 3.13},
                {"净值日期": "2026-07-15", "单位净值": 2.1604, "日增长率": -0.53},
            ]
        )
    )

    assert snapshot["nav_date"] == pd.Timestamp("2026-07-15")
    assert snapshot["unit_nav"] == 2.1604
    assert snapshot["daily_change_pct"] == -0.53
    assert snapshot["previous_unit_nav"] == 2.1720


def test_build_latest_snapshot_calculates_change_when_source_value_is_missing():
    snapshot = build_latest_fund_nav_snapshot(
        pd.DataFrame(
            [
                {"nav_date": "2026-07-14", "unit_nav": 1.0},
                {"nav_date": "2026-07-15", "unit_nav": 1.025},
            ]
        )
    )

    assert snapshot["daily_change_pct"] == pytest.approx(2.5)


def test_fetch_latest_snapshot_strips_market_suffix_and_stops_before_today():
    class FakeAkClient:
        def __init__(self):
            self.kwargs = None

        def fund_etf_fund_info_em(self, **kwargs):
            self.kwargs = kwargs
            return pd.DataFrame(
                [{"净值日期": "2026-07-15", "单位净值": 1.2345, "日增长率": 0.42}]
            )

    client = FakeAkClient()
    snapshot = fetch_latest_fund_nav_snapshot(
        "001938.OF",
        as_of_date=date(2026, 7, 16),
        lookback_days=30,
        ak_client=client,
    )

    assert client.kwargs == {
        "fund": "001938",
        "start_date": "20260615",
        "end_date": "20260715",
    }
    assert snapshot["unit_nav"] == 1.2345


def test_normalize_fund_code_for_nav_rejects_invalid_codes():
    assert normalize_fund_code_for_nav("510300.SH") == "510300"
    with pytest.raises(ValueError):
        normalize_fund_code_for_nav("not-a-fund")


def test_build_item_preserves_fund_when_one_query_failed():
    item = build_fund_watchlist_item(
        _watchlist_row(),
        pd.DataFrame(),
        pd.DataFrame(),
        load_error="持仓读取失败",
    )

    assert item["fund_name"] == "中欧时代先锋"
    assert item["top10_ratio"] is None
    assert item["holding_market_value"] is None
    assert item["holdings"] == []
    assert item["load_error"] == "持仓读取失败"


def test_summary_ignores_missing_values_and_counts_changes():
    first = build_fund_watchlist_item(_watchlist_row(), _meta_df(), _holding_df())
    second = {
        **first,
        "fund_code": "005827.OF",
        "top10_ratio": None,
        "latest_end_date": pd.NaT,
        "new_count": 2,
        "increase_count": 0,
        "decrease_count": 3,
    }

    summary = build_fund_watchlist_summary([first, second])

    assert summary["fund_count"] == 2
    assert summary["latest_end_date"] == pd.Timestamp("2026-03-31")
    assert summary["average_top10_ratio"] == 18.9
    assert summary["positive_change_count"] == 4
    assert summary["decrease_count"] == 4


def test_sort_and_table_use_the_same_normalized_models():
    base = build_fund_watchlist_item(_watchlist_row(), _meta_df(), _holding_df())
    other = {
        **base,
        "fund_code": "005827.OF",
        "fund_name": "易方达蓝筹精选",
        "top10_ratio": 62.4,
        "issue_amount": 425.1,
        "holding_market_value": 212.5,
        "latest_end_date": pd.Timestamp("2025-12-31"),
    }

    sorted_items = sort_fund_watchlist_items([base, other], "Top10 集中度")
    table = build_fund_watchlist_table(sorted_items)

    assert [item["fund_code"] for item in sorted_items] == ["005827.OF", "001938.OF"]
    assert table.iloc[0]["基金代码"] == "005827.OF"
    assert table.iloc[0]["Top10 集中度(%)"] == 62.4


def test_table_and_sort_expose_intraday_estimate_fields():
    base = build_fund_watchlist_item(_watchlist_row(), _meta_df(), _holding_df())
    first = {
        **base,
        "intraday_estimate_pct": 0.62,
        "intraday_covered_weight_pct": 18.9,
        "intraday_quote_count": 3,
        "intraday_holding_count": 3,
    }
    second = {
        **base,
        "fund_code": "005827.OF",
        "intraday_estimate_pct": -0.31,
        "intraday_covered_weight_pct": 12.5,
        "intraday_quote_count": 2,
        "intraday_holding_count": 3,
    }

    sorted_items = sort_fund_watchlist_items([second, first], "盘中估算")
    table = build_fund_watchlist_table(sorted_items)

    assert [item["fund_code"] for item in sorted_items] == ["001938.OF", "005827.OF"]
    assert table.iloc[0]["盘中估算(%)"] == 0.62
    assert table.iloc[0]["实时覆盖权重(%)"] == 18.9
    assert table.iloc[0]["实时行情"] == "3/3"


def test_table_and_sort_expose_confirmed_nav_fields():
    base = build_fund_watchlist_item(
        _watchlist_row(),
        _meta_df(),
        _holding_df(),
        nav_snapshot={
            "nav_date": "2026-07-15",
            "unit_nav": 2.1604,
            "daily_change_pct": -0.53,
        },
    )
    other = {
        **base,
        "fund_code": "005827.OF",
        "unit_nav": 1.5378,
        "daily_change_pct": 0.75,
    }

    sorted_items = sort_fund_watchlist_items([base, other], "日涨跌幅")
    table = build_fund_watchlist_table(sorted_items)

    assert [item["fund_code"] for item in sorted_items] == ["005827.OF", "001938.OF"]
    assert table.iloc[0]["净值日期"] == "2026-07-15"
    assert table.iloc[0]["前一日净值"] == 1.5378
    assert table.iloc[0]["日涨跌幅(%)"] == 0.75
