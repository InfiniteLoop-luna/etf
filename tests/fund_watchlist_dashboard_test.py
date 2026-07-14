import pandas as pd

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
    item = build_fund_watchlist_item(_watchlist_row(), _meta_df(), _holding_df())

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
    assert item["holdings"][0]["stock_name"] == "宁德时代"


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
