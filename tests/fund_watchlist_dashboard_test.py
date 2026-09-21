from datetime import date

import pandas as pd
import pytest
from sqlalchemy import create_engine

from src.fund_estimate_snapshot_store import (
    get_fund_estimate_snapshot,
    get_latest_fund_estimate_snapshot,
    list_fund_estimate_snapshot_history,
    upsert_fund_estimate_snapshot,
)
from src.fund_nav import (
    build_latest_fund_nav_snapshot,
    fetch_latest_fund_nav_snapshot,
    normalize_fund_code_for_nav,
)
from src.fund_watchlist_dashboard import (
    attach_current_position_metrics,
    attach_estimated_daily_amount,
    build_fund_holding_industry_heatmap_frame,
    build_fund_watchlist_item,
    build_fund_watchlist_summary,
    build_fund_watchlist_table,
    calculate_estimated_daily_amount,
    sort_fund_watchlist_items,
)


def _watchlist_row():
    return pd.Series(
        {
            "ts_code": "001938.OF",
            "security_name": "中欧时代先锋",
            "holding_shares": 10000,
            "holding_cost_amount": 20000,
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
                "stock_industry": "电池",
                "stock_market": "创业板",
                "stock_main_business": "动力电池与储能电池研发生产",
                "stock_product": "锂离子电池",
                "stock_introduction": "",
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
    assert item["holding_shares"] == 10000
    assert item["holding_cost_amount"] == 20000
    assert item["holdings"][0]["stock_name"] == "宁德时代"
    assert item["holdings"][0]["industry"] == "电池"
    assert item["holdings"][0]["market"] == "创业板"
    assert item["holdings"][0]["subsector"] == "锂电池"
    assert item["holdings"][1]["industry"] == "未识别"


def test_build_industry_heatmap_frame_keeps_industry_stock_and_positive_weight():
    item = build_fund_watchlist_item(_watchlist_row(), _meta_df(), _holding_df())
    item["holdings"].append(
        {
            "stock_name": "零权重股票",
            "symbol": "000000.SZ",
            "industry": "其他",
            "weight": 0,
        }
    )

    frame = build_fund_holding_industry_heatmap_frame(item["holdings"])

    assert sorted(frame["持仓权重(%)"].tolist(), reverse=True) == [7.9, 6.3, 4.7]
    ningde = frame[frame["股票代码"] == "300750.SZ"].iloc[0]
    assert ningde["所属行业"] == "电池"
    assert ningde["持仓股票"] == "宁德时代（300750.SZ）"
    assert ningde["细分板块"] == "锂电池"
    assert set(frame["所属行业"]) == {"电池", "未识别"}


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


def test_closing_estimate_deviation_only_uses_the_same_nav_date():
    matched = build_fund_watchlist_item(
        _watchlist_row(),
        _meta_df(),
        _holding_df(),
        nav_snapshot={
            "nav_date": "2026-07-15",
            "unit_nav": 2.1604,
            "daily_change_pct": -0.53,
        },
        estimate_snapshot={
            "estimate_date": "2026-07-15",
            "estimate_pct": -0.40,
            "covered_weight_pct": 18.9,
            "quote_time": "2026-07-15T15:00:00+08:00",
        },
    )
    mismatched = build_fund_watchlist_item(
        _watchlist_row(),
        _meta_df(),
        _holding_df(),
        nav_snapshot={
            "nav_date": "2026-07-15",
            "unit_nav": 2.1604,
            "daily_change_pct": -0.53,
        },
        estimate_snapshot={
            "estimate_date": "2026-07-16",
            "estimate_pct": 0.80,
        },
    )

    assert matched["closing_estimate_date"] == pd.Timestamp("2026-07-15")
    assert matched["closing_estimate_pct"] == -0.40
    assert matched["estimate_deviation_pct"] == pytest.approx(0.13)
    assert matched["closing_estimate_covered_weight_pct"] == 18.9
    assert mismatched["closing_estimate_pct"] is None
    assert mismatched["estimate_deviation_pct"] is None

    table = build_fund_watchlist_table([matched])
    assert table.iloc[0]["15:00估值(%)"] == -0.40
    assert table.iloc[0]["估值偏差(百分点)"] == pytest.approx(0.13)


def test_estimated_daily_amount_uses_position_nav_and_percentage():
    assert calculate_estimated_daily_amount(10000, 2.1604, 0.62) == pytest.approx(133.9448)
    assert calculate_estimated_daily_amount(10000, 2.1604, -0.62) == pytest.approx(-133.9448)
    assert calculate_estimated_daily_amount(10000, 2.1604, 0.0) == 0.0
    assert calculate_estimated_daily_amount(None, 2.1604, 0.62) is None
    assert calculate_estimated_daily_amount(10000, None, 0.62) is None


def test_attach_estimated_daily_amount_aligns_estimate_and_nav_dates():
    base = {
        "holding_shares": 10000,
        "nav_date": pd.Timestamp("2026-07-15"),
        "unit_nav": 2.1604,
        "previous_nav_date": pd.Timestamp("2026-07-14"),
        "previous_unit_nav": 2.1720,
        "intraday_estimate_pct": 0.62,
        "latest_closing_estimate_pct": 9.99,
        "latest_closing_estimate_date": pd.Timestamp("2026-07-15"),
    }

    intraday = attach_estimated_daily_amount(base, intraday_date="2026-07-16")
    assert intraday["estimated_daily_amount"] == pytest.approx(133.9448)
    assert intraday["estimated_daily_base_nav"] == 2.1604
    assert intraday["estimated_daily_amount_source"] == "盘中估算"

    same_nav_day = attach_estimated_daily_amount(
        {**base, "intraday_estimate_pct": None, "latest_closing_estimate_pct": -0.40},
        intraday_date="2026-07-15",
    )
    assert same_nav_day["estimated_daily_amount"] == pytest.approx(-86.88)
    assert same_nav_day["estimated_daily_base_nav"] == 2.1720
    assert same_nav_day["estimated_daily_amount_source"] == "15:00估值"

    stale = attach_estimated_daily_amount(
        {
            **base,
            "intraday_estimate_pct": None,
            "latest_closing_estimate_date": pd.Timestamp("2026-07-13"),
        }
    )
    assert stale["estimated_daily_amount"] is None

    prior_day_snapshot = attach_estimated_daily_amount(
        {
            **base,
            "intraday_estimate_pct": None,
            "latest_closing_estimate_pct": -0.40,
        },
        intraday_date="2026-07-16",
    )
    assert prior_day_snapshot["estimated_daily_amount"] is None
    assert pd.isna(prior_day_snapshot["estimated_daily_amount_date"])
    assert prior_day_snapshot["estimated_daily_amount_source"] == ""


def test_current_position_metrics_use_confirmed_nav_not_intraday_estimate():
    item = attach_current_position_metrics(
        {
            "holding_shares": 10000,
            "holding_cost_amount": 20000,
            "unit_nav": 2.1604,
            "nav_date": pd.Timestamp("2026-07-15"),
            "estimated_unit_nav": 2.25,
            "estimated_daily_amount_date": pd.Timestamp("2026-07-16"),
            "estimated_daily_amount_source": "盘中估算",
        }
    )

    assert item["actual_holding_amount"] == pytest.approx(21604)
    assert item["actual_holding_amount_nav"] == pytest.approx(2.1604)
    assert item["actual_holding_amount_date"] == pd.Timestamp("2026-07-15")
    assert item["actual_holding_amount_source"] == "最新确认净值"
    assert item["current_holding_profit"] == pytest.approx(1604)
    assert item["current_holding_profit_pct"] == pytest.approx(8.02)


def test_current_position_metrics_keep_value_when_cost_is_missing():
    item = attach_current_position_metrics(
        {
            "holding_shares": 10000,
            "holding_cost_amount": None,
            "unit_nav": 2.1604,
            "nav_date": pd.Timestamp("2026-07-15"),
        }
    )

    assert item["actual_holding_amount"] == pytest.approx(21604)
    assert item["current_holding_profit"] is None
    assert item["current_holding_profit_pct"] is None


def test_current_position_metrics_support_negative_profit():
    item = attach_current_position_metrics(
        {
            "holding_shares": 10000,
            "holding_cost_amount": 25000,
            "unit_nav": 2.1604,
            "nav_date": pd.Timestamp("2026-07-15"),
        }
    )

    assert item["actual_holding_amount"] == pytest.approx(21604)
    assert item["current_holding_profit"] == pytest.approx(-3396)
    assert item["current_holding_profit_pct"] == pytest.approx(-13.584)


def test_summary_uses_cost_weighted_return_and_reports_partial_coverage():
    first = attach_current_position_metrics(
        {
            "holding_shares": 10000,
            "holding_cost_amount": 20000,
            "unit_nav": 2.1604,
            "nav_date": pd.Timestamp("2026-07-15"),
            "latest_end_date": pd.NaT,
            "top10_ratio": None,
        }
    )
    second = attach_current_position_metrics(
        {
            "holding_shares": 5000,
            "holding_cost_amount": 12000,
            "unit_nav": 2.0,
            "nav_date": pd.Timestamp("2026-07-14"),
            "latest_end_date": pd.NaT,
            "top10_ratio": None,
        }
    )
    third = attach_current_position_metrics(
        {
            "holding_shares": 1000,
            "holding_cost_amount": None,
            "unit_nav": 1.5,
            "nav_date": pd.Timestamp("2026-07-13"),
            "latest_end_date": pd.NaT,
            "top10_ratio": None,
        }
    )

    summary = build_fund_watchlist_summary([first, second, third])

    assert summary["position_count"] == 3
    assert summary["actual_holding_amount"] == pytest.approx(33104)
    assert summary["actual_holding_amount_count"] == 3
    assert summary["actual_holding_amount_min_date"] == pd.Timestamp("2026-07-13")
    assert summary["actual_holding_amount_max_date"] == pd.Timestamp("2026-07-15")
    assert summary["current_holding_profit"] == pytest.approx(-396)
    assert summary["current_holding_profit_count"] == 2
    assert summary["current_holding_profit_pct"] == pytest.approx(-1.2375)


def test_summary_and_table_include_personal_position_estimate():
    item = attach_current_position_metrics(
        attach_estimated_daily_amount(
            {
                **build_fund_watchlist_item(
                    _watchlist_row(),
                    _meta_df(),
                    _holding_df(),
                    nav_snapshot={
                        "nav_date": "2026-07-15",
                        "unit_nav": 2.1604,
                        "previous_nav_date": "2026-07-14",
                        "previous_unit_nav": 2.1720,
                    },
                ),
                "intraday_estimate_pct": 0.62,
            },
            intraday_date="2026-07-16",
        )
    )

    summary = build_fund_watchlist_summary([item])
    table = build_fund_watchlist_table([item])

    assert summary["estimated_daily_amount"] == pytest.approx(133.9448)
    assert summary["estimated_daily_amount_count"] == 1
    assert summary["actual_holding_amount"] == pytest.approx(21604)
    assert summary["current_holding_profit"] == pytest.approx(1604)
    assert summary["current_holding_profit_pct"] == pytest.approx(8.02)
    assert table.iloc[0]["持有份额"] == 10000
    assert table.iloc[0]["持仓成本金额(元)"] == 20000
    assert table.iloc[0]["实际持仓金额(元)"] == pytest.approx(21604)
    assert table.iloc[0]["当前持仓收益(元)"] == pytest.approx(1604)
    assert table.iloc[0]["预计增减金额(元)"] == pytest.approx(133.9448)
    assert table.iloc[0]["金额估值日期"] == "2026-07-16"


def test_summary_does_not_mix_estimated_amounts_from_different_dates():
    summary = build_fund_watchlist_summary(
        [
            {
                "latest_end_date": pd.NaT,
                "top10_ratio": None,
                "new_count": 0,
                "increase_count": 0,
                "decrease_count": 0,
                "holding_shares": 1000,
                "estimated_daily_amount": 12.0,
                "estimated_daily_amount_date": pd.Timestamp("2026-07-16"),
            },
            {
                "latest_end_date": pd.NaT,
                "top10_ratio": None,
                "new_count": 0,
                "increase_count": 0,
                "decrease_count": 0,
                "holding_shares": 2000,
                "estimated_daily_amount": 99.0,
                "estimated_daily_amount_date": pd.Timestamp("2026-07-15"),
            },
        ]
    )

    assert summary["estimated_daily_amount"] == 12.0
    assert summary["estimated_daily_amount_date"] == pd.Timestamp("2026-07-16")
    assert summary["estimated_daily_amount_count"] == 1
    assert summary["position_count"] == 2


def test_summary_ignores_estimated_amounts_outside_target_date():
    summary = build_fund_watchlist_summary(
        [
            {
                "latest_end_date": pd.NaT,
                "top10_ratio": None,
                "new_count": 0,
                "increase_count": 0,
                "decrease_count": 0,
                "holding_shares": 1000,
                "estimated_daily_amount": 12.0,
                "estimated_daily_amount_date": pd.Timestamp("2026-07-15"),
            }
        ],
        target_date="2026-07-16",
    )

    assert summary["estimated_daily_amount"] is None
    assert pd.isna(summary["estimated_daily_amount_date"])
    assert summary["estimated_daily_amount_count"] == 0


def test_estimate_snapshot_store_round_trips_one_fund_date():
    engine = create_engine("sqlite:///:memory:")
    upsert_fund_estimate_snapshot(
        engine,
        {
            "fund_code": "001938.OF",
            "estimate_date": "2026-07-15",
            "estimate_pct": -0.40,
            "covered_weight_pct": 18.9,
            "top10_coverage_pct": 100.0,
            "quote_count": 3,
            "holding_count": 3,
            "quote_time": "2026-07-15T15:00:00+08:00",
            "holding_end_date": "2026-03-31",
            "source": "腾讯证券行情",
        },
    )

    snapshot = get_fund_estimate_snapshot(
        engine,
        "001938.OF",
        "2026-07-15",
    )

    assert snapshot["estimate_date"] == pd.Timestamp("2026-07-15")
    assert snapshot["estimate_pct"] == -0.40
    assert snapshot["covered_weight_pct"] == 18.9
    assert snapshot["quote_count"] == 3

    upsert_fund_estimate_snapshot(
        engine,
        {
            "fund_code": "001938.OF",
            "estimate_date": "2026-07-15",
            "estimate_pct": 9.99,
            "quote_time": "2026-07-15T16:00:00+08:00",
        },
    )
    retained_snapshot = get_fund_estimate_snapshot(
        engine,
        "001938.OF",
        "2026-07-15",
    )
    assert retained_snapshot["estimate_pct"] == -0.40

    latest_snapshot = get_latest_fund_estimate_snapshot(engine, "001938.OF")
    assert latest_snapshot["estimate_date"] == pd.Timestamp("2026-07-15")


def test_estimate_snapshot_history_returns_recent_days_newest_first():
    engine = create_engine("sqlite:///:memory:")
    for day, estimate_pct in [
        ("2026-07-15", -0.4),
        ("2026-07-16", 0.2),
        ("2026-07-17", -0.7),
    ]:
        upsert_fund_estimate_snapshot(
            engine,
            {
                "fund_code": "001938.OF",
                "estimate_date": day,
                "estimate_pct": estimate_pct,
                "quote_time": f"{day}T15:00:00+08:00",
            },
        )

    history = list_fund_estimate_snapshot_history(
        engine,
        "001938.OF",
        limit=2,
    )

    assert [row["estimate_date"] for row in history] == [
        pd.Timestamp("2026-07-17"),
        pd.Timestamp("2026-07-16"),
    ]
    assert [row["estimate_pct"] for row in history] == [-0.7, 0.2]
