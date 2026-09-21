from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from src.fund_add_position_llm import (
    analyze_fund_position_payload,
    build_fund_market_context,
    build_fund_position_fact_pack,
    build_fund_position_prompts,
    normalize_fund_position_result,
)


def _item():
    return {
        "fund_code": "001938.OF",
        "fund_name": "中欧时代先锋",
        "fund_type": "混合型",
        "management": "中欧基金",
        "top10_ratio": 71.31,
        "latest_end_date": pd.Timestamp("2026-06-30"),
        "holding_shares": 10000,
        "holding_cost_amount": 20000,
        "actual_holding_amount": 21604,
        "current_holding_profit": 1604,
        "current_holding_profit_pct": 8.02,
        "estimated_daily_amount": -86.4,
        "unit_nav": 2.1604,
        "nav_date": pd.Timestamp("2026-09-18"),
        "intraday_estimate_pct": -0.4,
        "intraday_covered_weight_pct": 68.0,
        "intraday_top10_coverage_pct": 95.36,
        "intraday_quote_count": 2,
        "intraday_holding_count": 2,
        "intraday_updated_at": pd.Timestamp("2026-09-21 10:30:00+08:00"),
        "intraday_source": "腾讯证券行情",
        "holdings": [
            {
                "stock_name": "芯片甲",
                "symbol": "600001.SH",
                "industry": "电子",
                "subsector": "半导体",
                "weight": 38.0,
                "realtime_pct_change": -1.5,
                "estimate_contribution_pct": -0.57,
                "realtime_quote_time": pd.Timestamp("2026-09-21 10:30:00+08:00"),
                "change_label": "增持",
            },
            {
                "stock_name": "软件乙",
                "symbol": "000002.SZ",
                "industry": "计算机",
                "subsector": "软件服务",
                "weight": 30.0,
                "realtime_pct_change": 0.6,
                "estimate_contribution_pct": 0.18,
                "realtime_quote_time": pd.Timestamp("2026-09-21 10:30:00+08:00"),
                "change_label": "稳定",
            },
        ],
    }


def _market_context():
    return {
        "session": {
            "status": "active",
            "is_active": True,
            "market_date": "2026-09-21",
            "generated_at": "2026-09-21T10:30:00+08:00",
        },
        "major_indices": [
            {"name": "上证指数", "symbol": "000001.SH", "pct_change": 0.3},
            {"name": "深证成指", "symbol": "399001.SZ", "pct_change": -0.1},
            {"name": "创业板指", "symbol": "399006.SZ", "pct_change": -0.7},
        ],
        "confirmed_snapshot": {
            "trade_date": "2026-09-18",
            "breadth": {"advancer_count": 2600, "decliner_count": 2300},
            "turnover": {"total_amount_yi": 12500},
        },
    }


def _nav_history():
    dates = pd.bdate_range("2026-05-20", periods=80)
    return pd.DataFrame(
        {
            "净值日期": dates,
            "单位净值": [1.8 + index * 0.004 for index in range(len(dates))],
            "日增长率": [0.2] * len(dates),
        }
    )


def _fact_pack(**overrides):
    params = {
        "market_context": _market_context(),
        "nav_history": _nav_history(),
        "portfolio_items": [_item(), {"actual_holding_amount": 10000}],
        "risk_profile": "均衡",
        "holding_horizon": "中期配置",
        "planned_budget": 0,
        "max_adjustment_pct": 20,
    }
    params.update(overrides)
    return build_fund_position_fact_pack(_item(), **params)


def test_fact_pack_uses_current_position_today_market_sector_and_confirmed_nav():
    fact_pack = _fact_pack()

    assert fact_pack["today_estimate"]["estimate_pct"] == pytest.approx(-0.4)
    assert fact_pack["today_estimate"]["kind"] == "盘中实时估算"
    assert fact_pack["fund_exposure"]["sector_aggregates"][0]["sector"] == "半导体"
    assert fact_pack["confirmed_nav_trend"]["return_20d_pct"] is not None
    assert fact_pack["market_context"]["major_indices"][2]["name"] == "创业板指"
    assert fact_pack["position"]["fund_weight_in_recorded_portfolio_pct"] == pytest.approx(
        21604 / 31604 * 100
    )
    assert fact_pack["data_quality"]["completeness_score"] == 100


def test_zero_budget_means_unspecified_and_does_not_create_a_warning():
    fact_pack = _fact_pack(planned_budget=0)

    constraints = fact_pack["user_constraints"]
    assert constraints["planned_additional_budget"] is None
    assert constraints["budget_note"] == "未指定"
    assert not any("预算" in warning for warning in fact_pack["data_quality"]["warnings"])


def test_stale_estimate_is_labeled_as_previous_snapshot_instead_of_intraday():
    stale_item = _item()
    stale_item["intraday_updated_at"] = pd.Timestamp("2026-09-18 14:55:00+08:00")
    fact_pack = build_fund_position_fact_pack(
        stale_item,
        market_context=_market_context(),
        nav_history=_nav_history(),
        portfolio_items=[stale_item],
    )

    assert fact_pack["today_estimate"]["kind"] == "最近交易时点估算"
    assert any(
        "不是 2026-09-21 的盘中行情" in warning
        for warning in fact_pack["data_quality"]["warnings"]
    )


def test_market_context_extracts_only_market_fields_from_saved_report():
    saved_report = {
        "fact_pack": {
            "report_trade_date": "2026-09-18",
            "fund_watchlist": {"funds": [{"fund_code": "private"}]},
            "market_breadth": {"daily": [{"advancer_count": 3000, "decliner_count": 1900}]},
            "volume": {"daily": [{"total_amount_yi": 13200}]},
            "market_sentiment": {"limitup": [{"up_cnt": 70, "zha_cnt": 15}]},
            "money_flow": {
                "ths_top_inflow": [
                    {"industry": "电子", "net_amount_yi": 20, "pct_change": 1.1}
                ]
            },
            "data_quality": {"coverage_score": 92, "warnings": []},
        }
    }
    context = build_fund_market_context(
        index_quotes={
            "000001.SH": {
                "pct_change": 0.5,
                "price": 3500,
                "quote_time": pd.Timestamp("2026-09-21 10:30:00+08:00"),
                "source": "腾讯证券行情",
            }
        },
        market_state={
            "status": "active",
            "is_active": True,
            "market_date": "2026-09-21",
            "now": pd.Timestamp("2026-09-21 10:30:00+08:00"),
        },
        saved_morning_report=saved_report,
    )

    assert context["major_indices"][0]["name"] == "上证指数"
    assert context["confirmed_snapshot"]["breadth"]["advancer_count"] == 3000
    assert "fund_watchlist" not in str(context)


def test_prompts_request_multifactor_action_without_historical_estimate_gate():
    system_prompt, user_prompt = build_fund_position_prompts(_fact_pack())

    assert "加仓、持有还是减仓" in system_prompt
    assert "当日估值只是单一截面" in system_prompt
    assert "预算为null" in system_prompt
    assert "前十大持仓的实时贡献" in user_prompt
    assert "不足3个交易日" not in system_prompt + user_prompt


def test_normalizer_caps_action_to_user_limit_and_computes_reference_amount():
    fact_pack = _fact_pack(max_adjustment_pct=20)
    normalized = normalize_fund_position_result(
        {
            "action": "加仓",
            "action_strength": "中度",
            "time_horizon": "未来1-5个交易日",
            "risk_level": "中",
            "headline": "板块分化下分批加仓",
            "summary": "综合判断偏向分批加仓。",
            "signal_alignment": {
                "level": "中",
                "supporting": ["净值中期趋势向上"],
                "conflicting": ["当日半导体回落"],
            },
            "factor_assessment": [
                {
                    "factor": "板块强弱",
                    "signal": "利空",
                    "importance": "高",
                    "analysis": "半导体贡献为负。",
                }
            ],
            "position_diagnosis": {},
            "action_plan": {
                "change_pct_range": {"min": 10, "max": 35},
                "batches": [
                    {
                        "action": "加仓",
                        "pct": 12,
                        "trigger": "估值跌幅收窄",
                        "invalidation": "创业板继续放量下跌",
                        "purpose": "首批确认",
                    },
                    {
                        "action": "加仓",
                        "pct": 12,
                        "trigger": "核心板块转正",
                        "invalidation": "跌破前低",
                        "purpose": "第二批",
                    },
                ],
            },
            "scenario_plan": [],
            "key_risks": [],
            "data_limitations": [],
            "next_review_trigger": "收盘后复核净值",
        },
        fact_pack,
    )

    plan = normalized["action_plan"]
    assert normalized["action"] == "加仓"
    assert plan["change_pct_range"]["max"] == 20
    assert sum(row["pct_of_current_position"] for row in plan["batches"]) == 20
    assert plan["batches"][0]["reference_amount"] == pytest.approx(2592.48)
    assert normalized["data_completeness"] == 100


def test_normalizer_caps_batches_to_model_change_range():
    normalized = normalize_fund_position_result(
        {
            "action": "减仓",
            "action_strength": "轻度",
            "action_plan": {
                "change_pct_range": {"min": 5, "max": 10},
                "batches": [
                    {
                        "action": "减仓",
                        "pct": 8,
                        "trigger": "板块继续转弱",
                        "invalidation": "板块重新转强",
                        "purpose": "第一批",
                    },
                    {
                        "action": "减仓",
                        "pct": 8,
                        "trigger": "指数跌破支撑",
                        "invalidation": "指数收复支撑",
                        "purpose": "第二批",
                    },
                ],
            },
        },
        _fact_pack(max_adjustment_pct=20),
    )

    assert sum(row["pct_of_current_position"] for row in normalized["action_plan"]["batches"]) == 10


def test_hold_action_clears_any_model_generated_trade_batches():
    normalized = normalize_fund_position_result(
        {
            "action": "持有",
            "action_strength": "明显",
            "signal_alignment": {"level": "中"},
            "action_plan": {
                "change_pct_range": {"min": 10, "max": 20},
                "batches": [
                    {
                        "action": "加仓",
                        "pct": 10,
                        "trigger": "上涨",
                        "invalidation": "下跌",
                        "purpose": "错误批次",
                    }
                ],
            },
        },
        _fact_pack(),
    )

    assert normalized["action"] == "持有"
    assert normalized["action_strength"] == "不操作"
    assert normalized["action_plan"]["batches"] == []


@patch("src.fund_add_position_llm.requests.post")
def test_analyzer_sends_transparent_multifactor_prompt_and_normalizes_json(mock_post):
    config = SimpleNamespace(
        configured=True,
        base_url="https://api.deepseek.com",
        api_key="sk-test",
        model="deepseek-v4-flash",
        timeout_seconds=30,
        temperature=0.2,
        max_tokens=3200,
    )
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {
        "choices": [
            {
                "message": {
                    "content": (
                        '{"action":"减仓","action_strength":"轻度",'
                        '"time_horizon":"未来1-5个交易日","risk_level":"中",'
                        '"headline":"板块转弱时轻度减仓","summary":"控制集中风险",'
                        '"signal_alignment":{"level":"中","supporting":["板块偏弱"],'
                        '"conflicting":["净值趋势仍向上"]},"factor_assessment":['
                        '{"factor":"当前持仓","signal":"中性","importance":"高",'
                        '"analysis":"已有浮盈但组合占比较高"},'
                        '{"factor":"市场环境","signal":"利空","importance":"高",'
                        '"analysis":"主要指数转弱"},'
                        '{"factor":"基金净值趋势","signal":"利多","importance":"中",'
                        '"analysis":"确认净值趋势仍向上"}],'
                        '"position_diagnosis":{"profit_loss_state":"已有浮盈",'
                        '"concentration_risk":"集中度较高","cost_position_comment":"成本有安全垫"},'
                        '"action_plan":{"change_pct_range":{"min":5,"max":10},"batches":['
                        '{"action":"减仓","pct":8,"trigger":"核心板块继续走弱",'
                        '"invalidation":"板块放量转强","purpose":"降低集中风险"}]},'
                        '"scenario_plan":[{"scenario":"转弱","observable_trigger":"指数转负",'
                        '"response":"执行减仓"},{"scenario":"转强","observable_trigger":"板块转正",'
                        '"response":"暂缓减仓"}],"key_risks":["披露持仓可能变化"],'
                        '"data_limitations":[],"next_review_trigger":"收盘后复核"}'
                    )
                }
            }
        ]
    }
    mock_post.return_value = response

    result = analyze_fund_position_payload(_fact_pack(), config=config)

    request_payload = mock_post.call_args.kwargs["json"]
    system_prompt = request_payload["messages"][0]["content"]
    user_prompt = request_payload["messages"][1]["content"]
    assert "多因素仓位研判" in system_prompt
    assert "market_context" in user_prompt
    assert "fund_exposure" in user_prompt
    assert result["action"] == "减仓"
    assert result["action_plan"]["batches"][0]["action"] == "减仓"
    assert result["model"] == "deepseek-v4-flash"
    assert result["request_attempt"] == 1


@patch("src.fund_add_position_llm.requests.post")
def test_analyzer_retries_when_first_json_is_structurally_valid_but_empty(mock_post):
    config = SimpleNamespace(
        configured=True,
        base_url="https://api.deepseek.com",
        api_key="sk-test",
        model="deepseek-v4-flash",
        timeout_seconds=30,
        temperature=0.2,
        max_tokens=3200,
    )
    empty_response = Mock()
    empty_response.raise_for_status.return_value = None
    empty_response.json.return_value = {
        "choices": [{"message": {"content": '{"action":"持有","summary":"信息不足"}'}}]
    }
    complete_response = Mock()
    complete_response.raise_for_status.return_value = None
    complete_response.json.return_value = {
        "choices": [
            {
                "message": {
                    "content": (
                        '{"action":"持有","action_strength":"不操作",'
                        '"summary":"多因素信号冲突，等待更清晰确认。",'
                        '"signal_alignment":{"level":"低","supporting":["净值趋势向上"],'
                        '"conflicting":["板块当日偏弱"]},"factor_assessment":['
                        '{"factor":"当前持仓","signal":"中性","analysis":"已有安全垫"},'
                        '{"factor":"当日估值","signal":"利空","analysis":"估值偏弱"},'
                        '{"factor":"市场环境","signal":"中性","analysis":"市场分化"}],'
                        '"action_plan":{"change_pct_range":{"min":0,"max":0},"batches":[]},'
                        '"scenario_plan":[{"scenario":"转强","observable_trigger":"板块转正",'
                        '"response":"再评估加仓"},{"scenario":"转弱","observable_trigger":"指数走弱",'
                        '"response":"再评估减仓"}],"next_review_trigger":"收盘后复核净值"}'
                    )
                }
            }
        ]
    }
    mock_post.side_effect = [empty_response, complete_response]

    result = analyze_fund_position_payload(_fact_pack(), config=config)

    assert mock_post.call_count == 2
    assert result["action"] == "持有"
    assert len(result["factor_assessment"]) == 3
    assert result["request_attempt"] == 2
    retry_user_prompt = mock_post.call_args_list[1].kwargs["json"]["messages"][1]["content"]
    assert retry_user_prompt.startswith("上一次输出虽然可能是JSON")
