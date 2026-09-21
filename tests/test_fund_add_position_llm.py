from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from src.fund_add_position_llm import (
    analyze_fund_add_position_payload,
    build_fund_add_position_fact_pack,
    normalize_fund_add_position_result,
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
    }


def _history(count=5):
    values = [-0.8, -0.4, 0.2, -0.3, -0.6]
    return [
        {
            "estimate_date": pd.Timestamp("2026-09-12") + pd.Timedelta(days=index),
            "estimate_pct": values[index],
            "covered_weight_pct": 65 + index,
            "quote_count": 8,
            "holding_count": 10,
            "source": "腾讯证券行情",
        }
        for index in range(count)
    ]


def test_build_fact_pack_summarizes_daily_estimate_history():
    fact_pack = build_fund_add_position_fact_pack(
        _item(),
        _history(),
        risk_profile="均衡",
        planned_budget=5000,
        max_batches=3,
        history_window=5,
    )

    summary = fact_pack["estimate_summary"]
    assert summary["day_count"] == 5
    assert summary["latest_pct"] == pytest.approx(-0.6)
    assert summary["negative_days"] == 4
    assert summary["current_streak"] == {"direction": "负向", "days": 2}
    assert fact_pack["user_constraints"]["planned_budget"] == 5000
    assert fact_pack["user_constraints"]["history_window"] == 5
    assert fact_pack["user_constraints"][
        "planned_budget_vs_current_holding_pct"
    ] == pytest.approx(23.1439)
    assert fact_pack["data_quality"]["sufficient_history"] is True


def test_fact_pack_tolerates_missing_snapshot_counts():
    history = _history()
    history[0]["quote_count"] = float("nan")
    history[0]["holding_count"] = None

    fact_pack = build_fund_add_position_fact_pack(_item(), history)

    assert fact_pack["daily_estimates"][0]["quote_count"] == 0
    assert fact_pack["daily_estimates"][0]["holding_count"] == 0


def test_normalizer_downgrades_aggressive_result_when_history_is_insufficient():
    fact_pack = build_fund_add_position_fact_pack(
        _item(),
        _history(count=2),
        risk_profile="进取",
    )
    normalized = normalize_fund_add_position_result(
        {
            "decision": "可分批加仓",
            "risk_level": "中",
            "confidence": 80,
            "execution_plan": {
                "batches": [
                    {"condition": "估值继续回落", "budget_pct": 50, "purpose": "首批"}
                ]
            },
        },
        fact_pack,
    )

    assert normalized["decision"] == "等待确认"
    assert normalized["execution_plan"]["batches"] == []
    assert "不足3个交易日" in normalized["guardrail_note"]


@patch("src.fund_add_position_llm.requests.post")
def test_analyzer_requests_json_and_normalizes_execution_plan(mock_post):
    config = SimpleNamespace(
        configured=True,
        base_url="https://api.deepseek.com",
        api_key="sk-test",
        model="deepseek-v4-flash",
        timeout_seconds=30,
        temperature=0.2,
        max_tokens=2400,
    )
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {
        "choices": [
            {
                "message": {
                    "content": (
                        '{"decision":"可分批加仓","risk_level":"中","confidence":68,'
                        '"summary":"等待回撤分批执行","rationale":["近期估值偏弱"],'
                        '"risks":["估值覆盖有限"],"preconditions":["不追涨"],'
                        '"execution_plan":{"batches":['
                        '{"condition":"单日估值低于-0.5%且覆盖率不下降",'
                        '"budget_pct":40,"purpose":"首批试探"},'
                        '{"condition":"后续实际净值确认未明显偏离",'
                        '"budget_pct":35,"purpose":"第二批确认"}],'
                        '"do_not_add_conditions":["估值覆盖明显下降"],'
                        '"review_trigger":"下一次净值公布后复核"}}'
                    )
                }
            }
        ]
    }
    mock_post.return_value = response
    fact_pack = build_fund_add_position_fact_pack(
        _item(),
        _history(),
        risk_profile="均衡",
        planned_budget=5000,
    )

    result = analyze_fund_add_position_payload(fact_pack, config=config)

    request_payload = mock_post.call_args.kwargs["json"]
    assert request_payload["response_format"] == {"type": "json_object"}
    assert "每日估值只是" in request_payload["messages"][0]["content"]
    assert result["decision"] == "可分批加仓"
    assert result["execution_plan"]["batches"][0]["budget_pct"] == 40
    assert result["model"] == "deepseek-v4-flash"
