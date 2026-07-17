import pandas as pd
from sqlalchemy.sql.elements import TextClause

from src import security_data_cache


def test_load_fund_peer_comparison_uses_text_clause_for_named_params(monkeypatch):
    captured = {}

    def fake_read_sql(sql, engine, params=None):
        captured["sql"] = sql
        captured["engine"] = engine
        captured["params"] = params
        return pd.DataFrame(
            [
                {
                    "fund_code": "002001.OF",
                    "name": "测试基金B",
                    "management": "中欧基金管理有限公司",
                    "fund_type": "混合型",
                    "issue_amount": 88.8,
                    "latest_end_date": "2026-06-30",
                    "compare_reason": "同管理人 + 同类型",
                }
            ]
        )

    def fake_load_fund_object_model(code, top_n=10):
        return {
            "item": {
                "fund_name": f"基金{code}",
                "management": "中欧基金管理有限公司",
                "fund_type": "混合型",
                "issue_amount": 100.0,
                "nav_date": pd.Timestamp("2026-07-16"),
                "unit_nav": 1.2345,
                "daily_change_pct": 0.56,
                "closing_estimate_pct": 0.61,
                "estimate_deviation_pct": 0.05,
                "top10_ratio": 42.0,
                "latest_end_date": pd.Timestamp("2026-06-30"),
            }
        }

    security_data_cache.load_fund_peer_comparison.clear()
    monkeypatch.setattr(security_data_cache.pd, "read_sql", fake_read_sql)
    monkeypatch.setattr(
        security_data_cache,
        "load_fund_object_model",
        fake_load_fund_object_model,
    )
    monkeypatch.setattr(
        __import__("src.fund_hot_stocks", fromlist=["get_engine"]),
        "get_engine",
        lambda: object(),
    )

    result = security_data_cache.load_fund_peer_comparison(
        "018993.OF",
        fund_type="混合型",
        management="中欧基金管理有限公司",
        limit=6,
    )

    assert isinstance(captured["sql"], TextClause)
    assert captured["params"]["fund_code"] == "018993.OF"
    assert captured["params"]["peer_slots"] == 5
    assert result.iloc[0]["标记"] == "当前基金"
    assert result.iloc[1]["比较来源"] == "同管理人 + 同类型"
