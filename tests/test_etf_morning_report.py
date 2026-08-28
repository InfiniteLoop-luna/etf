from datetime import date

import pandas as pd

from src.etf_morning_report import (
    _fallback_markdown,
    _build_industry_etf_groups,
    build_report_digest,
    find_previous_trade_date,
    generate_llm_markdown,
    save_report,
)


class FakeEngine:
    pass


def test_find_previous_trade_date_uses_latest_available_source(monkeypatch):
    values = {
        "etf_share_size": pd.DataFrame([{"latest_date": date(2026, 8, 27)}]),
        "ts_stock_daily": pd.DataFrame([{"latest_date": date(2026, 8, 27)}]),
        "ts_moneyflow": pd.DataFrame([{"latest_date": date(2026, 8, 26)}]),
        "ts_margin": pd.DataFrame([{"latest_date": date(2026, 8, 27)}]),
        "ts_limit_list_d": pd.DataFrame([{"latest_date": date(2026, 8, 27)}]),
    }

    def fake_query(engine, sql, params=None):
        for table in values:
            if f"FROM {table}" in sql:
                return values[table]
        return pd.DataFrame()

    monkeypatch.setattr("src.etf_morning_report._query_frame", fake_query)
    assert find_previous_trade_date(FakeEngine(), today=date(2026, 8, 28)) == "2026-08-27"


def test_industry_etf_groups_keep_all_etfs_under_industry():
    frame = pd.DataFrame([
        {"industry": "半导体", "ts_code": "159000.SZ", "industry_etf": "半导体ETF", "share_growth_pct": 5.0},
        {"industry": "半导体", "ts_code": "159001.SZ", "industry_etf": "芯片ETF", "share_growth_pct": -2.0},
        {"industry": "通信", "ts_code": "159002.SZ", "industry_etf": "通信ETF", "share_growth_pct": 1.0},
    ])
    groups = _build_industry_etf_groups(frame)

    assert [group["industry"] for group in groups] == ["半导体", "通信"]
    assert len(groups[0]["etfs"]) == 2
    assert groups[0]["etfs"][0]["ts_code"] == "159000.SZ"


def test_build_report_digest_calculates_risk_light_and_top_sector():
    digest = build_report_digest({
        "fund_watchlist": {"funds": [{"fund_name": "测试基金", "fund_code": "000001.OF"}]},
        "money_flow": {"ths_top_inflow": [{"industry": "半导体", "net_amount": 123.4}], "dc_top_inflow": []},
        "trend_recommendations": {"top_uptrend": [{"name": "甲"}], "top_avoid": [{"name": "乙"}]},
        "market_sentiment": {"limitup": [{"up_cnt": 20, "zha_cnt": 2}]},
        "data_quality": {"warnings": []},
    })

    assert digest["risk_color"] == "绿色"
    assert digest["top_sector"] == "半导体"
    assert digest["fund_count"] == 1
    assert digest["limitup_count"] == 20


def test_fallback_markdown_explains_holdings_disclosure_limit():
    markdown = _fallback_markdown({
        "report_trade_date": "2026-08-27",
        "etf_overview": {"category_share_rows": [{"secondary_category": "宽基"}], "industry_etf_growth": [], "industry_etf_groups": []},
        "money_flow": {"ths_top_inflow": [], "dc_top_inflow": []},
        "fund_watchlist": {"funds": []},
        "market_sentiment": {"limitup": []}, "northbound": {"daily": []},
        "margin": {"daily": []}, "volume": {"daily": []},
        "trend_recommendations": {}, "data_quality": {"warnings": ["行业资金流数据缺失"]},
    })

    assert "ETF 晨报｜2026-08-27" in markdown
    assert "不等同于上一交易日实时持仓" in markdown
    assert "行业资金流数据缺失" in markdown


def test_fallback_markdown_lists_fund_change_not_holdings():
    markdown = _fallback_markdown({
        "report_trade_date": "2026-08-27",
        "etf_overview": {"category_share_rows": [], "industry_etf_growth": [], "industry_etf_groups": []},
        "money_flow": {"ths_top_inflow": [], "dc_top_inflow": []},
        "fund_watchlist": {"funds": [{"fund_name": "测试基金", "fund_code": "000001.OF", "nav_date": "2026-08-27", "daily_change_pct": 1.23}]},
        "market_sentiment": {"limitup": []}, "northbound": {"daily": []},
        "margin": {"daily": []}, "volume": {"daily": []},
        "trend_recommendations": {}, "data_quality": {"warnings": []},
    })
    assert "测试基金（000001.OF）" in markdown
    assert "上一交易日涨跌幅：+1.23%" in markdown
    assert "前十大持仓可用" not in markdown


def test_save_report_marks_llm_or_fact_mode(tmp_path, monkeypatch):
    monkeypatch.setattr("src.etf_morning_report.REPORT_DIR", tmp_path)
    llm_saved = save_report({"report_trade_date": "2026-08-27"}, "# report\n", {"model": "demo"})
    facts_saved = save_report({"report_trade_date": "2026-08-28"}, "# report\n")

    assert llm_saved["report_mode"] == "llm"
    assert facts_saved["report_mode"] == "facts"


def test_save_report_writes_latest_and_date_files(tmp_path, monkeypatch):
    monkeypatch.setattr("src.etf_morning_report.REPORT_DIR", tmp_path)
    saved = save_report({"report_trade_date": "2026-08-27"}, "# report\n", {"model": "demo"})

    assert saved["llm"]["model"] == "demo"
    assert (tmp_path / "2026-08-27.md").read_text(encoding="utf-8") == "# report\n"
    assert (tmp_path / "latest.json").exists()
