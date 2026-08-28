from datetime import date

import pandas as pd

from src.etf_morning_report import (
    _fallback_markdown,
    build_report_digest,
    _build_llm_fact_pack,
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
        "etf_overview": {"category_share_rows": [{"secondary_category": "宽基"}]},
        "money_flow": {"ths_top_inflow": []},
        "fund_watchlist": {"funds": []},
        "data_quality": {"warnings": ["行业资金流数据缺失"]},
    })

    assert "ETF 晨报｜2026-08-27" in markdown
    assert "不等同于上一交易日实时持仓" in markdown
    assert "行业资金流数据缺失" in markdown


def test_llm_fact_pack_removes_verbose_company_text():
    compact = _build_llm_fact_pack({
        "report_trade_date": "2026-08-27",
        "fund_watchlist": {"funds": [{
            "fund_code": "000001.OF", "fund_name": "测试基金", "holding_count": 1,
            "industry_weight_summary": [{"industry": "半导体", "weight": 10}],
            "holdings": [{"symbol": "000001.SZ", "stock_name": "测试股", "stk_mkv_ratio": 10,
                          "stock_industry": "半导体", "stock_main_business": "x" * 1000,
                          "stock_product": "y" * 1000, "stock_introduction": "z" * 1000}]
        }]},
        "etf_overview": {"category_share_rows": []}, "money_flow": {}, "trend_recommendations": {},
    })
    holding = compact["fund_watchlist"]["funds"][0]["holdings"][0]
    assert "stock_main_business" not in holding
    assert "stock_product" not in holding
    assert "stock_introduction" not in holding


def test_generate_llm_markdown_falls_back_when_llm_unconfigured(monkeypatch):
    class Config:
        configured = False

    monkeypatch.setattr("src.etf_morning_report.load_stock_research_llm_config", lambda: Config())
    markdown, meta = generate_llm_markdown({"report_trade_date": "2026-08-27", "data_quality": {"warnings": []}})

    assert meta is None
    assert "结构化事实版报告" in markdown


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
