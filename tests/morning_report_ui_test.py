from src.etf_morning_report import build_evidence_ledger
from src.morning_report_ui import build_morning_report_dashboard_html


def _fact_pack():
    fact_pack = {
        "report_trade_date": "2026-09-16",
        "generated_at": "2026-09-17T08:30:00+08:00",
        "data_quality": {
            "report_status": "complete",
            "coverage_score": 92,
            "warnings": [],
            "sources": [
                {"key": "stock_daily", "label": "A股日行情", "required": True, "status": "fresh", "latest_date": "2026-09-16", "row_count": 5300},
                {"key": "limit_sentiment", "label": "涨停情绪", "required": True, "status": "fresh", "latest_date": "2026-09-16", "row_count": 1},
                {"key": "ths_industry_flow", "label": "THS行业资金流", "required": False, "status": "fresh", "latest_date": "2026-09-16", "row_count": 90},
                {"key": "etf_share", "label": "ETF份额", "required": True, "status": "fresh", "latest_date": "2026-09-16", "row_count": 600},
            ],
        },
        "market_breadth": {
            "daily": [{
                "trade_date": "2026-09-16",
                "advancer_count": 3280,
                "decliner_count": 1760,
                "flat_count": 120,
                "strong_advancer_count": 188,
                "strong_decliner_count": 45,
                "median_pct_chg": 0.68,
            }]
        },
        "market_sentiment": {"limitup": [{"up_cnt": 68, "zha_cnt": 12, "total_cnt": 80}]},
        "volume": {"daily": [{"trade_date": "2026-09-16", "total_amount_yi": 15328.6}]},
        "northbound": {"daily": [{"trade_date": "2026-09-16", "north_money_yi": 35.2}]},
        "margin": {"daily": [{"trade_date": "2026-09-16", "financing_net_buy_yi": 28.4, "financing_balance_yi": 19120.0}]},
        "dragon_tiger": {"daily": [{"distinct_stock_count": 52}]},
        "money_flow": {
            "ths_top_inflow": [
                {"industry": "半导体", "net_amount_yi": 42.6, "pct_change": 2.1, "lead_stock": "示例科技"},
                {"industry": "创新药", "net_amount_yi": 18.3, "pct_change": 1.2, "lead_stock": "示例医药"},
            ],
            "dc_top_inflow": [{"industry": "电子", "net_amount_yi": 38.2, "pct_change": 1.8}],
        },
        "etf_overview": {
            "industry_etf_groups": [
                {
                    "industry": "半导体",
                    "etf_count": 2,
                    "current_date": "2026-09-16",
                    "share_growth_pct": 2.35,
                    "share_change_yi": 8.2,
                    "current_share_yi": 357.0,
                    "etfs": [{"etf_name": "芯片ETF", "ts_code": "159995.SZ", "share_growth_pct": 2.5, "share_change_yi": 5.1, "current_share_yi": 210.0}],
                },
                {
                    "industry": "通信",
                    "etf_count": 1,
                    "current_date": "2026-09-16",
                    "share_growth_pct": -1.2,
                    "share_change_yi": -2.4,
                    "current_share_yi": 198.0,
                    "etfs": [],
                },
            ]
        },
        "trend_recommendations": {
            "trade_date": "2026-09-16",
            "top_uptrend": [{"name": "示例科技", "ts_code": "600000.SH", "industry": "半导体", "trend_score": 81, "prob_up_5d": 0.67}],
            "top_avoid": [{"name": "示例通信", "ts_code": "000001.SZ", "industry": "通信", "risk_score": 76, "prob_up_5d": 0.31}],
        },
        "fund_watchlist": {"funds": [{"fund_name": "示例基金", "fund_code": "000001.OF", "nav_date": "2026-09-16", "daily_change_pct": 1.25}]},
    }
    fact_pack["evidence"] = build_evidence_ledger(fact_pack)
    return fact_pack


def test_dashboard_uses_five_step_review_structure_and_key_metrics():
    fact_pack = _fact_pack()
    report = {
        "report_mode": "llm",
        "llm": {"analysis": {"headline": "资金与份额形成局部共振", "summary": {"text": "市场广度改善，主线仍需开盘后确认。"}}},
    }

    html = build_morning_report_dashboard_html(fact_pack, report)

    assert "A股 · ETF 晨间复盘" in html
    assert all(title in html for title in ["大盘环境", "市场情绪与赚钱效应", "板块复盘", "核心标的验证", "今日总判断"])
    assert "3280 / 1760" in html
    assert "+0.68%" in html
    assert "15,329 亿元" in html
    assert "炸板率" in html and "15.0%" in html
    assert "示例科技" in html and "示例通信" in html


def test_dashboard_surfaces_cross_signal_sector_and_complete_details():
    html = build_morning_report_dashboard_html(_fact_pack(), {"report_mode": "facts"})

    assert "优先验证半导体等资金流与 ETF 份额共振方向的持续性" in html
    assert "ETF 份额变化明细" in html
    assert "行业与板块资金流" in html
    assert "自选基金确认净值" in html
    assert "数据源就绪度" in html
    assert "@media(max-width:760px)" in html


def test_dashboard_escapes_untrusted_report_text_and_handles_missing_data():
    fact_pack = {
        "report_trade_date": "2026-09-16",
        "data_quality": {"report_status": "partial", "coverage_score": 20, "warnings": []},
    }
    report = {
        "report_mode": "llm",
        "llm": {"analysis": {"headline": "<script>alert(1)</script>", "summary": {"text": "<b>不要执行</b>"}}},
    }

    html = build_morning_report_dashboard_html(fact_pack, report)

    assert "<script>" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html
    assert "&lt;b&gt;不要执行&lt;/b&gt;" in html
    assert "部分数据可用" in html
    assert "数据待补" in html
    assert "-- / --" in html


def test_dashboard_keeps_zero_source_counts_visible():
    fact_pack = _fact_pack()
    fact_pack["data_quality"]["sources"][0]["row_count"] = 0

    html = build_morning_report_dashboard_html(fact_pack, {"report_mode": "facts"})

    assert "<td>0</td>" in html

