from copy import deepcopy

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
            "top_uptrend": [{"name": "示例科技", "ts_code": "600000.SH", "industry": "半导体", "trend_score": 81, "prob_up_5d": 0.67, "prob_up_20d": 0.72, "reason": "均线结构改善，成交额确认"}],
            "top_avoid": [{"name": "示例通信", "ts_code": "000001.SZ", "industry": "通信", "risk_score": 76, "prob_up_5d": 0.31, "prob_up_20d": 0.38, "reason": "波动与回撤压力偏高"}],
        },
        "trend_evaluation": {
            "available": True,
            "as_of_date": "2026-09-16",
            "lookback_runs": 60,
            "topn_limit": 10,
            "horizons": {
                "1d": {"run_count": 42, "up_sample": 160, "up_hit_rate": 0.575, "up_avg_return": 0.0068, "avoid_sample": 158, "avoid_effective_rate": 0.551, "avoid_avg_return": -0.0031, "average_probability": None, "calibration_gap": None},
                "5d": {"run_count": 38, "up_sample": 142, "up_hit_rate": 0.613, "up_avg_return": 0.0215, "avoid_sample": 140, "avoid_effective_rate": 0.586, "avoid_avg_return": -0.0128, "average_probability": 0.642, "calibration_gap": -0.029},
                "20d": {"run_count": 25, "up_sample": 91, "up_hit_rate": 0.593, "up_avg_return": 0.0472, "avoid_sample": 88, "avoid_effective_rate": 0.568, "avoid_avg_return": -0.0264, "average_probability": 0.681, "calibration_gap": -0.088},
            },
            "recent_outcomes": [
                {"trade_date": "2026-09-15", "reco_type": "uptrend", "rank_no": 1, "ts_code": "600111.SH", "name": "历史样本", "industry": "电子", "ret_fwd_1d": 0.018, "ret_fwd_5d": None, "ret_fwd_20d": None},
            ],
        },
        "overseas_news": {
            "status": "ok",
            "warnings": [],
            "countries": {
                "US": {
                    "label": "美国",
                    "items": [{"news_id": "overseas.US.fed", "title": "Federal Reserve issues FOMC statement", "source": "Federal Reserve", "published_at": "2026-09-17T02:00:00+08:00", "url": "https://www.federalreserve.gov/example", "source_type": "official", "source_tier": 1, "verification_status": "官方发布"}],
                    "impact": {"country": "美国", "direction": "双向", "strength": "中", "confidence": "中", "analysis": "利率路径可能通过美元与全球风险偏好影响A股。", "channels": ["美元与人民币汇率"], "affected_sectors": ["成长科技"], "invalidating_conditions": "人民币与亚洲股指期货未确认时下调权重。", "method": "规则兜底"},
                },
                "JP": {
                    "label": "日本",
                    "items": [{"news_id": "overseas.JP.boj", "title": "Bank of Japan releases statistics", "source": "Bank of Japan", "published_at": "2026-09-17T15:00:00+08:00", "url": "https://www.boj.or.jp/example", "source_type": "official", "source_tier": 1, "verification_status": "官方发布"}],
                    "impact": {"country": "日本", "direction": "中性", "strength": "低", "confidence": "低", "analysis": "现有资讯尚未形成单一方向。", "channels": ["日元与套息交易"], "affected_sectors": ["汽车"], "invalidating_conditions": "等待日元和区域市场确认。", "method": "规则兜底"},
                },
                "KR": {
                    "label": "韩国",
                    "items": [{"news_id": "overseas.KR.bok", "title": "Monetary and Liquidity Aggregates", "source": "Bank of Korea", "published_at": "2026-09-17T11:00:00+08:00", "url": "https://www.bok.or.kr/example", "source_type": "official", "source_tier": 1, "verification_status": "官方发布"}],
                    "impact": {"country": "韩国", "direction": "中性", "strength": "低", "confidence": "低", "analysis": "现有资讯更多是区域流动性观察变量。", "channels": ["韩元与亚洲货币"], "affected_sectors": ["半导体"], "invalidating_conditions": "等待韩元和电子链表现确认。", "method": "规则兜底"},
                },
            },
        },
        "fund_watchlist": {"funds": [{"fund_name": "示例基金", "fund_code": "000001.OF", "nav_date": "2026-09-16", "daily_change_pct": 1.25}]},
    }
    fact_pack["evidence"] = build_evidence_ledger(fact_pack)
    return fact_pack


def test_dashboard_uses_five_step_review_structure_and_key_metrics():
    fact_pack = _fact_pack()
    report = {
        "report_mode": "llm",
        "llm": {"analysis": {"headline": "资金与份额形成局部共振", "summary": {"text": "市场广度改善，主线仍需开盘后确认。"}, "overseas_impacts": [{"country": "美国", "direction": "偏利空", "strength": "中", "confidence": "中", "analysis": "美国利率信号可能经美元和贴现率压制成长估值。", "channels": ["海外贴现率"], "affected_sectors": ["成长科技"], "invalidating_conditions": "人民币走强且亚洲股指期货企稳时下调权重。", "method": "大模型情景推演（证据校验）"}]}},
    }

    html = build_morning_report_dashboard_html(fact_pack, report)

    assert "A股 · ETF 晨间复盘" in html
    assert all(title in html for title in ["大盘环境", "市场情绪与赚钱效应", "板块复盘", "今日验证清单 / 核心标的", "今日总判断"])
    assert "3280 / 1760" in html
    assert "+0.68%" in html
    assert "15,329 亿元" in html
    assert "炸板率" in html and "15.0%" in html
    assert "示例科技" in html and "示例通信" in html
    assert "证据完备度" in html and "不是涨跌概率" in html
    assert "继续确认" in html and "反向信号" in html
    assert "20日概率 72%" in html
    assert "均线结构改善，成交额确认" in html
    assert "模型历史兑现率" in html
    assert "强势命中 61.3%" in html
    assert "实际低 8.8pct" in html
    assert "历史候选兑现明细" in html
    assert "历史样本（600111.SH）" in html
    assert "海外隔夜资讯 / 次日A股影响" in html
    assert all(country in html for country in ["美国", "日本", "韩国"])
    assert "T1 · 官方" in html and "09-17 02:00 北京时间" in html
    assert "大模型情景推演（证据校验）" in html
    assert "美国利率信号可能经美元和贴现率压制成长估值" in html
    assert 'href="https://www.federalreserve.gov/example"' in html


def test_dashboard_surfaces_cross_signal_sector_and_complete_details():
    html = build_morning_report_dashboard_html(_fact_pack(), {"report_mode": "facts"})

    assert "优先验证半导体等资金流与 ETF 份额共振方向的持续性" in html
    assert "ETF 份额变化明细" in html
    etf_detail_start = html.rfind("<details", 0, html.index("ETF 份额变化明细"))
    etf_detail_tag = html[etf_detail_start:html.index(">", etf_detail_start) + 1]
    assert etf_detail_tag == '<details class="mr-detail">'
    assert "点击展开后可查看所含 ETF" in html
    assert "展开明细" in html
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


def test_dashboard_compares_with_previous_report_using_deterministic_changes():
    current = _fact_pack()
    previous = deepcopy(current)
    previous["report_trade_date"] = "2026-09-15"
    previous["market_breadth"]["daily"][0].update({
        "trade_date": "2026-09-15",
        "advancer_count": 2500,
        "decliner_count": 2500,
        "median_pct_chg": -0.12,
    })
    previous["volume"]["daily"][0].update({"trade_date": "2026-09-15", "total_amount_yi": 14000})
    previous["market_sentiment"]["limitup"][0].update({"up_cnt": 60, "zha_cnt": 20})
    previous["money_flow"]["ths_top_inflow"][0]["industry"] = "银行"
    previous["evidence"] = build_evidence_ledger(previous)

    html = build_morning_report_dashboard_html(current, {"report_mode": "facts"}, previous)

    assert "相较上一份晨报的变化" in html
    assert "2026-09-15 → 2026-09-16" in html
    assert "较前期 +1520 家" in html
    assert "较前期 +0.80pct" in html
    assert "前期 银行" in html
    assert "变化由结构化数据计算，不由大模型判断" in html

