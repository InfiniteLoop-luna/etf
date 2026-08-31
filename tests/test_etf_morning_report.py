from datetime import date
from types import SimpleNamespace

import pandas as pd

from src.etf_morning_report import (
    _fallback_markdown,
    _build_industry_etf_groups,
    build_report_digest,
    build_source_readiness,
    find_previous_trade_date,
    generate_llm_markdown,
    normalize_morning_llm_result,
    save_report,
)
from src.morning_report_notifier import (
    ServerChanNotifierConfig,
    build_serverchan_endpoint,
    build_serverchan_message,
    send_serverchan_report,
    send_serverchan_report_for_users,
)
from scripts.generate_etf_morning_report import is_sse_trading_day_today


class FakeEngine:
    pass


def test_scheduled_notification_uses_sse_trading_calendar(monkeypatch):
    class FakePro:
        def __init__(self, is_open):
            self.is_open = is_open

        def trade_cal(self, **kwargs):
            return pd.DataFrame([{"cal_date": kwargs["start_date"], "is_open": self.is_open}])

    monkeypatch.setattr("src.volume_fetcher._init_tushare", lambda: FakePro(0))
    assert is_sse_trading_day_today() is False

    monkeypatch.setattr("src.volume_fetcher._init_tushare", lambda: FakePro(1))
    assert is_sse_trading_day_today() is True


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
        {"industry": "半导体", "ts_code": "159000.SZ", "industry_etf": "半导体ETF", "current_date": "2026-08-27", "previous_date": "2026-08-26", "current_share": 110.0, "previous_share": 100.0, "share_growth_pct": 10.0},
        {"industry": "半导体", "ts_code": "159001.SZ", "industry_etf": "芯片ETF", "current_date": "2026-08-27", "previous_date": "2026-08-26", "current_share": 190.0, "previous_share": 200.0, "share_growth_pct": -5.0},
        {"industry": "通信", "ts_code": "159002.SZ", "industry_etf": "通信ETF", "current_date": "2026-08-27", "previous_date": "2026-08-26", "current_share": 101.0, "previous_share": 100.0, "share_growth_pct": 1.0},
    ])
    groups = _build_industry_etf_groups(frame)

    semiconductor = next(group for group in groups if group["industry"] == "半导体")
    assert len(semiconductor["etfs"]) == 2
    assert semiconductor["etfs"][0]["ts_code"] == "159000.SZ"
    assert semiconductor["current_share"] == 300.0
    assert semiconductor["previous_share"] == 300.0
    assert semiconductor["share_change"] == 0.0
    assert semiconductor["share_growth_pct"] == 0.0


def test_build_report_digest_calculates_risk_light_and_top_sector():
    digest = build_report_digest({
        "fund_watchlist": {"funds": [{"fund_name": "测试基金", "fund_code": "000001.OF"}]},
        "money_flow": {"ths_top_inflow": [{"industry": "半导体", "net_amount": 123.4}], "dc_top_inflow": []},
        "trend_recommendations": {"top_uptrend": [{"name": "甲"}], "top_avoid": [{"name": "乙"}]},
        "market_sentiment": {"limitup": [{"up_cnt": 20, "zha_cnt": 2}]},
        "data_quality": {"report_status": "partial", "warnings": []},
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


def test_source_readiness_requires_all_core_sources(monkeypatch):
    def fake_query(engine, sql, params=None):
        if "FROM ts_limit_list_d" in sql:
            return pd.DataFrame([{"latest_date": date(2026, 8, 26), "row_count": 0}])
        return pd.DataFrame([{"latest_date": date(2026, 8, 27), "row_count": 10}])

    monkeypatch.setattr("src.etf_morning_report._query_frame", fake_query)
    readiness = build_source_readiness(FakeEngine(), "2026-08-27")

    assert readiness["report_status"] == "partial"
    assert readiness["required_ready"] == 2
    assert readiness["required_total"] == 3


def test_normalize_morning_llm_result_requires_supported_evidence_and_numbers():
    fact_pack = {
        "evidence": [
            {"evidence_id": "flow.ths.0", "label": "半导体净流入", "value": 12.34, "unit": "亿元"}
        ]
    }
    valid = normalize_morning_llm_result(
        {
            "headline": "资金主线清晰",
            "summary": "半导体净流入 12.34 亿元",
            "summary_evidence_ids": ["flow.ths.0"],
            "focus_items": [],
        },
        fact_pack,
    )
    invalid = normalize_morning_llm_result(
        {
            "summary": "半导体净流入 99.99 亿元",
            "summary_evidence_ids": ["flow.ths.0"],
            "focus_items": [],
        },
        fact_pack,
    )

    assert valid is not None
    assert valid["summary"]["evidence_ids"] == ["flow.ths.0"]
    assert invalid is None


def test_normalize_morning_llm_result_rejects_missing_evidence():
    fact_pack = {
        "evidence": [
            {
                "evidence_id": "sentiment.limitup",
                "label": "涨停家数",
                "value": 0,
                "unit": "家",
                "status": "missing",
            }
        ]
    }

    result = normalize_morning_llm_result(
        {
            "summary": "涨停 0 家",
            "summary_evidence_ids": ["sentiment.limitup"],
            "focus_items": [],
        },
        fact_pack,
    )

    assert result is None


def test_normalize_morning_llm_result_requires_caveat_for_stale_or_generated_evidence():
    fact_pack = {
        "evidence": [
            {
                "evidence_id": "trend.up.0",
                "label": "趋势候选",
                "value": "示例证券",
                "unit": "",
                "status": "generated",
            }
        ]
    }
    without_caveat = normalize_morning_llm_result(
        {
            "focus_items": [{"text": "示例证券是趋势候选", "evidence_ids": ["trend.up.0"]}],
        },
        fact_pack,
    )
    with_caveat = normalize_morning_llm_result(
        {
            "focus_items": [{
                "text": "示例证券是趋势候选",
                "evidence_ids": ["trend.up.0"],
                "caveat": "这是模型生成结果，不是原始行情",
            }],
        },
        fact_pack,
    )

    assert without_caveat is None
    assert with_caveat is not None


def test_generate_llm_markdown_uses_validated_json(monkeypatch):
    captured = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "choices": [{
                    "message": {
                        "content": '{"headline":"主线明确","summary":"半导体净流入 12.34 亿元","summary_evidence_ids":["flow.ths.0"],"focus_items":[]}'
                    }
                }]
            }

    def fake_post(url, headers, json, timeout):
        captured.update(json)
        return FakeResponse()

    monkeypatch.setattr(
        "src.etf_morning_report.load_stock_research_llm_config",
        lambda: SimpleNamespace(
            configured=True,
            model="demo-model",
            temperature=0.2,
            max_tokens=1200,
            base_url="https://example.invalid",
            api_key="secret",
            timeout_seconds=5,
        ),
    )
    monkeypatch.setattr("requests.post", fake_post)
    fact_pack = {
        "schema_version": "etf-morning-report-v2",
        "report_trade_date": "2026-08-27",
        "generated_at": "2026-08-28T08:30:00+08:00",
        "data_quality": {"report_status": "partial", "coverage_score": 62, "warnings": ["辅助数据缺失"]},
        "evidence": [
            {"evidence_id": "flow.ths.0", "label": "半导体净流入", "value": 12.34, "unit": "亿元"},
            {"evidence_id": "northbound.net", "label": "北向资金", "value": 0, "unit": "亿元", "status": "missing"},
        ],
        "fund_watchlist": {"funds": []},
        "money_flow": {"ths_top_inflow": [], "dc_top_inflow": []},
        "trend_recommendations": {},
        "market_sentiment": {"limitup": []},
        "etf_overview": {"industry_etf_groups": []},
    }

    markdown, meta = generate_llm_markdown(fact_pack)

    assert "半导体净流入 12.34 亿元" in markdown
    assert meta["analysis"]["validated"] is True
    assert captured["response_format"] == {"type": "json_object"}
    user_prompt = captured["messages"][1]["content"]
    assert "flow.ths.0" in user_prompt
    assert "northbound.net" not in user_prompt
    assert "deterministic_digest" not in user_prompt


def test_generate_llm_markdown_skips_only_when_no_business_evidence(monkeypatch):
    monkeypatch.setattr(
        "src.etf_morning_report.load_stock_research_llm_config",
        lambda: SimpleNamespace(
            configured=True,
            model="demo-model",
            temperature=0.1,
            max_tokens=1200,
            base_url="https://example.invalid",
            api_key="secret",
            timeout_seconds=5,
        ),
    )
    monkeypatch.setattr(
        "requests.post",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("LLM should not be called")),
    )
    fact_pack = {
        "report_trade_date": "2026-08-27",
        "data_quality": {"report_status": "partial", "coverage_score": 0, "warnings": []},
        "evidence": [
            {"evidence_id": "quality.coverage", "value": 0, "status": "verified"},
            {"evidence_id": "sentiment.limitup", "value": 0, "status": "missing"},
        ],
    }

    markdown, meta = generate_llm_markdown(fact_pack)

    assert meta is None
    assert any(
        "没有可用于分析的业务证据" in warning
        for warning in fact_pack["data_quality"]["warnings"]
    )
    assert "结构化事实版" in markdown


def _notification_report(status="complete"):
    return {
        "report_hash": "hash-demo",
        "report_mode": "facts",
        "fact_pack": {
            "report_trade_date": "2026-08-27",
            "data_quality": {"report_status": status, "coverage_score": 88, "warnings": []},
            "fund_watchlist": {"funds": []},
            "money_flow": {"ths_top_inflow": [{"industry": "半导体", "net_amount_yi": 12.34}], "dc_top_inflow": []},
            "trend_recommendations": {},
            "market_sentiment": {"limitup": [{"up_cnt": 20, "zha_cnt": 2}]},
            "etf_overview": {"industry_etf_groups": []},
            "evidence": [
                {"evidence_id": "sentiment.limitup", "label": "涨停家数", "value": 20, "unit": "家"}
            ],
        },
    }


def test_build_serverchan_message_is_short_evidence_summary():
    title, content = build_serverchan_message(
        _notification_report(),
        report_url="https://example.com/morning",
    )

    assert "\n" not in title
    assert "晨报｜2026-08-27" in content
    assert "数据覆盖 88%" in content
    assert "查看完整晨报" in content
    assert len(content) < 10000


def test_serverchan_endpoint_supports_turbo_and_sc3_without_arbitrary_hosts():
    assert build_serverchan_endpoint("SCTabc_123") == "https://sctapi.ftqq.com/SCTabc_123.send"
    assert build_serverchan_endpoint("sctp123tabc_456") == "https://123.push.ft07.com/send/sctp123tabc_456.send"
    assert build_serverchan_endpoint("https://example.com/key") is None


def test_serverchan_delivery_can_suppress_partial_by_admin_policy(tmp_path, monkeypatch):
    monkeypatch.setattr("src.morning_report_notifier.NOTIFICATION_DIR", tmp_path)
    config = ServerChanNotifierConfig(
        enabled=True,
        sendkey="SCTtest",
        allow_partial=False,
    )

    result = send_serverchan_report(_notification_report("partial"), config=config)

    assert result["status"] == "suppressed_partial"
    assert not list(tmp_path.glob("*.json"))


def test_serverchan_delivery_allows_partial_report_by_default(tmp_path, monkeypatch):
    monkeypatch.setattr("src.morning_report_notifier.NOTIFICATION_DIR", tmp_path)

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"code": 0, "message": "ok"}

    class FakeSession:
        def post(self, url, json, headers, timeout):
            return FakeResponse()

    config = ServerChanNotifierConfig(enabled=True, sendkey="SCTtest")
    result = send_serverchan_report(
        _notification_report("partial"),
        config=config,
        recipient_id="partial-default",
        session=FakeSession(),
    )

    assert result["status"] == "delivered"


def test_serverchan_delivery_is_idempotent_by_trade_date(tmp_path, monkeypatch):
    monkeypatch.setattr("src.morning_report_notifier.NOTIFICATION_DIR", tmp_path)
    calls = []

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"code": 0, "message": "ok"}

    class FakeSession:
        def post(self, url, json, headers, timeout):
            calls.append((url, json, headers, timeout))
            return FakeResponse()

    config = ServerChanNotifierConfig(
        enabled=True,
        sendkey="SCTtest",
        report_url="https://example.com/morning",
    )
    report = _notification_report()

    first = send_serverchan_report(report, config=config, session=FakeSession())
    second = send_serverchan_report(report, config=config, session=FakeSession())

    assert first["status"] == "delivered"
    assert second["status"] == "duplicate_skipped"
    assert len(calls) == 1
    assert calls[0][0] == "https://sctapi.ftqq.com/SCTtest.send"
    assert set(calls[0][1]) == {"title", "desp"}


def test_serverchan_business_error_is_not_retried_and_redacts_sendkey(tmp_path, monkeypatch):
    monkeypatch.setattr("src.morning_report_notifier.NOTIFICATION_DIR", tmp_path)
    calls = []

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"code": 40001, "message": "invalid SCTsecret"}

    class FakeSession:
        def post(self, url, json, headers, timeout):
            calls.append(url)
            return FakeResponse()

    config = ServerChanNotifierConfig(
        enabled=True,
        sendkey="SCTsecret",
        max_attempts=3,
    )

    result = send_serverchan_report(_notification_report(), config=config, session=FakeSession())

    assert result["status"] == "failed"
    assert len(calls) == 1
    assert "SCTsecret" not in result["error"]


def test_serverchan_scheduled_delivery_has_independent_user_idempotency(tmp_path, monkeypatch):
    monkeypatch.setattr("src.morning_report_notifier.NOTIFICATION_DIR", tmp_path)
    monkeypatch.setattr(
        "src.user_notification_store.list_enabled_serverchan_credentials",
        lambda: [("alice", "SCTalice"), ("bob", "SCTbob")],
    )
    monkeypatch.setattr(
        "src.morning_report_notifier.load_serverchan_notifier_config",
        lambda: ServerChanNotifierConfig(enabled=False, report_url="https://example.com/morning"),
    )
    calls = []

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"code": 0, "message": "ok"}

    class FakeSession:
        def post(self, url, json, headers, timeout):
            calls.append(url)
            return FakeResponse()

    first = send_serverchan_report_for_users(_notification_report(), session=FakeSession())
    second = send_serverchan_report_for_users(_notification_report(), session=FakeSession())

    assert first["status"] == "delivered"
    assert first["recipient_count"] == 2
    assert second["status"] == "duplicate_skipped"
    assert len(calls) == 2
    assert {item["recipient"] for item in first["recipients"]} == {"user:alice", "user:bob"}
