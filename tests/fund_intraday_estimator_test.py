from datetime import date, datetime
from unittest.mock import Mock

from src.fund_intraday_estimator import (
    collect_fund_holding_symbols,
    enrich_fund_items_with_intraday_estimates,
    fetch_tencent_realtime_quotes,
    get_fund_intraday_market_state,
    normalize_realtime_stock_symbol,
    parse_tencent_quote_payload,
)


def test_market_state_only_enables_weekday_requested_window():
    assert get_fund_intraday_market_state(datetime(2026, 7, 15, 9, 30))["is_active"]
    assert get_fund_intraday_market_state(datetime(2026, 7, 15, 12, 0))["is_active"]
    assert get_fund_intraday_market_state(datetime(2026, 7, 15, 15, 0))["is_active"]
    assert not get_fund_intraday_market_state(datetime(2026, 7, 15, 9, 29))["is_active"]
    assert not get_fund_intraday_market_state(datetime(2026, 7, 15, 15, 1))["is_active"]
    assert not get_fund_intraday_market_state(datetime(2026, 7, 18, 10, 0))["is_active"]


def test_normalize_and_collect_unique_a_share_symbols():
    assert normalize_realtime_stock_symbol("600036.SH")["quote_code"] == "sh600036"
    assert normalize_realtime_stock_symbol("sz000001")["ts_code"] == "000001.SZ"
    assert normalize_realtime_stock_symbol("430090")["quote_code"] == "bj430090"
    assert normalize_realtime_stock_symbol("-") is None

    items = [
        {"holdings": [{"symbol": "600036.SH"}, {"symbol": "000001.SZ"}]},
        {"holdings": [{"symbol": "sh600036"}]},
    ]
    assert collect_fund_holding_symbols(items) == ("000001.SZ", "600036.SH")


def test_parse_tencent_quote_payload_builds_realtime_fields():
    payload = (
        'v_sh600036="1~招商银行~600036~37.40~37.18~37.00~0~0~0~0~0~0~0~0~0~0~0~'
        '0~0~0~0~0~0~0~0~0~0~0~0~~20260715103646~0.22~0.59~";'
    )

    quotes = parse_tencent_quote_payload(payload, {"sh600036": "600036.SH"})

    quote = quotes["600036.SH"]
    assert quote["stock_name"] == "招商银行"
    assert quote["price"] == 37.4
    assert quote["previous_close"] == 37.18
    assert quote["pct_change"] == 0.59
    assert quote["quote_time"].strftime("%Y-%m-%d %H:%M:%S") == "2026-07-15 10:36:46"


def test_fetch_tencent_quotes_batches_and_uses_expected_headers():
    response = Mock()
    response.text = (
        'v_sz000001="51~平安银行~000001~10.78~10.69~10.65~0~0~0~0~0~0~0~0~0~0~0~'
        '0~0~0~0~0~0~0~0~0~0~0~0~~20260715103648~0.09~0.84~";'
    )
    response.raise_for_status = Mock()
    session = Mock()
    session.get.return_value = response

    quotes = fetch_tencent_realtime_quotes(["000001.SZ"], session=session)

    assert quotes["000001.SZ"]["pct_change"] == 0.84
    request_url = session.get.call_args.args[0]
    request_kwargs = session.get.call_args.kwargs
    assert request_url.endswith("sz000001")
    assert request_kwargs["headers"]["Referer"] == "https://gu.qq.com/"
    assert request_kwargs["timeout"] == 6.0


def test_estimate_uses_disclosed_weight_and_reports_coverage():
    items = [
        {
            "fund_code": "001938.OF",
            "top10_ratio": 50.0,
            "holdings": [
                {"symbol": "600036.SH", "weight": 10.0},
                {"symbol": "000001.SZ", "weight": 20.0},
                {"symbol": "300750.SZ", "weight": 20.0},
            ],
        }
    ]
    quotes = {
        "600036.SH": {
            "status": "ok",
            "source": "腾讯证券行情",
            "price": 10.2,
            "pct_change": 2.0,
            "quote_time": datetime.fromisoformat("2026-07-15T10:30:00+08:00"),
        },
        "000001.SZ": {
            "status": "ok",
            "source": "腾讯证券行情",
            "price": 9.9,
            "pct_change": -1.0,
            "quote_time": datetime.fromisoformat("2026-07-15T10:30:01+08:00"),
        },
    }

    result = enrich_fund_items_with_intraday_estimates(
        items,
        quotes,
        market_date=date(2026, 7, 15),
    )[0]

    assert result["intraday_estimate_pct"] == 0.0
    assert result["intraday_covered_weight_pct"] == 30.0
    assert result["intraday_top10_coverage_pct"] == 60.0
    assert result["intraday_quote_count"] == 2
    assert result["holdings"][0]["estimate_contribution_pct"] == 0.2
    assert result["holdings"][1]["estimate_contribution_pct"] == -0.2
    assert result["holdings"][2]["realtime_pct_change"] is None


def test_estimate_rejects_stale_quotes_on_a_weekday_holiday():
    items = [
        {
            "fund_code": "001938.OF",
            "top10_ratio": 10.0,
            "holdings": [{"symbol": "600036.SH", "weight": 10.0}],
        }
    ]
    quotes = {
        "600036.SH": {
            "status": "ok",
            "source": "腾讯证券行情",
            "price": 10.2,
            "pct_change": 2.0,
            "quote_time": datetime.fromisoformat("2026-07-14T15:00:00+08:00"),
        }
    }

    result = enrich_fund_items_with_intraday_estimates(
        items,
        quotes,
        market_date=date(2026, 7, 15),
    )[0]

    assert result["intraday_estimate_pct"] is None
    assert result["intraday_quote_count"] == 0
    assert result["intraday_covered_weight_pct"] == 0.0
