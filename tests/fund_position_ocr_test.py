"""Regression tests for screenshot-based fund position extraction."""

from io import BytesIO
from unittest.mock import patch

import pandas as pd
import pytest
from PIL import Image

from src.fund_position_ocr import (
    FundPositionOcrError,
    build_image_batch_fingerprint,
    choose_unique_fund_match,
    extract_fund_position_text,
    parse_fund_position_text,
    parse_money_amount,
    parse_share_amount,
)


def _png_bytes() -> bytes:
    buffer = BytesIO()
    Image.new("RGB", (320, 180), "white").save(buffer, format="PNG")
    return buffer.getvalue()


def test_batch_fingerprint_tracks_file_name_content_and_order():
    first = build_image_batch_fingerprint(
        [("a.png", b"first"), ("b.png", b"second")]
    )

    assert first
    assert first == build_image_batch_fingerprint(
        [("a.png", b"first"), ("b.png", b"second")]
    )
    assert first != build_image_batch_fingerprint(
        [("b.png", b"second"), ("a.png", b"first")]
    )
    assert first != build_image_batch_fingerprint(
        [("a.png", b"changed"), ("b.png", b"second")]
    )
    assert build_image_batch_fingerprint([]) == ""


def test_parse_share_amount_supports_grouping_and_chinese_units():
    assert parse_share_amount("12,345.67 份") == pytest.approx(12345.67)
    assert parse_share_amount("1.25万份") == pytest.approx(12500)
    assert parse_share_amount("0.08 亿") == pytest.approx(8_000_000)
    assert parse_share_amount("0") is None


def test_broker_summary_with_values_above_labels_derives_cost():
    lines = [
        {"text": "15:13 1 100", "confidence": 0.751},
        {"text": "南方信息创新混合C 99", "confidence": 0.7421},
        {"text": "007491", "confidence": 0.8076},
        {"text": "产品详情》", "confidence": 0.7527},
        {"text": "晒收益", "confidence": 0.6842},
        {"text": "48,245.73", "confidence": 0.7985},
        {"text": "金额(元）", "confidence": 0.6894},
        {"text": "您已持有59天0", "confidence": 0.8002},
        {"text": "0.00 2,121.69 +4.65%", "confidence": 0.7781},
        {"text": "昨日收益 持仓收益 持仓收益率", "confidence": 0.736},
        {"text": "在途资金 500.00元 可用份额 9,698.70份", "confidence": 0.7977},
        {"text": "日涨幅 4.26% 最新净值 4.9229(09-18)", "confidence": 0.785},
        {"text": "持仓成本价 4.7041 银行卡尾号 4906", "confidence": 0.7961},
    ]

    rows = parse_fund_position_text("", lines=lines)

    assert len(rows) == 1
    assert rows[0]["fund_code"] == "007491"
    assert rows[0]["holding_shares"] == pytest.approx(9698.70)
    assert rows[0]["snapshot_holding_amount"] == pytest.approx(48245.73)
    assert rows[0]["snapshot_holding_profit"] == pytest.approx(2121.69)
    assert rows[0]["holding_cost_amount"] == pytest.approx(46124.04)
    assert rows[0]["holding_cost_source"] == "截图持有金额－累计持仓收益"


def test_profit_rate_and_transaction_amount_are_not_position_money():
    rows = parse_fund_position_text(
        "南方信息创新混合C\n007491\n可用份额 9,698.70份\n"
        "持仓收益率 +4.65%\n持仓盈亏率 +4.65%\n申购金额(元)\n1,000.00"
    )

    assert len(rows) == 1
    assert rows[0]["snapshot_holding_amount"] is None
    assert rows[0]["snapshot_holding_profit"] is None
    assert rows[0]["holding_cost_amount"] is None


@pytest.mark.parametrize(
    "value",
    ["-100", "1,23", "12abc", "1.2.3", "inf", "1e309"],
)
def test_parse_share_amount_rejects_partial_or_non_finite_input(value):
    assert parse_share_amount(value) is None


def test_parse_money_amount_supports_signed_currency_and_units():
    assert parse_money_amount("￥12,345.67元") == pytest.approx(12345.67)
    assert parse_money_amount("1.2万") == pytest.approx(12000)
    assert parse_money_amount("-800.50元", allow_negative=True) == pytest.approx(-800.5)
    assert parse_money_amount("0元", allow_negative=True) == 0
    assert parse_money_amount("-800.50元") is None
    assert parse_money_amount("8.2%", allow_negative=True) is None


def test_parse_multiple_fund_positions_across_visual_lines():
    lines = [
        {"text": "中欧时代先锋混合A", "confidence": 0.94},
        {"text": "001938", "confidence": 0.98},
        {"text": "持有份额", "confidence": 0.92},
        {"text": "12,345.67 份", "confidence": 0.96},
        {"text": "易方达蓝筹精选", "confidence": 0.91},
        {"text": "005827.OF", "confidence": 0.97},
        {"text": "基金份额 1.25万份", "confidence": 0.93},
    ]

    rows = parse_fund_position_text("", lines=lines)

    assert [row["fund_code"] for row in rows] == ["001938", "005827.OF"]
    assert rows[0]["fund_name_hint"] == "中欧时代先锋混合A"
    assert rows[0]["holding_shares"] == pytest.approx(12345.67)
    assert rows[1]["holding_shares"] == pytest.approx(12500)
    assert rows[0]["confidence"] in {"高", "中"}


def test_parser_prefers_share_label_over_money_and_nav_values():
    rows = parse_fund_position_text(
        "中欧时代先锋\n001938\n持有金额 25,888.00 元\n单位净值 2.1604\n持有份额 12,000.50 份"
    )

    assert len(rows) == 1
    assert rows[0]["holding_shares"] == pytest.approx(12000.50)


def test_parser_scopes_share_and_money_labels_and_infers_cost():
    rows = parse_fund_position_text(
        "中欧时代先锋\n001938\n"
        "持有份额 10000 持有金额 25,888.00元\n"
        "累计收益 -1,200.50元"
    )

    assert len(rows) == 1
    assert rows[0]["holding_shares"] == pytest.approx(10000)
    assert rows[0]["snapshot_holding_amount"] == pytest.approx(25888)
    assert rows[0]["snapshot_holding_profit"] == pytest.approx(-1200.5)
    assert rows[0]["holding_cost_amount"] == pytest.approx(27088.5)
    assert rows[0]["holding_cost_source"] == "截图持有金额－累计持仓收益"


def test_six_digit_money_on_next_line_is_not_parsed_as_fund_code():
    rows = parse_fund_position_text(
        "中欧时代先锋\n001938\n持有份额\n10000\n"
        "持有金额\n123456\n累计收益\n2345"
    )

    assert len(rows) == 1
    assert rows[0]["fund_code"] == "001938"
    assert rows[0]["snapshot_holding_amount"] == pytest.approx(123456)
    assert rows[0]["snapshot_holding_profit"] == pytest.approx(2345)
    assert rows[0]["holding_cost_amount"] == pytest.approx(121111)


def test_parser_ignores_today_profit_and_cost_nav_as_total_cost():
    rows = parse_fund_position_text(
        "中欧时代先锋\n001938\n持有份额 10000份\n"
        "持有金额 25,888元\n今日收益 88元\n成本价 2.40"
    )

    assert len(rows) == 1
    assert rows[0]["snapshot_holding_amount"] == pytest.approx(25888)
    assert rows[0]["snapshot_holding_profit"] is None
    assert rows[0]["holding_cost_amount"] is None


def test_explicit_cost_conflict_requires_manual_review():
    rows = parse_fund_position_text(
        "中欧时代先锋\n001938\n持有份额 10000份\n"
        "持有金额 25,888元\n累计收益 1,000元\n总成本 20,000元"
    )

    assert rows[0]["holding_cost_amount"] == pytest.approx(20000)
    assert "不一致" in rows[0]["holding_cost_warning"]
    assert "不一致" in rows[0]["warning"]


def test_broker_screenshot_recovers_code_separator_and_derives_cost():
    rows = parse_fund_position_text(
        "持有详情\n"
        "宏利复兴混合C 产品详情\n"
        "0176121混合型基金1高风险\n"
        "持有金额(元）\n"
        "21,314.29\n"
        "09月18日预估收益 持有收益 持有收益率\n"
        "+516.01 -1,685.71 -7.33%\n"
        "累计收益 -1,685.71 持有份额 4,648.70\n"
        "最新净值 4.6960(09月18日）"
    )

    assert len(rows) == 1
    assert rows[0]["fund_code"] == "017612"
    assert rows[0]["fund_name_hint"] == "宏利复兴混合C"
    assert rows[0]["holding_shares"] == pytest.approx(4648.70)
    assert rows[0]["snapshot_holding_amount"] == pytest.approx(21314.29)
    assert rows[0]["snapshot_holding_profit"] == pytest.approx(-1685.71)
    assert rows[0]["holding_cost_amount"] == pytest.approx(23000)
    assert rows[0]["holding_cost_source"] == "截图持有金额－累计持仓收益"


@pytest.mark.parametrize(
    "text",
    [
        "中欧时代先锋\n001938\n申购费率 0.15%",
        "中欧时代先锋\n001938\n最新规模 123.45亿",
        "中欧时代先锋\n001938\n风险等级 3",
    ],
)
def test_parser_does_not_treat_unlabelled_nearby_numbers_as_shares(text):
    rows = parse_fund_position_text(text)

    assert len(rows) == 1
    assert rows[0]["fund_code"] == "001938"
    assert rows[0]["holding_shares"] is None


def test_money_value_is_not_mistaken_for_fund_code():
    assert parse_fund_position_text("持有金额 123456.78 元") == []


def test_six_digit_share_value_is_not_mistaken_for_another_fund_code():
    rows = parse_fund_position_text(
        "中欧时代先锋\n001938\n持有份额\n100000\n累计收益 8,000.00"
    )

    assert len(rows) == 1
    assert rows[0]["fund_code"] == "001938"
    assert rows[0]["holding_shares"] == pytest.approx(100000)


def test_name_only_screenshot_is_kept_for_registry_review():
    rows = parse_fund_position_text("易方达蓝筹精选\n持有份额\n5,000.00份")

    assert len(rows) == 1
    assert rows[0]["fund_code"] == ""
    assert rows[0]["fund_name_hint"] == "易方达蓝筹精选"
    assert rows[0]["holding_shares"] == pytest.approx(5000)
    assert "确认代码" in rows[0]["warning"]


def test_choose_unique_registry_match_never_guesses_ambiguous_bare_code():
    matches = pd.DataFrame(
        [
            {"fund_code": "510300.SH", "name": "沪深300ETF"},
            {"fund_code": "510300.OF", "name": "另一个基金"},
        ]
    )
    assert choose_unique_fund_match("510300", matches) is None
    assert choose_unique_fund_match("510300.SH", matches)["name"] == "沪深300ETF"


def test_choose_unique_registry_match_requires_exact_name():
    matches = pd.DataFrame(
        [{"fund_code": "005827.OF", "name": "易方达蓝筹精选混合"}]
    )

    assert choose_unique_fund_match("易方达蓝筹", matches) is None
    assert (
        choose_unique_fund_match("易方达蓝筹精选混合", matches)["fund_code"]
        == "005827.OF"
    )


def test_extract_ocr_uses_rapidocr_as_primary_engine():
    with patch(
        "src.fund_position_ocr._extract_with_tesseract",
        side_effect=AssertionError("Tesseract should not run"),
    ), patch(
        "src.fund_position_ocr._extract_with_rapidocr",
        return_value=[{"text": "001938 持有份额 10000份", "confidence": 0.9}],
    ):
        result = extract_fund_position_text(_png_bytes())

    assert result["provider"] == "RapidOCR"
    assert "001938" in result["text"]
    assert result["warnings"] == []


def test_extract_ocr_falls_back_to_tesseract_when_rapidocr_is_unusable():
    with patch(
        "src.fund_position_ocr._extract_with_rapidocr",
        side_effect=RuntimeError("missing rapidocr"),
    ), patch(
        "src.fund_position_ocr._extract_with_tesseract",
        return_value=[{"text": "001938 持有份额 10000份", "confidence": 0.9}],
    ):
        result = extract_fund_position_text(_png_bytes())

    assert result["provider"] == "Tesseract"
    assert "001938" in result["text"]
    assert "missing rapidocr" in result["warnings"][0]


def test_extract_ocr_selects_more_complete_provider_result():
    tesseract_lines = [
        {"text": "宏利复兴混合C", "confidence": 0.95},
        {"text": "017612", "confidence": 0.95},
        {"text": "持有份额 4.648", "confidence": 0.95},
    ]
    rapidocr_lines = [
        {"text": "宏利复兴混合C 产品详情", "confidence": 0.82},
        {"text": "0176121混合型基金1高风险", "confidence": 0.80},
        {"text": "持有金额(元）", "confidence": 0.64},
        {"text": "21,314.29", "confidence": 0.81},
        {"text": "累计收益 -1,685.71 持有份额 4,648.70", "confidence": 0.77},
    ]
    with patch(
        "src.fund_position_ocr._extract_with_tesseract",
        return_value=tesseract_lines,
    ), patch(
        "src.fund_position_ocr._extract_with_rapidocr",
        return_value=rapidocr_lines,
    ):
        result = extract_fund_position_text(_png_bytes())

    rows = parse_fund_position_text(result["text"], lines=result["lines"])
    assert result["provider"] == "RapidOCR"
    assert rows[0]["fund_code"] == "017612"
    assert rows[0]["holding_shares"] == pytest.approx(4648.70)
    assert rows[0]["holding_cost_amount"] == pytest.approx(23000)


def test_extract_ocr_rejects_corrupt_image():
    with pytest.raises(FundPositionOcrError, match="无法读取截图"):
        extract_fund_position_text(b"not-an-image")
