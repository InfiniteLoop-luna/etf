import pandas as pd

from src.pages import fund_object_page


def _sample_holdings():
    return [
        {
            "stock_name": "宁德时代",
            "symbol": "300750.SZ",
            "market_value_yi": 18.2,
            "weight": 7.9,
            "change_flag": "increase",
            "change_label": "增持",
        },
        {
            "stock_name": "立讯精密",
            "symbol": "002475.SZ",
            "market_value_yi": 14.5,
            "weight": 6.3,
            "change_flag": "new",
            "change_label": "新进",
        },
        {
            "stock_name": "工业富联",
            "symbol": "601138.SH",
            "market_value_yi": 12.1,
            "weight": 5.2,
            "change_flag": "increase",
            "change_label": "增持",
        },
        {
            "stock_name": "美的集团",
            "symbol": "000333.SZ",
            "market_value_yi": 10.8,
            "weight": 4.7,
            "change_flag": "decrease",
            "change_label": "减持",
        },
        {
            "stock_name": "贵州茅台",
            "symbol": "600519.SH",
            "market_value_yi": 9.6,
            "weight": 4.1,
            "change_flag": "stable",
            "change_label": "稳定",
        },
    ]


def _stock_basic_export():
    return pd.DataFrame(
        [
            {"股票代码": "300750.SZ", "所属行业": "新能源设备"},
            {"股票代码": "002475.SZ", "所属行业": "消费电子"},
            {"股票代码": "601138.SH", "所属行业": "消费电子"},
            {"股票代码": "000333.SZ", "所属行业": "家电"},
            {"股票代码": "600519.SH", "所属行业": "白酒"},
        ]
    )


def test_build_change_summary_frames_rolls_up_industry_changes(monkeypatch):
    monkeypatch.setattr(
        fund_object_page,
        "load_stock_basic_summary_export",
        lambda: _stock_basic_export(),
    )

    industry_df, category_frames, summary_lines = fund_object_page._build_change_summary_frames(
        _sample_holdings()
    )

    assert len(summary_lines) == 3

    consumer = industry_df[industry_df["所属行业"] == "消费电子"].iloc[0]
    assert consumer["新进权重(%)"] == 6.3
    assert consumer["增持权重(%)"] == 5.2
    assert consumer["减持权重(%)"] == 0.0
    assert consumer["净变化(%)"] == 11.5
    assert consumer["变化方向"] == "增配"
    assert consumer["代表个股"] == "立讯精密、工业富联"

    decrease_df = category_frames["decrease"]
    assert decrease_df.iloc[0]["所属行业"] == "家电"
    assert decrease_df.iloc[0]["权重合计"] == 4.7


def test_build_industry_exposure_frame_reuses_same_industry_mapping(monkeypatch):
    monkeypatch.setattr(
        fund_object_page,
        "load_stock_basic_summary_export",
        lambda: _stock_basic_export(),
    )

    exposure_df, detailed_df = fund_object_page._build_industry_exposure_frame(
        _sample_holdings()
    )

    consumer = exposure_df[exposure_df["所属行业"] == "消费电子"].iloc[0]
    assert consumer["持仓股票数"] == 2
    assert consumer["权重合计"] == 11.5

    maotai = detailed_df[detailed_df["股票代码"] == "600519.SH"].iloc[0]
    assert maotai["所属行业"] == "白酒"
    assert maotai["变动"] == "稳定"
