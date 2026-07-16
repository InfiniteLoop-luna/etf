from pathlib import Path


APP_SOURCE = Path("app.py").read_text(encoding="utf-8", errors="ignore")


def _fund_hot_stocks_body():
    start = APP_SOURCE.index("def render_fund_hot_stocks_tab")
    end = APP_SOURCE.index("def render_moneyflow_tab", start)
    return APP_SOURCE[start:end]


def test_app_imports_and_routes_standalone_fund_watchlist_page():
    assert "ETF_FUND_WATCHLIST_PAGE_LABEL" in APP_SOURCE
    assert "elif mobile_page == ETF_FUND_WATCHLIST_PAGE_LABEL:" in APP_SOURCE
    assert "elif selected_page == ETF_FUND_WATCHLIST_PAGE_LABEL:" in APP_SOURCE
    assert APP_SOURCE.count("render_fund_watchlist_tab()") >= 2


def test_fund_hot_stocks_page_no_longer_embeds_watchlist_board():
    body = _fund_hot_stocks_body()

    assert "render_fund_watchlist_tab()" not in body
    assert "render_fund_watchlist_board()" not in body


def test_standalone_page_uses_only_fund_watchlist_rows():
    assert 'list_watchlist_items(current_username, security_type="fund")' in APP_SOURCE


def test_fund_watchlist_dashboard_exposes_view_sort_focus_and_batch_controls():
    assert "FUND_WATCHLIST_DASHBOARD_CSS" in APP_SOURCE
    assert "load_fund_watchlist_dashboard_data_session_cached" in APP_SOURCE
    assert "build_fund_watchlist_summary" in APP_SOURCE
    assert "sort_fund_watchlist_items" in APP_SOURCE
    assert "build_fund_watchlist_table" in APP_SOURCE
    assert '["看板", "表格"]' in APP_SOURCE
    assert '["盘中估算", "日涨跌幅", "Top10 集中度", "基金规模", "持仓市值", "披露日期"]' in APP_SOURCE
    assert "render_fund_watchlist_focus_detail" in APP_SOURCE
    assert "fund_watchlist_batch_mode" in APP_SOURCE
    assert "remove_watchlist_items_batch(current_username, pending_items)" in APP_SOURCE


def test_fund_watchlist_page_owns_the_add_and_manage_flow():
    assert "render_fund_watchlist_add_panel" in APP_SOURCE
    assert "查看持仓不再是添加自选的前置步骤" in APP_SOURCE
    assert "请从上方搜索并添加第一只基金" in APP_SOURCE
    assert "fund_watchlist_add_search_form" in APP_SOURCE
    assert 'security_type="fund"' in APP_SOURCE


def test_hot_stock_fund_query_keeps_a_direct_watchlist_shortcut():
    body = _fund_hot_stocks_body()

    assert '"查看自选基金"' in body
    assert "queue_fund_watchlist_navigation()" in body
    assert "btn_open_fund_watchlist" in body


def test_fund_watchlist_dark_surfaces_use_high_contrast_widget_colors():
    assert ".st-key-fund_watchlist_add_panel .stTextInput input" in APP_SOURCE
    assert "background:#f7faff !important" in APP_SOURCE
    assert "color:#13213b !important" in APP_SOURCE
    assert ".st-key-fund_watchlist_toolbar" in APP_SOURCE
    assert "color:#ffffff !important" in APP_SOURCE
    assert ".st-key-fund_watchlist_table_focus_controls" in APP_SOURCE


def test_fund_watchlist_card_and_percentage_colors_are_self_contained():
    assert ".ws-fund-watchboard__card,\n.ws-fund-watchboard__focus" in APP_SOURCE
    assert ".ws-fund-watchboard__ratio {" in APP_SOURCE
    assert "color:var(--fw-cyan) !important" in APP_SOURCE
    assert ".ws-fund-watchboard__ring strong" in APP_SOURCE
    assert "color:#ffffff !important" in APP_SOURCE
    assert ".ws-fund-watchboard__fact > span" in APP_SOURCE


def test_fund_watchlist_intraday_estimate_is_visible_and_auto_refreshes():
    assert "load_fund_watchlist_realtime_quotes_cached" in APP_SOURCE
    assert 'st.fragment(run_every="60s")' in APP_SOURCE
    assert "render_fund_watchlist_intraday_status" in APP_SOURCE
    assert "盘中估算" in APP_SOURCE
    assert "实时覆盖权重(%)" in APP_SOURCE
    assert "实时涨跌 × 披露权重 = 估值贡献（百分点）" in APP_SOURCE
    assert "刷新盘中估值" in APP_SOURCE
    assert "结果仅为盘中估算，不等同于基金公司公布的净值" in APP_SOURCE


def test_fund_watchlist_intraday_colors_follow_cn_market_convention():
    assert 'return " is-up" if float(number) > 0 else " is-down"' in APP_SOURCE
    assert ".ws-fund-watchboard__live.is-up strong" in APP_SOURCE
    assert ".ws-fund-watchboard__live.is-down strong" in APP_SOURCE
    assert ".ws-fund-watchboard__holdings td.is-up" in APP_SOURCE
    assert ".ws-fund-watchboard__holdings td.is-down" in APP_SOURCE
    assert "_fund_watchlist_cn_market_cell_style" in APP_SOURCE
    assert 'subset=["日涨跌幅(%)", "盘中估算(%)"]' in APP_SOURCE


def test_fund_watchlist_shows_previous_day_nav_and_daily_change():
    assert "fetch_latest_fund_nav_snapshot" in APP_SOURCE
    assert "前一日净值" in APP_SOURCE
    assert "日涨跌幅(%)" in APP_SOURCE
    assert "净值日期" in APP_SOURCE
    assert "确认净值显示最近已公布的单位净值与日涨跌幅" in APP_SOURCE


def test_fund_watchlist_copy_and_fields_are_chinese_fund_semantics():
    for text in [
        "请先登录用户名，再查看和管理你的自选基金。",
        "你的自选基金还是空的",
        "追踪自选基金的前一日净值、日涨跌幅、盘中估值、持仓结构、披露进度与集中度变化",
        "平均 Top10 集中度",
        "持仓变动",
        "基金管理人",
        "前十大持仓明细",
        "持仓市值",
        "持仓变化",
    ]:
        assert text in APP_SOURCE
