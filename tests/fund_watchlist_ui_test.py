from pathlib import Path


APP_SOURCE = Path("app.py").read_text(encoding="utf-8", errors="ignore")
ESTIMATE_TIMER_SOURCE = Path("systemd/etf-fund-estimate-snapshot.timer").read_text(encoding="utf-8")


def _fund_hot_stocks_body():
    start = APP_SOURCE.index("def render_fund_hot_stocks_tab")
    end = APP_SOURCE.index("def render_moneyflow_tab", start)
    return APP_SOURCE[start:end]


def _function_source(name: str) -> str:
    start = APP_SOURCE.index(f"def {name}(")
    end = APP_SOURCE.find("\ndef ", start + 1)
    return APP_SOURCE[start:] if end < 0 else APP_SOURCE[start:end]


def _html_section_source(source: str, marker: str) -> str:
    start = source.index(marker)
    end = source.index("</section>", start)
    return source[start:end]


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
    assert '["看板", "表格", "对比矩阵"]' in APP_SOURCE
    assert '["盘中估算", "预计增减金额", "实际持仓金额", "当前持仓收益", "日涨跌幅", "估值偏差", "Top10 集中度", "基金规模", "持仓市值", "披露日期"]' in APP_SOURCE
    assert "render_fund_watchlist_focus_detail" in APP_SOURCE
    assert "fund_watchlist_batch_mode" in APP_SOURCE
    assert "remove_watchlist_items_batch(current_username, pending_items)" in APP_SOURCE


def test_fund_watchlist_page_owns_the_add_and_manage_flow():
    assert "render_fund_watchlist_add_panel" in APP_SOURCE
    assert "同步保存份额与当前剩余持仓成本" in APP_SOURCE
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
    for column in ["日涨跌幅(%)", "15:00估值(%)", "估值偏差(百分点)", "盘中估算(%)", "预计增减金额(元)"]:
        assert column in APP_SOURCE


def test_fund_watchlist_shows_previous_day_nav_and_daily_change():
    assert "fetch_latest_fund_nav_snapshot" in APP_SOURCE
    assert "前一日净值" in APP_SOURCE
    assert "日涨跌幅(%)" in APP_SOURCE
    assert "净值日期" in APP_SOURCE
    assert "确认净值显示最近已公布的单位净值与日涨跌幅" in APP_SOURCE


def test_fund_watchlist_retains_same_day_close_estimate_and_deviation():
    assert "get_fund_estimate_snapshot" in APP_SOURCE
    assert "load_fund_watchlist_latest_closing_estimates_cached" in APP_SOURCE
    assert "最近15:00估值" in APP_SOURCE
    assert "15:00估值与实际净值同日对比" in APP_SOURCE
    assert "估值偏差（估值－实际）" in APP_SOURCE
    assert "每日估值偏差 = 同一净值日期的 15:00 估值涨跌幅－实际涨跌幅" in APP_SOURCE
    assert "OnCalendar=Mon..Fri *-*-* 07:05:00 UTC" in ESTIMATE_TIMER_SOURCE


def test_fund_watchlist_copy_and_fields_are_chinese_fund_semantics():
    for text in [
        "请先登录用户名，再查看和管理你的自选基金。",
        "你的自选基金还是空的",
        "追踪自选基金的前一日净值、15:00估值、每日估值偏差、盘中估值与持仓结构",
        "平均 Top10 集中度",
        "持仓变动",
        "基金管理人",
        "前十大持仓明细",
        "前十大持仓行业权重热力图",
        "持仓披露日期",
        "fund_root_label",
        "矩形面积代表持仓权重",
        "大行业 → 主细分板块 → 个股",
        "持仓市值",
        "持仓变化",
    ]:
        assert text in APP_SOURCE


def test_fund_watchlist_accepts_manual_positions_and_reviewed_screenshot_ocr():
    for text in [
        "fund_watchlist_manual_position_form",
        "持有份额",
        "保存持仓",
        "上传基金持仓截图",
        "extract_fund_position_text",
        "fund_watchlist_ocr_preview_editor",
        "确认导入勾选持仓",
        "截图结果会先让你核对，不会自动写入",
    ]:
        assert text in APP_SOURCE
    assert 'str(parsed.get("confidence") or "低") != "低"' in APP_SOURCE
    assert "add_watchlist_items_batch" in APP_SOURCE
    assert "清空持仓信息" in APP_SOURCE
    assert "holding_cost_amount" in APP_SOURCE
    assert "截图持有金额(元)" in APP_SOURCE
    assert "截图持仓收益(元)" in APP_SOURCE
    assert "existing_fund_matches" in APP_SOURCE
    assert "time.time_ns()" in APP_SOURCE
    assert "截图存在购买处理中或在途资金" in APP_SOURCE


def test_fund_watchlist_includes_on_demand_ai_add_position_analysis():
    for text in [
        "AI 加仓分析",
        "生成加仓分析",
        "计划追加预算（元，可选）",
        "估值观察期",
        "分批执行方案",
        "停止加仓条件",
        "list_fund_estimate_snapshot_history",
        "analyze_fund_add_position_payload",
    ]:
        assert text in APP_SOURCE
    assert "disabled=not config.configured" in APP_SOURCE


def test_fund_watchlist_primary_actions_align_with_neighboring_controls():
    add_panel_source = _function_source("render_fund_watchlist_add_panel")
    dashboard_source = _function_source("render_fund_watchlist_live_dashboard")

    assert """st.columns(
                    [1.3, 1.2, 1.3, 1],
                    vertical_alignment=\"bottom\",
                )""" in add_panel_source
    assert """st.columns(
            [1.1, 1.4, 1.2],
            vertical_alignment=\"bottom\",
        )""" in dashboard_source


def test_fund_watchlist_accepts_and_reviews_multiple_screenshots_as_one_batch():
    assert "accept_multiple_files=True" in APP_SOURCE
    assert "MAX_BATCH_IMAGE_COUNT" in APP_SOURCE
    assert "MAX_BATCH_IMAGE_BYTES" in APP_SOURCE
    assert "build_image_batch_fingerprint" in APP_SOURCE
    assert "批量上传基金持仓截图" in APP_SOURCE
    assert "批量识别" in APP_SOURCE
    assert "来源截图" in APP_SOURCE
    assert "本批截图存在重复基金" in APP_SOURCE
    assert "本批截图中存在重复基金代码" in APP_SOURCE


def test_fund_watchlist_displays_daily_estimated_position_amount_everywhere():
    assert "attach_estimated_daily_amount" in APP_SOURCE
    assert APP_SOURCE.count("每日预增金额") >= 4
    assert "预计增减金额(元)" in APP_SOURCE
    assert "持有份额 × 对应估值基准单位净值 × 估值涨跌幅" in APP_SOURCE
    assert "正数为预计增加、负数为预计减少" in APP_SOURCE


def test_fund_watchlist_displays_confirmed_position_value_and_profit():
    assert "attach_current_position_metrics" in APP_SOURCE
    assert APP_SOURCE.count("实际持仓金额") >= 5
    assert APP_SOURCE.count("当前持仓收益") >= 5
    assert "持仓成本金额(元)" in APP_SOURCE
    assert "实际持仓金额 = 持有份额 × 基金公司最新已公布单位净值" in APP_SOURCE
    assert "当前持仓收益 = 实际持仓金额 − 当前剩余持仓成本" in APP_SOURCE
    assert "盘中估值不会冒充实际收益" in APP_SOURCE


def test_fund_watchlist_groups_position_returns_into_clear_sections():
    summary_source = _function_source("render_fund_watchlist_summary")
    card_source = _function_source("_build_fund_watchlist_card_html")
    focus_source = _function_source("render_fund_watchlist_focus_detail")

    summary_returns = _html_section_source(
        summary_source,
        '<section class="ws-fund-watchboard__returns is-summary"',
    )
    for label in ["组合持仓与收益", "实际持仓总金额", "当前持仓总收益", "每日预增金额"]:
        assert label in summary_returns

    card_returns = _html_section_source(
        card_source,
        '<section class="ws-fund-watchboard__returns is-card"',
    )
    for section_source in [card_returns]:
        assert 'aria-label="我的持仓与收益"' in section_source
        for label in ["实际持仓金额", "当前持仓收益", "当前剩余持仓成本", "每日预增金额"]:
            assert label in section_source
        assert "holding_shares_label" in section_source

    assert '<section class="ws-fund-watchboard__returns is-focus"' not in focus_source
    assert 'aria-label="我的持仓与收益"' not in focus_source
    for label in ["实际持仓金额", "当前持仓收益", "当前剩余持仓成本", "每日预增金额"]:
        assert label not in focus_source

    assert "基金规模" not in card_returns
    assert "前十大持仓市值" not in card_returns
    assert card_source.index("ws-fund-watchboard__card-metrics") < card_source.index(
        "ws-fund-watchboard__returns is-card"
    ) < card_source.index("ws-fund-watchboard__changes")

    for selector in [
        ".ws-fund-watchboard__returns {",
        ".ws-fund-watchboard__returns-head {",
        ".ws-fund-watchboard__returns-grid {",
        ".ws-fund-watchboard__returns.is-focus .ws-fund-watchboard__returns-grid {",
        ".ws-fund-watchboard__return-item.is-up strong {",
        ".ws-fund-watchboard__return-item.is-down strong {",
    ]:
        assert selector in APP_SOURCE
    assert ".ws-fund-watchboard__returns-grid,\n    .ws-fund-watchboard__returns.is-summary .ws-fund-watchboard__returns-grid { grid-template-columns:1fr; }" in APP_SOURCE
