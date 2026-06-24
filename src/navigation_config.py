from src.sidebar_navigation import SIDEBAR_MODULES
from src.sidebar_navigation import get_page_labels


DECISION_PAGE_OPTIONS = get_page_labels("决策")
ETF_PAGE_OPTIONS = get_page_labels("基金")
STOCK_PAGE_OPTIONS = get_page_labels("股票")
MONEY_PAGE_OPTIONS = get_page_labels("资金")
MACRO_PAGE_OPTIONS = get_page_labels("宏观")


def _page_label(module_label: str, page_id: str) -> str:
    for module in SIDEBAR_MODULES:
        if module.label != module_label:
            continue
        for page in module.pages:
            if page.id == page_id:
                return page.label
        raise KeyError(f"Unknown page id {page_id!r} for module {module_label!r}")
    raise KeyError(f"Unknown module {module_label!r}")


DECISION_TODAY_PAGE_LABEL = _page_label("决策", "commercial_mvp")
DECISION_DAILY_RECO_PAGE_LABEL = _page_label("决策", "daily_trend_reco")
DECISION_RECO_EVAL_PAGE_LABEL = _page_label("决策", "reco_eval")
DECISION_ML_PAGE_LABEL = _page_label("决策", "ml_upgrade")

ETF_MAIN_PAGE_LABEL = _page_label("基金", "etf_main")
ETF_RATIO_PAGE_LABEL = _page_label("基金", "etf_ratio")
ETF_TREND_PAGE_LABEL = _page_label("基金", "etf_trend")
ETF_WIDE_INDEX_PAGE_LABEL = _page_label("基金", "wide_index")
ETF_FUND_MONITOR_PAGE_LABEL = _page_label("基金", "fund_monitor")

STOCK_SECURITY_SEARCH_LABEL = _page_label("股票", "security_search")
STOCK_LHB_PAGE_LABEL = _page_label("股票", "lhb_monitor")
STOCK_USER_WATCHLIST_LABEL = _page_label("股票", "user_watchlist")
STOCK_FUND_WATCHLIST_LABEL = _page_label("股票", "fund_watchlist")
STOCK_POOL_PAGE_LABEL = _page_label("股票", "stock_pool")
STOCK_COMPANY_SCREENER_LABEL = _page_label("股票", "company_screener")
STOCK_TECH_PICKER_LABEL = _page_label("股票", "tech_picker")

MONEY_FLOW_PAGE_LABEL = _page_label("资金", "moneyflow")
MONEY_VOLUME_PAGE_LABEL = _page_label("资金", "volume")
MONEY_FUND_HOT_PAGE_LABEL = _page_label("资金", "fund_hot_stocks")
MONEY_LIMITUP_PAGE_LABEL = _page_label("资金", "limitup")
MONEY_HOTMONEY_PAGE_LABEL = _page_label("资金", "hotmoney")

MACRO_MAIN_PAGE_LABEL = _page_label("宏观", "macro")
MACRO_DEPOSIT_PAGE_LABEL = _page_label("宏观", "deposit")
MACRO_INDEX_MONITOR_PAGE_LABEL = _page_label("宏观", "index_monitor")
