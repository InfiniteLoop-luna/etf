from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, MutableMapping

ToolbarVariant = Literal["light", "standard", "heavy"]

RECENT_VISITS_KEY = "sidebar_recent_pages"
MAX_RECENT_PAGES = 4


@dataclass(frozen=True)
class SidebarPage:
    id: str
    label: str
    description: str
    toolbar_variant: ToolbarVariant


@dataclass(frozen=True)
class SidebarModule:
    id: str
    label: str
    session_key: str
    pages: tuple[SidebarPage, ...]


SIDEBAR_MODULES = (
    SidebarModule(
        id="decision",
        label="决策",
        session_key="decision_subpage",
        pages=(
            SidebarPage("commercial_mvp", "💼 今日机会清单", "今天先看机会清单", "light"),
            SidebarPage("daily_trend_reco", "⭐ 每日趋势推荐", "趋势候选和理由", "light"),
            SidebarPage("reco_eval", "🧪 推荐评估", "回看策略表现", "heavy"),
            SidebarPage("ml_upgrade", "🧠 ML预测升级", "模型结果页", "standard"),
        ),
    ),
    SidebarModule(
        id="fund",
        label="基金",
        session_key="etf_subpage",
        pages=(
            SidebarPage("etf_main", "📈 主要宽基ETF份额", "核心ETF份额走势", "standard"),
            SidebarPage("etf_ratio", "🥧 ETF分类占比", "按分类看占比", "light"),
            SidebarPage("etf_trend", "📈 ETF分类趋势", "分类时间序列", "standard"),
            SidebarPage("wide_index", "📊 宽基指数ETF", "基准指数聚合", "standard"),
            SidebarPage("fund_monitor", "📈 基金监测", "基金监测看板", "standard"),
        ),
    ),
    SidebarModule(
        id="stock",
        label="个股",
        session_key="stock_subpage",
        pages=(
            SidebarPage("security_search", "🔎 个股/指数查询", "查询单只股票或指数", "heavy"),
            SidebarPage("company_screener", "🏢 公司筛选", "按公司维度筛选", "standard"),
            SidebarPage("tech_picker", "🎯 技术选股", "技术面候选清单", "standard"),
            SidebarPage("factor_workbench", "🧠 因子选股工作台", "多因子工作台", "heavy"),
            SidebarPage("author_tracking", "🧭 观点跟踪", "作者观点研究台", "heavy"),
        ),
    ),
    SidebarModule(
        id="money",
        label="资金",
        session_key="money_subpage",
        pages=(
            SidebarPage("moneyflow", "💹 资金流向", "个股与板块资金流", "standard"),
            SidebarPage("volume", "📊 每日成交量", "A股成交量时间序列", "standard"),
            SidebarPage("fund_hot_stocks", "🏦 公募持仓热股", "基金持仓热股透视", "standard"),
            SidebarPage("limitup", "🔥 打板情绪", "涨停接力情绪", "standard"),
            SidebarPage("hotmoney", "🧾 游资名录", "游资活跃明细", "standard"),
        ),
    ),
    SidebarModule(
        id="macro",
        label="宏观",
        session_key="macro_subpage",
        pages=(
            SidebarPage("macro", "🌏 宏观经济", "宏观指标总览", "standard"),
            SidebarPage("deposit", "🏦 本外币存款", "存款月度数据", "standard"),
            SidebarPage("index_monitor", "📊 指数监测", "指数观察看板", "standard"),
        ),
    ),
)

MODULE_BY_LABEL = {module.label: module for module in SIDEBAR_MODULES}
PAGE_TO_MODULE = {page.label: module.label for module in SIDEBAR_MODULES for page in module.pages}
DEFAULT_SHORTCUTS = ["💼 今日机会清单", "🔎 个股/指数查询", "💹 资金流向"]


def get_module_labels() -> list[str]:
    return [module.label for module in SIDEBAR_MODULES]


def get_module_by_label(module_label: str) -> SidebarModule:
    return MODULE_BY_LABEL[module_label]


def get_page_labels(module_label: str) -> list[str]:
    return [page.label for page in MODULE_BY_LABEL[module_label].pages]


def get_page_by_label(module_label: str, page_label: str) -> SidebarPage:
    module = MODULE_BY_LABEL[module_label]
    for page in module.pages:
        if page.label == page_label:
            return page
    raise KeyError(f"Unknown page {page_label!r} for module {module_label!r}")


def get_module_label_for_page(page_label: str) -> str:
    return PAGE_TO_MODULE[page_label]


def get_default_shortcuts() -> list[str]:
    return list(DEFAULT_SHORTCUTS)


def ensure_sidebar_state(session_state: MutableMapping[str, object]) -> None:
    if RECENT_VISITS_KEY not in session_state:
        session_state[RECENT_VISITS_KEY] = []


def record_recent_visit(session_state: MutableMapping[str, object], module_label: str, page_label: str) -> None:
    ensure_sidebar_state(session_state)
    latest = {"module": module_label, "page": page_label}
    existing = [item for item in session_state[RECENT_VISITS_KEY] if item != latest]
    session_state[RECENT_VISITS_KEY] = [latest] + existing[: MAX_RECENT_PAGES - 1]


def get_recent_visits(session_state: MutableMapping[str, object]) -> list[dict[str, str]]:
    ensure_sidebar_state(session_state)
    return list(session_state[RECENT_VISITS_KEY])
