from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, MutableMapping

ToolbarVariant = Literal["light", "standard", "heavy"]

RECENT_VISITS_KEY = "sidebar_recent_pages"
MAX_RECENT_PAGES = 6


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


@dataclass(frozen=True)
class SidebarSearchResult:
    module_id: str
    module_label: str
    page_id: str
    page_label: str
    description: str
    score: int


SIDEBAR_MODULES = (
    SidebarModule(
        id="fund",
        label="基金",
        session_key="etf_subpage",
        pages=(
            SidebarPage(
                "etf_main",
                "📈 主要宽基ETF份额",
                "核心ETF份额走势",
                "standard",
            ),
            SidebarPage(
                "etf_ratio",
                "🥧 ETF分类占比",
                "按分类看占比",
                "light",
            ),
            SidebarPage(
                "etf_trend",
                "📈 ETF分类趋势",
                "分类时间序列",
                "standard",
            ),
            SidebarPage(
                "wide_index",
                "📊 宽基指数ETF",
                "基准指数联合",
                "standard",
            ),
            SidebarPage(
                "fund_monitor",
                "📈 基金监测",
                "基金监测看板",
                "standard",
            ),
        ),
    ),
    SidebarModule(
        id="stock",
        label="股票",
        session_key="stock_subpage",
        pages=(
            SidebarPage(
                "security_search",
                "🔎 个股/指数查询",
                "查询单只股票或指数",
                "heavy",
            ),
            SidebarPage(
                "user_watchlist",
                "⭐ 自选管理",
                "登录后管理个人自选股票",
                "standard",
            ),
            SidebarPage(
                "company_screener",
                "🏢 公司筛选",
                "按公司维度筛选",
                "standard",
            ),
            SidebarPage(
                "tech_picker",
                "🎯 技术选股",
                "技术面候选清单",
                "standard",
            ),
            SidebarPage(
                "factor_workbench",
                "🧠 因子选股工作台",
                "多因子工作台",
                "heavy",
            ),
            SidebarPage(
                "author_tracking",
                "🧭 观点跟踪",
                "作者观点研究台",
                "heavy",
            ),
        ),
    ),
    SidebarModule(
        id="money",
        label="资金",
        session_key="money_subpage",
        pages=(
            SidebarPage(
                "moneyflow",
                "💹 资金流向",
                "个股与板块资金流",
                "standard",
            ),
            SidebarPage(
                "volume",
                "📊 每日成交量",
                "A股成交量时间序列",
                "standard",
            ),
            SidebarPage(
                "fund_hot_stocks",
                "🏦 公募持仓热股",
                "基金持仓热股透视",
                "standard",
            ),
            SidebarPage(
                "limitup",
                "🔥 打板情绪",
                "涨停接力情绪",
                "standard",
            ),
            SidebarPage(
                "hotmoney",
                "🧾 游资名录",
                "游资活跃明细",
                "standard",
            ),
        ),
    ),
    SidebarModule(
        id="macro",
        label="宏观",
        session_key="macro_subpage",
        pages=(
            SidebarPage(
                "macro",
                "🌏 宏观经济",
                "宏观指标总览",
                "standard",
            ),
            SidebarPage(
                "deposit",
                "🏦 本外币存款",
                "存款月度数据",
                "standard",
            ),
            SidebarPage(
                "index_monitor",
                "📊 指数监测",
                "指数观察看板",
                "standard",
            ),
        ),
    ),
    SidebarModule(
        id="decision",
        label="决策",
        session_key="decision_subpage",
        pages=(
            SidebarPage(
                "commercial_mvp",
                "💼 今日机会清单",
                "今天先看机会清单",
                "light",
            ),
            SidebarPage(
                "daily_trend_reco",
                "⭐ 每日趋势推荐",
                "趋势候选和理由",
                "light",
            ),
            SidebarPage(
                "reco_eval",
                "🧪 推荐评估",
                "回看策略表现",
                "heavy",
            ),
            SidebarPage(
                "ml_upgrade",
                "🧠 ML预测升级",
                "模型结果页",
                "standard",
            ),
        ),
    ),
)

MODULE_BY_LABEL = {module.label: module for module in SIDEBAR_MODULES}
MODULE_BY_ID = {module.id: module for module in SIDEBAR_MODULES}
PAGE_BY_ID = {page.id: page for module in SIDEBAR_MODULES for page in module.pages}
PAGE_TO_MODULE = {page.label: module.label for module in SIDEBAR_MODULES for page in module.pages}
PAGE_ID_TO_MODULE_ID = {page.id: module.id for module in SIDEBAR_MODULES for page in module.pages}
DEFAULT_SHORTCUT_PAGE_IDS = ("commercial_mvp", "security_search", "moneyflow")


def get_module_labels() -> list[str]:
    return [module.label for module in SIDEBAR_MODULES]


def get_module_by_label(module_label: str) -> SidebarModule:
    return MODULE_BY_LABEL[module_label]


def get_module_by_id(module_id: str) -> SidebarModule:
    return MODULE_BY_ID[module_id]


def get_page_labels(module_label: str) -> list[str]:
    return [page.label for page in MODULE_BY_LABEL[module_label].pages]


def get_page_by_label(module_label: str, page_label: str) -> SidebarPage:
    module = MODULE_BY_LABEL[module_label]
    for page in module.pages:
        if page.label == page_label:
            return page
    raise KeyError(f"Unknown page {page_label!r} for module {module_label!r}")


def get_page_by_id(page_id: str) -> SidebarPage:
    return PAGE_BY_ID[page_id]


def get_module_label_for_page(page_label: str) -> str:
    return PAGE_TO_MODULE[page_label]


def get_module_id_for_page_id(page_id: str) -> str:
    return PAGE_ID_TO_MODULE_ID[page_id]


def get_default_shortcuts() -> list[str]:
    return [get_page_by_id(page_id).label for page_id in DEFAULT_SHORTCUT_PAGE_IDS]


def _coerce_module(module_id: str | SidebarModule) -> SidebarModule:
    if isinstance(module_id, SidebarModule):
        return module_id
    return MODULE_BY_ID[module_id]


def _coerce_page(page_id: str | SidebarPage) -> SidebarPage:
    if isinstance(page_id, SidebarPage):
        return page_id
    return PAGE_BY_ID[page_id]


def get_recent_visits(session_state: MutableMapping[str, object]) -> list[dict[str, str]]:
    raw_visits = session_state.get(RECENT_VISITS_KEY, [])
    normalized_visits: list[dict[str, str]] = []

    if not isinstance(raw_visits, list):
        raw_visits = []

    for item in raw_visits:
        if not isinstance(item, dict):
            continue

        module_id = item.get("module_id")
        page_id = item.get("page_id")

        if module_id in MODULE_BY_ID and page_id in PAGE_BY_ID:
            module = MODULE_BY_ID[module_id]
            page = PAGE_BY_ID[page_id]
            if get_module_id_for_page_id(page.id) != module.id:
                continue
        else:
            legacy_module = item.get("module")
            legacy_page = item.get("page")
            if legacy_module not in MODULE_BY_LABEL:
                continue
            try:
                page = get_page_by_label(legacy_module, legacy_page)
            except KeyError:
                continue
            module = MODULE_BY_LABEL[legacy_module]
            module_id = module.id
            page_id = page.id

        normalized_visits.append(
            {
                "module": module.label,
                "module_id": module.id,
                "module_label": module.label,
                "page": page.label,
                "page_id": page.id,
                "page_label": page.label,
            }
        )

    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for visit in normalized_visits:
        key = (visit["module_id"], visit["page_id"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(visit)
        if len(deduped) >= MAX_RECENT_PAGES:
            break

    if list(raw_visits) != deduped:
        session_state[RECENT_VISITS_KEY] = deduped

    return deduped


def record_recent_visit(session_state: MutableMapping[str, object], module_id: str, page_id: str) -> None:
    module = _coerce_module(module_id)
    page = _coerce_page(page_id)

    visits = get_recent_visits(session_state)
    new_entry = {
        "module": module.label,
        "module_id": module.id,
        "module_label": module.label,
        "page": page.label,
        "page_id": page.id,
        "page_label": page.label,
    }

    filtered = [visit for visit in visits if (visit["module_id"], visit["page_id"]) != (module.id, page.id)]
    session_state[RECENT_VISITS_KEY] = [new_entry, *filtered][:MAX_RECENT_PAGES]


def _normalize_query(text: str) -> str:
    return str(text or "").strip().lower()


def search_sidebar_pages(query: str) -> list[SidebarSearchResult]:
    normalized_query = _normalize_query(query)
    if not normalized_query:
        return []

    results: list[SidebarSearchResult] = []
    for module_index, module in enumerate(SIDEBAR_MODULES):
        module_label_text = _normalize_query(module.label)
        module_id_text = _normalize_query(module.id)
        module_hit = (
            normalized_query == module_label_text
            or normalized_query == module_id_text
            or module_label_text.startswith(normalized_query)
            or module_id_text.startswith(normalized_query)
            or normalized_query in module_label_text
            or normalized_query in module_id_text
        )

        for page_index, page in enumerate(module.pages):
            haystacks = [
                _normalize_query(page.label),
                _normalize_query(page.id),
                _normalize_query(page.description),
            ]
            score = 0
            if normalized_query == haystacks[0] or normalized_query == haystacks[1]:
                score = 120
            elif haystacks[0].startswith(normalized_query) or haystacks[1].startswith(normalized_query):
                score = 100
            elif any(normalized_query in haystack for haystack in haystacks):
                score = 80
            elif module_hit:
                score = 40

            if score == 0:
                continue

            results.append(
                SidebarSearchResult(
                    module_id=module.id,
                    module_label=module.label,
                    page_id=page.id,
                    page_label=page.label,
                    description=page.description,
                    score=score - module_index - page_index,
                )
            )

    return sorted(results, key=lambda item: item.score, reverse=True)


def resolve_expanded_module_id(active_page_id: str, requested_module_id: str | None) -> str:
    if requested_module_id in MODULE_BY_ID:
        return requested_module_id
    return get_module_id_for_page_id(active_page_id)
