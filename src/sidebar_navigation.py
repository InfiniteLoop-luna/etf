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
        id="decision",
        label="\u51b3\u7b56",
        session_key="decision_subpage",
        pages=(
            SidebarPage(
                "commercial_mvp",
                "\U0001f4bc \u4eca\u65e5\u673a\u4f1a\u6e05\u5355",
                "\u4eca\u5929\u5148\u770b\u673a\u4f1a\u6e05\u5355",
                "light",
            ),
            SidebarPage(
                "daily_trend_reco",
                "\u2b50 \u6bcf\u65e5\u8d8b\u52bf\u63a8\u8350",
                "\u8d8b\u52bf\u5019\u9009\u548c\u7406\u7531",
                "light",
            ),
            SidebarPage(
                "reco_eval",
                "\U0001f9ea \u63a8\u8350\u8bc4\u4f30",
                "\u56de\u770b\u7b56\u7565\u8868\u73b0",
                "heavy",
            ),
            SidebarPage(
                "ml_upgrade",
                "\U0001f9e0 ML\u9884\u6d4b\u5347\u7ea7",
                "\u6a21\u578b\u7ed3\u679c\u9875",
                "standard",
            ),
        ),
    ),
    SidebarModule(
        id="fund",
        label="\u57fa\u91d1",
        session_key="etf_subpage",
        pages=(
            SidebarPage(
                "etf_main",
                "\U0001f4c8 \u4e3b\u8981\u5bbd\u57faETF\u4efd\u989d",
                "\u6838\u5fc3ETF\u4efd\u989d\u8d70\u52bf",
                "standard",
            ),
            SidebarPage(
                "etf_ratio",
                "\U0001f967 ETF\u5206\u7c7b\u5360\u6bd4",
                "\u6309\u5206\u7c7b\u770b\u5360\u6bd4",
                "light",
            ),
            SidebarPage(
                "etf_trend",
                "\U0001f4c8 ETF\u5206\u7c7b\u8d8b\u52bf",
                "\u5206\u7c7b\u65f6\u95f4\u5e8f\u5217",
                "standard",
            ),
            SidebarPage(
                "wide_index",
                "\U0001f4ca \u5bbd\u57fa\u6307\u6570ETF",
                "\u57fa\u51c6\u6307\u6570\u8054\u5408",
                "standard",
            ),
            SidebarPage(
                "fund_monitor",
                "\U0001f4c8 \u57fa\u91d1\u76d1\u6d4b",
                "\u57fa\u91d1\u76d1\u6d4b\u770b\u677f",
                "standard",
            ),
        ),
    ),
    SidebarModule(
        id="stock",
        label="\u80a1\u7968",
        session_key="stock_subpage",
        pages=(
            SidebarPage(
                "security_search",
                "\U0001f50e \u4e2a\u80a1/\u6307\u6570\u67e5\u8be2",
                "\u67e5\u8be2\u5355\u53ea\u80a1\u7968\u6216\u6307\u6570",
                "heavy",
            ),
            SidebarPage(
                "company_screener",
                "\U0001f3e2 \u516c\u53f8\u7b5b\u9009",
                "\u6309\u516c\u53f8\u7ef4\u5ea6\u7b5b\u9009",
                "standard",
            ),
            SidebarPage(
                "tech_picker",
                "\U0001f3af \u6280\u672f\u9009\u80a1",
                "\u6280\u672f\u9762\u5019\u9009\u6e05\u5355",
                "standard",
            ),
            SidebarPage(
                "factor_workbench",
                "\U0001f9e0 \u56e0\u5b50\u9009\u80a1\u5de5\u4f5c\u53f0",
                "\u591a\u56e0\u5b50\u5de5\u4f5c\u53f0",
                "heavy",
            ),
            SidebarPage(
                "author_tracking",
                "\U0001f9ed \u89c2\u70b9\u8ddf\u8e2a",
                "\u4f5c\u8005\u89c2\u70b9\u7814\u7a76\u53f0",
                "heavy",
            ),
        ),
    ),
    SidebarModule(
        id="money",
        label="\u8d44\u91d1",
        session_key="money_subpage",
        pages=(
            SidebarPage(
                "moneyflow",
                "\U0001f4b9 \u8d44\u91d1\u6d41\u5411",
                "\u4e2a\u80a1\u4e0e\u677f\u5757\u8d44\u91d1\u6d41",
                "standard",
            ),
            SidebarPage(
                "volume",
                "\U0001f4ca \u6bcf\u65e5\u6210\u4ea4\u91cf",
                "A\u80a1\u6210\u4ea4\u91cf\u65f6\u95f4\u5e8f\u5217",
                "standard",
            ),
            SidebarPage(
                "fund_hot_stocks",
                "\U0001f3e6 \u516c\u52df\u6301\u4ed3\u70ed\u80a1",
                "\u57fa\u91d1\u6301\u4ed3\u70ed\u80a1\u900f\u89c6",
                "standard",
            ),
            SidebarPage(
                "limitup",
                "\U0001f525 \u6253\u677f\u60c5\u7eea",
                "\u6da8\u505c\u63a5\u529b\u60c5\u7eea",
                "standard",
            ),
            SidebarPage(
                "hotmoney",
                "\U0001f9fe \u6e38\u8d44\u540d\u5f55",
                "\u6e38\u8d44\u6d3b\u8dc3\u660e\u7ec6",
                "standard",
            ),
        ),
    ),
    SidebarModule(
        id="macro",
        label="\u5b8f\u89c2",
        session_key="macro_subpage",
        pages=(
            SidebarPage(
                "macro",
                "\U0001f30f \u5b8f\u89c2\u7ecf\u6d4e",
                "\u5b8f\u89c2\u6307\u6807\u603b\u89c8",
                "standard",
            ),
            SidebarPage(
                "deposit",
                "\U0001f3e6 \u672c\u5916\u5e01\u5b58\u6b3e",
                "\u5b58\u6b3e\u6708\u5ea6\u6570\u636e",
                "standard",
            ),
            SidebarPage(
                "index_monitor",
                "\U0001f4ca \u6307\u6570\u76d1\u6d4b",
                "\u6307\u6570\u89c2\u5bdf\u770b\u677f",
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


def ensure_sidebar_state(session_state: MutableMapping[str, object]) -> None:
    if RECENT_VISITS_KEY not in session_state:
        session_state[RECENT_VISITS_KEY] = []


def _normalize_query(value: str) -> str:
    return value.strip().lower()


def _coerce_module(module_value: str) -> SidebarModule:
    if module_value in MODULE_BY_ID:
        return get_module_by_id(module_value)
    return get_module_by_label(module_value)


def _coerce_page(module: SidebarModule, page_value: str) -> SidebarPage:
    if page_value in PAGE_BY_ID:
        page = get_page_by_id(page_value)
        if get_module_id_for_page_id(page.id) != module.id:
            raise KeyError(f"Unknown page {page_value!r} for module {module.id!r}")
        return page
    return get_page_by_label(module.label, page_value)


def _build_recent_visit(module: SidebarModule, page: SidebarPage) -> dict[str, str]:
    return {
        "module_id": module.id,
        "module_label": module.label,
        "page_id": page.id,
        "page_label": page.label,
    }


def _normalize_recent_visit(item: object) -> dict[str, str] | None:
    if not isinstance(item, dict):
        return None

    module_id = item.get("module_id")
    page_id = item.get("page_id")
    if isinstance(module_id, str) and isinstance(page_id, str):
        try:
            module = get_module_by_id(module_id)
            page = get_page_by_id(page_id)
        except KeyError:
            return None
        if get_module_id_for_page_id(page.id) != module.id:
            return None
        return _build_recent_visit(module, page)

    module_label = item.get("module")
    page_label = item.get("page")
    if isinstance(module_label, str) and isinstance(page_label, str):
        try:
            module = get_module_by_label(module_label)
            page = get_page_by_label(module.label, page_label)
        except KeyError:
            return None
        return _build_recent_visit(module, page)

    return None


def record_recent_visit(session_state: MutableMapping[str, object], module_id: str, page_id: str) -> None:
    module = _coerce_module(module_id)
    page = _coerce_page(module, page_id)
    visits = get_recent_visits(session_state)
    latest = _build_recent_visit(module, page)
    existing = [item for item in visits if item["page_id"] != page.id]
    session_state[RECENT_VISITS_KEY] = [latest] + existing[: MAX_RECENT_PAGES - 1]


def get_recent_visits(session_state: MutableMapping[str, object]) -> list[dict[str, str]]:
    ensure_sidebar_state(session_state)
    normalized: list[dict[str, str]] = []
    seen_page_ids: set[str] = set()

    for item in session_state[RECENT_VISITS_KEY]:
        normalized_item = _normalize_recent_visit(item)
        if normalized_item is None or normalized_item["page_id"] in seen_page_ids:
            continue
        seen_page_ids.add(normalized_item["page_id"])
        normalized.append(normalized_item)
        if len(normalized) == MAX_RECENT_PAGES:
            break

    session_state[RECENT_VISITS_KEY] = normalized
    return [dict(item) for item in normalized]


def search_sidebar_pages(query: str) -> list[SidebarSearchResult]:
    normalized_query = _normalize_query(query)
    if not normalized_query:
        return []

    matched_module_ids = {
        module.id
        for module in SIDEBAR_MODULES
        if _normalize_query(module.id).startswith(normalized_query)
        or normalized_query in _normalize_query(module.id)
        or _normalize_query(module.label).startswith(normalized_query)
        or normalized_query in _normalize_query(module.label)
    }

    ranked_results: list[tuple[int, int, int, SidebarSearchResult]] = []
    for module_index, module in enumerate(SIDEBAR_MODULES):
        module_id_text = _normalize_query(module.id)
        module_label_text = _normalize_query(module.label)
        module_matches = (
            module_id_text.startswith(normalized_query)
            or normalized_query in module_id_text
            or module_label_text.startswith(normalized_query)
            or normalized_query in module_label_text
        )
        if matched_module_ids and module.id not in matched_module_ids:
            continue

        for page_index, page in enumerate(module.pages):
            page_id_text = _normalize_query(page.id)
            page_label_text = _normalize_query(page.label)
            description_text = _normalize_query(page.description)

            rank: int | None = None
            if page_id_text.startswith(normalized_query) or page_label_text.startswith(normalized_query):
                rank = 0
            elif normalized_query in page_id_text or normalized_query in page_label_text:
                rank = 1
            elif module_matches:
                rank = 2
            elif normalized_query in description_text:
                rank = 3

            if rank is None:
                continue

            ranked_results.append(
                (
                    rank,
                    module_index,
                    page_index,
                    SidebarSearchResult(
                        module_id=module.id,
                        module_label=module.label,
                        page_id=page.id,
                        page_label=page.label,
                        description=page.description,
                        score=4 - rank,
                    ),
                )
            )

    ranked_results.sort(key=lambda item: (item[0], item[1], item[2], item[3].page_id))
    return [item[3] for item in ranked_results]


def resolve_expanded_module_id(active_page_id: str, requested_module_id: str | None) -> str:
    if requested_module_id in MODULE_BY_ID:
        return requested_module_id
    return get_module_id_for_page_id(active_page_id)
