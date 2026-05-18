from __future__ import annotations

from collections.abc import Mapping, Sequence


def build_metric_categories(metric_types: Sequence[str]) -> dict[str, list[str]]:
    """Group ETF metric labels into the sidebar categories used by the pages."""
    categories: dict[str, list[str]] = {
        "市值类": [],
        "份额类": [],
        "变动类": [],
        "比例类": [],
        "涨跌类": [],
        "其他": [],
    }

    for metric in metric_types:
        if "市值" in metric:
            categories["市值类"].append(metric)
        elif "变动" in metric or "申赎" in metric:
            categories["变动类"].append(metric)
        elif "份额" in metric:
            categories["份额类"].append(metric)
        elif "比例" in metric:
            categories["比例类"].append(metric)
        elif "涨跌" in metric:
            categories["涨跌类"].append(metric)
        else:
            categories["其他"].append(metric)

    return {category: values for category, values in categories.items() if values}


def build_quick_metric_groups(metric_types: Sequence[str]) -> dict[str, list[str]]:
    """Build the representative metric buckets used by the quick-switch buttons."""
    return {
        "总市值": [metric for metric in metric_types if "总市值" in metric],
        "份额": [metric for metric in metric_types if "份额" in metric and "总市值" not in metric],
        "涨跌幅": [metric for metric in metric_types if "涨跌" in metric],
    }


def build_secondary_category_options(
    selected_primary: str,
    category_tree: Mapping[str, Sequence[str]],
) -> list[str]:
    """Return the linked secondary-category options for a selected primary category."""
    if not selected_primary or selected_primary == "全部":
        return []

    secondary_categories = category_tree.get(selected_primary)
    if not secondary_categories:
        return []

    return ["全部(小计)", *secondary_categories]


def resolve_trend_category_key(
    selected_primary: str,
    selected_secondary: str | None,
    category_tree: Mapping[str, Sequence[str]],
) -> str:
    """Resolve the time-series lookup key for the current primary/secondary selection."""
    if not selected_primary or selected_primary == "全部":
        return "全部"

    if not category_tree.get(selected_primary):
        return selected_primary

    if not selected_secondary or selected_secondary == "全部(小计)":
        return selected_primary

    return f"{selected_primary}-{selected_secondary}"
