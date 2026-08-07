from __future__ import annotations

from functools import wraps
from html import escape
import re
from typing import Any, Callable


ICON_ASSET_ROOT = "app/static/icons"

# Emoji remain valid internal labels for backward-compatible navigation and
# persisted state. At render time they are replaced with local Lucide SVGs.
EMOJI_ICON_MAP: dict[str, tuple[str, str]] = {
    "➕": ("plus", "添加"),
    "⬜": ("square", "取消选择"),
    "⏳": ("hourglass", "处理中"),
    "☑": ("square-check-big", "选择"),
    "✏": ("pencil", "编辑"),
    "✖": ("circle-x", "关闭"),
    "✅": ("circle-check", "完成"),
    "❌": ("circle-x", "错误"),
    "⚠": ("triangle-alert", "警告"),
    "⚡": ("zap", "快速"),
    "⚖": ("scale", "比较"),
    "⚙": ("settings", "设置"),
    "⚫": ("circle", "状态"),
    "ℹ": ("info", "信息"),
    "⏱": ("timer", "计时"),
    "⭐": ("star", "收藏"),
    "🆕": ("badge-plus", "新增"),
    "🌏": ("globe", "全球"),
    "🌐": ("network", "网络"),
    "🌳": ("tree-pine", "目录"),
    "🎬": ("clapperboard", "演示"),
    "🎯": ("crosshair", "目标"),
    "🏆": ("trophy", "排行"),
    "🏛": ("landmark", "机构"),
    "🏢": ("building-2", "公司"),
    "🏦": ("landmark", "金融机构"),
    "🏭": ("factory", "行业"),
    "🏷": ("tags", "标签"),
    "🐉": ("trophy", "龙虎榜"),
    "👀": ("eye", "观察"),
    "👑": ("crown", "重点"),
    "👤": ("user-round", "用户"),
    "💡": ("lightbulb", "提示"),
    "💧": ("droplets", "流动性"),
    "💰": ("coins", "资金"),
    "💹": ("badge-dollar-sign", "资金走势"),
    "💼": ("briefcase-business", "决策"),
    "📂": ("folder-open", "筛选"),
    "📄": ("file-text", "文档"),
    "📅": ("calendar-days", "日期"),
    "📈": ("trending-up", "上涨趋势"),
    "📉": ("trending-down", "下跌趋势"),
    "📊": ("chart-no-axes-column-increasing", "数据图表"),
    "📋": ("clipboard-list", "清单"),
    "📌": ("pin", "重点"),
    "📍": ("map-pin", "位置"),
    "📑": ("files", "报告"),
    "📗": ("book-open", "资料"),
    "📘": ("book-open", "资料"),
    "📚": ("book-open", "资料"),
    "📜": ("scroll-text", "记录"),
    "📝": ("notebook-pen", "编辑记录"),
    "📢": ("megaphone", "公告"),
    "📤": ("upload", "上传"),
    "📥": ("download", "下载"),
    "📰": ("newspaper", "新闻"),
    "🔁": ("repeat-2", "切换"),
    "🔄": ("refresh-cw", "刷新"),
    "🔍": ("search", "搜索"),
    "🔎": ("search", "查询"),
    "🔐": ("lock-keyhole", "权限"),
    "🔓": ("lock-keyhole-open", "公开"),
    "🔥": ("flame", "热门"),
    "🔮": ("telescope", "预测"),
    "🔴": ("circle", "下跌"),
    "🔸": ("circle-dot", "要点"),
    "🔻": ("arrow-down-right", "弱势"),
    "🕒": ("clock-3", "时间"),
    "🕯": ("chart-candlestick", "K线"),
    "🖥": ("monitor", "终端"),
    "🗂": ("folder-tree", "目录"),
    "🗑": ("trash-2", "删除"),
    "🚀": ("rocket", "执行"),
    "🚨": ("siren", "风险"),
    "🛠": ("wrench", "工具"),
    "🟡": ("circle", "等待"),
    "🟢": ("circle", "上涨"),
    "🤖": ("bot", "模型"),
    "🥧": ("chart-pie", "占比"),
    "🧠": ("brain-circuit", "智能分析"),
    "🧨": ("bomb", "异常"),
    "🧩": ("component", "对象"),
    "🧪": ("flask-conical", "实验"),
    "🧭": ("compass", "跟踪"),
    "🧱": ("brick-wall", "结构"),
    "🧺": ("shopping-basket", "持仓"),
    "🧾": ("receipt-text", "明细"),
    "🩺": ("heart-pulse", "健康度"),
}


def _normalized_emoji(value: str) -> str:
    return value.replace("\ufe0f", "").replace("\ufe0e", "")


_KNOWN_EMOJI_PATTERN = "|".join(
    re.escape(key) + "(?:\\ufe0f|\\ufe0e)?"
    for key in sorted(EMOJI_ICON_MAP, key=len, reverse=True)
)
_EMOJI_PATTERN = re.compile(
    rf"(?:{_KNOWN_EMOJI_PATTERN}|[\U0001F000-\U0001FAFF](?:\ufe0f|\ufe0e)?(?:\u200d[\U0001F000-\U0001FAFF](?:\ufe0f|\ufe0e)?)*)"
)


def icon_asset_url(icon_name: str) -> str:
    safe_name = re.sub(r"[^a-z0-9-]", "", str(icon_name).lower()) or "activity"
    return f"{ICON_ASSET_ROOT}/{safe_name}.svg"


def icon_markdown(icon_name: str, alt: str) -> str:
    return f"![{alt}]({icon_asset_url(icon_name)})"


def replace_emoji_icons(value: Any) -> Any:
    """Replace Emoji glyphs in Markdown-capable UI text with local SVG images."""
    if not isinstance(value, str) or not value:
        return value

    def replace(match: re.Match[str]) -> str:
        icon_name, alt = EMOJI_ICON_MAP.get(
            _normalized_emoji(match.group(0)),
            ("activity", "状态"),
        )
        return icon_markdown(icon_name, alt)

    return _EMOJI_PATTERN.sub(replace, value)


def replace_emoji_icons_html(value: Any) -> Any:
    """Replace Emoji glyphs inside unsafe HTML blocks with inline SVG images."""
    if not isinstance(value, str) or not value:
        return value

    def replace(match: re.Match[str]) -> str:
        icon_name, alt = EMOJI_ICON_MAP.get(
            _normalized_emoji(match.group(0)),
            ("activity", "状态"),
        )
        return (
            '<img class="ws-inline-svg-icon" '
            f'src="{icon_asset_url(icon_name)}" alt="{escape(alt)}">'
        )

    return _EMOJI_PATTERN.sub(replace, value)


def strip_emoji_icons(value: Any) -> Any:
    """Remove Emoji from controls that cannot render Markdown images."""
    if not isinstance(value, str) or not value:
        return value
    cleaned = _EMOJI_PATTERN.sub("", value)
    return re.sub(r"[ \t]{2,}", " ", cleaned).strip()


def _replace_first_text_argument(
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    transform: Callable[[Any], Any],
    keyword_names: tuple[str, ...],
    argument_index: int,
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    new_args = list(args)
    new_kwargs = dict(kwargs)
    if len(new_args) > argument_index:
        new_args[argument_index] = transform(new_args[argument_index])
    else:
        for keyword in keyword_names:
            if keyword in new_kwargs:
                new_kwargs[keyword] = transform(new_kwargs[keyword])
                break
    if "help" in new_kwargs:
        new_kwargs["help"] = replace_emoji_icons(new_kwargs["help"])
    return tuple(new_args), new_kwargs


def _wrap_label_callable(original: Callable[..., Any], argument_index: int) -> Callable[..., Any]:
    @wraps(original)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        new_args, new_kwargs = _replace_first_text_argument(
            args,
            kwargs,
            replace_emoji_icons,
            ("label", "body"),
            argument_index,
        )
        return original(*new_args, **new_kwargs)

    return wrapped


def _wrap_markdown_callable(original: Callable[..., Any], argument_index: int) -> Callable[..., Any]:
    @wraps(original)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        unsafe_allow_html = bool(kwargs.get("unsafe_allow_html"))
        if len(args) > argument_index + 1:
            unsafe_allow_html = bool(args[argument_index + 1])
        transform = replace_emoji_icons_html if unsafe_allow_html else replace_emoji_icons
        new_args, new_kwargs = _replace_first_text_argument(
            args,
            kwargs,
            transform,
            ("body",),
            argument_index,
        )
        return original(*new_args, **new_kwargs)

    return wrapped


def _wrap_tabs_callable(original: Callable[..., Any], argument_index: int) -> Callable[..., Any]:
    @wraps(original)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        new_args = list(args)
        new_kwargs = dict(kwargs)
        if len(new_args) > argument_index:
            new_args[argument_index] = [
                replace_emoji_icons(label) for label in new_args[argument_index]
            ]
        elif "tabs" in new_kwargs:
            new_kwargs["tabs"] = [replace_emoji_icons(label) for label in new_kwargs["tabs"]]
        if new_kwargs.get("default") is not None:
            new_kwargs["default"] = replace_emoji_icons(new_kwargs["default"])
        return original(*new_args, **new_kwargs)

    return wrapped


def _wrap_metric_callable(original: Callable[..., Any], argument_index: int) -> Callable[..., Any]:
    @wraps(original)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        new_args = list(args)
        new_kwargs = dict(kwargs)
        if len(new_args) > argument_index:
            new_args[argument_index] = replace_emoji_icons(new_args[argument_index])
        elif "label" in new_kwargs:
            new_kwargs["label"] = replace_emoji_icons(new_kwargs["label"])
        for index in (argument_index + 1, argument_index + 2):
            if len(new_args) > index:
                new_args[index] = strip_emoji_icons(new_args[index])
        for keyword in ("value", "delta"):
            if keyword in new_kwargs:
                new_kwargs[keyword] = strip_emoji_icons(new_kwargs[keyword])
        return original(*new_args, **new_kwargs)

    return wrapped


def _wrap_choice_callable(
    original: Callable[..., Any],
    argument_index: int,
    option_transform: Callable[[Any], Any],
) -> Callable[..., Any]:
    @wraps(original)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        new_args, new_kwargs = _replace_first_text_argument(
            args,
            kwargs,
            replace_emoji_icons,
            ("label",),
            argument_index,
        )
        new_args_list = list(new_args)
        format_index = argument_index + 3
        if len(new_args_list) > format_index:
            original_format = new_args_list[format_index]
            new_args_list[format_index] = (
                lambda option, formatter=original_format: option_transform(formatter(option))
            )
        else:
            original_format = new_kwargs.get("format_func", str)
            new_kwargs["format_func"] = (
                lambda option, formatter=original_format: option_transform(formatter(option))
            )
        return original(*new_args_list, **new_kwargs)

    return wrapped


def _wrap_alert_callable(
    original: Callable[..., Any],
    fallback_icon: str,
    fallback_alt: str,
    argument_index: int,
) -> Callable[..., Any]:
    @wraps(original)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        new_args = list(args)
        new_kwargs = dict(kwargs)
        if len(new_args) > argument_index:
            original_body = new_args[argument_index]
            transformed = replace_emoji_icons(original_body)
            if transformed == original_body and isinstance(original_body, str):
                transformed = f"{icon_markdown(fallback_icon, fallback_alt)} {original_body}"
            new_args[argument_index] = transformed
        elif "body" in new_kwargs:
            original_body = new_kwargs["body"]
            transformed = replace_emoji_icons(original_body)
            if transformed == original_body and isinstance(original_body, str):
                transformed = f"{icon_markdown(fallback_icon, fallback_alt)} {original_body}"
            new_kwargs["body"] = transformed
        return original(*new_args, **new_kwargs)

    return wrapped


def _wrap_plain_text_callable(original: Callable[..., Any], argument_index: int) -> Callable[..., Any]:
    @wraps(original)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        new_args, new_kwargs = _replace_first_text_argument(
            args,
            kwargs,
            strip_emoji_icons,
            ("body", "label"),
            argument_index,
        )
        return original(*new_args, **new_kwargs)

    return wrapped


def _sanitize_table_data(value: Any) -> Any:
    try:
        import pandas as pd
    except ImportError:
        return value

    def sanitize_cell(cell: Any) -> Any:
        if not isinstance(cell, str):
            return cell
        if "://" in cell or cell.lstrip("/").startswith("app/static/"):
            return cell
        return strip_emoji_icons(cell)

    if isinstance(value, pd.DataFrame):
        cleaned = value.copy()
        for column in cleaned.columns:
            if pd.api.types.is_object_dtype(cleaned[column].dtype) or isinstance(
                cleaned[column].dtype,
                pd.StringDtype,
            ):
                cleaned[column] = cleaned[column].map(sanitize_cell)
        return cleaned
    if isinstance(value, pd.Series):
        return value.map(sanitize_cell)
    return value


def _wrap_table_callable(original: Callable[..., Any], argument_index: int) -> Callable[..., Any]:
    @wraps(original)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        new_args = list(args)
        new_kwargs = dict(kwargs)
        if len(new_args) > argument_index:
            new_args[argument_index] = _sanitize_table_data(new_args[argument_index])
        elif "data" in new_kwargs:
            new_kwargs["data"] = _sanitize_table_data(new_kwargs["data"])
        return original(*new_args, **new_kwargs)

    return wrapped


def _install_wrappers(target: Any, *, bound_module: bool) -> None:
    if getattr(target, "_wealthspark_svg_icons_installed", False):
        return

    argument_index = 0 if bound_module else 1
    label_methods = (
        "button",
        "download_button",
        "link_button",
        "checkbox",
        "toggle",
        "slider",
        "text_input",
        "text_area",
        "number_input",
        "date_input",
        "time_input",
        "file_uploader",
        "color_picker",
        "camera_input",
        "title",
        "header",
        "subheader",
        "caption",
        "expander",
        "status",
        "toast",
    )
    for name in label_methods:
        original = getattr(target, name, None)
        if callable(original):
            setattr(target, name, _wrap_label_callable(original, argument_index))

    for name, option_transform in (
        ("radio", replace_emoji_icons),
        ("selectbox", strip_emoji_icons),
        ("multiselect", strip_emoji_icons),
        ("select_slider", strip_emoji_icons),
    ):
        original = getattr(target, name, None)
        if callable(original):
            setattr(
                target,
                name,
                _wrap_choice_callable(original, argument_index, option_transform),
            )

    for name, wrapper in (
        ("markdown", _wrap_markdown_callable),
        ("tabs", _wrap_tabs_callable),
        ("metric", _wrap_metric_callable),
        ("text", _wrap_plain_text_callable),
    ):
        original = getattr(target, name, None)
        if callable(original):
            setattr(target, name, wrapper(original, argument_index))

    for name in ("dataframe", "table"):
        original = getattr(target, name, None)
        if callable(original):
            setattr(target, name, _wrap_table_callable(original, argument_index))

    for name, icon_name, alt in (
        ("info", "info", "信息"),
        ("success", "circle-check", "完成"),
        ("warning", "triangle-alert", "警告"),
        ("error", "circle-x", "错误"),
    ):
        original = getattr(target, name, None)
        if callable(original):
            setattr(
                target,
                name,
                _wrap_alert_callable(original, icon_name, alt, argument_index),
            )

    setattr(target, "_wealthspark_svg_icons_installed", True)


def install_streamlit_svg_icon_renderer(streamlit_module: Any) -> None:
    """Install SVG-aware render adapters for root, sidebar, and container APIs."""
    from streamlit.delta_generator import DeltaGenerator

    _install_wrappers(DeltaGenerator, bound_module=False)
    _install_wrappers(streamlit_module, bound_module=True)

    original_dialog = getattr(streamlit_module, "dialog", None)
    if callable(original_dialog) and not getattr(original_dialog, "_wealthspark_svg_icons_installed", False):
        wrapped_dialog = _wrap_label_callable(original_dialog, 0)
        setattr(wrapped_dialog, "_wealthspark_svg_icons_installed", True)
        streamlit_module.dialog = wrapped_dialog
