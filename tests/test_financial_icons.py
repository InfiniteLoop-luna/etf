from base64 import b64decode
from functools import wraps
from pathlib import Path
import re
from types import SimpleNamespace

import pandas as pd

from src.apple_theme import build_global_apple_theme_css
from src.financial_icons import (
    EMOJI_ICON_MAP,
    _install_wrappers,
    _sanitize_table_data,
    replace_emoji_icons,
    replace_emoji_icons_html,
    strip_emoji_icons,
)
from src.page_shell import PageStatus, build_page_loading_mask_html, build_page_status_bar_html


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_markdown_renderer_replaces_financial_emoji_with_local_svg_assets():
    rendered = replace_emoji_icons("📈 趋势 ⚠️ 风险 ⭐ 收藏 🔎 查询")

    assert "📈" not in rendered
    assert "⚠" not in rendered
    assert "⭐" not in rendered
    assert "🔎" not in rendered
    payloads = re.findall(r"data:image/svg\+xml;base64,([A-Za-z0-9+/=]+)", rendered)

    assert len(payloads) == 4
    assert all(b"<svg" in b64decode(payload) for payload in payloads)
    assert "app/static/icons/" not in rendered


def test_html_renderer_uses_real_image_elements_in_unsafe_html_blocks():
    rendered = replace_emoji_icons_html("<strong>💹 资金流向</strong>")

    assert "💹" not in rendered
    assert 'class="ws-inline-svg-icon"' in rendered
    assert 'src="app/static/icons/badge-dollar-sign.svg"' in rendered


def test_plain_text_controls_remove_emoji_without_changing_business_copy():
    assert strip_emoji_icons("✅ 已在自选") == "已在自选"
    assert strip_emoji_icons("📈 12上涨 / 📉 3下跌") == "12上涨 / 3下跌"


def test_table_sanitizer_cleans_a_display_copy_and_preserves_source_data():
    source = pd.DataFrame({"状态": ["🔥 强势", "⚠️ 震荡"], "数值": [1, 2]})

    rendered = _sanitize_table_data(source)

    assert rendered["状态"].tolist() == ["强势", "震荡"]
    assert source["状态"].tolist() == ["🔥 强势", "⚠️ 震荡"]
    assert rendered["数值"].tolist() == [1, 2]


def test_every_mapped_icon_asset_is_packaged_with_the_app():
    missing = sorted(
        icon_name
        for icon_name, _ in set(EMOJI_ICON_MAP.values())
        if not (PROJECT_ROOT / "static" / "icons" / f"{icon_name}.svg").is_file()
    )

    assert missing == []


def test_sidebar_module_icons_use_svg_masks_instead_of_character_glyphs():
    css = build_global_apple_theme_css()

    assert 'mask-image: url("app/static/icons/chart-candlestick.svg")' in css
    assert 'mask-image: url("app/static/icons/landmark.svg")' in css
    assert 'mask-image: url("app/static/icons/database.svg")' in css
    assert 'mask: url("app/static/icons/chevron-right.svg")' in css
    assert 'url("/app/static/icons/' not in css
    assert 'content: "\\25A1"' not in css
    assert 'content: "\\203A"' not in css


def test_html_icon_generators_inline_svg_for_direct_and_proxied_deployments():
    app_source = (PROJECT_ROOT / "app.py").read_text(encoding="utf-8", errors="ignore")
    status_html = build_page_status_bar_html(PageStatus())
    loading_html = build_page_loading_mask_html()

    assert 'financial_icons.icon_asset_data_uri("user-round")' in app_source
    assert 'src="app/static/icons/' not in app_source
    assert status_html.count('src="data:image/svg+xml;base64,') == 2
    assert loading_html.count('src="data:image/svg+xml;base64,') == 1
    assert 'src="app/static/icons/' not in status_html
    assert 'src="app/static/icons/' not in loading_html


def test_icon_renderer_upgrades_an_existing_legacy_wrapper():
    emoji = next(iter(EMOJI_ICON_MAP))

    def original_button(label):
        return label

    @wraps(original_button)
    def legacy_button(label):
        return label.replace(
            emoji,
            "![legacy](/app/static/icons/search.svg)",
        )
    legacy_button._wealthspark_svg_icon_wrapper = True

    def original_info(body):
        return body

    @wraps(original_info)
    def legacy_info(body):
        return f"![legacy](/app/static/icons/info.svg) {body}"
    legacy_info._wealthspark_svg_icon_wrapper = True

    @wraps(legacy_info)
    def current_info(body):
        return legacy_info(f"![current](data:image/svg+xml;base64,AAAA) {body}")
    current_info._wealthspark_svg_icon_wrapper = True

    legacy_target = SimpleNamespace(
        _wealthspark_svg_icons_installed=True,
        _wealthspark_svg_icons_revision="streamlit-base-path-v2",
        button=legacy_button,
        info=current_info,
    )

    _install_wrappers(legacy_target, bound_module=True)

    rendered = legacy_target.button(f"{emoji} Search")
    assert "data:image/svg+xml;base64," in rendered
    assert "/app/static/icons/" not in rendered
    rendered_info = legacy_target.info("No selection")
    assert "data:image/svg+xml;base64," in rendered_info
    assert "/app/static/icons/" not in rendered_info
    assert legacy_target._wealthspark_svg_icons_revision == "streamlit-base-path-v3"


def test_streamlit_entry_reloads_icon_renderer_for_hot_deploys():
    app_source = (PROJECT_ROOT / "app.py").read_text(encoding="utf-8", errors="ignore")

    assert "importlib.reload(financial_icons)" in app_source
