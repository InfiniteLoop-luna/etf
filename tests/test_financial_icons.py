from pathlib import Path

import pandas as pd

from src.apple_theme import build_global_apple_theme_css
from src.financial_icons import (
    EMOJI_ICON_MAP,
    _sanitize_table_data,
    replace_emoji_icons,
    replace_emoji_icons_html,
    strip_emoji_icons,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_markdown_renderer_replaces_financial_emoji_with_local_svg_assets():
    rendered = replace_emoji_icons("📈 趋势 ⚠️ 风险 ⭐ 收藏 🔎 查询")

    assert "📈" not in rendered
    assert "⚠" not in rendered
    assert "⭐" not in rendered
    assert "🔎" not in rendered
    assert "/app/static/icons/trending-up.svg" in rendered
    assert "/app/static/icons/triangle-alert.svg" in rendered
    assert "/app/static/icons/star.svg" in rendered
    assert "/app/static/icons/search.svg" in rendered


def test_html_renderer_uses_real_image_elements_in_unsafe_html_blocks():
    rendered = replace_emoji_icons_html("<strong>💹 资金流向</strong>")

    assert "💹" not in rendered
    assert 'class="ws-inline-svg-icon"' in rendered
    assert 'src="/app/static/icons/badge-dollar-sign.svg"' in rendered


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

    assert 'mask-image: url("/app/static/icons/chart-candlestick.svg")' in css
    assert 'mask-image: url("/app/static/icons/landmark.svg")' in css
    assert 'mask-image: url("/app/static/icons/database.svg")' in css
    assert 'mask: url("/app/static/icons/chevron-right.svg")' in css
    assert 'content: "\\25A1"' not in css
    assert 'content: "\\203A"' not in css
