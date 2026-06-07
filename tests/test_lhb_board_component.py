from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_lhb_board_component_supports_stable_click_and_drag_pan():
    component_html = PROJECT_ROOT / "src" / "lhb_board_component" / "index.html"

    html = component_html.read_text(encoding="utf-8")

    assert "streamlit:componentReady" in html
    assert "streamlit:setComponentValue" in html
    assert "squarify" in html
    assert "overflow: auto" in html
    assert "mousedown" in html
    assert "wheel" in html
    assert "<a " not in html.lower()


def test_lhb_board_component_squarify_uses_columns_for_wide_rectangles():
    component_html = PROJECT_ROOT / "src" / "lhb_board_component" / "index.html"

    html = component_html.read_text(encoding="utf-8")

    assert "if (rect.w < rect.h)" in html
    assert "const columnWidth = Math.max(1, sum / rect.h)" in html


def test_lhb_monitor_uses_component_value_instead_of_static_html_linking():
    app_source = (PROJECT_ROOT / "app.py").read_text(encoding="utf-8")

    assert "_LHB_TODAY_BOARD_COMPONENT" in app_source
    assert "board_component_value" in app_source
    assert "components.html(\n            render_lhb_today_board_html" not in app_source
