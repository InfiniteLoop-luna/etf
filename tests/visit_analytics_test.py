from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine

from src.sidebar_navigation import get_visible_modules, is_module_visible, search_sidebar_pages
from src.visit_analytics import (
    extract_client_ip,
    extract_request_metadata,
    infer_device_type,
    is_visit_analytics_admin,
    list_page_visits,
    record_page_visit,
)


def _record(engine, *, username="alice", ip_address="203.0.113.8", page_id="security_search", visited_at=None):
    return record_page_visit(
        engine,
        username=username,
        ip_address=ip_address,
        module_id="stock",
        module_label="股票",
        page_id=page_id,
        page_label="个股/指数查询",
        session_id="session-1",
        user_agent="Mozilla/5.0 (iPhone; Mobile)",
        referrer="https://example.test/",
        visited_at=visited_at,
    )


def test_page_visit_store_round_trips_and_filters():
    engine = create_engine("sqlite:///:memory:")
    now = datetime.now(timezone.utc)
    first_id = _record(engine, visited_at=now - timedelta(hours=2))
    second_id = _record(
        engine,
        username="bob",
        ip_address="198.51.100.20",
        page_id="etf_main",
        visited_at=now - timedelta(hours=1),
    )

    visits = list_page_visits(
        engine,
        start_at=now - timedelta(days=1),
        end_at=now + timedelta(minutes=1),
    )
    assert list(visits["visit_id"]) == [second_id, first_id]
    assert set(visits["username"]) == {"alice", "bob"}

    filtered = list_page_visits(engine, username="alice", ip_address="203.0.113.8")
    assert len(filtered) == 1
    assert filtered.iloc[0]["page_id"] == "security_search"
    assert filtered.iloc[0]["session_id"] == "session-1"


def test_extract_client_ip_prefers_proxy_client_and_rejects_invalid_values():
    headers = {
        "X-Forwarded-For": "203.0.113.5, 10.0.0.4",
        "X-Real-IP": "198.51.100.7",
        "User-Agent": "Browser/1.0",
        "Referer": "https://example.test/start",
    }
    assert extract_client_ip(headers) == "203.0.113.5"
    assert extract_client_ip({"X-Forwarded-For": "not-an-ip"}) == ""
    assert extract_client_ip({"X-Real-IP": "192.0.2.10:443"}) == "192.0.2.10"
    assert extract_client_ip({"X-Real-IP": "[2001:db8::1]:443"}) == "2001:db8::1"
    assert extract_request_metadata(headers)["referrer"] == "https://example.test/start"


def test_visit_admin_visibility_is_lijing_only():
    assert is_visit_analytics_admin("lijing")
    assert is_visit_analytics_admin(" LIJING ")
    assert not is_visit_analytics_admin("alice")
    assert is_module_visible("admin", "lijing")
    assert not is_module_visible("admin", "alice")
    assert "admin" in {module.id for module in get_visible_modules("lijing")}
    assert "admin" not in {module.id for module in get_visible_modules("alice")}
    assert any(result.page_id == "visit_analytics" for result in search_sidebar_pages("浏览数据"))
    assert not search_sidebar_pages(
        "浏览数据",
        visible_module_ids={module.id for module in get_visible_modules("alice")},
    )


def test_device_type_inference():
    assert infer_device_type("Mozilla/5.0 (iPhone; Mobile)") == "手机"
    assert infer_device_type("Mozilla/5.0 (iPad; Tablet)") == "平板"
    assert infer_device_type("Googlebot/2.1") == "机器人"
    assert infer_device_type("Mozilla/5.0 (Windows NT 10.0)") == "桌面"
