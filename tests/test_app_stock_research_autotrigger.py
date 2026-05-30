import app


def test_trigger_single_distribution_refresh_bg_invokes_refresh_with_single_stock(monkeypatch):
    captured = {}

    monkeypatch.setattr(app, "normalize_username", lambda value: value.strip().lower())

    class ImmediateThread:
        def __init__(self, target=None, daemon=None):
            self._target = target
            self.daemon = daemon

        def start(self):
            if self._target:
                self._target()

    def fake_refresh(engine, report_generator=None, *, username=None, only_code=None):
        captured["engine"] = engine
        captured["username"] = username
        captured["only_code"] = only_code
        return {"processed": 1, "generated": 1, "skipped": 0, "failed": 0, "locked": 0}

    monkeypatch.setattr("threading.Thread", ImmediateThread)
    monkeypatch.setattr(app, "refresh_watchlist_distribution_reports", fake_refresh)

    fake_engine = object()
    app.trigger_single_distribution_refresh_bg(" Alice ", "000733.SZ", fake_engine)

    assert captured == {
        "engine": fake_engine,
        "username": "alice",
        "only_code": "000733.SZ",
    }


def test_trigger_single_distribution_refresh_bg_skips_invalid_input(monkeypatch):
    called = {"refresh": False}

    def fake_refresh(*args, **kwargs):
        called["refresh"] = True
        return {}

    monkeypatch.setattr(app, "refresh_watchlist_distribution_reports", fake_refresh)

    app.trigger_single_distribution_refresh_bg("", "000733.SZ", object())
    app.trigger_single_distribution_refresh_bg("alice", "", object())
    app.trigger_single_distribution_refresh_bg("alice", "000733.SZ", None)

    assert called["refresh"] is False


def test_trigger_single_stock_research_refresh_bg_invokes_refresh_with_single_stock(monkeypatch):
    captured = {}

    monkeypatch.setattr(app, "normalize_username", lambda value: value.strip().lower())

    class ImmediateThread:
        def __init__(self, target=None, daemon=None):
            self._target = target
            self.daemon = daemon

        def start(self):
            if self._target:
                self._target()

    def fake_refresh(engine, report_generator=None, *, username=None, limit=None, only_code=None, force=False):
        captured["engine"] = engine
        captured["username"] = username
        captured["only_code"] = only_code
        captured["force"] = force
        return {"processed": 1, "generated": 1, "skipped": 0, "failed": 0, "locked": 0}

    monkeypatch.setattr("threading.Thread", ImmediateThread)
    monkeypatch.setattr(app, "refresh_watchlist_stock_research_reports", fake_refresh)

    fake_engine = object()
    app.trigger_single_stock_research_refresh_bg(" Alice ", "000733.SZ", fake_engine)

    assert captured == {
        "engine": fake_engine,
        "username": "alice",
        "only_code": "000733.SZ",
        "force": False,
    }


def test_trigger_single_stock_research_refresh_bg_skips_invalid_input(monkeypatch):
    called = {"refresh": False}

    def fake_refresh(*args, **kwargs):
        called["refresh"] = True
        return {}

    monkeypatch.setattr(app, "refresh_watchlist_stock_research_reports", fake_refresh)

    app.trigger_single_stock_research_refresh_bg("", "000733.SZ", object())
    app.trigger_single_stock_research_refresh_bg("alice", "", object())
    app.trigger_single_stock_research_refresh_bg("alice", "000733.SZ", None)

    assert called["refresh"] is False
