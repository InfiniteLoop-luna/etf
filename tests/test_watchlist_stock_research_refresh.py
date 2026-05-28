import unittest
from unittest.mock import Mock, patch

from sqlalchemy import create_engine, text

from src.stock_research_llm_analysis import (
    STOCK_RESEARCH_LLM_SCHEMA_VERSION,
    STOCK_RESEARCH_LLM_SECTION_MARKER,
)
from src.stock_research_report_store import (
    ensure_tables,
    get_daily_report,
    get_daily_report_record,
    get_report_status,
    release_refresh_lock,
    save_daily_report,
    try_acquire_refresh_lock,
)
from src.watchlist_stock_research_refresh import refresh_watchlist_stock_research_reports


class WatchlistStockResearchRefreshTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        self.akshare_enabled_patcher = patch(
            "src.watchlist_stock_research_refresh.should_enable_stock_research_akshare",
            return_value=False,
        )
        self.mock_should_enable_akshare = self.akshare_enabled_patcher.start()
        self.addCleanup(self.akshare_enabled_patcher.stop)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE app_user_watchlist (
                        username VARCHAR(64) NOT NULL,
                        ts_code VARCHAR(20) NOT NULL,
                        security_type VARCHAR(20) NOT NULL,
                        security_name VARCHAR(120),
                        updated_at TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE vw_ts_stock_daily (
                        ts_code VARCHAR(20) NOT NULL,
                        trade_date DATE NOT NULL,
                        close NUMERIC
                    )
                    """
                )
            )
        ensure_tables(self.engine)

    def _insert_watchlist_row(self, username: str, ts_code: str, security_type: str = "stock", security_name: str = ""):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO app_user_watchlist (username, ts_code, security_type, security_name, updated_at)
                    VALUES (:username, :ts_code, :security_type, :security_name, CURRENT_TIMESTAMP)
                    """
                ),
                {
                    "username": username,
                    "ts_code": ts_code,
                    "security_type": security_type,
                    "security_name": security_name,
                },
            )

    def _insert_daily_row(self, ts_code: str, trade_date: str):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO vw_ts_stock_daily (ts_code, trade_date, close)
                    VALUES (:ts_code, :trade_date, 10.5)
                    """
                ),
                {"ts_code": ts_code, "trade_date": trade_date},
            )

    def test_ready_current_research_report_skips_recompute(self):
        self._insert_watchlist_row("alice", "000733.SZ", security_name="振华科技")
        self._insert_daily_row("000733.SZ", "2026-05-23")
        save_daily_report(
            self.engine,
            "000733.SZ",
            "2026-05-23",
            f"# cached\n\n{STOCK_RESEARCH_LLM_SECTION_MARKER}\n{STOCK_RESEARCH_LLM_SCHEMA_VERSION}",
            report_html="<html>ready</html>",
        )
        report_generator = Mock(side_effect=AssertionError("report generator should not run"))

        summary = refresh_watchlist_stock_research_reports(self.engine, report_generator=report_generator)

        status = get_report_status(self.engine, "000733.SZ")
        self.assertEqual(summary["skipped"], 1)
        self.assertEqual(summary["generated"], 0)
        self.assertEqual(status["status"], "ready")
        self.assertEqual(status["latest_ready_trade_date"], "2026-05-23")
        report_generator.assert_not_called()

    def test_cached_research_report_missing_current_schema_is_recomputed(self):
        self._insert_watchlist_row("alice", "000733.SZ", security_name="振华科技")
        self._insert_daily_row("000733.SZ", "2026-05-23")
        save_daily_report(self.engine, "000733.SZ", "2026-05-23", "# old cached")
        report_generator = Mock(
            return_value={
                "report_md": f"# regenerated\n\n{STOCK_RESEARCH_LLM_SECTION_MARKER}\n{STOCK_RESEARCH_LLM_SCHEMA_VERSION}",
                "fact_pack": {"ts_code": "000733.SZ"},
                "llm_result": {"verdict": "重点跟踪"},
            }
        )

        summary = refresh_watchlist_stock_research_reports(self.engine, report_generator=report_generator)

        cached_report = get_daily_report(self.engine, "000733.SZ", "2026-05-23")
        self.assertEqual(summary["generated"], 1)
        self.assertIn(STOCK_RESEARCH_LLM_SCHEMA_VERSION, cached_report)
        report_generator.assert_called_once_with(
            "000733.SZ",
            "振华科技",
            engine=self.engine,
            asof_trade_date="2026-05-23",
            allow_live_fetch=False,
            use_report_cache=False,
            save_report=False,
        )

    def test_cached_current_research_report_missing_html_is_rendered_without_recompute(self):
        self._insert_watchlist_row("alice", "000733.SZ", security_name="stock")
        self._insert_daily_row("000733.SZ", "2026-05-23")
        save_daily_report(
            self.engine,
            "000733.SZ",
            "2026-05-23",
            f"# cached\n\n{STOCK_RESEARCH_LLM_SECTION_MARKER}\n{STOCK_RESEARCH_LLM_SCHEMA_VERSION}",
            fact_pack={
                "ts_code": "000733.SZ",
                "stock_name": "stock",
                "asof_trade_date": "2026-05-23",
                "profile": {"industry": "tech", "market": "SZ"},
                "price_metrics": {"latest_close": 10.5},
                "valuation_snapshot": {"pe_ttm": 12.3},
                "financial_metrics": {"latest": {"roe": 9.8}},
                "price_tail": [
                    {"trade_date": "2026-05-22", "open": 10, "close": 10.5, "low": 9.9, "high": 10.8},
                ],
                "data_quality": {"profile_rows": 1, "daily_rows": 1, "kline_rows": 1, "financial_rows": 0},
            },
            llm_result={
                "verdict": "观察",
                "risk_level": "中",
                "confidence": 61,
                "summary": "cached report",
                "quality_score": {"score": 60, "grade": "C"},
                "step_analysis": {"step0": "watchlist"},
            },
        )
        report_generator = Mock(side_effect=AssertionError("report generator should not run"))

        summary = refresh_watchlist_stock_research_reports(self.engine, report_generator=report_generator)

        record = get_daily_report_record(self.engine, "000733.SZ", "2026-05-23")
        self.assertEqual(summary["generated"], 1)
        self.assertIn("stock-research-html-v1", record["report_html"])
        report_generator.assert_not_called()

    def test_akshare_enabled_recomputes_cached_v1_fact_pack(self):
        self.mock_should_enable_akshare.return_value = True
        self._insert_watchlist_row("alice", "000733.SZ", security_name="振华科技")
        self._insert_daily_row("000733.SZ", "2026-05-23")
        save_daily_report(
            self.engine,
            "000733.SZ",
            "2026-05-23",
            f"# cached\n\n{STOCK_RESEARCH_LLM_SECTION_MARKER}\n{STOCK_RESEARCH_LLM_SCHEMA_VERSION}",
            report_html="<html>old</html>",
            fact_pack={"schema_version": "stock-research-fact-pack-v1"},
            llm_result={"verdict": "观察"},
        )
        report_generator = Mock(
            return_value={
                "report_md": f"# regenerated\n\n{STOCK_RESEARCH_LLM_SECTION_MARKER}\n{STOCK_RESEARCH_LLM_SCHEMA_VERSION}",
                "report_html": "<html>new</html>",
                "fact_pack": {"schema_version": "stock-research-fact-pack-v2", "supplemental": {"news": {"status": "ok"}}},
                "llm_result": {"verdict": "观察"},
            }
        )

        summary = refresh_watchlist_stock_research_reports(self.engine, report_generator=report_generator)

        record = get_daily_report_record(self.engine, "000733.SZ", "2026-05-23")
        self.assertEqual(summary["generated"], 1)
        self.assertEqual(record["report_html"], "<html>new</html>")
        report_generator.assert_called_once_with(
            "000733.SZ",
            "振华科技",
            engine=self.engine,
            asof_trade_date="2026-05-23",
            allow_live_fetch=True,
            use_report_cache=False,
            save_report=False,
        )

    def test_refresh_can_scope_to_single_user_watchlist(self):
        self._insert_watchlist_row("alice", "000733.SZ", security_name="振华科技")
        self._insert_watchlist_row("alice", "000001.SZ", security_name="平安银行")
        self._insert_watchlist_row("bob", "300274.SZ", security_name="阳光电源")
        self._insert_daily_row("000733.SZ", "2026-05-23")
        self._insert_daily_row("000001.SZ", "2026-05-23")
        self._insert_daily_row("300274.SZ", "2026-05-23")
        report_generator = Mock(return_value=f"# generated\n\n{STOCK_RESEARCH_LLM_SECTION_MARKER}\n{STOCK_RESEARCH_LLM_SCHEMA_VERSION}")

        summary = refresh_watchlist_stock_research_reports(
            self.engine,
            report_generator=report_generator,
            username="alice",
        )

        self.assertEqual(summary["processed"], 2)
        self.assertEqual(summary["generated"], 2)
        called_codes = [call.args[0] for call in report_generator.call_args_list]
        self.assertEqual(called_codes, ["000001.SZ", "000733.SZ"])

    def test_refresh_skips_when_lock_is_held(self):
        acquired = try_acquire_refresh_lock(
            self.engine,
            "watchlist_stock_research_refresh",
            owner_id="holder-1",
            timeout_seconds=1800,
        )
        self.assertTrue(acquired)
        report_generator = Mock(side_effect=AssertionError("report generator should not run while locked"))

        summary = refresh_watchlist_stock_research_reports(self.engine, report_generator=report_generator)

        self.assertEqual(summary["locked"], 1)
        self.assertEqual(summary["processed"], 0)
        report_generator.assert_not_called()
        release_refresh_lock(self.engine, "watchlist_stock_research_refresh", owner_id="holder-1")


if __name__ == "__main__":
    unittest.main()
