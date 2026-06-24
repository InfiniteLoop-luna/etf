import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, text

from src.user_watchlist_store import list_watchlist_items, normalize_username


class UserWatchlistStoreTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE app_user_watchlist (
                        username VARCHAR(64) NOT NULL,
                        ts_code VARCHAR(20) NOT NULL,
                        security_type VARCHAR(20) NOT NULL DEFAULT 'stock',
                        security_name VARCHAR(120),
                        created_at TIMESTAMP,
                        updated_at TIMESTAMP,
                        PRIMARY KEY (username, ts_code, security_type)
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO app_user_watchlist (username, ts_code, security_type, security_name, created_at, updated_at)
                    VALUES
                        ('alice', '000001.SZ', 'stock', '平安银行', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
                        ('alice', '005827.OF', 'fund', '易方达蓝筹精选', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """
                )
            )

    def test_normalize_username_trims_and_collapses_spaces(self):
        self.assertEqual(normalize_username("  Alice   Bob  "), "Alice Bob")

    def test_normalize_username_limits_length(self):
        raw = "a" * 100
        self.assertEqual(len(normalize_username(raw)), 64)

    def test_normalize_username_handles_empty(self):
        self.assertEqual(normalize_username("   "), "")

    def test_list_watchlist_items_can_filter_fund_entries(self):
        with patch("src.user_watchlist_store.ensure_user_watchlist_table"):
            fund_df = list_watchlist_items("alice", engine=self.engine, security_type="fund")
        self.assertEqual(len(fund_df), 1)
        self.assertEqual(fund_df.iloc[0]["ts_code"], "005827.OF")
        self.assertEqual(fund_df.iloc[0]["security_type"], "fund")


if __name__ == "__main__":
    unittest.main()
