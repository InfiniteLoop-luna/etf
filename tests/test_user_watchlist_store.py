import unittest
from unittest.mock import patch

import pandas as pd
from sqlalchemy import create_engine, text

from src.user_watchlist_store import (
    add_watchlist_item,
    add_watchlist_items_batch,
    ensure_user_watchlist_table,
    list_watchlist_items,
    normalize_username,
)


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
        self.assertTrue(fund_df.iloc[0]["holding_shares"] is None)
        self.assertTrue(fund_df.iloc[0]["holding_cost_amount"] is None)

    def test_schema_migration_and_upsert_persist_fund_holding_shares(self):
        ensure_user_watchlist_table(self.engine)

        add_watchlist_item(
            "alice",
            "005827.OF",
            security_name="易方达蓝筹精选",
            security_type="fund",
            holding_shares=12345.6789,
            holding_cost_amount=20000.25,
            engine=self.engine,
        )
        fund_df = list_watchlist_items("alice", engine=self.engine, security_type="fund")

        self.assertAlmostEqual(float(fund_df.iloc[0]["holding_shares"]), 12345.6789)
        self.assertAlmostEqual(
            float(fund_df.iloc[0]["holding_cost_amount"]), 20000.25
        )

        # Adding from another existing entry point must not erase a saved
        # personal position when no new share value was supplied.
        add_watchlist_item(
            "alice",
            "005827.OF",
            security_name="易方达蓝筹精选",
            security_type="fund",
            engine=self.engine,
        )
        retained = list_watchlist_items("alice", engine=self.engine, security_type="fund")
        self.assertAlmostEqual(float(retained.iloc[0]["holding_shares"]), 12345.6789)
        self.assertAlmostEqual(
            float(retained.iloc[0]["holding_cost_amount"]), 20000.25
        )

    def test_add_watchlist_item_rejects_non_positive_holding_shares(self):
        ensure_user_watchlist_table(self.engine)
        with self.assertRaisesRegex(ValueError, "holding_shares"):
            add_watchlist_item(
                "alice",
                "001938.OF",
                security_type="fund",
                holding_shares=0,
                engine=self.engine,
            )

    def test_add_watchlist_item_rejects_non_finite_holding_shares(self):
        ensure_user_watchlist_table(self.engine)
        for invalid_value in [float("inf"), float("-inf"), float("nan")]:
            with self.subTest(invalid_value=invalid_value):
                with self.assertRaisesRegex(ValueError, "holding_shares"):
                    add_watchlist_item(
                        "alice",
                        "001938.OF",
                        security_type="fund",
                        holding_shares=invalid_value,
                        engine=self.engine,
                    )

    def test_add_watchlist_item_rejects_invalid_holding_cost_amount(self):
        ensure_user_watchlist_table(self.engine)
        for invalid_value in [0, -1, float("inf"), float("nan")]:
            with self.subTest(invalid_value=invalid_value):
                with self.assertRaisesRegex(ValueError, "holding_cost_amount"):
                    add_watchlist_item(
                        "alice",
                        "001938.OF",
                        security_type="fund",
                        holding_shares=1000,
                        holding_cost_amount=invalid_value,
                        engine=self.engine,
                    )

    def test_clear_holding_shares_keeps_fund_in_watchlist(self):
        ensure_user_watchlist_table(self.engine)
        add_watchlist_item(
            "alice",
            "005827.OF",
            security_type="fund",
            holding_shares=12345.67,
            holding_cost_amount=20000,
            engine=self.engine,
        )
        add_watchlist_item(
            "alice",
            "005827.OF",
            security_type="fund",
            clear_holding_shares=True,
            engine=self.engine,
        )

        fund_df = list_watchlist_items("alice", engine=self.engine, security_type="fund")
        self.assertEqual(len(fund_df), 1)
        self.assertTrue(pd.isna(fund_df.iloc[0]["holding_shares"]))
        self.assertTrue(pd.isna(fund_df.iloc[0]["holding_cost_amount"]))

    def test_clear_only_holding_cost_preserves_shares(self):
        ensure_user_watchlist_table(self.engine)
        add_watchlist_item(
            "alice",
            "005827.OF",
            security_type="fund",
            holding_shares=12345.67,
            holding_cost_amount=20000,
            engine=self.engine,
        )
        add_watchlist_item(
            "alice",
            "005827.OF",
            security_type="fund",
            clear_holding_cost_amount=True,
            engine=self.engine,
        )

        fund_df = list_watchlist_items("alice", engine=self.engine, security_type="fund")
        self.assertAlmostEqual(float(fund_df.iloc[0]["holding_shares"]), 12345.67)
        self.assertTrue(pd.isna(fund_df.iloc[0]["holding_cost_amount"]))

    def test_batch_upsert_is_atomic(self):
        ensure_user_watchlist_table(self.engine)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TRIGGER reject_test_fund
                    BEFORE INSERT ON app_user_watchlist
                    WHEN NEW.ts_code = '999999.OF'
                    BEGIN
                        SELECT RAISE(ABORT, 'rejected for atomicity test');
                    END
                    """
                )
            )

        with self.assertRaises(Exception):
            add_watchlist_items_batch(
                "alice",
                [
                    {
                        "ts_code": "001938.OF",
                        "security_type": "fund",
                        "holding_shares": 1000,
                        "holding_cost_amount": 1800,
                    },
                    {
                        "ts_code": "999999.OF",
                        "security_type": "fund",
                        "holding_shares": 2000,
                        "holding_cost_amount": 3600,
                    },
                ],
                engine=self.engine,
            )

        with self.engine.begin() as conn:
            inserted_count = conn.execute(
                text(
                    "SELECT COUNT(*) FROM app_user_watchlist "
                    "WHERE ts_code IN ('001938.OF', '999999.OF')"
                )
            ).scalar_one()
        self.assertEqual(inserted_count, 0)


if __name__ == "__main__":
    unittest.main()
