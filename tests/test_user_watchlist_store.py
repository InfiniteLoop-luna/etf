import unittest

from src.user_watchlist_store import normalize_username


class UserWatchlistStoreTests(unittest.TestCase):
    def test_normalize_username_trims_and_collapses_spaces(self):
        self.assertEqual(normalize_username("  Alice   Bob  "), "Alice Bob")

    def test_normalize_username_limits_length(self):
        raw = "a" * 100
        self.assertEqual(len(normalize_username(raw)), 64)

    def test_normalize_username_handles_empty(self):
        self.assertEqual(normalize_username("   "), "")


if __name__ == "__main__":
    unittest.main()
