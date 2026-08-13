from __future__ import annotations

import unittest

from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from src.user_preference_store import (
    get_user_preference,
    get_user_theme,
    set_user_preference,
    set_user_theme,
)


class UserPreferenceStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )

    def test_theme_is_persisted_and_updated_per_user(self) -> None:
        self.assertTrue(set_user_theme(" lijing ", "doraemon", engine=self.engine))
        self.assertEqual(get_user_theme("lijing", engine=self.engine), "doraemon")

        self.assertTrue(set_user_theme("lijing", "apple", engine=self.engine))
        self.assertEqual(get_user_theme("lijing", engine=self.engine), "apple")

    def test_preferences_are_isolated_between_users(self) -> None:
        set_user_theme("lijing", "doraemon", engine=self.engine)
        set_user_theme("guest", "apple", engine=self.engine)

        self.assertEqual(get_user_theme("lijing", engine=self.engine), "doraemon")
        self.assertEqual(get_user_theme("guest", engine=self.engine), "apple")

    def test_generic_preference_round_trip(self) -> None:
        self.assertTrue(
            set_user_preference("lijing", "dashboard_density", "compact", engine=self.engine)
        )
        self.assertEqual(
            get_user_preference("lijing", "dashboard_density", engine=self.engine),
            "compact",
        )

    def test_empty_user_is_not_persisted(self) -> None:
        self.assertFalse(set_user_theme("", "doraemon", engine=self.engine))
        self.assertIsNone(get_user_theme("", engine=self.engine))


if __name__ == "__main__":
    unittest.main()
