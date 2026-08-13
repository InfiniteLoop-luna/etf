from __future__ import annotations

import unittest

from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from src.nasdaq_a_share_mapping import (
    add_user_mapping,
    list_combined_mappings,
    list_default_mappings,
    list_user_mappings,
    remove_user_mapping,
)


class NasdaqAShareMappingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )

    def test_default_semiconductor_mapping_exists(self) -> None:
        frame = list_default_mappings("半导体")
        self.assertIn("002371.SZ", frame["ts_code"].tolist())
        self.assertTrue((frame["source"] == "system").all())

    def test_user_can_add_update_and_remove_mapping(self) -> None:
        self.assertTrue(add_user_mapping("lijing", "半导体", "000001.SZ", "平安银行", "测试方向", "测试理由", engine=self.engine))
        user = list_user_mappings("lijing", "半导体", engine=self.engine)
        self.assertEqual(user.iloc[0]["name"], "平安银行")

        add_user_mapping("lijing", "半导体", "000001.SZ", "平安银行", "更新方向", "更新理由", engine=self.engine)
        updated = list_user_mappings("lijing", "半导体", engine=self.engine)
        self.assertEqual(updated.iloc[0]["theme"], "更新方向")
        self.assertEqual(remove_user_mapping("lijing", "半导体", "000001.SZ", engine=self.engine), 1)
        self.assertTrue(list_user_mappings("lijing", "半导体", engine=self.engine).empty)

    def test_user_mapping_overrides_duplicate_system_code(self) -> None:
        add_user_mapping("lijing", "半导体", "002371.SZ", "北方华创自定义", "自定义", "我的理由", engine=self.engine)
        combined = list_combined_mappings("lijing", "半导体", engine=self.engine)
        row = combined[combined["ts_code"] == "002371.SZ"].iloc[0]
        self.assertEqual(row["name"], "北方华创自定义")
        self.assertEqual(row["source"], "user")


if __name__ == "__main__":
    unittest.main()
