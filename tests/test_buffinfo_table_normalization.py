from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from tests.test_db_backend import db_backend


class BuffInfoTableNormalizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.sqlite3"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def normalize(self, conn) -> None:
        manager = object.__new__(XiuxianDateManage)
        manager.conn = conn
        manager._normalize_legacy_buffinfo_table(conn.cursor())

    def test_legacy_mixed_case_table_is_normalized_without_data_loss(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute('CREATE TABLE "BuffInfo" (user_id TEXT PRIMARY KEY, main_buff INTEGER)')
            conn.execute('INSERT INTO "BuffInfo" VALUES (%s, %s)', ("user", 42))
            self.normalize(conn)
        with db_backend.connection(self.database) as conn:
            self.assertIn("buffinfo", conn.list_tables())
            self.assertNotIn("BuffInfo", conn.list_tables())
            self.assertEqual(conn.execute("SELECT main_buff FROM buffinfo").fetchone()[0], 42)

    def test_canonical_table_is_left_unchanged(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE buffinfo (user_id TEXT PRIMARY KEY)")
            self.normalize(conn)
            self.assertEqual(conn.list_tables(), ["buffinfo"])


if __name__ == "__main__":
    unittest.main()
