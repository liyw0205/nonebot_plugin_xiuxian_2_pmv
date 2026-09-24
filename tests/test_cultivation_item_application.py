from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.cultivation_item_application import CultivationItemApplication
from nonebot_plugin_xiuxian_2.features.back.migrations import apply_cultivation_item
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class CultivationItemApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "cultivation-item.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, exp INTEGER, hp INTEGER, "
                "mp INTEGER, atk INTEGER, power INTEGER)"
            )
            conn.execute(
                "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_num INTEGER, bind_num INTEGER, "
                "day_num INTEGER DEFAULT 0, all_num INTEGER DEFAULT 0, UNIQUE(user_id,goods_id))"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s,%s,%s,%s,%s)", ("user", 1000, 500, 700, 100, 1500))
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,0,0)", ("user", 9001, 3, 2))
        with DatabaseUnitOfWork(self.database) as uow:
            apply_cultivation_item(uow)
        self.application = CultivationItemApplication(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _state(self):
        with db_backend.connection(self.database) as conn:
            user = conn.execute(
                "SELECT exp,hp,mp,atk,power FROM user_xiuxian WHERE user_id=%s", ("user",)
            ).fetchone()
            item = conn.execute(
                "SELECT goods_num,bind_num,day_num,all_num FROM back WHERE user_id=%s AND goods_id=%s",
                ("user", 9001),
            ).fetchone()
            count = conn.execute("SELECT COUNT(*) FROM cultivation_item_operations").fetchone()[0]
        return tuple(map(int, user)), tuple(map(int, item)), int(count)

    def test_success_duplicate_and_usage_tracking(self) -> None:
        first = self.application.apply(
            "use-1", "user", 9001, 2, 200,
            hp_gain=100, mp_gain=200, atk_gain=20, power_multiplier=1.5, track_usage=True,
        )
        duplicate = self.application.apply(
            "use-1", "user", 9001, 1, 1,
            hp_gain=1, mp_gain=1, atk_gain=1, power_multiplier=1.0,
        )
        self.assertEqual((first.status, duplicate.status, duplicate.quantity, duplicate.exp_gain), ("applied", "duplicate", 2, 200))
        self.assertEqual(((1200, 600, 900, 120, 1800), (1, 0, 2, 2), 1), self._state())

    def test_insufficient_item_leaves_state_unchanged(self) -> None:
        result = self.application.apply(
            "use-poor", "user", 9001, 4, 400,
            hp_gain=200, mp_gain=400, atk_gain=40, power_multiplier=1.5,
        )
        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual(((1000, 500, 700, 100, 1500), (3, 2, 0, 0), 0), self._state())

    def test_missing_schema_is_not_created_at_request_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "empty.sqlite3"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, exp INTEGER, hp INTEGER, mp INTEGER, atk INTEGER, power INTEGER)")
                uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_num INTEGER)")
            with self.assertRaises(sqlite3.OperationalError):
                CultivationItemApplication(database).apply(
                    "missing-schema", "user", 1, 1, 1,
                    hp_gain=0, mp_gain=0, atk_gain=0, power_multiplier=1,
                )


if __name__ == "__main__":
    unittest.main()
