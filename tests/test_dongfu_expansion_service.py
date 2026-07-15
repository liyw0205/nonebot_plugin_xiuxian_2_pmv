from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dongfu.transaction_service import (
    DongfuExpansionService,
)
from tests.test_db_backend import db_backend


class DongfuExpansionServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "xiuxian.sqlite3"
        self.player_database = root / "player.sqlite3"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)"
            )
            conn.execute(
                """
                CREATE TABLE back (
                    user_id TEXT NOT NULL, goods_id INTEGER NOT NULL, goods_num INTEGER NOT NULL,
                    UNIQUE (user_id, goods_id)
                )
                """
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("user-1", 100))
            conn.execute("INSERT INTO back VALUES (%s, %s, %s)", ("user-1", 21008, 10))
        with db_backend.transaction(self.player_database) as conn:
            conn.execute(
                'CREATE TABLE dongfu_status (user_id TEXT PRIMARY KEY, built TEXT, plot_count TEXT)'
            )
            conn.execute("INSERT INTO dongfu_status VALUES (%s, %s, %s)", ("user-1", "1", "3"))
        self.service = DongfuExpansionService(self.game_database, self.player_database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    @staticmethod
    def scalar(database: Path, sql: str, params=()):
        with db_backend.connection(database) as conn:
            return conn.execute(sql, params).fetchone()[0]

    def expand(self, operation_id="dongfu-expand-1"):
        return self.service.expand(
            operation_id,
            "user-1",
            deed_id=21008,
            base_plot_count=3,
            max_plot_count=6,
            stone_cost_per_level=20,
        )

    def test_expansion_updates_all_assets(self) -> None:
        result = self.expand()

        self.assertEqual(result.status, "expanded")
        self.assertEqual(result.current_count, 4)
        self.assertEqual(self.scalar(self.game_database, "SELECT stone FROM user_xiuxian"), 80)
        self.assertEqual(self.scalar(self.game_database, "SELECT goods_num FROM back"), 9)
        self.assertEqual(self.scalar(self.player_database, "SELECT plot_count FROM dongfu_status"), "4")

    def test_insufficient_assets_do_not_change_state(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("UPDATE back SET goods_num=0")
        self.assertEqual(self.expand().status, "deed_insufficient")

        with db_backend.transaction(self.game_database) as conn:
            conn.execute("UPDATE back SET goods_num=10")
            conn.execute("UPDATE user_xiuxian SET stone=0")
        self.assertEqual(self.expand("dongfu-expand-2").status, "stone_insufficient")
        self.assertEqual(self.scalar(self.game_database, "SELECT goods_num FROM back"), 10)
        self.assertEqual(self.scalar(self.player_database, "SELECT plot_count FROM dongfu_status"), "3")

    def test_duplicate_operation_does_not_charge_twice(self) -> None:
        first = self.expand()
        second = self.expand()

        self.assertEqual(first.status, "expanded")
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(second.current_count, 4)
        self.assertEqual(self.scalar(self.game_database, "SELECT stone FROM user_xiuxian"), 80)
        self.assertEqual(self.scalar(self.game_database, "SELECT goods_num FROM back"), 9)

    def test_player_write_failure_rolls_back_assets_and_operation(self) -> None:
        with db_backend.transaction(self.player_database) as conn:
            conn.execute(
                """
                CREATE TRIGGER fail_dongfu_update BEFORE UPDATE OF plot_count ON dongfu_status
                BEGIN SELECT RAISE(ABORT, 'dongfu update failed'); END
                """
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.expand()

        self.assertEqual(self.scalar(self.game_database, "SELECT stone FROM user_xiuxian"), 100)
        self.assertEqual(self.scalar(self.game_database, "SELECT goods_num FROM back"), 10)
        self.assertEqual(self.scalar(self.player_database, "SELECT plot_count FROM dongfu_status"), "3")
        with db_backend.connection(self.game_database) as conn:
            self.assertFalse(conn.table_exists("dongfu_expansion_operations"))


if __name__ == "__main__":
    unittest.main()
