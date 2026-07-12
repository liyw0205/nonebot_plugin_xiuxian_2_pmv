from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_bank.upgrade_service import BankUpgradeService
from tests.test_db_backend import db_backend


class BankUpgradeServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.sqlite3"
        self.player_database = root / "player.sqlite3"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("user", 1000))
        with db_backend.transaction(self.player_database) as conn:
            conn.execute("CREATE TABLE bankinfo (user_id TEXT PRIMARY KEY, banklevel TEXT)")
            conn.execute("INSERT INTO bankinfo VALUES (%s, %s)", ("user", "1"))
        self.service = BankUpgradeService(self.game_database, self.player_database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.game_database) as conn:
            stone = int(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone()[0])
        with db_backend.connection(self.player_database) as conn:
            level = str(conn.execute("SELECT banklevel FROM bankinfo WHERE user_id=%s", ("user",)).fetchone()[0])
        return stone, level

    def upgrade(self, operation_id="upgrade", expected="1", next_level="2", cost=300):
        return self.service.upgrade(operation_id, "user", expected, next_level, cost)

    def test_success_charges_wallet_and_advances_level(self) -> None:
        result = self.upgrade()
        self.assertEqual((result.status, result.wallet_stone, result.bank_level), ("applied", 700, "2"))
        self.assertEqual(self.state(), (700, "2"))

    def test_insufficient_wallet_changes_nothing(self) -> None:
        result = self.upgrade(cost=1100)
        self.assertEqual(result.status, "stone_insufficient")
        self.assertEqual(self.state(), (1000, "1"))

    def test_stale_level_changes_nothing(self) -> None:
        result = self.upgrade(expected="2", next_level="3")
        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self.state(), (1000, "1"))

    def test_duplicate_reuses_result_and_conflict_is_rejected(self) -> None:
        first = self.upgrade("repeat")
        duplicate = self.upgrade("repeat")
        conflict = self.upgrade("repeat", cost=301)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "state_changed"))
        self.assertEqual((duplicate.cost, duplicate.wallet_stone, duplicate.bank_level), (300, 700, "2"))
        self.assertEqual(self.state(), (700, "2"))

    def test_operation_failure_rolls_back_wallet_and_level(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TABLE bank_upgrade_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, "
                "cost INTEGER NOT NULL, wallet_stone INTEGER NOT NULL, bank_level TEXT NOT NULL, created_at TIMESTAMP)"
            )
            conn.execute(
                "CREATE TRIGGER fail_upgrade BEFORE INSERT ON bank_upgrade_operations "
                "BEGIN SELECT RAISE(ABORT, 'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.upgrade("rollback")
        self.assertEqual(self.state(), (1000, "1"))


if __name__ == "__main__":
    unittest.main()
