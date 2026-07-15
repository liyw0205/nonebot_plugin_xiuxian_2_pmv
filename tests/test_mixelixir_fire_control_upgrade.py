from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_mixelixir.transaction_service import (
    MixelixirFireControlUpgradeService,
)
from tests.test_db_backend import db_backend


class MixelixirFireControlUpgradeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game = root / "game.sqlite3"
        self.player = root / "player.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,stone INTEGER NOT NULL)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("user", 5000))
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                f"CREATE TABLE mix_elixir_info (user_id TEXT PRIMARY KEY,"
                f"{db_backend.quote_ident('丹药控火')} TEXT,{db_backend.quote_ident('炼丹经验')} TEXT)"
            )
            conn.execute("INSERT INTO mix_elixir_info VALUES (%s,%s,%s)", ("user", "0", "900"))
        self.service = MixelixirFireControlUpgradeService(self.game, self.player)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.game) as conn:
            stone = int(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone()[0])
        with db_backend.connection(self.player) as conn:
            mix = conn.execute(
                f"SELECT {db_backend.quote_ident('丹药控火')},{db_backend.quote_ident('炼丹经验')} "
                "FROM mix_elixir_info WHERE user_id=%s",
                ("user",),
            ).fetchone()
        return stone, int(mix[0]), int(mix[1])

    def upgrade(self, operation_id="upgrade", level=0, experience=900, stone=5000, next_level=1, cost=1000):
        return self.service.upgrade(operation_id, "user", level, experience, stone, next_level, cost)

    def test_success_charges_stones_and_upgrades_without_spending_experience(self) -> None:
        result = self.upgrade()
        self.assertEqual((result.status, result.cost, result.wallet_stone, result.level), ("applied", 1000, 4000, 1))
        self.assertEqual(self.state(), (4000, 1, 900))

    def test_duplicate_replays_result_and_conflicting_payload_is_rejected(self) -> None:
        first = self.upgrade("repeat")
        duplicate = self.upgrade("repeat")
        conflict = self.upgrade("repeat", cost=1001)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "state_changed"))
        self.assertEqual((duplicate.cost, duplicate.wallet_stone, duplicate.level), (1000, 4000, 1))
        self.assertEqual(self.state(), (4000, 1, 900))

    def test_changed_mix_or_wallet_snapshot_changes_nothing(self) -> None:
        self.assertEqual(self.upgrade("mix-conflict", experience=901).status, "state_changed")
        self.assertEqual(self.upgrade("wallet-conflict", stone=4999).status, "state_changed")
        self.assertEqual(self.state(), (5000, 0, 900))

    def test_insufficient_stones_changes_nothing(self) -> None:
        result = self.upgrade("short", cost=5001)
        self.assertEqual(result.status, "stone_insufficient")
        self.assertEqual(self.state(), (5000, 0, 900))

    def test_operation_write_failure_rolls_back_charge_and_level(self) -> None:
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE mixelixir_fire_control_upgrade_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,cost INTEGER NOT NULL,"
                "wallet_stone INTEGER NOT NULL,level INTEGER NOT NULL,experience INTEGER NOT NULL,created_at TIMESTAMP)"
            )
            conn.execute(
                "CREATE TRIGGER fail_fire_control_upgrade BEFORE INSERT ON mixelixir_fire_control_upgrade_operations "
                "BEGIN SELECT RAISE(ABORT, 'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.upgrade("rollback")
        self.assertEqual(self.state(), (5000, 0, 900))


if __name__ == "__main__":
    unittest.main()
