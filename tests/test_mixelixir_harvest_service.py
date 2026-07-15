from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_mixelixir.harvest_service import (
    MixelixirHarvestService,
)
from tests.test_db_backend import db_backend


class MixelixirHarvestServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.sqlite3"
        self.player_database = root / "player.sqlite3"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY)")
            conn.execute(
                "CREATE TABLE back (user_id TEXT NOT NULL, goods_id INTEGER NOT NULL, goods_name TEXT, "
                "goods_type TEXT, goods_num INTEGER NOT NULL, bind_num INTEGER DEFAULT 0, "
                "UNIQUE(user_id, goods_id))"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s)", ("user",))
        with db_backend.transaction(self.player_database) as conn:
            conn.execute('CREATE TABLE mix_elixir_info (user_id TEXT PRIMARY KEY, "收取时间" TEXT)')
            conn.execute('INSERT INTO mix_elixir_info VALUES (%s, %s)', ("user", "old"))
        self.service = MixelixirHarvestService(self.game_database, self.player_database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.game_database) as conn:
            items = {
                int(row[0]): int(row[1])
                for row in conn.execute(
                    "SELECT goods_id, goods_num FROM back WHERE user_id=%s", ("user",)
                ).fetchall()
            }
        with db_backend.connection(self.player_database) as conn:
            harvested_at = str(
                conn.execute('SELECT "收取时间" FROM mix_elixir_info WHERE user_id=%s', ("user",)).fetchone()[0]
            )
        return harvested_at, items

    def harvest(self, operation_id="harvest", expected="old", rewards=None):
        return self.service.harvest(
            operation_id,
            "user",
            expected,
            "new",
            rewards or [(1, "一号药材", 2), (2, "二号药材", 3)],
            max_goods_num=999,
        )

    def test_success_grants_all_rewards_and_advances_time(self) -> None:
        result = self.harvest()
        self.assertEqual(result.status, "applied")
        self.assertEqual([(reward.item_id, reward.quantity) for reward in result.rewards], [(1, 2), (2, 3)])
        self.assertEqual(self.state(), ("new", {1: 2, 2: 3}))

    def test_duplicate_reward_ids_are_merged(self) -> None:
        result = self.harvest(rewards=[(1, "一号药材", 2), (1, "一号药材", 4)])
        self.assertEqual(result.rewards[0].quantity, 6)
        self.assertEqual(self.state(), ("new", {1: 6}))

    def test_changed_harvest_time_grants_nothing(self) -> None:
        result = self.harvest(expected="stale")
        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self.state(), ("old", {}))

    def test_duplicate_reuses_rewards_and_conflict_is_rejected(self) -> None:
        first = self.harvest("repeat")
        duplicate = self.harvest("repeat")
        conflict = self.service.harvest(
            "repeat", "user", "old", "different", [(1, "一号药材", 2)], max_goods_num=999
        )
        # mutable rewards/time must not break same-op replay
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "duplicate"))
        self.assertEqual(duplicate.rewards, first.rewards)
        self.assertEqual(self.state(), ("new", {1: 2, 2: 3}))

    def test_operation_failure_rolls_back_rewards_and_time(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TABLE mixelixir_harvest_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, "
                "harvested_at TEXT NOT NULL, rewards_json TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE TRIGGER fail_harvest BEFORE INSERT ON mixelixir_harvest_operations "
                "BEGIN SELECT RAISE(ABORT, 'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.harvest("rollback")
        self.assertEqual(self.state(), ("old", {}))


if __name__ == "__main__":
    unittest.main()
