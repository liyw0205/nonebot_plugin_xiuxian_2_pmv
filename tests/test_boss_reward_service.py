from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_boss.reward_service import BossRewardService
from tests.test_db_backend import db_backend


class BossRewardServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.sqlite3"
        self.player_database = root / "player.sqlite3"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("user", 1000))
        with db_backend.transaction(self.player_database) as conn:
            conn.execute(
                "CREATE TABLE boss (user_id TEXT PRIMARY KEY, boss_stone INTEGER, boss_integral INTEGER)"
            )
            conn.execute("INSERT INTO boss VALUES (%s, %s, %s)", ("user", 100, 20))
            conn.execute("CREATE TABLE boss_limit (user_id TEXT PRIMARY KEY, integral INTEGER)")
            conn.execute("INSERT INTO boss_limit VALUES (%s, %s)", ("user", 200))
        self.service = BossRewardService(self.game_database, self.player_database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.game_database) as conn:
            wallet = int(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone()[0])
        with db_backend.connection(self.player_database) as conn:
            daily = conn.execute(
                "SELECT boss_stone, boss_integral FROM boss WHERE user_id=%s", ("user",)
            ).fetchone()
            total = conn.execute("SELECT integral FROM boss_limit WHERE user_id=%s", ("user",)).fetchone()[0]
        return wallet, (int(daily[0]), int(daily[1])), int(total)

    def grant(self, operation_id="reward", daily_stone=100, daily_integral=20, total_integral=200):
        return self.service.grant(
            operation_id, "user", daily_stone, daily_integral, total_integral, 30, 5
        )

    def test_success_updates_wallet_daily_limits_and_total_integral(self) -> None:
        result = self.grant()
        self.assertEqual(
            (result.status, result.stone, result.integral, result.wallet_stone),
            ("applied", 30, 5, 1030),
        )
        self.assertEqual(self.state(), (1030, (130, 25), 205))

    def test_zero_reward_is_recorded_without_changing_balances(self) -> None:
        result = self.service.grant("zero", "user", 100, 20, 200, 0, 0)
        self.assertEqual((result.status, result.wallet_stone), ("applied", 1000))
        self.assertEqual(self.state(), (1000, (100, 20), 200))

    def test_stale_counters_change_nothing(self) -> None:
        result = self.grant(daily_stone=99)
        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self.state(), (1000, (100, 20), 200))

    def test_duplicate_reuses_result_and_conflict_is_rejected(self) -> None:
        first = self.grant("repeat")
        duplicate = self.grant("repeat")
        conflict = self.service.grant("repeat", "user", 100, 20, 200, 31, 5)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "state_changed"))
        self.assertEqual((duplicate.wallet_stone, duplicate.daily_stone, duplicate.total_integral), (1030, 130, 205))
        self.assertEqual(self.state(), (1030, (130, 25), 205))

    def test_operation_failure_rolls_back_all_reward_state(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TABLE boss_reward_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, "
                "stone INTEGER NOT NULL, integral INTEGER NOT NULL, wallet_stone INTEGER NOT NULL, "
                "daily_stone INTEGER NOT NULL, daily_integral INTEGER NOT NULL, total_integral INTEGER NOT NULL, "
                "created_at TIMESTAMP)"
            )
            conn.execute(
                "CREATE TRIGGER fail_boss_reward BEFORE INSERT ON boss_reward_operations "
                "BEGIN SELECT RAISE(ABORT, 'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.grant("rollback")
        self.assertEqual(self.state(), (1000, (100, 20), 200))


if __name__ == "__main__":
    unittest.main()
