from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.transaction_service import (
    NormalPvpSettlementService,
)
from tests.test_db_backend import db_backend


class NormalPvpSettlementTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_db, self.player_db = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game_db) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian("
                "user_id TEXT PRIMARY KEY,hp INTEGER,mp INTEGER,user_stamina INTEGER,exp INTEGER)"
            )
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES(%s,%s,%s,%s,%s)",
                [("challenger", 100, 80, 3, 500), ("opponent", 120, 90, 4, 600)],
            )
        self.service = NormalPvpSettlementService(self.game_db, self.player_db)

    def tearDown(self):
        self.temp_dir.cleanup()

    def settle(self, operation_id="pvp-op", **changes):
        request = {
            "expected_challenger_hp": 100,
            "expected_challenger_mp": 80,
            "expected_challenger_stamina": 3,
            "expected_challenger_exp": 500,
            "expected_opponent_hp": 120,
            "expected_opponent_mp": 90,
            "expected_opponent_stamina": 4,
            "expected_opponent_exp": 600,
            "challenger_final_hp": 55,
            "challenger_final_mp": 30,
            "opponent_final_hp": 70,
            "opponent_final_mp": 40,
            "winner_id": "challenger",
            "winner_name": "challenger-name",
            "battle_messages": ["battle finished"],
        }
        request.update(changes)
        return self.service.settle(operation_id, "challenger", "opponent", **request)

    def player_state(self):
        with db_backend.connection(self.game_db) as conn:
            return conn.execute(
                "SELECT user_id,hp,mp,user_stamina,exp FROM user_xiuxian ORDER BY user_id"
            ).fetchall()

    def test_applies_once_and_replays_exact_request(self):
        first = self.settle()
        duplicate = self.settle()

        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual((duplicate.winner_id, duplicate.battle_messages), ("challenger", ["battle finished"]))
        self.assertEqual(
            [tuple(row) for row in self.player_state()],
            [("challenger", 55, 30, 2, 500), ("opponent", 70, 40, 4, 600)],
        )
        with db_backend.connection(self.player_db) as conn:
            self.assertEqual(
                tuple(conn.execute('SELECT "切磋胜利" FROM statistics WHERE user_id=%s', ("challenger",)).fetchone()),
                (1,),
            )
            self.assertEqual(
                tuple(conn.execute('SELECT "切磋失败" FROM statistics WHERE user_id=%s', ("opponent",)).fetchone()),
                (1,),
            )

    def test_same_operation_replays_even_if_battle_result_differs(self):
        self.assertEqual("applied", self.settle().status)
        # Identity-only payload: mutable battle outcomes must not break replay.
        self.assertEqual("duplicate", self.settle(challenger_final_hp=54).status)
        self.assertEqual(
            [tuple(row) for row in self.player_state()],
            [("challenger", 55, 30, 2, 500), ("opponent", 70, 40, 4, 600)],
        )

    def test_rechecks_both_players_before_any_settlement(self):
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("UPDATE user_xiuxian SET mp=89 WHERE user_id=%s", ("opponent",))

        self.assertEqual("state_changed", self.settle("stale-opponent").status)
        self.assertEqual(
            [tuple(row) for row in self.player_state()],
            [("challenger", 100, 80, 3, 500), ("opponent", 120, 89, 4, 600)],
        )
        with db_backend.connection(self.game_db) as conn:
            self.assertIsNone(
                conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='normal_pvp_operations'"
                ).fetchone()
            )

    def test_statistics_failure_rolls_back_both_player_updates_and_operation(self):
        with db_backend.transaction(self.player_db) as conn:
            conn.execute('CREATE TABLE statistics(user_id TEXT PRIMARY KEY,"切磋胜利" INTEGER,"切磋失败" INTEGER)')
            conn.execute(
                "CREATE TRIGGER fail_pvp_stats BEFORE INSERT ON statistics "
                "BEGIN SELECT RAISE(ABORT,'fail'); END"
            )

        with self.assertRaises(Exception):
            self.settle("rollback")

        self.assertEqual(
            [tuple(row) for row in self.player_state()],
            [("challenger", 100, 80, 3, 500), ("opponent", 120, 90, 4, 600)],
        )
        with db_backend.connection(self.game_db) as conn:
            self.assertIsNone(
                conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='normal_pvp_operations'"
                ).fetchone()
            )
        with db_backend.connection(self.player_db) as conn:
            self.assertIsNone(conn.execute("SELECT user_id FROM statistics").fetchone())


if __name__ == "__main__":
    unittest.main()
