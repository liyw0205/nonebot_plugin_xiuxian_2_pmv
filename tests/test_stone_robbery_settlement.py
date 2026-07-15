from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base.transaction_service import (
    StoneRobberySettlementService,
)
from tests.test_db_backend import db_backend


class StoneRobberySettlementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_db = root / "game.sqlite3"
        self.player_db = root / "player.sqlite3"
        with db_backend.transaction(self.game_db) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian("
                "user_id TEXT PRIMARY KEY,hp INTEGER,mp INTEGER,user_stamina INTEGER,"
                "exp INTEGER,stone INTEGER)"
            )
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES(%s,%s,%s,%s,%s,%s)",
                (
                    ("robber", 100, 80, 20, 500, 200),
                    ("victim", 120, 90, 30, 600, 1000),
                    ("third", 140, 100, 40, 700, 300),
                ),
            )
        self.service = StoneRobberySettlementService(self.game_db, self.player_db)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    @staticmethod
    def expected(hp, mp, stamina, exp, stone):
        return {
            "hp": hp,
            "mp": mp,
            "user_stamina": stamina,
            "exp": exp,
            "stone": stone,
        }

    def settle(self, operation_id="robbery-op", **changes):
        request = {
            "expected_robber": self.expected(100, 80, 20, 500, 200),
            "expected_victim": self.expected(120, 90, 30, 600, 1000),
            "robber_final": (70, 80),
            "victim_final": (1, 90),
            "winner_id": "robber",
            "battle_messages": ["robber attacks", "robber wins"],
            "stamina_cost": 15,
        }
        request.update(changes)
        return self.service.settle(
            operation_id, "robber", "victim", **request
        )

    def states(self):
        with db_backend.connection(self.game_db) as conn:
            return [
                tuple(row)
                for row in conn.execute(
                    "SELECT user_id,hp,mp,user_stamina,exp,stone FROM user_xiuxian "
                    "WHERE user_id IN ('robber','victim') ORDER BY user_id"
                ).fetchall()
            ]

    def stat(self, user_id, key):
        with db_backend.connection(self.player_db) as conn:
            row = conn.execute(
                f'SELECT {db_backend.quote_ident(key)} FROM statistics WHERE user_id=%s',
                (user_id,),
            ).fetchone()
            return int(row[0] or 0) if row else 0

    def test_robber_win_commits_combat_stamina_stones_and_statistics(self) -> None:
        result = self.settle()

        self.assertEqual(
            (
                result.status,
                result.winner_id,
                result.transferred_amount,
                result.loser_balance,
                result.stamina_cost,
                result.robber_stamina,
            ),
            ("applied", "robber", 100, 900, 15, 5),
        )
        self.assertEqual(
            self.states(),
            [
                ("robber", 70, 80, 5, 500, 300),
                ("victim", 1, 90, 30, 600, 900),
            ],
        )
        self.assertEqual(self.stat("robber", "抢灵石成功"), 1)
        self.assertEqual(self.stat("victim", "抢灵石失败"), 1)

    def test_victim_win_transfers_robber_stones_and_charges_robber_stamina(self) -> None:
        result = self.settle(
            "robbery-counter",
            robber_final=(1, 80),
            victim_final=(75, 90),
            winner_id="victim",
            battle_messages=["victim counters"],
        )

        self.assertEqual(
            (result.status, result.transferred_amount, result.loser_balance),
            ("applied", 20, 180),
        )
        self.assertEqual(
            self.states(),
            [
                ("robber", 1, 80, 5, 500, 180),
                ("victim", 75, 90, 30, 600, 1020),
            ],
        )
        self.assertEqual(self.stat("victim", "抢灵石成功"), 1)
        self.assertEqual(self.stat("robber", "抢灵石失败"), 1)

    def test_zero_stone_loser_still_commits_battle_and_statistics(self) -> None:
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=0 WHERE user_id=%s", ("victim",))
        result = self.settle(
            "robbery-empty",
            expected_victim=self.expected(120, 90, 30, 600, 0),
        )

        self.assertEqual((result.status, result.transferred_amount), ("applied", 0))
        self.assertEqual(
            self.states(),
            [
                ("robber", 70, 80, 5, 500, 200),
                ("victim", 1, 90, 30, 600, 0),
            ],
        )
        self.assertEqual(self.stat("robber", "抢灵石成功"), 1)
        self.assertEqual(self.stat("victim", "抢灵石失败"), 1)

    def test_replay_returns_first_battle_without_second_settlement(self) -> None:
        first = self.settle("robbery-repeat")
        duplicate = self.settle(
            "robbery-repeat",
            robber_final=(1, 1),
            victim_final=(1, 1),
            winner_id="victim",
            battle_messages=["different battle"],
            stamina_cost=99,
        )
        replay = self.service.replay("robbery-repeat", "robber", "victim")

        self.assertEqual(first.status, "applied")
        self.assertEqual(
            (duplicate.status, duplicate.winner_id, duplicate.battle_messages),
            ("duplicate", "robber", ["robber attacks", "robber wins"]),
        )
        self.assertEqual(replay, duplicate)
        self.assertEqual(self.stat("robber", "抢灵石成功"), 1)
        self.assertEqual(self.stat("victim", "抢灵石失败"), 1)
        self.assertEqual(self.states()[0][3], 5)

    def test_replay_rejects_changed_user_or_target(self) -> None:
        self.settle("robbery-conflict")

        changed_target = self.service.settle(
            "robbery-conflict",
            "robber",
            "third",
            expected_robber=self.expected(100, 80, 20, 500, 200),
            expected_victim=self.expected(140, 100, 40, 700, 300),
            robber_final=(1, 80),
            victim_final=(100, 100),
            winner_id="third",
            battle_messages=["changed target"],
        )
        changed_user = self.service.replay("robbery-conflict", "third", "victim")

        self.assertEqual(changed_target.status, "operation_conflict")
        self.assertEqual(changed_user.status, "operation_conflict")
        self.assertEqual(self.states()[0][3], 5)

    def test_stale_snapshot_or_insufficient_stamina_changes_nothing(self) -> None:
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("UPDATE user_xiuxian SET mp=89 WHERE user_id=%s", ("victim",))
        stale = self.settle("robbery-stale")
        self.assertEqual(stale.status, "state_changed")

        with db_backend.transaction(self.game_db) as conn:
            conn.execute("UPDATE user_xiuxian SET mp=90 WHERE user_id=%s", ("victim",))
            conn.execute(
                "UPDATE user_xiuxian SET user_stamina=14 WHERE user_id=%s", ("robber",)
            )
        tired = self.settle(
            "robbery-tired",
            expected_robber=self.expected(100, 80, 14, 500, 200),
        )

        self.assertEqual(tired.status, "stamina_insufficient")
        self.assertEqual(
            self.states(),
            [
                ("robber", 100, 80, 14, 500, 200),
                ("victim", 120, 90, 30, 600, 1000),
            ],
        )

    def test_null_legacy_hp_is_initialized_only_with_successful_settlement(self) -> None:
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("UPDATE user_xiuxian SET hp=NULL WHERE user_id=%s", ("robber",))
        result = self.settle(
            "robbery-null-hp",
            expected_robber=self.expected(None, 80, 20, 500, 200),
        )

        self.assertEqual(result.status, "applied")
        self.assertEqual(self.states()[0][1], 70)

    def test_injured_player_rejection_does_not_apply_partial_battle(self) -> None:
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("UPDATE user_xiuxian SET hp=50 WHERE user_id=%s", ("robber",))
        robber_injured = self.settle(
            "robbery-robber-injured",
            expected_robber=self.expected(50, 80, 20, 500, 200),
        )

        with db_backend.transaction(self.game_db) as conn:
            conn.execute("UPDATE user_xiuxian SET hp=100 WHERE user_id=%s", ("robber",))
            conn.execute("UPDATE user_xiuxian SET hp=60 WHERE user_id=%s", ("victim",))
        victim_injured = self.settle(
            "robbery-victim-injured",
            expected_victim=self.expected(60, 90, 30, 600, 1000),
        )

        self.assertEqual(robber_injured.status, "robber_injured")
        self.assertEqual(victim_injured.status, "victim_injured")
        self.assertEqual(
            self.states(),
            [
                ("robber", 100, 80, 20, 500, 200),
                ("victim", 60, 90, 30, 600, 1000),
            ],
        )

    def test_operation_insert_failure_rolls_back_both_databases(self) -> None:
        with db_backend.transaction(self.game_db) as conn:
            conn.execute(
                "CREATE TABLE stone_robbery_operations("
                "operation_id TEXT PRIMARY KEY,robber_id TEXT NOT NULL,victim_id TEXT NOT NULL,"
                "result_json TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE TRIGGER fail_robbery_operation BEFORE INSERT ON stone_robbery_operations "
                "BEGIN SELECT RAISE(ABORT,'operation failed'); END"
            )
        with db_backend.transaction(self.player_db) as conn:
            conn.execute(
                'CREATE TABLE statistics('
                'user_id TEXT PRIMARY KEY,"抢灵石成功" INTEGER,"抢灵石失败" INTEGER)'
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.settle("robbery-write-fail")

        self.assertEqual(
            self.states(),
            [
                ("robber", 100, 80, 20, 500, 200),
                ("victim", 120, 90, 30, 600, 1000),
            ],
        )
        with db_backend.connection(self.player_db) as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM statistics").fetchone()[0], 0
            )

    def test_statistics_failure_rolls_back_players_and_operation(self) -> None:
        with db_backend.transaction(self.player_db) as conn:
            conn.execute(
                'CREATE TABLE statistics('
                'user_id TEXT PRIMARY KEY,"抢灵石成功" INTEGER,"抢灵石失败" INTEGER)'
            )
            conn.execute(
                "CREATE TRIGGER fail_robbery_statistics BEFORE INSERT ON statistics "
                "BEGIN SELECT RAISE(ABORT,'statistics failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.settle("robbery-stats-fail")

        self.assertEqual(
            self.states(),
            [
                ("robber", 100, 80, 20, 500, 200),
                ("victim", 120, 90, 30, 600, 1000),
            ],
        )
        with db_backend.connection(self.game_db) as conn:
            self.assertIsNone(
                conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name='stone_robbery_operations'"
                ).fetchone()
            )


if __name__ == "__main__":
    unittest.main()
