import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import (
    apply_base_stone_robbery_operations,
    apply_base_stone_robbery_player_statistics,
)
from ..robbery_repository import BaseStoneRobberySqlRepository


class BaseStoneRobberyRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game = root / "game.sqlite3"
        self.player = root / "player.sqlite3"
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE user_xiuxian("
                "user_id TEXT PRIMARY KEY,hp INTEGER,mp INTEGER,user_stamina INTEGER,"
                "exp INTEGER,stone INTEGER)"
            )
            uow.executemany(
                "INSERT INTO user_xiuxian VALUES(?,?,?,?,?,?)",
                (("robber", 100, 80, 20, 500, 200), ("victim", 120, 90, 30, 600, 1000)),
            )
            apply_base_stone_robbery_operations(uow)
        with DatabaseUnitOfWork(self.player, immediate=True) as uow:
            apply_base_stone_robbery_player_statistics(uow)
        self.repository = BaseStoneRobberySqlRepository(self.game, self.player)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    @staticmethod
    def _expected(hp, mp, stamina, exp, stone):
        return {"hp": hp, "mp": mp, "user_stamina": stamina, "exp": exp, "stone": stone}

    def _settle(self, operation_id="op", **changes):
        request = {
            "expected_robber": self._expected(100, 80, 20, 500, 200),
            "expected_victim": self._expected(120, 90, 30, 600, 1000),
            "robber_final": (70, 80),
            "victim_final": (1, 90),
            "winner_id": "robber",
            "battle_messages": ["robber wins"],
        }
        request.update(changes)
        return self.repository.settle(operation_id, "robber", "victim", **request)

    def _states(self):
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            return [
                tuple(row.values())
                for row in uow.query_all(
                    "SELECT user_id,hp,mp,user_stamina,exp,stone FROM user_xiuxian "
                    "WHERE user_id IN ('robber','victim') ORDER BY user_id"
                )
            ]

    def test_success_replay_and_statistics_are_atomic(self):
        first = self._settle()
        duplicate = self._settle("op", robber_final=(1, 1), victim_final=(1, 1), winner_id="victim")
        replay = self.repository.get_result("op", "robber", "victim")

        self.assertEqual((first.status, first.transferred_amount, first.loser_balance), ("applied", 100, 900))
        self.assertEqual((duplicate.status, duplicate.battle_messages), ("duplicate", ["robber wins"]))
        self.assertEqual(replay, duplicate)
        self.assertEqual(self._states(), [("robber", 70, 80, 5, 500, 300), ("victim", 1, 90, 30, 600, 900)])
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            rows = uow.query_all('SELECT user_id,"抢灵石成功","抢灵石失败" FROM statistics ORDER BY user_id')
        self.assertEqual([tuple(row.values()) for row in rows], [("robber", 1, 0), ("victim", 0, 1)])

    def test_snapshot_and_operation_conflicts_fail_without_mutation(self):
        stale = self._settle("stale", expected_victim=self._expected(120, 89, 30, 600, 1000))
        first = self._settle("conflict")
        changed = self.repository.settle(
            "conflict", "robber", "other",
            expected_robber=self._expected(100, 80, 20, 500, 200),
            expected_victim=self._expected(120, 90, 30, 600, 1000),
            robber_final=(70, 80), victim_final=(1, 90), winner_id="robber", battle_messages=[],
        )
        self.assertEqual(stale.status, "state_changed")
        self.assertEqual(first.status, "applied")
        self.assertEqual(changed.status, "operation_conflict")
        self.assertEqual(self._states(), [("robber", 70, 80, 5, 500, 300), ("victim", 1, 90, 30, 600, 900)])

    def test_partial_player_cas_failure_rolls_back_first_update(self):
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            uow.execute(
                "CREATE TRIGGER skip_victim_robbery_update BEFORE UPDATE ON user_xiuxian "
                "WHEN OLD.user_id='victim' BEGIN SELECT RAISE(IGNORE); END"
            )

        result = self._settle("partial-cas")

        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self._states(), [
            ("robber", 100, 80, 20, 500, 200),
            ("victim", 120, 90, 30, 600, 1000),
        ])
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            self.assertEqual(uow.query_all("SELECT * FROM statistics"), [])

    def test_receipt_failure_rolls_back_game_and_player_databases(self):
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            uow.execute(
                "CREATE TRIGGER fail_robbery_receipt BEFORE INSERT ON stone_robbery_operations "
                "BEGIN SELECT RAISE(ABORT,'receipt failed'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self._settle("write-fail")
        self.assertEqual(self._states(), [("robber", 100, 80, 20, 500, 200), ("victim", 120, 90, 30, 600, 1000)])
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            self.assertEqual(uow.query_all("SELECT * FROM statistics"), [])

    def test_missing_startup_schema_fails_closed_without_creating_databases(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "missing-game.sqlite3"
            player = Path(temp) / "missing-player.sqlite3"
            result = BaseStoneRobberySqlRepository(game, player).settle(
                "missing", "robber", "victim",
                expected_robber=self._expected(100, 80, 20, 500, 200),
                expected_victim=self._expected(120, 90, 30, 600, 1000),
                robber_final=(70, 80), victim_final=(1, 90), winner_id="robber", battle_messages=[],
            )
            self.assertEqual(result.status, "schema_missing")
            self.assertFalse(game.exists())
            self.assertFalse(player.exists())

    def test_player_statistics_migration_preserves_existing_rows(self):
        with tempfile.TemporaryDirectory() as temp:
            player = Path(temp) / "legacy.sqlite3"
            with DatabaseUnitOfWork(player, immediate=True) as uow:
                uow.execute("CREATE TABLE statistics(user_id TEXT PRIMARY KEY,old_total INTEGER)")
                uow.execute("INSERT INTO statistics VALUES('u',3)")
                apply_base_stone_robbery_player_statistics(uow)
                apply_base_stone_robbery_player_statistics(uow)
                columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(statistics)")}
                row = uow.query_one('SELECT user_id,old_total,"抢灵石成功","抢灵石失败" FROM statistics')
            self.assertTrue({"old_total", "抢灵石成功", "抢灵石失败"} <= columns)
            self.assertEqual(tuple(row.values()), ("u", 3, 0, 0))


if __name__ == "__main__":
    unittest.main()
