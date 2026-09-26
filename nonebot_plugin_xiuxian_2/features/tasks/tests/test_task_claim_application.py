from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork, ReconcileService
from ....plugin import apply_platform_schema
from ..application import TaskClaimApplication
from ..migrations import (
    apply_task_claim,
    apply_task_claim_player,
    apply_task_claim_recovery,
    apply_task_progress,
)


class TaskClaimApplicationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with sqlite3.connect(self.game) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u')")
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,"
                "bind_num INTEGER,UNIQUE(user_id,goods_id))"
            )
        with sqlite3.connect(self.player) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                "CREATE TABLE xiuxian_tasks(user_id TEXT PRIMARY KEY,daily_period TEXT,"
                "daily_progress TEXT,daily_claimed TEXT,weekly_period TEXT,"
                "weekly_progress TEXT,weekly_claimed TEXT)"
            )
            conn.execute(
                "INSERT INTO xiuxian_tasks VALUES(?,?,?,?,?,?,?)",
                (
                    "u",
                    "2026-09-27",
                    json.dumps({"d1": 1, "d2": 2}),
                    "[]",
                    "2026-W39",
                    json.dumps({"w1": 3}),
                    "[]",
                ),
            )
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            apply_platform_schema(uow)
            apply_task_claim(uow)
            apply_task_claim_recovery(uow)
        with DatabaseUnitOfWork(self.player, immediate=True) as uow:
            apply_task_progress(uow)
            apply_task_claim_player(uow)
        self.app = TaskClaimApplication(self.game, self.player)

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def tasks():
        return (
            {
                "key": "d1",
                "cycle": "daily",
                "name": "日常一",
                "target": 1,
                "rewards": {"items": [{"id": 10, "name": "奖励甲", "type": "道具", "amount": 1}]},
            },
            {
                "key": "d2",
                "cycle": "daily",
                "name": "日常二",
                "target": 2,
                "rewards": {"items": [{"id": 10, "name": "奖励甲", "type": "道具", "amount": 2}]},
            },
            {
                "key": "w1",
                "cycle": "weekly",
                "name": "周常一",
                "target": 3,
                "rewards": {"items": [{"id": 11, "name": "奖励乙", "type": "道具", "amount": 1}]},
            },
        )

    def claim(self, operation_id="claim", cycle=None, max_goods_num=10):
        cycles = (cycle,) if cycle else ("daily", "weekly")
        return self.app.claim_rewards(
            operation_id=operation_id,
            user_id="u",
            cycle=cycle,
            periods={"daily": "2026-09-27", "weekly": "2026-W39"},
            tasks=tuple(task for task in self.tasks() if task["cycle"] in cycles),
            max_goods_num=max_goods_num,
        )

    def quantity(self, item_id: int) -> int:
        with sqlite3.connect(self.game) as conn:
            row = conn.execute(
                "SELECT goods_num FROM back WHERE user_id='u' AND goods_id=?",
                (item_id,),
            ).fetchone()
        return int(row[0]) if row else 0

    def claimed(self, cycle: str) -> list[str]:
        with sqlite3.connect(self.player) as conn:
            value = conn.execute(
                f"SELECT {cycle}_claimed FROM xiuxian_tasks WHERE user_id='u'"
            ).fetchone()[0]
        return json.loads(value)

    def test_claim_replay_conflict_and_inventory_rejection_are_stable(self) -> None:
        full = self.claim("full", max_goods_num=2)
        full_replay = self.claim("full", max_goods_num=10)
        first = self.claim("first")
        duplicate = self.claim("first")
        conflict = self.claim("first", cycle="daily")
        second = self.claim("second")

        self.assertEqual((full.status, full_replay.status), ("inventory_full", "inventory_full"))
        self.assertEqual(first.status, "applied")
        self.assertEqual(duplicate.status, "duplicate")
        self.assertEqual(conflict.status, "operation_conflict")
        self.assertEqual(second.status, "applied")
        self.assertEqual((self.quantity(10), self.quantity(11)), (3, 1))
        self.assertEqual(self.claimed("daily"), ["d1", "d2"])
        self.assertEqual(self.claimed("weekly"), ["w1"])

    def test_reconcile_resumes_after_game_grant_before_player_confirmation(self) -> None:
        with sqlite3.connect(self.player) as conn:
            conn.execute(
                "CREATE TRIGGER fail_claim_confirmation BEFORE UPDATE OF daily_claimed "
                "ON xiuxian_tasks BEGIN SELECT RAISE(ABORT,'confirmation failed'); END"
            )

        with self.assertRaises(sqlite3.IntegrityError):
            self.claim("crash-after-grant")

        self.assertEqual((self.quantity(10), self.quantity(11)), (3, 1))
        self.assertEqual(self.claimed("daily"), [])
        with DatabaseUnitOfWork(self.game) as uow:
            self.assertEqual(
                uow.query_one(
                    "SELECT status FROM task_reward_claim_operations WHERE operation_id=?",
                    ("crash-after-grant",),
                )["status"],
                "granted",
            )
            handlers = {
                "tasks.claim_rewards": self.app.reconcile,
            }
        with sqlite3.connect(self.player) as conn:
            conn.execute("DROP TRIGGER fail_claim_confirmation")

        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            report = ReconcileService().run(uow, operation_handlers=handlers)

        self.assertTrue(report.clean)
        self.assertEqual(self.claim("crash-after-grant").status, "duplicate")
        self.assertEqual((self.quantity(10), self.quantity(11)), (3, 1))
        self.assertEqual(self.claimed("daily"), ["d1", "d2"])
        self.assertEqual(self.claimed("weekly"), ["w1"])

    def test_reconcile_resumes_after_player_reservation_before_game_grant(self) -> None:
        with sqlite3.connect(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER fail_claim_economy_log BEFORE INSERT ON economy_log "
                "BEGIN SELECT RAISE(ABORT,'grant failed'); END"
            )

        with self.assertRaises(sqlite3.IntegrityError):
            self.claim("crash-before-grant")

        self.assertEqual((self.quantity(10), self.quantity(11)), (0, 0))
        self.assertEqual(self.claimed("daily"), [])
        self.assertEqual(self.claim("competing").status, "claim_in_progress")
        with sqlite3.connect(self.game) as conn:
            conn.execute("DROP TRIGGER fail_claim_economy_log")

        result = self.claim("crash-before-grant")
        self.assertEqual(result.status, "applied")
        self.assertEqual((self.quantity(10), self.quantity(11)), (3, 1))
        self.assertEqual(self.claimed("daily"), ["d1", "d2"])


if __name__ == "__main__":
    unittest.main()
