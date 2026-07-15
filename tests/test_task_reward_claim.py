from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tasks.transaction_service import (
    TaskRewardClaimService,
)
from tests.test_db_backend import db_backend


class TaskRewardClaimTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game_database = root / "game.db"
        self.player_database = root / "player.db"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u')")
            conn.execute(
                "CREATE TABLE back("
                "user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,"
                "UNIQUE(user_id,goods_id))"
            )
        with db_backend.transaction(self.player_database) as conn:
            conn.execute(
                "CREATE TABLE xiuxian_tasks("
                "user_id TEXT PRIMARY KEY,daily_period TEXT,daily_progress TEXT,"
                "daily_claimed TEXT,weekly_period TEXT,weekly_progress TEXT,"
                "weekly_claimed TEXT)"
            )
            conn.execute(
                "INSERT INTO xiuxian_tasks VALUES(%s,%s,%s,%s,%s,%s,%s)",
                (
                    "u",
                    "2026-07-14",
                    json.dumps({"d1": 1, "d2": 2}),
                    "[]",
                    "2026-W29",
                    json.dumps({"w1": 3}),
                    "[]",
                ),
            )
        self.service = TaskRewardClaimService(
            self.game_database, self.player_database
        )

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
                "rewards": {
                    "items": [
                        {"id": 10, "name": "奖励甲", "type": "道具", "amount": 1}
                    ]
                },
            },
            {
                "key": "d2",
                "cycle": "daily",
                "name": "日常二",
                "target": 2,
                "rewards": {
                    "items": [
                        {"id": 10, "name": "奖励甲", "type": "道具", "amount": 2}
                    ]
                },
            },
            {
                "key": "w1",
                "cycle": "weekly",
                "name": "周常一",
                "target": 3,
                "rewards": {
                    "items": [
                        {"id": 11, "name": "奖励乙", "type": "道具", "amount": 1}
                    ]
                },
            },
        )

    def claim(self, operation="claim", cycles=("daily", "weekly"), **changes):
        selected_tasks = tuple(
            task for task in self.tasks() if task["cycle"] in cycles
        )
        values = dict(
            operation_id=operation,
            user_id="u",
            cycles=cycles,
            periods={"daily": "2026-07-14", "weekly": "2026-W29"},
            tasks=selected_tasks,
            max_goods_num=10,
        )
        values.update(changes)
        return self.service.claim(**values)

    def quantity(self, item_id):
        with db_backend.connection(self.game_database) as conn:
            row = conn.execute(
                "SELECT goods_num FROM back WHERE user_id='u' AND goods_id=%s",
                (item_id,),
            ).fetchone()
            return int(row[0]) if row else 0

    def claimed(self, cycle):
        with db_backend.connection(self.player_database) as conn:
            return json.loads(
                conn.execute(
                    f"SELECT {cycle}_claimed FROM xiuxian_tasks WHERE user_id='u'"
                ).fetchone()[0]
            )

    def test_all_eligible_tasks_claim_atomically_with_aggregated_items(self) -> None:
        result = self.claim()

        self.assertEqual(result.status, "applied")
        self.assertEqual([task["key"] for task in result.tasks], ["d1", "d2", "w1"])
        self.assertEqual((self.quantity(10), self.quantity(11)), (3, 1))
        self.assertEqual(self.claimed("daily"), ["d1", "d2"])
        self.assertEqual(self.claimed("weekly"), ["w1"])
        with db_backend.connection(self.game_database) as conn:
            self.assertEqual(
                1,
                conn.execute(
                    "SELECT COUNT(*) FROM economy_log "
                    "WHERE action='claim_task_reward'"
                ).fetchone()[0],
            )

    def test_duplicate_replays_first_tasks_and_new_event_does_not_regrant(self) -> None:
        first = self.claim("replay")
        duplicate = self.claim("replay")
        second = self.claim("second")
        conflict = self.claim("replay", cycles=("daily",))

        self.assertEqual(
            (first.status, duplicate.status, second.status, conflict.status),
            ("applied", "duplicate", "applied", "operation_conflict"),
        )
        self.assertEqual(first.tasks, duplicate.tasks)
        self.assertEqual(second.tasks, ())
        self.assertEqual((self.quantity(10), self.quantity(11)), (3, 1))

    def test_cycle_filter_and_period_rollover_do_not_claim_stale_progress(self) -> None:
        result = self.claim(
            "rollover",
            cycles=("daily",),
            periods={"daily": date(2026, 7, 15).isoformat()},
        )

        self.assertEqual(result.tasks, ())
        self.assertEqual(self.quantity(10), 0)
        with db_backend.connection(self.player_database) as conn:
            row = conn.execute(
                "SELECT daily_period,daily_progress,daily_claimed "
                "FROM xiuxian_tasks WHERE user_id='u'"
            ).fetchone()
            self.assertEqual((row[0], json.loads(row[1]), json.loads(row[2])), ("2026-07-15", {}, []))

    def test_inventory_full_rolls_back_all_tasks(self) -> None:
        result = self.claim("full", max_goods_num=2)

        self.assertEqual(result.status, "inventory_full")
        self.assertEqual((self.quantity(10), self.quantity(11)), (0, 0))
        self.assertEqual(self.claimed("daily"), [])
        self.assertEqual(self.claimed("weekly"), [])

    def test_operation_failure_rolls_back_items_and_claimed_state(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            self.service._ensure_game_schema(conn)
            conn.execute(
                "CREATE TRIGGER fail_task_claim_operation BEFORE INSERT ON "
                "task_reward_claim_operations "
                "BEGIN SELECT RAISE(ABORT,'failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.claim("failed")

        self.assertEqual((self.quantity(10), self.quantity(11)), (0, 0))
        self.assertEqual(self.claimed("daily"), [])
        self.assertEqual(self.claimed("weekly"), [])

    def test_production_claim_entry_uses_cross_database_service(self) -> None:
        root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
        manager_source = (root / "xiuxian_tasks/task_data.py").read_text(encoding="utf-8")
        entry_source = (root / "xiuxian_tasks/__init__.py").read_text(encoding="utf-8")
        self.assertIn("self.reward_claim_service.claim(", manager_source)
        self.assertNotIn("grant_reward(", manager_source)
        self.assertIn("task_manager.claim_rewards(operation_id, user_id, cycle)", entry_source)


if __name__ == "__main__":
    unittest.main()
