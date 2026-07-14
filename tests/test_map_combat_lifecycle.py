from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_map.combat_lifecycle_service import (
    MapCombatLifecycleService,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_map.combat_settlement_service import (
    MapCombatSettlementService,
)
from tests.test_db_backend import db_backend


class MapCombatLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game = root / "game.sqlite3"
        self.player = root / "player.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian("
                "user_id TEXT PRIMARY KEY,user_stamina INTEGER,stone INTEGER)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES('u',12,10)")
            conn.execute(
                "CREATE TABLE back("
                "user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,"
                "UNIQUE(user_id,goods_id))"
            )
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TABLE map_status("
                "user_id TEXT PRIMARY KEY,realm TEXT,heaven TEXT,node_id TEXT)"
            )
            conn.execute("INSERT INTO map_status VALUES('u','凡界','一重天','n1')")
            conn.execute(
                "CREATE TABLE map_daily_limit("
                "user_id TEXT PRIMARY KEY,date TEXT,combat_count INTEGER,"
                "resource_total_count INTEGER)"
            )
            conn.execute(
                "INSERT INTO map_daily_limit VALUES('u','2026-07-14',2,5)"
            )
            conn.execute(
                "CREATE TABLE map_cooldown("
                "user_id TEXT PRIMARY KEY,combat_cd_until TEXT)"
            )
            conn.execute("INSERT INTO map_cooldown VALUES('u','')")
            conn.execute(
                "CREATE TABLE map_combat_settlement("
                "user_id TEXT PRIMARY KEY,snapshot TEXT)"
            )
            conn.execute("INSERT INTO map_combat_settlement VALUES('u','')")
        self.service = MapCombatLifecycleService(self.game, self.player)
        self.settlement_service = MapCombatSettlementService(
            self.game, self.player
        )
        self.position = {
            "realm": "凡界",
            "heaven": "一重天",
            "node_id": "n1",
        }
        self.daily = {
            "date": "2026-07-14",
            "combat_count": 2,
            "resource_total_count": 5,
        }

    def tearDown(self):
        self.temp_dir.cleanup()

    def task(self, operation_id="start", **changes):
        value = {
            "task_id": operation_id,
            "status": "running",
            "started_at": "2026-07-14 10:00:00",
            "cooldown_until": "2026-07-14 10:00:30",
            "daily": self.daily,
            "decay": 1.0,
            "enemy": {"name": "守关石傀", "气血": 1000, "攻击": 200},
            "node_name": "试炼台",
            "node_type": "试炼",
        }
        value.update(changes)
        return value

    def start(self, operation_id="start", **changes):
        values = {
            "stamina": 12,
            "cost": 8,
            "position": self.position,
            "daily": self.daily,
            "limit": 7,
            "cooldown": "",
            "task": self.task(operation_id),
        }
        values.update(changes)
        return self.service.start(
            operation_id,
            "u",
            values["stamina"],
            values["cost"],
            values["position"],
            values["daily"],
            values["limit"],
            values["cooldown"],
            values["task"],
        )

    def state(self):
        with db_backend.connection(self.game) as conn:
            stamina = int(
                conn.execute(
                    "SELECT user_stamina FROM user_xiuxian WHERE user_id='u'"
                ).fetchone()[0]
            )
        with db_backend.connection(self.player) as conn:
            cooldown = str(
                conn.execute(
                    "SELECT combat_cd_until FROM map_cooldown WHERE user_id='u'"
                ).fetchone()[0]
                or ""
            )
            snapshot = str(
                conn.execute(
                    "SELECT snapshot FROM map_combat_settlement WHERE user_id='u'"
                ).fetchone()[0]
                or ""
            )
        return stamina, cooldown, snapshot

    def test_start_atomically_spends_stamina_sets_cooldown_and_saves_task(self):
        result = self.start()

        self.assertEqual(("applied", 4), (result.status, result.stamina))
        self.assertEqual("running", result.task["status"])
        stamina, cooldown, snapshot = self.state()
        self.assertEqual((4, "2026-07-14 10:00:30"), (stamina, cooldown))
        self.assertEqual(result.snapshot, snapshot)

    def test_start_event_replays_fixed_enemy_without_second_cost(self):
        first = self.start("event")

        replayed = self.service.replay_start("event", "u")
        duplicate = self.start(
            "event", task=self.task("event", enemy={"name": "另一只怪"})
        )

        self.assertEqual("duplicate", replayed.status)
        self.assertEqual(first.task["enemy"], replayed.task["enemy"])
        self.assertEqual("duplicate", duplicate.status)
        self.assertEqual(first.task["enemy"], duplicate.task["enemy"])
        self.assertEqual(4, self.state()[0])

    def test_limit_rejection_replays_after_daily_state_changes(self):
        limited = self.start("limit", limit=2)
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "UPDATE map_daily_limit SET combat_count=0 WHERE user_id='u'"
            )

        replayed = self.service.replay_start("limit", "u")

        self.assertEqual("limit_reached", limited.status)
        self.assertEqual("limit_reached", replayed.status)
        self.assertEqual((12, "", ""), self.state())

    def test_start_operation_failure_rolls_back_cost_cooldown_and_task(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE map_combat_start_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                "result_status TEXT NOT NULL,stamina INTEGER NOT NULL DEFAULT 0,"
                "task_json TEXT NOT NULL DEFAULT '{}',created_at TIMESTAMP)"
            )
            conn.execute(
                "CREATE TRIGGER fail_combat_start "
                "BEFORE INSERT ON map_combat_start_operations "
                "BEGIN SELECT RAISE(ABORT,'forced failure'); END"
            )

        with self.assertRaises(Exception):
            self.start("rollback")

        self.assertEqual((12, "", ""), self.state())

    def test_running_task_is_recoverable_and_first_plan_wins(self):
        self.start("recover")
        recovered = self.service.get_pending("u")
        plan = dict(recovered.task)
        plan.update(
            {
                "status": "planned",
                "items": [],
                "rewards": [],
                "stone": 0,
                "title": "战败",
                "won": False,
            }
        )

        saved = self.service.save_plan("u", "recover", plan)
        changed = dict(plan, title="另一结果")
        duplicate = self.service.save_plan("u", "recover", changed)

        self.assertEqual("applied", saved.status)
        self.assertEqual("duplicate", duplicate.status)
        self.assertEqual("战败", duplicate.task["title"])
        self.assertEqual(saved.snapshot, self.state()[2])

    def test_planned_task_settles_rewards_and_clears_snapshot(self):
        self.start("reward")
        pending = self.service.get_pending("u")
        items = [{"id": 1, "name": "材料", "type": "材料", "amount": 2}]
        plan = dict(pending.task)
        plan.update(
            {
                "status": "planned",
                "items": items,
                "rewards": ["材料x2"],
                "stone": 7,
                "title": "战而胜之",
                "won": True,
            }
        )
        planned = self.service.save_plan("u", "reward", plan)

        result = self.settlement_service.settle(
            "map-combat-settle:reward",
            "u",
            self.daily,
            planned.snapshot,
            7,
            7,
            items,
            99,
        )

        self.assertEqual("applied", result.status)
        self.assertEqual("", self.state()[2])
        with db_backend.connection(self.game) as conn:
            user = conn.execute(
                "SELECT stone FROM user_xiuxian WHERE user_id='u'"
            ).fetchone()
            item = conn.execute(
                "SELECT goods_num FROM back WHERE user_id='u' AND goods_id=1"
            ).fetchone()
        self.assertEqual((17, 2), (int(user[0]), int(item[0])))

    def test_inventory_failure_preserves_planned_task(self):
        self.start("full")
        pending = self.service.get_pending("u")
        items = [{"id": 1, "name": "材料", "type": "材料", "amount": 2}]
        plan = dict(pending.task)
        plan.update(
            {
                "status": "planned",
                "items": items,
                "rewards": ["材料x2"],
                "stone": 7,
                "title": "战而胜之",
                "won": True,
            }
        )
        planned = self.service.save_plan("u", "full", plan)
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "INSERT INTO back VALUES('u',1,'材料','材料',99,'','',99)"
            )

        result = self.settlement_service.settle(
            "map-combat-settle:full",
            "u",
            self.daily,
            planned.snapshot,
            7,
            7,
            items,
            99,
        )

        self.assertEqual("inventory_full", result.status)
        self.assertEqual(planned.snapshot, self.state()[2])


if __name__ == "__main__":
    unittest.main()
