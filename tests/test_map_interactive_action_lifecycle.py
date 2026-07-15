from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_map.transaction_service import (
    MapInteractiveActionService,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_map.transaction_service import (
    MapResourceRewardService,
)
from tests.test_db_backend import db_backend


class MapInteractiveActionLifecycleTests(unittest.TestCase):
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
            conn.execute("INSERT INTO user_xiuxian VALUES('u',10,5)")
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
                "user_id TEXT PRIMARY KEY,date TEXT,gather_count INTEGER,"
                "resource_total_count INTEGER)"
            )
            conn.execute(
                "INSERT INTO map_daily_limit VALUES('u','2026-07-14',2,5)"
            )
            conn.execute(
                "CREATE TABLE map_cooldown("
                "user_id TEXT PRIMARY KEY,gather_cd_until TEXT)"
            )
            conn.execute("INSERT INTO map_cooldown VALUES('u','')")
        self.service = MapInteractiveActionService(self.game, self.player)
        self.reward_service = MapResourceRewardService(self.game, self.player)
        self.position = {
            "realm": "凡界",
            "heaven": "一重天",
            "node_id": "n1",
        }
        self.daily = {
            "date": "2026-07-14",
            "gather_count": 2,
            "resource_total_count": 5,
        }

    def tearDown(self):
        self.temp_dir.cleanup()

    @staticmethod
    def action(operation_id="start", **changes):
        value = {
            "action_id": operation_id,
            "action": "采集",
            "node_name": "灵林",
            "node_type": "灵林",
            "pool_key": "herb_low",
            "start_ts": "2026-07-14 10:00:00",
            "ready_ts": "2026-07-14 10:00:10",
            "expire_ts": "2026-07-14 10:00:30",
            "wait_sec": 10,
            "cost": 4,
            "cooldown_sec": 22,
            "success": True,
        }
        value.update(changes)
        return value

    def start(self, operation_id="start", **changes):
        values = {
            "stamina": 10,
            "cost": 4,
            "position": self.position,
            "daily": self.daily,
            "limit": 30,
            "cooldown": "",
            "action": self.action(operation_id),
        }
        values.update(changes)
        return self.service.start(
            operation_id,
            "u",
            "采集",
            values["stamina"],
            values["cost"],
            values["position"],
            values["daily"],
            values["limit"],
            values["cooldown"],
            values["action"],
        )

    def state(self):
        with db_backend.connection(self.game) as conn:
            stamina = int(
                conn.execute(
                    "SELECT user_stamina FROM user_xiuxian WHERE user_id='u'"
                ).fetchone()[0]
            )
        with db_backend.connection(self.player) as conn:
            action = (
                conn.execute(
                    "SELECT action_id,status,settlement_json "
                    "FROM map_interactive_actions WHERE user_id='u'"
                ).fetchone()
                if conn.table_exists("map_interactive_actions")
                else None
            )
            cooldown = conn.execute(
                "SELECT gather_cd_until FROM map_cooldown WHERE user_id='u'"
            ).fetchone()[0]
        return stamina, None if action is None else tuple(action), str(cooldown or "")

    def test_start_atomically_spends_stamina_and_persists_action(self):
        result = self.start()

        self.assertEqual(("applied", 6), (result.status, result.stamina))
        self.assertEqual("start", result.action["action_id"])
        self.assertEqual((6, ("start", "active", ""), ""), self.state())

    def test_start_event_replays_without_spending_again(self):
        self.assertEqual("applied", self.start("event").status)

        replayed = self.service.replay_start("event", "u", "采集")
        duplicate = self.start(
            "event", action=self.action("event", success=False, wait_sec=18)
        )
        conflict = self.service.replay_start("event", "u", "挖矿")

        self.assertEqual("duplicate", replayed.status)
        self.assertTrue(replayed.action["success"])
        self.assertEqual("duplicate", duplicate.status)
        self.assertTrue(duplicate.action["success"])
        self.assertEqual("operation_conflict", conflict.status)
        self.assertEqual(6, self.state()[0])

    def test_limit_rejection_replays_after_limit_changes(self):
        limited = self.start("limit", limit=2)
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "UPDATE map_daily_limit SET gather_count=0 WHERE user_id='u'"
            )

        replayed = self.service.replay_start("limit", "u", "采集")

        self.assertEqual("limit_reached", limited.status)
        self.assertEqual("limit_reached", replayed.status)
        self.assertEqual(10, self.state()[0])

    def test_start_operation_failure_rolls_back_stamina_and_action(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE map_interactive_start_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                "result_status TEXT NOT NULL,stamina INTEGER NOT NULL DEFAULT 0,"
                "action_json TEXT NOT NULL DEFAULT '{}',created_at TIMESTAMP)"
            )
            conn.execute(
                "CREATE TRIGGER fail_interactive_start "
                "BEFORE INSERT ON map_interactive_start_operations "
                "BEGIN SELECT RAISE(ABORT,'forced failure'); END"
            )

        with self.assertRaises(Exception):
            self.start("rollback")

        self.assertEqual((10, None, ""), self.state())

    def test_failure_terminal_and_cooldown_commit_together(self):
        self.start("failed")

        result = self.service.finish_failure(
            "terminal", "u", "failed", "failed", "2026-07-14 10:01:00"
        )
        replayed = self.service.finish_failure(
            "terminal", "u", "failed", "failed", "2026-07-14 10:01:00"
        )

        self.assertEqual("applied", result.status)
        self.assertEqual("duplicate", replayed.status)
        self.assertEqual(
            (6, ("failed", "failed", ""), "2026-07-14 10:01:00"),
            self.state(),
        )

    def test_terminal_operation_failure_rolls_back_status_and_cooldown(self):
        self.start("failed")
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TRIGGER fail_interactive_terminal "
                "BEFORE INSERT ON map_interactive_terminal_operations "
                "BEGIN SELECT RAISE(ABORT,'forced failure'); END"
            )

        with self.assertRaises(Exception):
            self.service.finish_failure(
                "terminal", "u", "failed", "expired", "2026-07-14 10:01:00"
            )

        self.assertEqual((6, ("failed", "active", ""), ""), self.state())

    def test_reward_settlement_closes_action_and_starts_cooldown(self):
        self.start("reward")
        settlement = {
            "daily": self.daily,
            "decay": 1.0,
            "rewards": ["材料x2"],
            "stone": 7,
            "items": [{"id": 1, "name": "材料", "type": "材料", "amount": 2}],
            "extra_msg": "",
        }
        planned = self.service.save_settlement("u", "reward", settlement)

        result = self.reward_service.settle(
            "map-resource:reward",
            "u",
            self.daily,
            30,
            7,
            settlement["items"],
            99,
            action_id="reward",
            action_settlement=planned.action["settlement"],
            cooldown_until="2026-07-14 10:01:00",
        )

        self.assertEqual("applied", result.status)
        self.assertEqual(
            (6, ("reward", "completed", json.dumps(settlement, ensure_ascii=True, sort_keys=True, separators=(",", ":"))), "2026-07-14 10:01:00"),
            self.state(),
        )
        with db_backend.connection(self.game) as conn:
            user = conn.execute(
                "SELECT stone FROM user_xiuxian WHERE user_id='u'"
            ).fetchone()
            item = conn.execute(
                "SELECT goods_num FROM back WHERE user_id='u' AND goods_id=1"
            ).fetchone()
        self.assertEqual((12, 2), (int(user[0]), int(item[0])))

    def test_inventory_failure_keeps_action_active_without_cooldown(self):
        self.start("full")
        settlement = {
            "daily": self.daily,
            "decay": 1.0,
            "rewards": ["材料x2"],
            "stone": 7,
            "items": [{"id": 1, "name": "材料", "type": "材料", "amount": 2}],
            "extra_msg": "",
        }
        planned = self.service.save_settlement("u", "full", settlement)
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "INSERT INTO back VALUES('u',1,'材料','材料',99,'','',99)"
            )

        result = self.reward_service.settle(
            "map-resource:full",
            "u",
            self.daily,
            30,
            7,
            settlement["items"],
            99,
            action_id="full",
            action_settlement=planned.action["settlement"],
            cooldown_until="2026-07-14 10:01:00",
        )

        self.assertEqual("inventory_full", result.status)
        self.assertEqual("active", self.state()[1][1])
        self.assertEqual("", self.state()[2])

    def test_reward_operation_failure_rolls_back_entire_lifecycle(self):
        self.start("reward-fail")
        settlement = {
            "daily": self.daily,
            "decay": 1.0,
            "rewards": ["材料x2"],
            "stone": 7,
            "items": [{"id": 1, "name": "材料", "type": "材料", "amount": 2}],
            "extra_msg": "",
        }
        planned = self.service.save_settlement("u", "reward-fail", settlement)
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE map_resource_reward_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                "stone INTEGER NOT NULL,rewards TEXT NOT NULL,created_at TIMESTAMP)"
            )
            conn.execute(
                "CREATE TRIGGER fail_lifecycle_reward "
                "BEFORE INSERT ON map_resource_reward_operations "
                "BEGIN SELECT RAISE(ABORT,'forced failure'); END"
            )

        with self.assertRaises(Exception):
            self.reward_service.settle(
                "map-resource:reward-fail",
                "u",
                self.daily,
                30,
                7,
                settlement["items"],
                99,
                action_id="reward-fail",
                action_settlement=planned.action["settlement"],
                cooldown_until="2026-07-14 10:01:00",
            )

        self.assertEqual("active", self.state()[1][1])
        self.assertEqual("", self.state()[2])
        with db_backend.connection(self.game) as conn:
            user = conn.execute(
                "SELECT stone FROM user_xiuxian WHERE user_id='u'"
            ).fetchone()
            item = conn.execute(
                "SELECT goods_num FROM back WHERE user_id='u' AND goods_id=1"
            ).fetchone()
        with db_backend.connection(self.player) as conn:
            daily = conn.execute(
                "SELECT gather_count,resource_total_count "
                "FROM map_daily_limit WHERE user_id='u'"
            ).fetchone()
        self.assertEqual(5, int(user[0]))
        self.assertIsNone(item)
        self.assertEqual((2, 5), tuple(map(int, daily)))


if __name__ == "__main__":
    unittest.main()
