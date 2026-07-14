from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life.final_settlement_service import (
    JSON_FIELDS,
    PAST_LIFE_FIELDS,
    PastLifeFinalSettlementService,
)
from tests.test_db_backend import db_backend


class PastLifeFinalSettlementTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,stone INTEGER,level TEXT)")
            conn.execute("INSERT INTO user_xiuxian VALUES(%s,%s,%s,%s)", ("u", 100, 200, "江湖好手"))
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT)")
        with db_backend.transaction(self.player) as conn:
            columns = ",".join(f'"{field}" TEXT' for field in PAST_LIFE_FIELDS)
            conn.execute(f"CREATE TABLE past_life(user_id TEXT PRIMARY KEY,{columns})")
            self.initial = self.make_state()
            values = [self.encode(field, self.initial[field]) for field in PAST_LIFE_FIELDS]
            conn.execute(
                f"INSERT INTO past_life VALUES({','.join('%s' for _ in range(len(values) + 1))})",
                ("u", *values),
            )
        self.service = PastLifeFinalSettlementService(self.game, self.player)

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def encode(field, value):
        if field in JSON_FIELDS:
            return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return value

    @staticmethod
    def make_state():
        return {
            "state": 2, "stage": 9, "revision": 10,
            "alloc": {"悟性": 4}, "accumulated": {"悟性": 8},
            "talent": "test", "birth_scenario": "test birth",
            "total_score": 70, "score_breakdown": {"total": 70},
            "event_indices": [0], "event_snapshots": [{"text": "event"}],
            "early_death_rolls": {}, "history": [{"choice": 1}], "last_run_time": None,
            "total_runs": 1, "best_ending": "旧结局", "best_score": 60,
            "endings_log": [{"run_number": 1, "name": "旧结局", "score": 60, "time": "2026-01-01 00:00:00"}],
            "achievement_points": 10,
        }

    def read(self):
        with db_backend.connection(self.game) as conn:
            user = tuple(conn.execute("SELECT exp,stone FROM user_xiuxian WHERE user_id=%s", ("u",)).fetchone())
            bag = [tuple(row) for row in conn.execute("SELECT goods_id,goods_num FROM back WHERE user_id=%s", ("u",)).fetchall()]
        with db_backend.connection(self.player) as conn:
            row = tuple(conn.execute(
                "SELECT state,total_runs,best_ending,best_score,endings_log,achievement_points,last_run_time FROM past_life WHERE user_id=%s",
                ("u",),
            ).fetchone())
        return user, bag, row

    def settle(self, operation="normal", expected=None, final=None, **overrides):
        args = {
            "operation_id": operation, "user_id": "u", "expected_state": expected or self.initial,
            "final_state": final or {**self.initial, "stage": 10, "total_score": 80},
            "ending_name": "证道", "score": 80, "exp_reward": 30, "stone_reward": 40,
            "achievement_points": 50,
            "item_reward": {"id": 1001, "name": "定物", "type": "药材", "num": 1},
            "completed_at": "2026-07-13 12:00:00",
        }
        args.update(overrides)
        return self.service.settle(**args)

    def test_normal_final_is_atomic_idempotent_and_persists_rewards(self):
        self.assertEqual("applied", self.settle().status)
        self.assertEqual("duplicate", self.settle().status)
        user, bag, state = self.read()
        self.assertEqual((130, 240), user)
        self.assertEqual([(1001, 1)], bag)
        self.assertEqual(("0", "2", "证道", "80"), state[:4])
        self.assertEqual(60, int(state[5]))
        self.assertEqual(2, len(json.loads(state[4])))

    def test_early_final_without_item(self):
        early = {**self.initial, "stage": 3, "total_score": 25}
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE past_life SET stage=%s,total_score=%s WHERE user_id=%s", (3, 25, "u"))
        result = self.settle("early", expected=early, final=early, ending_name="夭折", score=25, exp_reward=5, stone_reward=6, achievement_points=7, item_reward=None)
        self.assertEqual("applied", result.status)
        user, bag, state = self.read()
        self.assertEqual((105, 206), user)
        self.assertEqual([], bag)
        self.assertEqual(("0", "2", "旧结局", "60"), state[:4])
        self.assertEqual(17, int(state[5]))

    def test_state_change_rejects_all_writes(self):
        before = self.read()
        self.assertEqual("state_changed", self.settle("stale", expected={**self.initial, "stage": 8}).status)
        self.assertEqual(before, self.read())

    def test_bag_failure_rolls_back_everything(self):
        before = self.read()
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TRIGGER reject_past_item BEFORE INSERT ON back BEGIN SELECT RAISE(ABORT,'reject item'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.settle("bag-fail")
        self.assertEqual(before, self.read())

    def test_operation_injection_failure_rolls_back_everything(self):
        before = self.read()
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE past_life_final_operations(operation_id TEXT PRIMARY KEY,payload TEXT,result_json TEXT)")
            conn.execute("CREATE TRIGGER reject_past_operation BEFORE INSERT ON past_life_final_operations BEGIN SELECT RAISE(ABORT,'reject operation'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.settle("operation-fail")
        self.assertEqual(before, self.read())


if __name__ == "__main__":
    unittest.main()
