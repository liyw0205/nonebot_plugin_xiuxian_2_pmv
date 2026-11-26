import copy
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests import mixed_rank_gameplay_fixture as fixture


class RankGameplayFixtureTests(unittest.TestCase):
    def test_frozen_phases_are_bounded_distinct_and_deterministic(self):
        manifest = {"actors": [str(900000 + index) for index in range(128)]}
        specs = fixture.phase_specs()
        self.assertEqual(13, len(specs))
        self.assertEqual(1920, sum(spec["requests"] for spec in specs))
        numbers = set()
        for index, spec in enumerate(specs):
            plan = fixture.request_plan(manifest, spec["mode"], spec["requests"], index)
            self.assertEqual(plan, fixture.request_plan(manifest, spec["mode"], spec["requests"], index))
            self.assertTrue(all(row["number"] not in numbers for row in plan))
            numbers.update(row["number"] for row in plan)
            self.assertLessEqual(spec["requests"] / spec["arrival_rate"], 60)
            if spec["mode"] == "cold":
                self.assertEqual(128, len({row["actor"] for row in plan}))
                self.assertEqual({"profile_info"}, {row["key"] for row in plan})
            elif spec["mode"] == "contention":
                self.assertEqual({manifest["actors"][0]}, {row["actor"] for row in plan if row["key"] == "stone_training"})
            elif spec["mode"] == "mixed":
                self.assertEqual(len(plan) // 2, sum(row["key"] == "stone_training" for row in plan))
        self.assertEqual([35, 50], [spec["arrival_rate"] for spec in specs[-2:]])
        with self.assertRaises(ValueError):
            fixture.request_plan(manifest, "mixed", 257)

    def test_classification_does_not_count_replay_or_cooldown_as_success(self):
        request = {"key": "stone_training"}
        success = "修炼结束，共增加修为：100,消耗灵石：1000"
        for text, outcome in (
            (success, "success"),
            (success + "该修炼请求已经处理，无需重复提交。", "unclassified"),
            ("你的灵石还不够呢，快去赚点灵石吧！", "business_rejection"),
            ("修为与操作前不一致", "business_rejection"),
            ("慢...慢一..点", "cooldown"),
            ("unexpected", "unclassified"),
        ):
            with self.subTest(text=text):
                self.assertEqual(outcome, fixture.classify_result(request, {"replies": [text]})["outcome"])
        self.assertEqual("exception", fixture.classify_result(request, {"errors": ["bad"], "replies": [success]})["outcome"])

    def test_invariants_reject_lost_updates_wrong_receipts_and_ledger_rewards(self):
        actor, number = "900001", 890000001
        operation = f"training:{number}:stone:{actor}"
        manifest = {"actors": [actor], "initial": {actor: {"exp": 100, "stone": 5000, "power": 200}},
                    "exp_cap": 10000, "power_multiplier": 2, "inventory": []}
        request = {"key": "stone_training", "actor": actor, "number": number}
        result = {"request": request, "outcome": "success"}
        receipt = {"operation_id": operation, "payload": json.dumps([actor, 1000, 10000, 2]),
                   "result_json": json.dumps({"exp_gain": 100, "stone_cost": 1000, "power": 400})}
        ledger = {"operation_id": operation, "status": "applied",
                  "result_json": json.dumps({"operation_id": operation, "action": "buff.stone_training",
                    "status": "applied", "data": {"exp_gain": 100, "stone_cost": 1000, "power": 400}})}
        state = {"receipts": [receipt], "ledgers": [ledger],
                 "users": [{"user_id": actor, "exp": 200, "stone": 4000, "power": 400}]}

        def check(data):
            def rows(database, sql, params=()):
                if "FROM stone_training_operations" in sql:
                    return data["receipts"]
                if "FROM operation_ledger" in sql:
                    return data["ledgers"]
                if "COUNT(*)" in sql:
                    return [{"n": 1_000_000}]
                if sql == "PRAGMA quick_check":
                    return [{"quick_check": "ok"}]
                return data["users"]
            with patch.object(fixture.shared, "_rows", side_effect=rows), patch.object(fixture.shared, "_inventory", return_value=[]):
                return fixture.check_invariants(SimpleNamespace(paths=SimpleNamespace(game_db="isolated.db", player_db="player.db")), [], manifest, [result])
        self.assertTrue(check(state)["ok"])
        for change in ("lost_exp", "stone", "power", "receipt", "ledger"):
            bad = copy.deepcopy(state)
            if change == "lost_exp":
                bad["users"][0]["exp"] = 100
            elif change in {"stone", "power"}:
                bad["users"][0][change] = -1
            elif change == "receipt":
                bad["receipts"][0]["payload"] = json.dumps(["wrong", 1000, 10000, 2])
            else:
                bad["ledgers"][0]["result_json"] = json.dumps({"status": "applied", "data": {"exp_gain": 99, "stone_cost": 1000}})
            with self.subTest(change=change):
                self.assertFalse(check(bad)["ok"])


if __name__ == "__main__":
    unittest.main()
