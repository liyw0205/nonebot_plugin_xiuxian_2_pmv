import copy
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests import mixed_read_strata_fixture as fixture


def manifest():
    actors = [str(900_000 + index) for index in range(128)]
    return {"actors": actors,
            "groups": {"training_low": actors[:64], "preserved_background": actors[64:96], "exact_blob": actors[96:]},
            "expected_ranks": {actor: {"exp": index + 1, "stone": 128 - index} for index, actor in enumerate(actors)}}


class ReadStrataPlanTests(unittest.TestCase):
    def test_five_bounded_phases_keep_writes_low_and_profiles_exact(self):
        saved, numbers = manifest(), set()
        specs = fixture.phase_specs()
        self.assertEqual(["cold", "pure", "readmostly", "mixed", "contention"], [row["label"] for row in specs])
        self.assertEqual(1024, sum(row["requests"] for row in specs))
        for phase, spec in enumerate(specs):
            plan = fixture.request_plan(saved, spec["mode"], spec["requests"], phase)
            self.assertEqual(plan, fixture.request_plan(saved, spec["mode"], spec["requests"], phase))
            self.assertEqual(len(plan), len({row["number"] for row in plan} - numbers))
            numbers.update(row["number"] for row in plan)
            self.assertLess(spec["requests"] / spec["arrival_rate"], 60)
            writes = [row for row in plan if row["key"] == "stone_training"]
            expected = [0, 0, 22, 66, 112][phase]
            self.assertEqual(expected, len(writes))
            self.assertTrue(all(row["actor"] in saved["groups"]["training_low"] for row in writes))
            if phase == 0:
                self.assertEqual(set(saved["actors"]), {row["actor"] for row in plan})
                self.assertEqual({"profile_info"}, {row["key"] for row in plan})
            else:
                profiles = [row for row in plan if row["key"] == "profile_info"]
                self.assertTrue(profiles)
                self.assertTrue(all(row["actor"] in saved["groups"]["exact_blob"] for row in profiles))
                for key in ("asset_overview", "mind_state", "backpack", "activity_rank"):
                    self.assertTrue(any(row["key"] == key for row in plan), key)
            if spec["mode"] == "contention":
                self.assertEqual({saved["actors"][0]}, {row["actor"] for row in writes})
            for row in plan:
                if row["key"] == "profile_info":
                    self.assertEqual(saved["expected_ranks"][row["actor"]], row["expected_ranks"])

    def test_invalid_plan_is_rejected(self):
        for mode, count, phase in (("cold", 128, 0), ("mixed", 128, 1), ("mixed", 256, 1), ("pure", 224, 90)):
            with self.subTest(mode=mode, count=count, phase=phase), self.assertRaises(ValueError):
                fixture.request_plan(manifest(), mode, count, phase)

    def test_exact_reply_oracle_preserves_error_and_cooldown_classification(self):
        request = {"key": "profile_info", "success_markers": ["道号", "修为排行", "灵石排行"],
                   "expected_ranks": {"exp": 12, "stone": 7}}
        reply = "道号: example\n修为排行: 第12位\n灵石排行: 第7位"
        self.assertEqual("success", fixture.classify_result(request, {"replies": [reply]})["outcome"])
        for altered, reason in ((reply.replace("第12位", "第13位"), "exact_exp_rank_mismatch"),
                                (reply.replace("第7位", "第8位"), "exact_stone_rank_mismatch"),
                                (reply + "\n修为排行: 第12位", "exact_exp_rank_mismatch")):
            self.assertEqual(reason, fixture.classify_result(request, {"replies": [altered]})["reason"])
        self.assertEqual("exception", fixture.classify_result(request, {"errors": ["bad"], "replies": [reply]})["outcome"])
        self.assertEqual("cooldown", fixture.classify_result(request, {"replies": ["慢...慢一..点"]})["outcome"])

    def test_legacy_real_profiles_are_declared_refusals_not_rank_successes(self):
        saved = manifest()
        saved["precision_rejected_actors"] = saved["groups"]["preserved_background"]
        plan = fixture.request_plan(saved, "pure", 128, 0)
        rejected = [row for row in plan if row.get("expected_guard")]
        self.assertEqual(32, len(rejected))
        request = rejected[0]
        result = {"replies": ["我的修仙信息处理提示：发起方，灵石余额的历史 REAL 记录已无法确定精确整数，未结算；请管理员核查。"]}
        self.assertEqual({"outcome": "business_rejection", "reason": "stone_precision_lost_real"},
                         fixture.classify_result(request, result))
        self.assertEqual("exception", fixture.classify_result(request, {**result, "errors": ["failure"]})["outcome"])
        self.assertEqual("unclassified", fixture.classify_result(request, {"replies": ["道号"]})["outcome"])


class ReadStrataOracleTests(unittest.TestCase):
    def setUp(self):
        from nonebot_plugin_xiuxian_2.core.numeric import register_numeric_functions
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "isolated.db"
        self.connection = sqlite3.connect(self.database)
        self.addCleanup(self.connection.close)
        register_numeric_functions(self.connection)
        self.connection.executescript(
            "CREATE TABLE user_xiuxian(id INTEGER PRIMARY KEY,user_id TEXT UNIQUE,exp INTEGER,stone INTEGER,power INTEGER);"
            "CREATE TABLE xiuxian_numeric_rank_generation(singleton INTEGER PRIMARY KEY,generation INTEGER,exp_generation INTEGER,stone_generation INTEGER);"
            "INSERT INTO xiuxian_numeric_rank_generation VALUES(1,5,3,4);"
            "CREATE INDEX oracle_exp ON user_xiuxian(xiuxian_int_sort_key_compact(exp));"
            "CREATE INDEX oracle_stone ON user_xiuxian(xiuxian_int_sort_key_compact(stone));"
        )

    def seed(self, rows):
        self.connection.executemany("INSERT INTO user_xiuxian(user_id,exp,stone,power) VALUES(?,?,?,1)", rows)
        self.connection.commit()

    def test_oracle_ties_real_blob_and_int64_neighbors_use_stored_values(self):
        from nonebot_plugin_xiuxian_2.core.numeric import bind_sqlite_param
        stored = float(10**20)
        above = bind_sqlite_param(int(stored) + 1, exact=True)
        below = bind_sqlite_param(int(stored) - 1, exact=True)
        self.seed([("real-a", stored, 9), ("real-b", stored, 9), ("above", above, 7),
                   ("below", below, 10), ("edge", bind_sqlite_param(2**63, exact=True), 1)])
        actors = ["real-a", "real-b", "above", "below", "edge"]
        ranks, evidence = fixture._rank_oracle(self.database, actors)
        self.assertEqual({"real-a": 2, "real-b": 2, "above": 1, "below": 3, "edge": 4},
                         {actor: row["exp"] for actor, row in ranks.items()})
        self.assertEqual({"real-a": 2, "real-b": 2, "above": 3, "below": 1, "edge": 4},
                         {actor: row["stone"] for actor, row in ranks.items()})
        self.assertEqual(4, evidence["columns"]["exp"]["seeks"])
        self.assertEqual({"generation": 5, "exp_generation": 3, "stone_generation": 4}, evidence["generation"])
        raw = fixture._raw_scores(self.database, actors)
        saved = fixture._score_evidence(raw)
        self.assertEqual("real", saved["real-a"]["exp"]["storage"])
        self.assertEqual("blob", saved["above"]["exp"]["storage"])
        self.assertEqual(int(stored) + 1, saved["above"]["exp"]["canonical"])
        import json
        json.dumps(saved)

    def test_oracle_refuses_unbounded_or_unindexed_fallback(self):
        self.seed([(str(index), index, index) for index in range(8)])
        with self.assertRaisesRegex(ValueError, "exceeded 3"):
            fixture._rank_oracle(self.database, ["0"], max_steps=3)
        self.connection.execute("DROP INDEX oracle_exp")
        self.connection.commit()
        with self.assertRaisesRegex(ValueError, "requires existing expression indexes"):
            fixture._rank_oracle(self.database, ["7"])

    def test_exact_seed_covers_blob_boundaries_and_preserves_background_objects(self):
        from nonebot_plugin_xiuxian_2.core.numeric import bind_sqlite_param
        background = [{"exp_canonical": str(int(float(10**20 + index * 65_536))),
                       "stone_canonical": str(int(float(10**21 + index * 262_144)))} for index in range(32)]
        original = copy.deepcopy(background)
        pairs = fixture._exact_scores(background)
        self.assertEqual(background, original)
        self.assertEqual(32, len(pairs))
        self.assertEqual({2**63 + index for index in range(4)}, {row["exp"] for row in pairs[:8]})
        for row in pairs:
            self.assertIsInstance(bind_sqlite_param(row["exp"], exact=True), bytes)
            self.assertIsInstance(bind_sqlite_param(row["stone"], exact=True), bytes)
        self.assertGreater(len({row["exp"] for row in pairs}), 20)


class ReadStrataInvariantTests(unittest.TestCase):
    def test_readonly_features_and_pure_generation_are_checked_separately(self):
        saved = manifest()
        saved.update(inventory=[], initial={}, score_evidence={actor: {"score": 1} for actor in saved["actors"]},
                     feature_snapshot={"status": [{"user_id": "a", "two_exp_protect": "on"}]},
                     setup_generation={"generation": 1})
        readonly = saved["actors"][64:]
        base = {"ok": True, "errors": [], "invariants": "gameplay"}
        context = SimpleNamespace(paths=SimpleNamespace(game_db="game.db", player_db="player.db", impart_db="impart.db"))

        def check(*, generation=None, features=None, scores=None):
            with patch.object(fixture.gameplay, "check_invariants", return_value=base) as reused, \
                    patch.object(fixture, "_raw_scores", return_value=[]), \
                    patch.object(fixture, "_score_evidence", return_value=scores or {actor: saved["score_evidence"][actor] for actor in readonly}), \
                    patch.object(fixture, "_feature_snapshot", return_value=features or saved["feature_snapshot"]), \
                    patch.object(fixture.shared, "_inventory", return_value=[]), \
                    patch.object(fixture.shared, "_uow"), \
                    patch.object(fixture, "_generation", return_value=generation or saved["setup_generation"]), \
                    patch.object(fixture.shared, "_rows", return_value=[{"quick_check": "ok"}]):
                result = fixture.check_invariants(context, [], saved, [])
                self.assertEqual(saved["groups"]["training_low"], reused.call_args.args[2]["actors"])
                return result

        self.assertTrue(check()["ok"])
        self.assertFalse(check(generation={"generation": 2})["ok"])
        self.assertFalse(check(features={"status": [{"user_id": "a", "two_exp_protect": "off"}]})["ok"])
        bad = {actor: {"score": 1} for actor in readonly}
        bad[readonly[0]]["score"] = 2
        self.assertFalse(check(scores=bad)["ok"])


if __name__ == "__main__":
    unittest.main()
