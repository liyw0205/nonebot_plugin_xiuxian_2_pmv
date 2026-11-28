from collections import Counter
from contextlib import contextmanager
from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests import mixed_activity_rank_fixture as fixture


class ActivityRankOracleTests(unittest.TestCase):
    def test_full_setup_rows_produce_exact_tie_broken_personal_and_public_ranks(self):
        sign = [
            {"user_id": "b", "sign_days": 5, "total_sign_days": 10,
             "last_sign_date": "2026-01-01", "user_name": "Beta"},
            {"user_id": "c", "sign_days": 6, "total_sign_days": 1,
             "last_sign_date": "2026-01-01", "user_name": "Gamma"},
            {"user_id": "a", "sign_days": 5, "total_sign_days": 10,
             "last_sign_date": "2026-01-01", "user_name": "Alpha"},
        ]
        boss = [
            {"user_id": "z", "total_damage": 100, "user_name": "Zulu"},
            {"user_id": "a", "total_damage": 100, "user_name": "Alpha"},
            {"user_id": "b", "total_damage": 200, "user_name": "Beta"},
        ]
        sign.extend({"user_id": f"s{index:05d}", "sign_days": 1,
                     "total_sign_days": 1, "last_sign_date": "2026-01-01",
                     "user_name": ""} for index in range(8189))
        boss.extend({"user_id": f"d{index:05d}", "total_damage": 1,
                     "user_name": ""} for index in range(8189))
        @contextmanager
        def uow(_database, *, read_only=False):
            yield SimpleNamespace(query_all=lambda sql, params=():
                                  deepcopy(sign if "activity_user" in sql else boss))

        with patch.object(fixture.shared, "_uow", uow), \
                patch.object(fixture, "_state_digest", return_value={"sign_rows": 8192, "boss_rows": 8192}):
            actual = fixture._oracle(SimpleNamespace(game_db="fixture"), ["a", "b"], "boss-key")
        self.assertEqual(["c", "a", "b"], [row["user_id"] for row in actual["sign"][:3]])
        self.assertEqual(["b", "a", "z"], [row["user_id"] for row in actual["boss_display"][:3]])
        self.assertEqual({"sign_rank": 2, "sign_value": 5, "boss_rank": 2, "boss_value": 100},
                         actual["actors"]["a"])
        self.assertEqual({"sign": 8192, "boss": 8192}, actual["totals"])

    def test_rank_loader_requires_populated_sign_and_selected_boss_cohorts(self):
        @contextmanager
        def uow(_database, *, read_only=False):
            yield SimpleNamespace(query_all=lambda sql, params=():
                                  [{"user_id": str(index), "sign_days": 1,
                                    "total_sign_days": index, "last_sign_date": "",
                                    "total_damage": index + 1, "user_name": ""}
                                   for index in range(8191)])

        paths = SimpleNamespace(game_db="fixture.db")
        with patch.object(fixture.shared, "_uow", uow):
            with self.assertRaisesRegex(ValueError, "at least 8192"):
                fixture._ranked_rows(paths, "boss-key")


class ActivityRankPlanTests(unittest.TestCase):
    @staticmethod
    def _manifest():
        actors = [str(index + 1) for index in range(128)]
        safe_actors = actors[:64]
        rows = []
        for index in range(56):
            rows.append({"key": "profile_info", "command": "我的修仙信息", "input_text": "我的修仙信息",
                         "actor": actors[index], "number": index, "rank_cohort": "writer"})
        writes = []
        for index in range(32):
            writes.append({"key": "stone_training", "command": "灵石修炼", "input_text": "灵石修炼 1000",
                           "actor": actors[index], "number": 1000 + index})
        relations = [{"key": "my_mentor", "command": "我的师徒", "input_text": "我的师徒",
                      "actor": actors[index], "number": 2000 + index} for index in range(72)]
        actors_by_rank = {actor: {"sign_rank": index + 1, "sign_value": index + 1,
                                  "boss_rank": index + 1, "boss_value": index + 10}
                          for index, actor in enumerate(actors)}
        return actors, (rows, writes, relations), {"actors": actors_by_rank,
                              "totals": {"sign": 8192, "boss": 8192},
                              "sign": [], "boss": [], "boss_display": []}

    def test_frozen_plan_replaces_only_relationship_reads_with_three_real_paths(self):
        specs = fixture.phase_specs()
        self.assertEqual([(128, 10, 20), (128, 20, 35), (128, 20, 50)],
                         [(row["requests"], row["concurrency"], row["arrival_rate"]) for row in specs])
        actors, (profiles, writes, relations), oracle = self._manifest()
        safe_actors = actors[:64]
        manifest = {"actors": actors, "groups": {"training_low": safe_actors},
                    "score_evidence": {actor: {"exp": {"storage": "integer"},
                                                "stone": {"storage": "integer"}}
                                      for actor in safe_actors},
                    "activity_rank": {"oracle": oracle}}
        for phase, spec in enumerate(specs):
            relation_count = 72 if phase == 0 else 40
            seeded = [*deepcopy(profiles), *deepcopy(writes if phase else []),
                      *deepcopy(relations[:relation_count])]
            seeded = seeded[:128]
            for index, row in enumerate(seeded):
                row["number"] += phase * 100_000
            with patch.object(fixture.player, "request_plan", return_value=seeded):
                plan = fixture.request_plan(manifest, spec["mode"], 128, phase)
            self.assertEqual(128, len(plan))
            counts = Counter(row["key"] for row in plan)
            self.assertEqual(12, counts["activity_sign_rank"])
            self.assertEqual(12, counts["activity_boss_board"])
            self.assertEqual(12, counts["activity_boss_display"])
            sign_request = next(row for row in plan if row["key"] == "activity_sign_rank")
            self.assertEqual("活动排行", sign_request["command"])
            self.assertEqual("活动排行 签到榜", sign_request["input_text"])
            self.assertEqual(32 if phase else 0, counts["stone_training"])
            self.assertEqual(56, counts["profile_info"])
            self.assertEqual({phase}, {row["number"] // 100_000 for row in plan})
            activity_actors = [row["actor"] for row in plan
                               if row["key"].startswith("activity_")]
            self.assertEqual(36, len(set(activity_actors)))
            self.assertTrue(set(activity_actors) <= set(safe_actors))

    def test_activity_plan_rejects_players_with_non_integer_live_profile_values(self):
        specs = fixture.phase_specs()
        actors, (profiles, writes, relations), oracle = self._manifest()
        safe_actors = actors[:64]
        evidence = {actor: {"exp": {"storage": "integer"},
                            "stone": {"storage": "integer"}}
                    for actor in safe_actors}
        evidence[safe_actors[7]]["stone"]["storage"] = "real"
        manifest = {"actors": actors, "groups": {"training_low": safe_actors},
                    "score_evidence": evidence, "activity_rank": {"oracle": oracle}}
        seeded = [*profiles[:56], *relations[:72]]
        with patch.object(fixture.player, "request_plan", return_value=seeded):
            with self.assertRaisesRegex(ValueError, "exact integer exp and stone"):
                fixture.request_plan(manifest, specs[0]["mode"], 128, 0)


class ActivityRankReplyOracleTests(unittest.TestCase):
    def test_live_position_value_and_reward_must_match_full_table_oracle(self):
        request = {"key": "activity_sign_rank", "expected_activity": {
            "board": "sign", "rank": 2, "value": 5, "total": 8192,
            "tier": "排行前十",
            "top": [{"rank": 1, "name": "Gamma", "value": 6},
                    {"rank": 2, "name": "Alpha", "value": 5}],
        }}
        reply = ("【节日活动季 · 签到榜】\n"
                 "1. Gamma 累计签到 6天\n2. Alpha 累计签到 5天\n"
                 "公共榜单快照截至 2026-11-26 00:00:00 UTC（最长缓存 60 分钟）；"
                 "个人名次与资格按实时数据计算。\n"
                 "我的名次：第 2 名 / 共 8192 人（累计签到 5天）\n"
                 "可领取：排行前十（灵石x1）")
        result = {"replies": [reply], "status": "pass"}
        self.assertEqual("success", fixture.classify_result(request, result)["outcome"])
        wrong_rank = {**result, "replies": [reply.replace("第 2 名", "第 3 名")]}
        self.assertEqual("activity_exact_rank_or_value_mismatch",
                         fixture.classify_result(request, wrong_rank)["reason"])
        wrong_reward = {**result, "replies": [reply.replace("排行前十", "排行前128", 1)]}
        self.assertEqual("activity_live_reward_tier_mismatch",
                         fixture.classify_result(request, wrong_reward)["reason"])

    def test_public_boss_display_requires_expected_order_and_freshness_label(self):
        request = {"key": "activity_boss_display", "expected_activity": {"top": [
            {"rank": 1, "name": "Beta", "value": 200},
            {"rank": 2, "name": "Alpha", "value": 100},
        ]}}
        reply = ("【庆典魔尊伤害排行】\n1. Beta 伤害 200\n2. Alpha 伤害 100\n"
                 "公共榜单快照截至 2026-11-26 00:00:00 UTC（最长缓存 60 分钟）；"
                 "个人名次与资格按实时数据计算。")
        self.assertEqual("success", fixture.classify_result(request, {"replies": [reply]})["outcome"])
        stale = reply.replace("公共榜单快照截至", "榜单更新于")
        self.assertEqual("activity_display_freshness_missing",
                         fixture.classify_result(request, {"replies": [stale]})["reason"])


class ActivityRankInvariantTests(unittest.TestCase):
    def test_seed_preflight_checks_fixture_state_without_requiring_dispatch_samples(self):
        state = {"sign_rows": 8192, "sign_sha256": "sign",
                 "boss_rows": 8192, "boss_sha256": "boss"}
        manifest = {"activity_rank": {"boss_key": "boss-key", "oracle": {"state_digest": state}}}
        inherited = {"ok": True, "errors": [], "invariants": "fixture state unchanged"}
        with patch.object(fixture.player, "check_invariants", return_value=inherited), \
                patch.object(fixture, "_state_digest", return_value=state):
            checked = fixture.check_invariants(SimpleNamespace(paths="fixture"), [], manifest, [])
        self.assertTrue(checked["ok"])
        self.assertEqual([], checked["errors"])
        self.assertEqual({}, checked["activity_rank_checks"]["per_command"])

    def test_dispatched_phase_still_requires_all_frozen_activity_samples(self):
        state = {"sign_rows": 8192, "sign_sha256": "sign",
                 "boss_rows": 8192, "boss_sha256": "boss"}
        manifest = {"activity_rank": {"boss_key": "boss-key", "oracle": {"state_digest": state}}}
        inherited = {"ok": True, "errors": [], "invariants": "fixture state unchanged"}
        result = {"request": {"key": "unrelated_path"}, "outcome": "success"}
        with patch.object(fixture.player, "check_invariants", return_value=inherited), \
                patch.object(fixture, "_state_digest", return_value=state):
            checked = fixture.check_invariants(SimpleNamespace(paths="fixture"), [], manifest, [result])
        self.assertFalse(checked["ok"])
        self.assertIn("activity route sample counts differ", checked["errors"][-1])


if __name__ == "__main__":
    unittest.main()
