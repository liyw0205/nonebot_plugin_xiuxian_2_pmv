from collections import Counter
from contextlib import contextmanager
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests import mixed_activity_write_fixture as fixture
from tests.test_mixed_cold_rank_fixture import saved_manifest
from tests.test_mixed_relation_fixture import reply_for


def traffic_manifest():
    saved = saved_manifest()
    saved["json_snapshot"] = {}
    saved["activity_write"] = {
        "actors": saved["groups"]["training_low"][:16],
        "damage": {actor: 10 + index % 2 for index, actor in enumerate(saved["groups"]["training_low"][:16])},
        "boss_name": "SyntheticBoss", "max_hp": 1_000_000, "daily_limit": 3,
    }
    return saved


def row_case():
    from nonebot_plugin_xiuxian_2.infrastructure.database import request_hash

    original = {
        "sign": {"rows": 8192, "sha256": "sign"},
        "sign_logs": {"rows": 0, "sha256": "sign-logs"},
        "other_damage": {"rows": 24576, "sha256": "other-damage"},
        "nonwriter_damage": {"rows": 8190, "sha256": "damage"},
        "historical_logs": {"rows": 8192, "sha256": "logs"},
        "other_states": {"rows": 0, "sha256": "states"},
        "items": {"rows": 0, "sha256": "items"}, "milestones": [],
        "writer_damage": {actor: {"activity_key": "boss", "user_id": actor, "total_damage": value, "update_time": "before"}
                          for actor, value in (("a", 100), ("b", 200))},
        "state": {"activity_key": "boss", "hp_left": 1000, "max_hp": 1000},
    }
    saved = {"key": "boss", "actors": ["a", "b"], "damage": {"a": 10, "b": 11},
             "max_hp": 1000, "initial_hp": 1000, "daily_limit": 3, "date": "2026-11-27",
             "milestones": [{"key": "p99", "hp_percent": 99}], "snapshot": original,
             "historical_max_id": 8192, "historical_rows": 8192, "rank_proof": "bounded gaps",
             "compatibility_hits": {"relative_path": "xiuxian/compatibility_hits.json",
                 "baseline": {}, "existed": False, "sha256": None, "allowed_delta_key": "feature:activity"}}
    current = deepcopy(original)
    current["state"]["hp_left"] = 979
    current["writer_damage"]["a"]["total_damage"] = 110
    current["writer_damage"]["b"]["total_damage"] = 211
    current["milestones"] = [{"activity_key": "boss", "milestone_key": "p99", "unlocked_time": "now"}]
    requests = [{"key": fixture.BOSS_KEY, "actor": actor, "number": 890100000 + index,
                 "expected_damage": saved["damage"][actor]} for index, actor in enumerate(saved["actors"])]
    receipts, logs, ledgers = [], [], []
    hp = 1000
    for request in reversed(requests):
        actor, amount = request["actor"], request["expected_damage"]
        hp -= amount
        operation = fixture._operation(request)
        receipts.append({"operation_id": operation,
            "payload": json.dumps([actor, "boss", 1000, 3, amount, saved["date"], "coop", [["p99", 99.0]], None, 0]),
            "damage": amount, "hp_left": hp, "max_hp": 1000, "fight_count": 1, "inventory": None})
        logs.append({"activity_key": "boss", "user_id": actor, "fight_date": saved["date"],
                     "source": "coop", "damage": amount})
        ledgers.append({"operation_id": operation, "action": fixture.LEDGER_ACTION, "status": "applied",
            "request_hash": request_hash({"user_id": actor}),
            "result_json": json.dumps({"operation_id": operation, "action": fixture.LEDGER_ACTION,
                "status": "applied", "consumed": {}, "granted": {},
                "data": {"status": "applied", "message": f"【SyntheticBoss】\n{actor} 首领剩余 {hp} / 1000\n今日剩余挑战 2 次"}})})
    return saved, current, requests, receipts, logs, ledgers


class ActivityWritePlanTests(unittest.TestCase):
    def test_expansion_preserves_original_coverage_order_and_resource_controls(self):
        saved = traffic_manifest()
        numbers = set()
        self.assertEqual([128, 160, 160], [row["requests"] for row in fixture.phase_specs()])
        for phase, spec in enumerate(fixture.phase_specs()):
            base_spec = fixture.cold.phase_specs()[phase]
            for field in ("concurrency", "arrival_rate", "mode"):
                self.assertEqual(base_spec[field], spec[field])
            plan = fixture.request_plan(saved, spec["mode"], spec["requests"], phase)
            self.assertEqual(plan, fixture.request_plan(saved, spec["mode"], spec["requests"], phase))
            self.assertEqual(len(plan), len({row["number"] for row in plan}))
            self.assertFalse(numbers & {row["number"] for row in plan})
            numbers.update(row["number"] for row in plan)
            counts = Counter(row["key"] for row in plan)
            self.assertEqual(56, counts["profile_info"])
            self.assertEqual((0, 32, 32)[phase], counts["stone_training"])
            self.assertEqual((0, 16, 16)[phase], counts[fixture.BOSS_KEY])
            for key, *_ in fixture.activity.ACTIVITY_COMMANDS:
                self.assertEqual(12, counts[key])
            base = fixture.cold.request_plan(saved, base_spec["mode"], 128, phase)
            if phase:
                inherited = [row for index, row in enumerate(plan) if index % 5 != 4]
                self.assertEqual([(row["key"], row["actor"]) for row in base],
                                 [(row["key"], row["actor"]) for row in inherited])
                added = [row for index, row in enumerate(plan) if index % 5 == 4]
                self.assertEqual({fixture.BOSS_KEY: 16, "my_mentor": 8, "my_partner": 8},
                                 dict(Counter(row["key"] for row in added)))
                self.assertEqual(16, len({row["actor"] for row in added if row["key"] == fixture.BOSS_KEY}))
                for row in added:
                    if row["key"] != fixture.BOSS_KEY:
                        self.assertEqual("success", fixture.classify_result(row, {"replies": [reply_for(row)]})["outcome"])
            else:
                self.assertEqual(base, plan)

    def test_writer_selection_excludes_personal_readers_top_ties_and_unsafe_scores(self):
        saved = traffic_manifest()
        actors = saved["actors"]
        rows = [{"user_id": f"leader{index}", "total_damage": 1_000_000 - index * 1234}
                for index in range(20)]
        rows += [{"user_id": actor, "total_damage": 500_000 - index * 1234}
                 for index, actor in enumerate(actors)]
        profiles = {actor: {"atk": 100 + index % 17} for index, actor in enumerate(actors)}
        config = {"max_hp": 1_000_000, "atk_ratio": .1, "hit_hp_cap_ratio": .01}
        selected, damage, excluded = fixture._select_writers(saved, rows, profiles, config, 1.0)
        self.assertEqual(16, len(set(selected)))
        self.assertFalse(set(selected) & excluded)
        self.assertTrue(set(selected) <= set(saved["groups"]["training_low"]))
        self.assertTrue(set(damage.values()) <= {10, 11})
        self.assertEqual(actors, saved["actors"])
        tied = [{**row, "total_damage": 1} for row in rows]
        with self.assertRaisesRegex(ValueError, "16 safe"):
            fixture._select_writers(saved, tied, profiles, config, 1.0)
        for actor in saved["groups"]["training_low"]:
            saved["score_evidence"][actor]["stone"]["storage"] = "real"
        with self.assertRaisesRegex(ValueError, "integer"):
            fixture._select_writers(saved, rows, profiles, config, 1.0)

    def test_invalid_phase_or_inherited_request_count_is_rejected(self):
        for mode, count, phase in (("pure", 160, 0), ("mixed", 128, 1), ("contention", 160, 3)):
            with self.subTest(mode=mode, count=count, phase=phase), self.assertRaises(ValueError):
                fixture.request_plan(traffic_manifest(), mode, count, phase)


class ActivityWriteReplyTests(unittest.TestCase):
    def test_fresh_damage_reply_requires_actual_damage_boss_max_hp_and_quota(self):
        request = {"key": fixture.BOSS_KEY, "expected_damage": 10, "expected_boss_name": "SyntheticBoss",
                   "expected_max_hp": 1000, "expected_remaining": 2}
        text = "【SyntheticBoss】\nActor 造成伤害 10（单次上限为首领血量 1%）\n首领剩余 990 / 1000（99.00%）\n今日剩余挑战 2 次"
        self.assertEqual("success", fixture.classify_result(request, {"replies": [text]})["outcome"])
        for newline in ("\r", "\r\n"):
            self.assertEqual("success", fixture.classify_result(request, {"replies": [text.replace("\n", newline)]})["outcome"])
        for wrong in (text.replace("伤害 10", "伤害 11"), text.replace("SyntheticBoss", "WrongBoss"),
                      text.replace(" / 1000", " / 2000"), text.replace("挑战 2", "挑战 3")):
            self.assertEqual("unclassified", fixture.classify_result(request, {"replies": [wrong]})["outcome"])
        self.assertEqual("exception", fixture.classify_result(request, {"errors": ["injected"], "replies": [text]})["outcome"])
        self.assertEqual("boundary", fixture.classify_result(request, {"blocked": ["outside"], "replies": [text]})["outcome"])

    def test_normal_quota_hp_and_cas_rejections_are_distinct_from_success_and_unknown(self):
        request = {"key": fixture.BOSS_KEY, "expected_damage": 10, "expected_boss_name": "SyntheticBoss",
                   "expected_max_hp": 1000, "expected_remaining": 2}
        for text in ("今日挑战次数已用完（3次）", "首领已被全服击破", "首领已被击退",
                     "讨伐未计入：首领状态已更新，请重新讨伐"):
            self.assertEqual("business_rejection", fixture.classify_result(request, {"replies": [text]})["outcome"])


class ActivityWriteDeltaTests(unittest.TestCase):
    def test_receipt_order_can_differ_but_all_atomic_deltas_must_match(self):
        case = row_case()
        self.assertEqual([], fixture._check_boss_rows(*case))
        for target, field, value, reason in (
            (3, "damage", 999, "receipt"), (3, "hp_left", 999, "HP chain"),
            (3, "fight_count", 2, "quota"), (4, "source", "world_boss", "source"),
            (4, "damage", 999, "log actor/damage"), (5, "status", "started", "ledger"),
            (5, "request_hash", "tampered", "request hash"),
        ):
            damaged = deepcopy(case)
            damaged[target][0][field] = value
            with self.subTest(field=field):
                self.assertTrue(any(reason in error for error in fixture._check_boss_rows(*damaged)))
        damaged = deepcopy(case)
        damaged[1]["writer_damage"]["a"]["total_damage"] = 111
        self.assertIn("boss cumulative actor damage delta mismatch", fixture._check_boss_rows(*damaged))
        damaged = deepcopy(case)
        damaged[1]["milestones"] = []
        self.assertIn("boss milestone unlock delta mismatch", fixture._check_boss_rows(*damaged))
        damaged = deepcopy(case)
        damaged[1]["writer_damage"]["a"]["activity_key"] = "other"
        self.assertIn("boss cumulative damage row changed unrelated fields", fixture._check_boss_rows(*damaged))
        damaged = deepcopy(case)
        damaged[2].append(dict(damaged[2][0]))
        self.assertTrue(any("distinct permitted" in error for error in fixture._check_boss_rows(*damaged)))

    def test_pure_preflight_requires_no_mutation_receipt_log_or_ledger(self):
        saved, _current, _requests, receipts, _logs, _ledgers = row_case()
        current = deepcopy(saved["snapshot"])
        self.assertEqual([], fixture._check_boss_rows(saved, current, [], [], [], []))
        self.assertTrue(fixture._check_boss_rows(saved, current, [], receipts, [], []))
        current["state"]["hp_left"] -= 1
        self.assertIn("pure traffic changed boss state", fixture._check_boss_rows(saved, current, [], [], [], []))
        current = deepcopy(saved["snapshot"])
        current["writer_damage"]["a"]["update_time"] = "changed"
        self.assertIn("unattacked writer damage row changed", fixture._check_boss_rows(saved, current, [], [], [], []))

    def test_reply_uses_each_own_receipt_hp_and_persisted_ledger_message(self):
        _saved, _current, requests, receipts, _logs, ledgers = row_case()
        by_operation = {row["operation_id"]: row for row in ledgers}
        results = [{"request": request, "replies": [json.loads(by_operation[fixture._operation(request)]["result_json"])["data"]["message"]]}
                   for request in requests]
        self.assertEqual([], fixture._check_boss_replies(results, receipts, ledgers))
        for newline in ("\r", "\r\n"):
            wire = deepcopy(results)
            for row in wire:
                row["replies"] = [text.replace("\n", newline) for text in row["replies"]]
            self.assertEqual([], fixture._check_boss_replies(wire, receipts, ledgers))
        results[0]["replies"] = results[1]["replies"]
        self.assertTrue(fixture._check_boss_replies(results, receipts, ledgers))

    def test_new_invariants_preserve_inherited_failures_and_protected_snapshots(self):
        saved, _current, *_ = row_case()
        manifest = traffic_manifest()
        manifest.update(activity_write=saved,
                        cold_rank_background={"actors": ["background"], "snapshot": {"sha256": "background"}})
        context = SimpleNamespace(paths=SimpleNamespace(game_db="not-opened"))
        current = deepcopy(saved["snapshot"])

        @contextmanager
        def no_sql(*args, **kwargs):
            yield SimpleNamespace(query_all=lambda *args, **kwargs: [])

        with patch.object(fixture.player, "check_invariants", return_value={"errors": ["inherited"], "invariants": "base"}) as inherited, \
                patch.object(fixture.cold, "_background_snapshot", return_value={"sha256": "background"}), \
                patch.object(fixture.cold, "_natal_snapshot", return_value=manifest["natal_snapshot"]), \
                patch.object(fixture.shared, "_uow", no_sql), \
                patch.object(fixture, "_snapshot", return_value=current):
            checked = fixture.check_invariants(context, [], manifest, [])
            self.assertEqual(["inherited"], checked["errors"])
            expected_manifest = inherited.call_args.args[2]
            self.assertIsNot(manifest, expected_manifest)
            self.assertEqual(manifest["json_snapshot"], expected_manifest["json_snapshot"])
            current["historical_logs"]["sha256"] = "tampered"
            checked = fixture.check_invariants(context, [], manifest, [])
            self.assertIn("activity writes changed protected historical_logs", checked["errors"])
            self.assertIn("inherited", checked["errors"])


class ActivityCompatibilityDiagnosticTests(unittest.TestCase):
    def test_only_planned_activity_counter_delta_is_allowed_and_all_other_json_stays_strict(self):
        from nonebot_plugin_xiuxian_2.compatibility import commands

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "xiuxian"
            root.mkdir()
            path = root / "compatibility_hits.json"
            business = root / "activity_config.json"
            baseline = {"feature:activity": 7, "feature:other": 3}
            path.write_text(json.dumps(baseline, indent=2), encoding="utf-8")
            business.write_text('{"enabled":true}', encoding="utf-8")
            paths = SimpleNamespace(game_db=root / "xiuxian.db")
            original = fixture.player.relation._json_snapshot(paths)
            with patch.object(commands, "_hit_path", return_value=path):
                saved = fixture._compatibility_hit_snapshot(paths, original)
            manifest = {"json_snapshot": original, "activity_write": {"compatibility_hits": saved}}
            before = deepcopy(manifest)
            requests = [{"key": fixture.BOSS_KEY}] * 16 + [{"key": "profile_info"}] * 56
            expected = fixture._expected_json_snapshot(manifest, requests)
            counters = {"feature:activity": 23, "feature:other": 3}
            encoded = json.dumps(counters, ensure_ascii=False, sort_keys=True).encode("utf-8")
            self.assertEqual(baseline, saved["baseline"])
            self.assertEqual("xiuxian/compatibility_hits.json", saved["relative_path"])
            self.assertEqual(hashlib.sha256(encoded).hexdigest(), expected[saved["relative_path"]])
            self.assertEqual(original, fixture._expected_json_snapshot(manifest, []))
            self.assertEqual(before, manifest)
            path.write_bytes(encoded)
            self.assertEqual(expected, fixture.player.relation._json_snapshot(paths))
            for values in ({**counters, "feature:other": 4}, {**counters, "unexpected": 1},
                           {**counters, "feature:activity": 22}, {**counters, "feature:activity": 24}):
                path.write_text(json.dumps(values, ensure_ascii=False, sort_keys=True), encoding="utf-8")
                self.assertNotEqual(expected, fixture.player.relation._json_snapshot(paths))
            path.write_bytes(encoded)
            business.write_text('{"enabled":false}', encoding="utf-8")
            self.assertNotEqual(expected, fixture.player.relation._json_snapshot(paths))
            self.assertEqual(before, manifest)

    def test_diagnostic_path_is_inside_restore_root_and_absent_or_invalid_baseline_is_explicit(self):
        from nonebot_plugin_xiuxian_2.compatibility import commands

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "xiuxian"
            root.mkdir()
            path = root / "compatibility_hits.json"
            paths = SimpleNamespace(game_db=root / "xiuxian.db")
            with patch.object(commands, "_hit_path", return_value=path):
                saved = fixture._compatibility_hit_snapshot(paths, {})
                self.assertFalse(saved["existed"])
                manifest = {"json_snapshot": {}, "activity_write": {"compatibility_hits": saved}}
                self.assertEqual({}, fixture._expected_json_snapshot(manifest, []))
                expected = fixture._expected_json_snapshot(manifest, [{"key": fixture.BOSS_KEY}])
                self.assertEqual({saved["relative_path"]}, set(expected))
                for invalid in ([], {"feature:activity": "7"}, {"feature:activity": -1}, {"feature:activity": True}):
                    path.write_text(json.dumps(invalid), encoding="utf-8")
                    with self.assertRaisesRegex(ValueError, "counter dict"):
                        fixture._compatibility_hit_snapshot(paths, {})
                path.write_text('{"feature:activity":7}', encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "original JSON snapshot"):
                    fixture._compatibility_hit_snapshot(paths, {})
            for outside in (None, Path(directory) / "compatibility_hits.json", root / "other.json"):
                with patch.object(commands, "_hit_path", return_value=outside), self.assertRaises(ValueError):
                    fixture._compatibility_hit_snapshot(paths, {})


if __name__ == "__main__":
    unittest.main()
