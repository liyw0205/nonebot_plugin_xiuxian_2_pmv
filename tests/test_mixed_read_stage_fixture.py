"""V23 route, population, cooldown and independent reward-oracle preflight."""
import ast
from collections import Counter
from copy import deepcopy
from pathlib import Path
import unittest

from tests import mixed_read_stage_fixture as fixture


def manifest():
    actors = [str(100_000 + index) for index in range(128)]
    return {"actors": actors, "names": {actor: f"SyntheticReadStage{index:03d}"
                                       for index, actor in enumerate(actors)}}


class MixedReadStageFixtureTests(unittest.TestCase):
    def test_frozen_distinct_routes_avoid_cooldown_collisions_and_writer_overlap(self):
        saved, messages, cooldowns = manifest(), set(), set()
        self.assertEqual([(128, 10, 20), (128, 20, 20), (128, 20, 35)], [
            (row["requests"], row["concurrency"], row["arrival_rate"]) for row in fixture.phase_specs()])
        for phase, spec in enumerate(fixture.phase_specs()):
            plan = fixture.request_plan(saved, spec["mode"], 128, phase)
            self.assertEqual(plan, fixture.request_plan(saved, spec["mode"], 128, phase))
            counts = Counter(row["key"] for row in plan)
            self.assertEqual({"cultivation_value": 64, "mind_state": 32, "asset_overview": 32}
                             if phase == 0 else {"cultivation_value": 32, "mind_state": 32,
                             "asset_overview": 32, "dual_cultivation": 16, "stone_training": 16}, counts)
            keys = {(row["actor"], row["key"]) for row in plan}
            self.assertEqual(len(keys), len(plan))
            self.assertFalse(keys & cooldowns)
            cooldowns |= keys
            identities = {row["number"] for row in plan}
            self.assertEqual(len(identities), 128)
            self.assertFalse(messages & identities)
            messages |= identities
            dual = [row for row in plan if row["key"] == "dual_cultivation"]
            targets = [actor for row in dual for actor in (row["actor"], row["target"])]
            self.assertEqual(len(set(targets)), len(targets))
            self.assertFalse(set(targets) & {row["actor"] for row in plan if row["key"] == "stone_training"})
            for row in dual:
                self.assertEqual(row["input_text"], f"双修 {saved['names'][row['target']]} 3次")
                self.assertLess(saved["actors"].index(row["target"]), saved["actors"].index(row["actor"]))
        self.assertEqual(saved, manifest())

    def test_all_command_names_have_exactly_one_real_registered_handler(self):
        root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
        trees = [ast.parse((root / relative).read_text(encoding="utf-8")) for relative in
                 ("xiuxian_base/__init__.py", "xiuxian_buff/__init__.py", "xiuxian_buff/partner.py")]
        for _, command in fixture.COMMANDS:
            matches = [(tree, node.targets[0].id) for tree in trees for node in ast.walk(tree)
                       if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
                       and isinstance(node.value.func, ast.Name) and node.value.func.id == "on_command"
                       and node.value.args and isinstance(node.value.args[0], ast.Constant)
                       and node.value.args[0].value == command]
            self.assertEqual(len(matches), 1, command)
            tree, matcher = matches[0]
            handlers = [node for node in ast.walk(tree) if isinstance(node, ast.AsyncFunctionDef)
                        and any(isinstance(item, ast.Call) and isinstance(item.func, ast.Attribute)
                                and isinstance(item.func.value, ast.Name) and item.func.value.id == matcher
                                and item.func.attr == "handle" for item in node.decorator_list)]
            self.assertEqual(len(handlers), 1, command)

    def test_invalid_frozen_phase_or_actor_population_stops_before_dispatch(self):
        for args in (("pure", 127, 0), ("mixed", 128, 0), ("pure", 128, 3)):
            with self.subTest(args=args), self.assertRaises(ValueError):
                fixture.request_plan(manifest(), *args)
        bad = deepcopy(manifest())
        bad["actors"][0] = bad["actors"][1]
        with self.assertRaises(ValueError):
            fixture.request_plan(bad, "pure", 128)

    def test_gain_oracle_preserves_iterative_flooring_and_all_special_event_patterns(self):
        outcomes = fixture._possible_gains(1000, 1000)
        self.assertIn((30, 30), outcomes)
        self.assertIn((45, 45), outcomes)
        self.assertNotIn((46, 45), outcomes)
        self.assertNotIn((0, 0), outcomes)
        self.assertLessEqual(len(outcomes), 8)
        for first, second in ((832256, 832000), (10**24, 10**24 - 1)):
            self.assertTrue(all(a == b and a > 0 for a, b in fixture._possible_gains(first, second)))

    def test_dual_response_requires_both_awards_without_cap_or_generic_success(self):
        request = {"key": "dual_cultivation"}
        self.assertEqual("success", fixture.classify_result(request, {
            "replies": ["A获得修为：30\nB获得修为：30"]})["outcome"])
        for text in ("A获得修为：30", "双修未开始", "A获得修为：30\nB获得修为：30\n后续双修停止：已达上限"):
            self.assertEqual("unclassified", fixture.classify_result(request, {"replies": [text]})["outcome"])


if __name__ == "__main__":
    unittest.main()
