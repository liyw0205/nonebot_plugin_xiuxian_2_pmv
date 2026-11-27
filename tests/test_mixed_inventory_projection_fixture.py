"""Small frozen-plan/classification checks; no million-user workload here."""
from collections import Counter
import unittest

from scripts import mixed_command_probe as probe
from tests import mixed_inventory_projection_fixture as fixture


def manifest():
    return {"actors": [str(index + 1) for index in range(128)],
            "inventory_projection": {"recipe": "配方 主药恒心草1药引恒心草1辅药恒心草1丹炉虚构不存在丹炉",
                "missing_furnace_reply": "请检查炼丹炉：虚构不存在丹炉 是否在背包中！",
                "oracles": {key: [key + " expected block"] for key, _ in fixture.COMMANDS}}}


class InventoryProjectionFixtureTests(unittest.TestCase):
    def test_three_frozen_plans_keep_cooldowns_real_writes_and_refusals_separate(self):
        saved = manifest()
        for phase, spec in enumerate(fixture.phase_specs()):
            plan = fixture.request_plan(saved, spec["mode"], spec["requests"], phase)
            counts = Counter(row["key"] for row in plan)
            expected = Counter({key: 24 if phase == 0 else 16 for key, _ in fixture.COMMANDS})
            expected.update(recipe_preflight=32, stone_training=0 if phase == 0 else 32)
            self.assertEqual(+expected, counts)
            self.assertEqual(128, len({row["number"] for row in plan}))
            for key, _ in fixture.COMMANDS:
                actors = [row["actor"] for row in plan if row["key"] == key]
                self.assertEqual(len(actors), len(set(actors)))
            writers = [row["actor"] for row in plan if row["key"] == "stone_training"]
            self.assertEqual((0, 32, 1)[phase], len(set(writers)))
            self.assertLessEqual(spec["concurrency"], 20)
            self.assertLessEqual(spec["arrival_rate"], 35)

    def test_exact_replies_and_refusals_are_not_interchangeable(self):
        saved = manifest()
        plan = fixture.request_plan(saved, "pure", 128)
        for row in plan:
            if row["key"] == "recipe_preflight":
                text, outcome = row["expected_reply"], "business_rejection"
            else:
                text, outcome = row["expected_blocks"][0] + "\n第 1/1 页", "success"
            self.assertEqual(outcome, fixture.classify_result(row, {"replies": [text]})["outcome"])
            self.assertEqual("unclassified", fixture.classify_result(row, {"replies": ["unrelated"]})["outcome"])
        display = plan[0]
        self.assertEqual("unclassified", fixture.classify_result(display, {"replies": [
            display["expected_blocks"][0] * 2 + "\n第 1/1 页"]})["outcome"])
        self.assertEqual("exception", fixture.classify_result(display, {"errors": ["SQL read failed"]})["outcome"])

    def test_new_scenario_uses_existing_limits_and_exact_participant_count(self):
        args = probe.parse_args(["--fixture", "/tmp/fixture", "--report", "/tmp/report",
                                 "--scenario", "inventory-projection-v26"])
        self.assertEqual(128, args.participants)
        with self.assertRaises(ValueError):
            fixture.request_plan(manifest(), "pure", 64)

    def test_markdown_fallback_requires_every_exact_quantity_binding_and_use_line(self):
        for key in ("pill_backpack", "herb_backpack"):
            request = {"key": key, "expected_blocks": ["名字：甲\nID：1\n拥有数量：20，绑定数量：0",
                                                       "名字：乙\nID：2\n拥有数量：20，绑定数量：0"]}
            body = "- 乙 数量:20 绑定:0\n- 甲 数量:20 绑定:0\n"
            if key == "pill_backpack":
                body += "  使用：使用 甲\n  使用：使用 乙\n"
            body += "第 1/1 页"
            self.assertEqual("success", fixture.classify_result(request, {"replies": [body]})["outcome"])
            for bad in (body.replace("数量:20", "数量:19", 1),
                        body.replace("绑定:0", "绑定:1", 1),
                        body.replace("- 乙 数量:20 绑定:0\n", ""),
                        body + "\n- 甲 数量:20 绑定:0",
                        body.replace("第 1/1 页", "第 1/2 页")):
                with self.subTest(key=key, bad=bad):
                    self.assertEqual("unclassified", fixture.classify_result(request, {"replies": [bad]})["outcome"])


if __name__ == "__main__":
    unittest.main()
