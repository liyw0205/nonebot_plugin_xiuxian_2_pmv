"""Small plan/classification contracts, without loading game fixtures."""
from copy import deepcopy
import unittest

from tests.mixed_command_fixture import (
    READ_SPECS, _activity_seed_config, classify_result, request_plan,
)


def manifest():
    actors = [str(900001 + index) for index in range(32)]
    return {
        "actors": actors, "buyers": actors[:-1],
        "listings": [{"id": f"offer-{index}"} for index in range(32)],
    }


class MixedCommandPlanTests(unittest.TestCase):
    def test_pure_reads_interleave_all_32_participants(self):
        fixture = manifest()
        plan = request_plan(fixture, "pure", 64)
        self.assertEqual([row["actor"] for row in plan[:32]], fixture["actors"])
        self.assertEqual(len({row["actor"] for row in plan}), 32)
        self.assertEqual({row["command"] for row in plan}, {spec[1] for spec in READ_SPECS})
        self.assertEqual(len({row["number"] for row in plan}), len(plan))

    def test_mixed_plan_covers_13_real_commands_deterministically(self):
        first = request_plan(manifest(), "mixed", 96, phase_number=3)
        self.assertEqual(first, request_plan(manifest(), "mixed", 96, phase_number=3))
        self.assertEqual(len({row["command"] for row in first}), 13)
        self.assertEqual(sum(row["key"] in {"guishi_deposit", "xianshi_purchase"} for row in first), 48)
        self.assertTrue(all(row["number"] >= 880_300_000 for row in first))

    def test_readmostly_and_contention_have_distinct_write_and_resource_mix(self):
        fixture = manifest()
        mostly = request_plan(fixture, "readmostly", 100)
        self.assertEqual(sum(row["key"] in {"guishi_deposit", "xianshi_purchase"} for row in mostly), 10)
        contention = request_plan(fixture, "contention", 32)
        deposits = [row for row in contention if row["key"] == "guishi_deposit"]
        purchases = [row for row in contention if row["key"] == "xianshi_purchase"]
        self.assertEqual({row["actor"] for row in deposits}, {fixture["buyers"][0]})
        self.assertEqual({row["listing_id"] for row in purchases}, {fixture["listings"][-1]["id"]})
        self.assertGreater(len({row["actor"] for row in purchases}), 1)

    def test_invalid_plan_does_not_silently_expand_workload(self):
        for mode, count, phase in (("unknown", 10, 0), ("pure", 0, 0), ("mixed", 4097, 0), ("pure", 2, 90)):
            with self.subTest(mode=mode, count=count, phase=phase), self.assertRaises(ValueError):
                request_plan(manifest(), mode, count, phase)

    def test_activity_seed_only_changes_availability_without_mutating_original(self):
        original = {
            "enabled": False, "start_time": "2020-01-01", "end_time": "2020-02-01",
            "daily_rewards": [{"reward": "灵石x50000"}],
            "gameplay_activities": [{"key": "points", "enabled": False, "cost": 20, "weekly_limit": 3}],
            "extensions": {"stages": [{"features": ["points"], "multiplier": 1.0}]},
        }
        before = deepcopy(original)
        seeded = _activity_seed_config(original)
        self.assertEqual(original, before)
        self.assertTrue(seeded["enabled"])
        self.assertEqual((seeded["start_time"], seeded["end_time"]), ("0", "无限"))
        self.assertEqual(seeded["daily_rewards"], before["daily_rewards"])
        self.assertEqual(seeded["gameplay_activities"][0]["cost"], 20)
        self.assertEqual(seeded["gameplay_activities"][0]["weekly_limit"], 3)
        self.assertEqual(seeded["extensions"]["stages"][0]["features"], ["points"])


class MixedCommandClassificationTests(unittest.TestCase):
    def classify(self, key, reply, **extra):
        return classify_result({"key": key}, {"status": "pass", "replies": [reply], **extra})

    def test_actual_writes_and_replays_are_successful(self):
        for key, reply in (
            ("guishi_deposit", "成功存入 1000 灵石到鬼市账户！"),
            ("xianshi_purchase", "成功购买 恒心草 x1\n花费 60万 灵石"),
            ("xianshi_purchase", "该购买请求已经处理，无需重复提交。"),
        ):
            with self.subTest(key=key, reply=reply):
                self.assertEqual(self.classify(key, reply)["outcome"], "success")

    def test_business_rejections_and_cooldowns_are_not_success(self):
        rejected = self.classify("xianshi_purchase", "未找到仙肆ID为 offer-1 的物品！")
        self.assertEqual(rejected, {"outcome": "business_rejection", "reason": "listing_missing"})
        self.assertEqual(self.classify("guishi_deposit", "灵石不足，存入失败！")["outcome"], "business_rejection")
        self.assertEqual(self.classify("backpack", "冷静一下，还有3秒，让我再歇会！")["outcome"], "cooldown")

    def test_unknown_responses_and_missing_markers_fail_closed(self):
        for request, reply in (({"key": "unknown"}, "some reply"), ({"key": "profile_info"}, "道号 修为排行 灵石排行"),
                               ({"key": "profile_info", "success_markers": ["道号", "灵石排行"]}, "道号 查询失败")):
            with self.subTest(request=request, reply=reply):
                self.assertEqual(classify_result(request, {"replies": [reply]})["outcome"], "unclassified")

    def test_boundary_exception_timeout_take_precedence_over_success_text(self):
        reply = "成功存入 1000 灵石到鬼市账户！"
        for extra, expected in (({"blocked": ["denied"]}, "boundary"), ({"errors": ["traceback"]}, "exception"),
                                ({"timed_out": True}, "timeout")):
            with self.subTest(extra=extra):
                self.assertEqual(self.classify("guishi_deposit", reply, **extra)["outcome"], expected)


if __name__ == "__main__":
    unittest.main()
