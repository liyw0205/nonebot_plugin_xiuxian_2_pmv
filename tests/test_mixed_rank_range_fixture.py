import unittest

from tests import mixed_rank_range_fixture as fixture


class RankRangeFixtureTests(unittest.TestCase):
    def test_frozen_orders_cover_same_users_and_real_dual_field_writes(self):
        actors = [str(index) for index in range(128)]
        manifest = {"actors": actors}
        specs = fixture.phase_specs()
        self.assertEqual(1280, sum(row["requests"] for row in specs))
        commands, numbers = set(), set()
        for index, spec in enumerate(specs):
            plan = fixture.request_plan(manifest, spec["mode"], spec["requests"], index)
            self.assertEqual(plan, fixture.request_plan(manifest, spec["mode"], spec["requests"], index))
            self.assertTrue(numbers.isdisjoint(row["number"] for row in plan))
            numbers.update(row["number"] for row in plan)
            commands.update(row["key"] for row in plan)
            self.assertLessEqual(spec["requests"] / spec["arrival_rate"], 60)
            self.assertLessEqual(spec["concurrency"], 20)
            if spec["mode"] == "cold":
                order = [row["actor"] for row in plan]
                self.assertEqual(set(actors), set(order))
                self.assertEqual({"profile_info"}, {row["key"] for row in plan})
                self.assertEqual(order == actors[::-1], spec["order"] == "descending")
                self.assertNotEqual(actors, order)
            writes = [row for row in plan if row["key"] == "stone_training"]
            self.assertTrue(all(row["requested_stone"] == 1000 and row["exp_gain"] == 100 for row in writes))
            if spec["mode"] == "contention":
                self.assertEqual(1, len({row["actor"] for row in writes}))
        self.assertEqual(7, len(commands))
        self.assertEqual([str(index) for index in range(128)], manifest["actors"])
        with self.assertRaises(ValueError):
            fixture.request_plan(manifest, "mixed", 128)
