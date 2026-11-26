from pathlib import Path
import tempfile
import unittest

from tests import mixed_rank_seek_fixture as fixture
from tests.test_mixed_read_strata_fixture import manifest


class RankSeekFixtureTests(unittest.TestCase):
    def test_plan_covers_cohorts_and_writes_with_unique_messages(self):
        saved = manifest()
        readers = saved["groups"]["exact_blob"]
        saved["cohorts"] = {"near": readers[:8], "mid": readers[8:16], "deep": readers[16:]}
        numbers = set()
        self.assertEqual(416, sum(row["requests"] for row in fixture.phase_specs()))
        for index, spec in enumerate(fixture.phase_specs()):
            plan = fixture.request_plan(saved, spec["mode"], spec["requests"], index)
            self.assertEqual(plan, fixture.request_plan(saved, spec["mode"], spec["requests"], index))
            self.assertFalse(numbers.intersection(row["number"] for row in plan))
            numbers.update(row["number"] for row in plan)
            profiles = [row for row in plan if row["key"] == "profile_info"]
            self.assertEqual([48, 64, 32][index], len(profiles))
            self.assertEqual({"near", "mid", "deep"}, {row["rank_cohort"] for row in profiles})
            writes = [row for row in plan if row["key"] == "stone_training"]
            self.assertEqual([0, 64, 64][index], len(writes))
            self.assertTrue(all(row["actor"] in saved["groups"]["training_low"] for row in writes))
            if spec["mode"] == "contention":
                self.assertEqual(1, len({row["actor"] for row in writes}))
            for row in profiles:
                self.assertEqual(saved["expected_ranks"][row["actor"]], row["expected_ranks"])
        with self.assertRaises(ValueError):
            fixture.request_plan(saved, "pure", 192, 0)

    def test_streaming_grouped_oracle_matches_independent_python_dense_rank(self):
        from nonebot_plugin_xiuxian_2.core.numeric import sqlite_int_sort_key
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import db_backend
        values = [None, "invalid", -3, "-3", "1e3", b"1000", 2**63 - 1, b"999999999999999999999999999999"]
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "oracle.db"
            with db_backend.transaction(database) as connection:
                connection.execute("CREATE TABLE user_xiuxian(user_id TEXT,exp,stone)")
                connection.executemany("INSERT INTO user_xiuxian VALUES(?,?,?)",
                                       [(str(index), value, value) for index, value in enumerate(values)])
                connection.execute("CREATE TABLE xiuxian_numeric_rank_generation(singleton,generation,exp_generation,stone_generation)")
                connection.execute("INSERT INTO xiuxian_numeric_rank_generation VALUES(1,0,0,0)")
            actors = [str(index) for index in range(1, len(values))]
            ranks, evidence = fixture._grouped_oracle(database, actors)
            keys = [sqlite_int_sort_key(value) for value in values]
            for index, actor in enumerate(actors, 1):
                expected = 1 + len({key for key in keys if key is not None and key > keys[index]})
                self.assertEqual({"exp": expected, "stone": expected}, ranks[actor])
                higher = sum(key is not None and key > keys[index] for key in keys)
                self.assertEqual(higher, evidence["columns"]["exp"]["target_higher_rows"][actor])
            self.assertEqual(0, evidence["request_time_oracle_sql"])
            with self.assertRaisesRegex(ValueError, "non-NULL"):
                fixture._grouped_oracle(database, ["0"])
