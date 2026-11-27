"""Freeze V25 owner cases and reuse exact known dispatch/receipt oracles."""
from collections import Counter
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from scripts.mixed_sect_seek_probe import INDEX, measure_owner_index_cost
from tests import mixed_sect_seek_fixture as fixture
from tests.test_mixed_sect_directory_fixture import manifest


class SectSeekFixtureTests(unittest.TestCase):
    def test_owner_cases_are_bounded_and_leave_actor_sects_unchanged(self):
        ids = list(range(128))
        late = [{"user_id": str(index)} for index in range(999996, 1000000)]
        early = [{"user_id": str(index)} for index in range(1, 5)]
        cases = fixture._owner_cases(ids, late, early)
        self.assertEqual(list(range(4, 20)), [sect for sect, _ in cases])
        self.assertEqual([row["user_id"] for row in late], [owner for _, owner in cases[:4]])
        self.assertTrue(all(owner.startswith("00") for _, owner in cases[8:12]))
        self.assertTrue(all(owner.startswith("v25-absent-") for _, owner in cases[12:]))
        with self.assertRaises(ValueError):
            fixture._owner_cases(ids[:-1], late, early)

    def test_frozen_plans_keep_real_writes_and_cooldown_identities(self):
        saved, seen = manifest(), set()
        with patch.object(fixture.base.base, "_number", str):
            for index, spec in enumerate(fixture.phase_specs()):
                plan = fixture.request_plan(saved, spec["mode"], 128, index)
                expected = {"sect_directory": 16, "sect_main_view": 56, "sect_sec_view": 56} if index == 0 else {
                    "sect_directory": 16, "sect_main_view": 32, "sect_sec_view": 32,
                    "asset_overview": 16, "sect_attack_upgrade": 11, "sect_health_upgrade": 11,
                    "sect_mana_upgrade": 10}
                self.assertEqual(expected, Counter(row["key"] for row in plan))
                self.assertEqual(16, sum(row["performance_role"] == "optimized_target" for row in plan))
                for row in plan:
                    if row["key"] not in fixture.base.PRACTICES:
                        continue
                    identity = (row["actor"], row["key"])
                    self.assertNotIn(identity, seen)
                    seen.add(identity)

    def test_index_cost_control_restores_exact_rows_generations_and_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "cost.db"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE user_xiuxian(id INTEGER PRIMARY KEY,user_id TEXT,exp,stone)")
                connection.executemany("INSERT INTO user_xiuxian VALUES(?,?,?,?)", [
                    (index, str(index), str(2**100 + index), str(2**110 + index)) for index in range(1, 33)])
                connection.execute("CREATE TABLE xiuxian_numeric_rank_generation(singleton,generation)")
                connection.execute("INSERT INTO xiuxian_numeric_rank_generation VALUES(1,0)")
                connection.execute("CREATE TRIGGER count_updates AFTER UPDATE ON user_xiuxian BEGIN "
                                   "UPDATE xiuxian_numeric_rank_generation SET generation=generation+1; END")
            for indexed in (False, True):
                if indexed:
                    with sqlite3.connect(database) as connection:
                        connection.execute(f"CREATE INDEX {INDEX} ON user_xiuxian(CAST(user_id AS NUMERIC))")
                with sqlite3.connect(database) as connection:
                    before = tuple(connection.iterdump())
                result = measure_owner_index_cost(database)
                self.assertEqual("pass", result["status"])
                self.assertEqual(indexed, result["index_present"])
                self.assertEqual(12 if indexed else 6, len(result["samples"]))
                self.assertEqual(indexed, result["index_bytes"] > 0)
                with sqlite3.connect(database) as connection:
                    self.assertEqual(before, tuple(connection.iterdump()))


if __name__ == "__main__":
    unittest.main()
