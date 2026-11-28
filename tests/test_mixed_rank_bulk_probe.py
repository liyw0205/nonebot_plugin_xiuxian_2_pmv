from __future__ import annotations

from pathlib import Path
import sqlite3
import tempfile
import unittest

from scripts.mixed_rank_bulk_probe import measure_rank_generation_cost


class MixedRankBulkProbeTests(unittest.TestCase):
    def test_probe_restores_scores_triggers_and_both_generation_schemas(self):
        for split_fields in (False, True):
            with self.subTest(split_fields=split_fields), tempfile.TemporaryDirectory() as root:
                database = Path(root) / "synthetic.db"
                with sqlite3.connect(database) as connection:
                    connection.execute("CREATE TABLE user_xiuxian(id INTEGER PRIMARY KEY, exp, stone)")
                    connection.executemany("INSERT INTO user_xiuxian VALUES(?,?,?)", [
                        (1, 100, 10), (2, 200, 2.75),
                        (3, 300, b"123456789012345678901234567890"), (4, 400, 40)])
                    fields = ", exp_generation INTEGER, stone_generation INTEGER" if split_fields else ""
                    connection.execute("CREATE TABLE xiuxian_numeric_rank_generation(singleton INTEGER PRIMARY KEY, generation INTEGER" + fields + ")")
                    values = "1,0,0,0" if split_fields else "1,0"
                    connection.execute("INSERT INTO xiuxian_numeric_rank_generation VALUES(" + values + ")")
                    for action in ("insert", "delete", "update"):
                        extra = (",exp_generation=exp_generation+(NEW.exp IS NOT OLD.exp),"
                                 "stone_generation=stone_generation+(NEW.stone IS NOT OLD.stone)"
                                 if action == "update" else
                                 ",exp_generation=exp_generation+1,stone_generation=stone_generation+1") if split_fields else ""
                        connection.execute(
                            f"CREATE TRIGGER xiuxian_numeric_rank_generation_{action} AFTER {action.upper()} ON user_xiuxian "
                            "BEGIN UPDATE xiuxian_numeric_rank_generation SET generation=generation+1" + extra + "; END"
                        )
                result = measure_rank_generation_cost(database, rows=3, repeats=1)
                self.assertEqual(result["status"], "pass")
                self.assertTrue(result["triggers_and_generation_restored"])
                self.assertEqual(result["selected_data_sha256_before"], result["selected_data_sha256_after"])
                self.assertEqual(result["selection"]["original_inexact_rows"], 1)
                self.assertEqual(set(result["by_columns"]), {"stone", "exp", "exp+stone"})
                with sqlite3.connect(database) as connection:
                    self.assertEqual(connection.execute("SELECT stone FROM user_xiuxian ORDER BY id").fetchall(),
                                     [(10,), (2.75,), (b"123456789012345678901234567890",), (40,)])

    def test_probe_rejects_unbounded_parameters_before_opening_database(self):
        for kwargs in ({"rows": 4097}, {"repeats": 4}, {"rows": 0}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                measure_rank_generation_cost(Path("not-created.db"), **kwargs)


if __name__ == "__main__":
    unittest.main()
