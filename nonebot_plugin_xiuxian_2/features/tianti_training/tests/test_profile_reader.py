from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from ..repository import TiantiProfileSqlReader


class Profile:
    def default_data(self):
        return {
            "tianti_level": "one", "tianti_hp": 0, "last_settle_time": None,
            "medicine_last_time": None, "medicine_end_time": None,
            "medicine_effect": 0.0, "medicine_name": "", "opened_qiaoxue": [],
            "opened_qiaoxue_detail": [], "qiaoxue_stage_opened": {},
        }

    def clean(self, row):
        data = self.default_data()
        data.update({key: row[key] for key in data if row and row.get(key) is not None})
        data["tianti_hp"] = max(0, int(data.get("tianti_hp", 0) or 0))
        return data


class TiantiProfileSqlReaderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.database = Path(self.tmp.name) / "player.db"
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                "CREATE TABLE tianti_info ("
                "user_id TEXT PRIMARY KEY, tianti_level TEXT, tianti_hp TEXT, last_settle_time TEXT, "
                "medicine_last_time TEXT, medicine_end_time TEXT, medicine_effect TEXT, medicine_name TEXT, "
                "opened_qiaoxue TEXT, opened_qiaoxue_detail TEXT, qiaoxue_stage_opened TEXT)"
            )
        self.reader = TiantiProfileSqlReader(self.database, profile_reader=Profile())

    def tearDown(self):
        self.tmp.cleanup()

    def test_existing_profile_is_decoded_without_mutation(self):
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                "INSERT INTO tianti_info VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("u", "one", "42", None, None, None, "0", "", '["窍一"]', '[{"name":"窍一"}]', '{"one":1}'),
            )
        result = self.reader.read("u")
        self.assertEqual(result["tianti_hp"], 42)
        self.assertEqual(result["opened_qiaoxue"], ["窍一"])
        self.assertEqual(result["opened_qiaoxue_detail"], [{"name": "窍一"}])
        self.assertEqual(result["qiaoxue_stage_opened"], {"one": 1})
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(conn.execute("SELECT tianti_hp FROM tianti_info WHERE user_id='u'").fetchone()[0], "42")

    def test_missing_profile_returns_defaults_without_inserting(self):
        result = self.reader.read("missing")
        self.assertEqual((result["tianti_level"], result["tianti_hp"], result["opened_qiaoxue"]), ("one", 0, []))
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM tianti_info").fetchone()[0], 0)

    def test_missing_schema_fails_without_creating_it(self):
        empty_database = Path(self.tmp.name) / "empty.db"
        sqlite3.connect(empty_database).close()
        reader = TiantiProfileSqlReader(empty_database, profile_reader=Profile())
        with self.assertRaisesRegex(RuntimeError, "run migrations"):
            reader.read("u")
        with sqlite3.connect(empty_database) as conn:
            self.assertIsNone(
                conn.execute("SELECT name FROM sqlite_master WHERE name='tianti_info'").fetchone()
            )


if __name__ == "__main__":
    unittest.main()
