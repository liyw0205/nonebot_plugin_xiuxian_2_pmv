from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ..repository import TiantiProfileReader, TiantiProfileSqlReader


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

    def test_real_profile_reader_normalizes_legacy_qiaoxue_data(self):
        data_directory = Path(self.tmp.name) / "xiuxian"
        training_directory = data_directory / "炼体"
        training_directory.mkdir(parents=True)
        (training_directory / "炼体境界.json").write_text(
            json.dumps({"凡体": {"rank": 1, "need_hp": 10}}, ensure_ascii=False),
            encoding="utf-8",
        )
        qiaoxue = [
            {"name": "窍一", "group": "天罡", "effect_type": "ratio", "effect_value": 0.1},
            {"name": "窍二", "group": "地煞", "effect_type": "gain", "effect_value": 0.2},
        ]
        (training_directory / "炼体窍穴.json").write_text(
            json.dumps({"窍穴": qiaoxue}, ensure_ascii=False),
            encoding="utf-8",
        )
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                "INSERT INTO tianti_info VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "legacy", "不存在的境界", "bad", "null", "", "None", "bad", "null",
                    json.dumps(["窍二", "无效窍", "窍一", "窍二", 1], ensure_ascii=False),
                    json.dumps(
                        [
                            {"name": "窍一", "effect_value": "bad"},
                            {"name": "窍二"},
                            {"name": "窍二", "effect_value": 9},
                            {"name": "无效窍"},
                        ],
                        ensure_ascii=False,
                    ),
                    json.dumps({"天罡": "-2", "地煞": "bad"}, ensure_ascii=False),
                ),
            )

        reader = TiantiProfileSqlReader(self.database, profile_reader=TiantiProfileReader(data_directory))
        result = reader.read("legacy")

        self.assertEqual(result["tianti_level"], "凡体")
        self.assertEqual(result["tianti_hp"], 0)
        self.assertIsNone(result["last_settle_time"])
        self.assertEqual((result["medicine_effect"], result["medicine_name"]), (0.0, ""))
        self.assertEqual(result["opened_qiaoxue"], ["窍二", "窍一"])
        self.assertEqual(
            result["opened_qiaoxue_detail"],
            [
                {"name": "窍二", "group": "地煞", "effect_type": "gain", "effect_value": 0.2},
                {"name": "窍一", "group": "天罡", "effect_type": "ratio", "effect_value": 0.1},
            ],
        )
        self.assertEqual(result["qiaoxue_stage_opened"], {"天罡": 0, "地煞": 0})
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(conn.execute("SELECT tianti_hp FROM tianti_info WHERE user_id='legacy'").fetchone()[0], "bad")

    def test_profile_cap_uses_the_next_rank_and_closing_multiplier(self):
        data_directory = Path(self.tmp.name) / "cap-xiuxian"
        training_directory = data_directory / "炼体"
        training_directory.mkdir(parents=True)
        (training_directory / "炼体境界.json").write_text(
            json.dumps({
                "凡体": {"rank": 1, "need_hp": 0},
                "炼体境": {"rank": 2, "need_hp": 100},
            }, ensure_ascii=False),
            encoding="utf-8",
        )
        (training_directory / "炼体窍穴.json").write_text("{\"窍穴\": []}", encoding="utf-8")
        profile = TiantiProfileReader(data_directory, closing_multiplier=2.0)
        reader = TiantiProfileSqlReader(self.database, profile_reader=profile)

        self.assertEqual(reader.cap({"tianti_level": "凡体"}), 200)
        self.assertEqual(reader.cap({"tianti_level": "炼体境"}), 10**30)


if __name__ == "__main__":
    unittest.main()
