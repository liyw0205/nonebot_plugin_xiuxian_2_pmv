import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from ..weekly_progress_repository import SectWeeklyProgressSqlRepository


class SectWeeklyProgressRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "sect.db"
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "CREATE TABLE sect_weekly_goal("
                "sect_id INTEGER NOT NULL,week_key TEXT NOT NULL,goal_key TEXT NOT NULL,"
                "progress INTEGER NOT NULL DEFAULT 0,target INTEGER NOT NULL,"
                "participants TEXT NOT NULL DEFAULT '{}',claimed_users TEXT NOT NULL DEFAULT '[]',"
                "updated_at TEXT NOT NULL DEFAULT '',PRIMARY KEY(sect_id,week_key,goal_key))"
            )
            connection.execute("CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT)")
            connection.executemany("INSERT INTO sects VALUES(?,?)", ((1, "青云宗"), (2, "赤霄宗")))
        self.repository = SectWeeklyProgressSqlRepository(self.database)
        self.goals = (
            {"key": "diligence", "target": 3},
            {"key": "supply", "target": 10},
        )

    def tearDown(self):
        self.temp.cleanup()

    def test_listing_initializes_goals_once_and_preserves_existing_state(self):
        self.repository.ensure_goals(1, "2026-W39", self.goals, "first")
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "UPDATE sect_weekly_goal SET progress=2,claimed_users='[\"u\"]' "
                "WHERE sect_id=1 AND week_key='2026-W39' AND goal_key='diligence'"
            )
        rows = self.repository.list_goals(1, "2026-W39", self.goals, "second")
        self.assertEqual(["diligence", "supply"], [row["goal_key"] for row in rows])
        diligence = rows[0]
        self.assertEqual((2, 3, '["u"]'), (diligence["progress"], diligence["target"], diligence["claimed_users"]))
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(2, connection.execute("SELECT COUNT(*) FROM sect_weekly_goal").fetchone()[0])

    def test_progress_is_capped_but_participant_totals_continue(self):
        first = self.repository.record_progress(1, "2026-W39", "u", 2, self.goals, "t1")
        second = self.repository.record_progress(1, "2026-W39", "u", 4, self.goals, "t2")
        self.assertEqual((False, True), (first[0]["completed"], second[0]["completed"]))
        with sqlite3.connect(self.database) as connection:
            rows = connection.execute(
                "SELECT goal_key,progress,participants FROM sect_weekly_goal ORDER BY goal_key"
            ).fetchall()
        self.assertEqual(("diligence", 3, {"u": 6}), (rows[0][0], rows[0][1], json.loads(rows[0][2])))
        self.assertEqual(("supply", 6, {"u": 6}), (rows[1][0], rows[1][1], json.loads(rows[1][2])))

    def test_concurrent_events_do_not_lose_participant_or_progress_updates(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(
                    self.repository.record_progress,
                    1,
                    "2026-W39",
                    user_id,
                    1,
                    self.goals,
                    "now",
                )
                for user_id in ("u1", "u2")
            ]
            for future in futures:
                future.result()
        with sqlite3.connect(self.database) as connection:
            row = connection.execute(
                "SELECT progress,participants FROM sect_weekly_goal "
                "WHERE sect_id=1 AND week_key='2026-W39' AND goal_key='diligence'"
            ).fetchone()
        self.assertEqual(2, row[0])
        self.assertEqual({"u1": 1, "u2": 1}, json.loads(row[1]))

    def test_rank_is_ordered_and_limited(self):
        self.repository.record_progress(1, "2026-W39", "u", 5, self.goals, "now")
        self.repository.record_progress(2, "2026-W39", "v", 8, self.goals, "now")
        rows = self.repository.weekly_rank(1, "2026-W39")
        self.assertEqual(1, len(rows))
        self.assertEqual((2, "赤霄宗", 11), (rows[0]["sect_id"], rows[0]["sect_name"], rows[0]["total_progress"]))

    def test_missing_schema_fails_without_creating_tables(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "unmigrated.db"
            with sqlite3.connect(database):
                pass
            repository = SectWeeklyProgressSqlRepository(database)
            with self.assertRaisesRegex(RuntimeError, "run migrations first"):
                repository.ensure_goals(1, "2026-W39", self.goals, "now")
            with sqlite3.connect(database) as connection:
                tables = connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            self.assertEqual([], tables)

if __name__ == "__main__":
    unittest.main()
