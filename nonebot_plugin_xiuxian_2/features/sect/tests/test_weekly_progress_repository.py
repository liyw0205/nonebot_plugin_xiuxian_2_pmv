import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from ....core.errors import DomainError
from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import (
    apply_sect_task_settlement_operations,
    apply_sect_task_weekly_event_status,
)
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
        with DatabaseUnitOfWork(str(self.database), immediate=True) as uow:
            apply_sect_task_settlement_operations(uow)
            apply_sect_task_weekly_event_status(uow)
        self.repository = SectWeeklyProgressSqlRepository(self.database)
        self.goals = (
            {"key": "diligence", "target": 3},
            {"key": "supply", "target": 10},
        )

    def tearDown(self):
        self.temp.cleanup()

    def add_pending_settlement(self, operation_id="task-op"):
        self.repository.ensure_goals(1, "2026-W47", self.goals, "before")
        with DatabaseUnitOfWork(str(self.database), immediate=True) as uow:
            uow.execute(
                "INSERT INTO sect_task_settlement_operations("
                "operation_id,user_id,sect_id,period,cost_type,cost,exp_reward,"
                "sect_reward,materials_reward,weekly_event_status) "
                "VALUES(?, 'u', 1, '2026-11-18', 'stone', 5, 1, 1, 10, 'pending')",
                (operation_id,),
            )

    def linked_state(self, operation_id="task-op"):
        with sqlite3.connect(self.database) as connection:
            goal = connection.execute(
                "SELECT progress,participants FROM sect_weekly_goal "
                "WHERE sect_id=1 AND week_key='2026-W47' AND goal_key='diligence'"
            ).fetchone()
            marker = connection.execute(
                "SELECT weekly_event_status FROM sect_task_settlement_operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            effects = connection.execute(
                "SELECT COUNT(*) FROM isolated_effect"
            ).fetchone()[0] if connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='isolated_effect'"
            ).fetchone() else 0
        return goal, marker, effects

    def record_linked_event(self, operation_id="task-op"):
        return self.repository.record_progress(
            1,
            "2026-W47",
            "u",
            1,
            ({"key": "diligence", "target": 3},),
            "now",
            settlement_operation_id=operation_id,
        )

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

    def test_ignored_goal_write_rolls_back_side_effect_and_pending_marker_then_recovers_once(self):
        self.add_pending_settlement()
        with sqlite3.connect(self.database) as connection:
            connection.execute("CREATE TABLE isolated_effect(name TEXT)")
            connection.execute(
                "CREATE TRIGGER ignore_weekly_goal BEFORE UPDATE OF progress ON sect_weekly_goal "
                "WHEN OLD.goal_key='diligence' BEGIN "
                "INSERT INTO isolated_effect VALUES('goal'); SELECT RAISE(IGNORE); END"
            )

        with self.assertRaises(DomainError) as caught:
            self.record_linked_event()
        self.assertEqual("sect_weekly_progress_goal_write_rejected", caught.exception.code)
        self.assertEqual(
            {"reason": "sect_weekly_progress_goal_write_rejected", "subject": "1/2026-W47/diligence"},
            caught.exception.details,
        )
        self.assertEqual(((0, "{}"), ("pending",), 0), self.linked_state())

        with sqlite3.connect(self.database) as connection:
            connection.execute("DROP TRIGGER ignore_weekly_goal")
        first = self.record_linked_event()
        second = self.record_linked_event()
        self.assertEqual((1, False), (first[0]["progress"], first[0]["completed"]))
        self.assertEqual([], second)
        self.assertEqual(((1, '{"u": 1}'), ("recorded",), 0), self.linked_state())

    def test_rewritten_goal_update_is_detected_and_rolled_back(self):
        self.add_pending_settlement()
        with sqlite3.connect(self.database) as connection:
            connection.execute("CREATE TABLE isolated_effect(name TEXT)")
            connection.execute(
                "CREATE TRIGGER rewrite_weekly_goal AFTER UPDATE OF progress ON sect_weekly_goal "
                "WHEN NEW.goal_key='diligence' BEGIN "
                "INSERT INTO isolated_effect VALUES('goal'); "
                "UPDATE sect_weekly_goal SET progress=OLD.progress,participants=OLD.participants "
                "WHERE sect_id=NEW.sect_id AND week_key=NEW.week_key AND goal_key=NEW.goal_key; END"
            )

        with self.assertRaises(DomainError) as caught:
            self.record_linked_event()
        self.assertEqual("sect_weekly_progress_goal_write_rejected", caught.exception.code)
        self.assertEqual(((0, "{}"), ("pending",), 0), self.linked_state())

    def test_ignored_settlement_marker_rolls_back_progress_and_side_effect(self):
        self.add_pending_settlement()
        with sqlite3.connect(self.database) as connection:
            connection.execute("CREATE TABLE isolated_effect(name TEXT)")
            connection.execute(
                "CREATE TRIGGER ignore_weekly_marker BEFORE UPDATE OF weekly_event_status "
                "ON sect_task_settlement_operations BEGIN "
                "INSERT INTO isolated_effect VALUES('marker'); SELECT RAISE(IGNORE); END"
            )

        with self.assertRaises(DomainError) as caught:
            self.record_linked_event()
        self.assertEqual("sect_weekly_progress_settlement_write_rejected", caught.exception.code)
        self.assertEqual(
            {"reason": "sect_weekly_progress_settlement_write_rejected", "subject": "task-op"},
            caught.exception.details,
        )
        self.assertEqual(((0, "{}"), ("pending",), 0), self.linked_state())

    def test_rewritten_settlement_marker_rolls_back_progress_and_side_effect(self):
        self.add_pending_settlement()
        with sqlite3.connect(self.database) as connection:
            connection.execute("CREATE TABLE isolated_effect(name TEXT)")
            connection.execute(
                "CREATE TRIGGER rewrite_weekly_marker AFTER UPDATE OF weekly_event_status "
                "ON sect_task_settlement_operations WHEN NEW.weekly_event_status='recorded' BEGIN "
                "INSERT INTO isolated_effect VALUES('marker'); "
                "UPDATE sect_task_settlement_operations SET weekly_event_status='pending' "
                "WHERE operation_id=NEW.operation_id; END"
            )

        with self.assertRaises(DomainError) as caught:
            self.record_linked_event()
        self.assertEqual("sect_weekly_progress_settlement_write_rejected", caught.exception.code)
        self.assertEqual(((0, "{}"), ("pending",), 0), self.linked_state())

    def test_marker_trigger_cannot_tamper_with_already_verified_goal(self):
        self.add_pending_settlement()
        with sqlite3.connect(self.database) as connection:
            connection.execute("CREATE TABLE isolated_effect(name TEXT)")
            connection.execute(
                "CREATE TRIGGER late_goal_tamper AFTER UPDATE OF weekly_event_status "
                "ON sect_task_settlement_operations BEGIN "
                "INSERT INTO isolated_effect VALUES('late-goal'); "
                "UPDATE sect_weekly_goal SET progress=0,participants='{}'; END"
            )
        with self.assertRaises(DomainError) as caught:
            self.record_linked_event()
        self.assertEqual("sect_weekly_progress_goal_write_rejected", caught.exception.code)
        self.assertEqual(((0, "{}"), ("pending",), 0), self.linked_state())

    def test_delayed_pending_event_uses_settlement_business_week(self):
        self.add_pending_settlement()
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "UPDATE sect_task_settlement_operations SET period='2026-11-15'"
            )
        self.record_linked_event()
        with sqlite3.connect(self.database) as connection:
            previous_week = connection.execute(
                "SELECT progress,participants FROM sect_weekly_goal "
                "WHERE week_key='2026-W46' AND goal_key='diligence'"
            ).fetchone()
        self.assertEqual((1, '{"u": 1}'), previous_week)
        self.assertEqual(((0, "{}"), ("recorded",), 0), self.linked_state())

    def test_corrupt_participants_are_preserved_and_pending_is_retryable(self):
        self.add_pending_settlement()
        with sqlite3.connect(self.database) as connection:
            connection.execute("UPDATE sect_weekly_goal SET participants='broken-json'")
        with self.assertRaises(DomainError) as caught:
            self.record_linked_event()
        self.assertEqual("sect_weekly_progress_data_invalid", caught.exception.code)
        self.assertEqual(((0, "broken-json"), ("pending",), 0), self.linked_state())

    def test_sqlite_abort_remains_visible_and_same_operation_recovers(self):
        self.add_pending_settlement()
        with sqlite3.connect(self.database) as connection:
            connection.execute("CREATE TABLE isolated_effect(name TEXT)")
            connection.execute(
                "CREATE TRIGGER abort_weekly_goal BEFORE UPDATE OF progress ON sect_weekly_goal "
                "WHEN OLD.goal_key='diligence' BEGIN "
                "INSERT INTO isolated_effect VALUES('goal'); SELECT RAISE(ABORT,'weekly write aborted'); END"
            )

        with self.assertRaisesRegex(sqlite3.IntegrityError, "weekly write aborted"):
            self.record_linked_event()
        self.assertEqual(((0, "{}"), ("pending",), 0), self.linked_state())

        with sqlite3.connect(self.database) as connection:
            connection.execute("DROP TRIGGER abort_weekly_goal")
        self.record_linked_event()
        self.assertEqual(((1, '{"u": 1}'), ("recorded",), 0), self.linked_state())

if __name__ == "__main__":
    unittest.main()
