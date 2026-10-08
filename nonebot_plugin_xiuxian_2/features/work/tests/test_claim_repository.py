import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ..claim_repository import WorkClaimSqlRepository
from ..migrations import (
    apply_work_abort_cleanup,
    apply_work_claim_operations,
    apply_work_offer_snapshots,
)
from ....infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class WorkClaimRepositoryTests(unittest.TestCase):
    def test_claim_uses_display_order_after_sorted_snapshot_round_trip(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,work_num INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',3)")
                conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
                conn.execute("INSERT INTO user_cd VALUES('u',0,'0',NULL)")
            with DatabaseUnitOfWork(db) as uow:
                apply_work_abort_cleanup(uow)
                apply_work_offer_snapshots(uow)
                apply_work_claim_operations(uow)

            offer = {
                "tasks": {
                    "采药": {"time": 5},
                    "镇妖": {"time": 8},
                    "炼器": {"time": 12},
                },
                "task_order": ["镇妖", "炼器", "采药"],
                "status": 1,
            }
            # Refresh persists snapshots with sort_keys=True, which reorders tasks.
            persisted_offer = json.loads(json.dumps(offer, sort_keys=True))
            self.assertNotEqual(list(persisted_offer["tasks"]), offer["task_order"])

            result = WorkClaimSqlRepository(db).claim(
                "ordered-claim", "u", 3, persisted_offer, 1, "started"
            )

            self.assertEqual(result.task_name, "镇妖")
            active = WorkClaimSqlRepository(db).get_active_snapshot("u")
            self.assertEqual(active["scheduled_time"], "镇妖")

    def test_applied_duplicate_and_state_changed(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,work_num INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',3)")
                conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
                conn.execute("INSERT INTO user_cd VALUES('u',0,'0',NULL)")
            with DatabaseUnitOfWork(db) as uow:
                apply_work_abort_cleanup(uow)
                apply_work_offer_snapshots(uow)
                apply_work_claim_operations(uow)
            repo = WorkClaimSqlRepository(db)
            offer = {
                "tasks": {"采药": {"time": 5}}, "task_order": ["采药"],
                "status": 1, "refresh_time": "2026-01-01 00:00:00", "user_level": "筑基",
            }
            first = repo.claim("c1", "u", 3, offer, 1, "2026-01-01 00:00:00")
            duplicate = repo.claim("c1", "u", 99, offer, 1, "2099")
            stale = repo.claim("c2", "u", 3, offer, 1, "2026-01-01 00:00:00")
            self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))
            active = repo.get_active_snapshot("u")
            self.assertEqual(active["status"], 2)
            self.assertEqual(active["scheduled_time"], "采药")
            self.assertEqual(active["create_time"], "2026-01-01 00:00:00")
            with db_backend.connection(db) as conn:
                projection = json.loads(
                    conn.execute(
                        "SELECT snapshot FROM work_offer_snapshots WHERE user_id='u'"
                    ).fetchone()[0]
                )
            self.assertEqual(projection["status"], 2)
            self.assertEqual(projection["task_order"], ["采药"])

    def test_offer_projection_failure_rolls_back_the_entire_claim(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,work_num INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',3)")
                conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
                conn.execute("INSERT INTO user_cd VALUES('u',0,'0',NULL)")
            with DatabaseUnitOfWork(db) as uow:
                apply_work_abort_cleanup(uow)
                apply_work_offer_snapshots(uow)
                apply_work_claim_operations(uow)
                uow.execute(
                    "CREATE TRIGGER reject_offer_snapshot BEFORE INSERT ON work_offer_snapshots "
                    "BEGIN SELECT RAISE(ABORT,'projection unavailable'); END"
                )

            with self.assertRaises(sqlite3.IntegrityError):
                WorkClaimSqlRepository(db).claim(
                    "c1", "u", 3, {"tasks": {"采药": {"time": 5}}}, 1, "start"
                )

            with db_backend.connection(db) as conn:
                self.assertEqual(
                    conn.execute("SELECT type FROM user_cd WHERE user_id='u'").fetchone()[0],
                    0,
                )
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM work_active_snapshots").fetchone()[0],
                    0,
                )
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM work_claim_operations").fetchone()[0],
                    0,
                )

    def test_missing_startup_schema_is_not_created_by_request(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,work_num INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',3)")
                conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
                conn.execute("INSERT INTO user_cd VALUES('u',0,'0',NULL)")

            result = WorkClaimSqlRepository(db).claim(
                "missing", "u", 3, {"tasks": {"采药": {"time": 5}}}, 1, "now"
            )

            self.assertEqual(result.status, "schema_missing")
            with db_backend.connection(db) as conn:
                tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn("work_claim_operations", tables)
            self.assertNotIn("work_active_snapshots", tables)

    def test_active_snapshot_read_is_read_only_and_legacy_missing_schema_is_absent(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE unrelated(value TEXT)")
            repo = WorkClaimSqlRepository(db)

            self.assertIsNone(repo.get_active_snapshot("u"))
            with db_backend.connection(db) as conn:
                tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertEqual(tables, {"unrelated"})

    def test_legacy_offer_snapshot_is_completed_from_active_cooldown(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
                conn.execute("INSERT INTO user_cd VALUES('u',2,'started','采药')")
                conn.execute("CREATE TABLE work_active_snapshots(user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)")
                conn.execute(
                    "INSERT INTO work_active_snapshots VALUES('u',%s,'started')",
                    (json.dumps({"tasks": {"采药": {"time": 5}}, "status": 1}),),
                )

            active = WorkClaimSqlRepository(db).get_active_snapshot("u")

        self.assertEqual(active["status"], 2)
        self.assertEqual(active["scheduled_time"], "采药")
        self.assertEqual(active["create_time"], "started")

    def test_startup_migration_preserves_existing_claim_receipt(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE work_claim_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,task_name TEXT NOT NULL,started_at TEXT NOT NULL,remaining_count INTEGER NOT NULL,created_at TEXT)")
                conn.execute("INSERT INTO work_claim_operations VALUES('historic','[\"u\",1]','采药','old',3,'old')")
                conn.execute("CREATE TABLE work_active_snapshots(user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)")
            with DatabaseUnitOfWork(db) as uow:
                apply_work_abort_cleanup(uow)
                apply_work_offer_snapshots(uow)
                apply_work_claim_operations(uow)

            replay = WorkClaimSqlRepository(db).claim(
                "historic", "u", 99, {"tasks": {"采药": {"time": 5}}}, 1, "new"
            )

            self.assertEqual((replay.status, replay.task_name, replay.started_at, replay.remaining_count), ("duplicate", "采药", "old", 3))
