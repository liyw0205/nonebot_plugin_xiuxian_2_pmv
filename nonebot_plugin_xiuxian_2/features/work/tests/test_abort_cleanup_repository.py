from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from ..abort_cleanup_application import WorkAbortCleanupApplication
from ..migrations import apply_work_abort_cleanup, apply_work_offer_snapshots
from ....infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class WorkAbortCleanupRepositoryTests(unittest.TestCase):
    @staticmethod
    def _seed_database(database: Path, offer: dict) -> None:
        with db_backend.transaction(database) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute(
                "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time,scheduled_time)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES('u',5000000)")
            conn.execute("INSERT INTO user_cd VALUES('u',2,'start','镇妖')")
        with DatabaseUnitOfWork(database) as uow:
            apply_work_offer_snapshots(uow)
            apply_work_abort_cleanup(uow)
            uow.execute(
                "INSERT INTO work_offer_snapshots VALUES(?,?,?)",
                ("u", json.dumps(offer), "old"),
            )
            uow.execute("INSERT INTO work_active_snapshots VALUES('u','{}','start')")

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.sqlite3"
        self._seed_database(self.database, self.offer)
        self.application = WorkAbortCleanupApplication(self.database)
        self.expected_cd = {"type": 2, "create_time": "start", "scheduled_time": "镇妖"}

    @property
    def offer(self):
        return {"tasks": {"镇妖": {"time": 5}}, "status": 2, "refresh_time": "old"}

    def tearDown(self) -> None:
        self.temp.cleanup()

    def cleanup(self, operation_id="cleanup", **overrides):
        values = {
            "user_id": "u",
            "reason": "active_abort",
            "expected_cd": self.expected_cd,
            "expected_offer": self.offer,
            "expected_stone": 5_000_000,
            "penalty": 4_000_000,
        }
        values.update(overrides)
        return self.application.cleanup(operation_id, **values)

    def state(self):
        with db_backend.connection(self.database) as conn:
            stone = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id='u'").fetchone()[0]
            cd = conn.execute(
                "SELECT type,create_time,scheduled_time FROM user_cd WHERE user_id='u'"
            ).fetchone()
            offers = conn.execute("SELECT COUNT(*) FROM work_offer_snapshots").fetchone()[0]
            active = conn.execute("SELECT COUNT(*) FROM work_active_snapshots").fetchone()[0]
        return int(stone), tuple(cd), int(offers), int(active)

    def test_active_abort_applies_capped_penalty_and_clears_snapshots_atomically(self):
        result = self.cleanup(penalty=8_000_000)

        self.assertEqual((result.status, result.penalty, result.stone_remaining), ("applied", 5_000_000, 0))
        self.assertEqual(self.state(), (0, (0, 0, None), 0, 0))

    def test_replay_conflict_and_stale_state_are_distinguished(self):
        first = self.cleanup("same")
        duplicate = self.cleanup("same")
        conflict = self.cleanup("same", penalty=1)
        stale = self.cleanup("stale", expected_stone=4_000_000)

        self.assertEqual(first.status, "applied")
        self.assertEqual((duplicate.status, duplicate.penalty, duplicate.stone_remaining), ("duplicate", 4_000_000, 1_000_000))
        self.assertEqual(conflict.status, "operation_conflict")
        self.assertEqual(stale.status, "state_changed")

    def test_legacy_projection_is_removed_after_applied_and_duplicate_cleanup(self):
        deleter = Mock()
        application = WorkAbortCleanupApplication(
            self.database, legacy_projection_deleter=deleter
        )
        self.application = application

        self.assertEqual(self.cleanup("projection-cleanup").status, "applied")
        self.assertEqual(self.cleanup("projection-cleanup").status, "duplicate")

        self.assertEqual(deleter.call_args_list, [(('u',), {}), (('u',), {})])

    def test_offer_abort_expiry_and_reset_do_not_charge_penalty(self):
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id='u'")
        idle_cd = {"type": 0, "create_time": 0, "scheduled_time": None}

        offer_abort = self.cleanup(
            "offer", reason="offer_abort", expected_cd=idle_cd, expected_stone=None, penalty=4_000_000
        )
        expired = self.cleanup(
            "expired", reason="expired", expected_cd=idle_cd, expected_offer=None,
            expected_stone=None, penalty=4_000_000,
        )
        reset = self.cleanup(
            "reset", reason="reset", expected_cd=idle_cd, expected_offer=None,
            expected_stone=None, penalty=4_000_000,
        )

        self.assertEqual((offer_abort.penalty, expired.penalty, reset.penalty), (0, 0, 0))
        self.assertEqual(self.state(), (5_000_000, (0, 0, None), 0, 0))

    def test_missing_startup_schema_is_not_created_by_request(self):
        database = Path(self.temp.name) / "unmigrated.sqlite3"
        with db_backend.transaction(database) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',5000000)")
            conn.execute("CREATE TABLE user_cd(user_id TEXT,type INTEGER,create_time,scheduled_time)")
            conn.execute("INSERT INTO user_cd VALUES('u',2,'start','镇妖')")

        result = WorkAbortCleanupApplication(database).cleanup(
            "missing", "u", "active_abort", self.expected_cd, self.offer, 5_000_000, 4_000_000
        )

        self.assertEqual(result.status, "schema_missing")
        with db_backend.connection(database) as conn:
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertNotIn("work_abort_cleanup_operations", tables)
        self.assertNotIn("work_offer_snapshots", tables)
        self.assertNotIn("work_active_snapshots", tables)

    def test_startup_migration_preserves_existing_cleanup_receipts(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO work_abort_cleanup_operations "
                "(operation_id,payload,reason,penalty,stone_remaining) VALUES(?,?,?,?,?)",
                ("historic", '["u","active_abort",{"create_time":"start","scheduled_time":"镇妖","type":2},{"refresh_time":"old","status":2,"tasks":{"镇妖":{"time":5}}},5000000,4000000]', "active_abort", 4_000_000, 1_000_000),
            )
            apply_work_abort_cleanup(uow)

        replay = self.cleanup("historic")
        self.assertEqual((replay.status, replay.penalty, replay.stone_remaining), ("duplicate", 4_000_000, 1_000_000))

    def test_late_operation_insert_failure_rolls_back_state(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TRIGGER fail_cleanup BEFORE INSERT ON work_abort_cleanup_operations "
                "BEGIN SELECT RAISE(ABORT,'failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.cleanup("failure")

        self.assertEqual(self.state(), (5_000_000, (2, "start", "镇妖"), 1, 1))

    def test_ignore_cleanup_writes_are_rejected_and_recoverable(self):
        cases = (
            (
                "stone",
                "CREATE TRIGGER reject_cleanup BEFORE UPDATE ON user_xiuxian "
                "BEGIN SELECT RAISE(IGNORE); END",
                "work_abort_cleanup_stone_write_rejected",
                "u",
            ),
            (
                "cooldown",
                "CREATE TRIGGER reject_cleanup BEFORE UPDATE ON user_cd "
                "BEGIN SELECT RAISE(IGNORE); END",
                "work_abort_cleanup_cooldown_write_rejected",
                "u",
            ),
            (
                "active_snapshot",
                "CREATE TRIGGER reject_cleanup BEFORE DELETE ON work_active_snapshots "
                "BEGIN SELECT RAISE(IGNORE); END",
                "work_abort_cleanup_active_snapshot_write_rejected",
                "u",
            ),
            (
                "offer_snapshot",
                "CREATE TRIGGER reject_cleanup BEFORE DELETE ON work_offer_snapshots "
                "BEGIN SELECT RAISE(IGNORE); END",
                "work_abort_cleanup_offer_snapshot_write_rejected",
                "u",
            ),
            (
                "receipt",
                "CREATE TRIGGER reject_cleanup BEFORE INSERT ON work_abort_cleanup_operations "
                "BEGIN SELECT RAISE(IGNORE); END",
                "work_abort_cleanup_receipt_write_rejected",
                "reject-receipt",
            ),
        )
        for stage, trigger, reason, subject in cases:
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as temp:
                self.database = Path(temp) / "case.sqlite3"
                self._seed_database(self.database, self.offer)
                self.application = WorkAbortCleanupApplication(self.database)
                with db_backend.transaction(self.database) as conn:
                    conn.execute(trigger)

                rejected = self.cleanup(f"reject-{stage}")
                self.assertEqual(
                    (rejected.status, rejected.reason, rejected.subject),
                    ("state_changed", reason, subject),
                )
                self.assertEqual(
                    self.state(), (5_000_000, (2, "start", "镇妖"), 1, 1)
                )

                with db_backend.transaction(self.database) as conn:
                    conn.execute("DROP TRIGGER reject_cleanup")
                self.assertEqual(self.cleanup(f"reject-{stage}").status, "applied")

    def test_after_rewrite_of_penalty_or_receipt_is_rejected(self):
        cases = (
            (
                "stone",
                "CREATE TRIGGER rewrite_cleanup AFTER UPDATE ON user_xiuxian "
                "BEGIN UPDATE user_xiuxian SET stone=123 WHERE user_id='u'; END",
                "work_abort_cleanup_stone_write_rejected",
                "u",
            ),
            (
                "receipt",
                "CREATE TRIGGER rewrite_cleanup AFTER INSERT ON work_abort_cleanup_operations "
                "BEGIN UPDATE work_abort_cleanup_operations SET penalty=1 "
                "WHERE operation_id='rewrite-receipt'; END",
                "work_abort_cleanup_receipt_write_rejected",
                "rewrite-receipt",
            ),
        )
        for stage, trigger, reason, subject in cases:
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as temp:
                self.database = Path(temp) / "case.sqlite3"
                self._seed_database(self.database, self.offer)
                self.application = WorkAbortCleanupApplication(self.database)
                with db_backend.transaction(self.database) as conn:
                    conn.execute(trigger)

                rejected = self.cleanup("rewrite-receipt" if stage == "receipt" else "rewrite-stone")
                self.assertEqual(
                    (rejected.status, rejected.reason, rejected.subject),
                    ("state_changed", reason, subject),
                )
                self.assertEqual(
                    self.state(), (5_000_000, (2, "start", "镇妖"), 1, 1)
                )

                with db_backend.transaction(self.database) as conn:
                    conn.execute("DROP TRIGGER rewrite_cleanup")
                self.assertEqual(
                    self.cleanup("rewrite-receipt" if stage == "receipt" else "rewrite-stone").status,
                    "applied",
                )


if __name__ == "__main__":
    unittest.main()
