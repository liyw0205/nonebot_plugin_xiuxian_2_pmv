from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ..migrations import apply_work_offer_snapshots, apply_work_refresh_operations
from ..refresh_application import WorkRefreshApplication
from ....infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class WorkRefreshRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT,work_num TEXT)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u','3')")
            conn.execute(
                "CREATE TABLE user_cd(user_id TEXT,type INTEGER,create_time,scheduled_time)"
            )
            conn.execute("INSERT INTO user_cd VALUES('u',0,'0',NULL)")
        with DatabaseUnitOfWork(self.database) as uow:
            apply_work_offer_snapshots(uow)
            apply_work_refresh_operations(uow)
        self.application = WorkRefreshApplication(self.database)
        self.expected_cd = {"type": 0, "create_time": "0", "scheduled_time": None}
        self.offer = {
            "tasks": {"采药": {"rate": 80, "award": 10, "time": 5, "item_id": 0}},
            "status": 1,
            "refresh_time": "2026-09-28 10:00:00",
            "user_level": "筑基",
        }

    def tearDown(self) -> None:
        self.temp.cleanup()

    def refresh(self, operation_id: str = "refresh", **overrides):
        values = {
            "user_id": "u",
            "expected_count": 3,
            "expected_cd": self.expected_cd,
            "expected_offer": None,
            "new_offer": self.offer,
            "force": False,
        }
        values.update(overrides)
        return self.application.refresh(operation_id=operation_id, **values)

    def test_refresh_commits_count_snapshot_and_operation_together(self) -> None:
        result = self.refresh()

        self.assertEqual((result.status, result.remaining_count, result.offer), ("applied", 2, self.offer))
        with db_backend.connection(self.database) as conn:
            count = conn.execute("SELECT work_num FROM user_xiuxian WHERE user_id='u'").fetchone()[0]
            snapshot = conn.execute(
                "SELECT snapshot FROM work_offer_snapshots WHERE user_id='u'"
            ).fetchone()[0]
            operations = conn.execute("SELECT COUNT(*) FROM work_refresh_operations").fetchone()[0]
        self.assertEqual(int(count), 2)
        self.assertEqual(json.loads(snapshot), self.offer)
        self.assertEqual(operations, 1)

    def test_replay_keeps_first_offer_and_request_identity(self) -> None:
        first = self.refresh("same")
        changed_offer = {**self.offer, "refresh_time": "later"}
        duplicate = self.refresh("same", new_offer=changed_offer)
        force_conflict = self.refresh("same", new_offer=changed_offer, force=True)
        replay = self.application.get_result("same")

        self.assertEqual((first.status, duplicate.status, force_conflict.status), ("applied", "duplicate", "operation_conflict"))
        self.assertEqual(duplicate.offer, self.offer)
        self.assertEqual(replay.status, "duplicate")
        self.assertEqual(replay.remaining_count, 2)

    def test_expiration_updates_only_the_matching_offer_and_is_idempotent(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO work_offer_snapshots VALUES(?,?,?)",
                ("u", json.dumps(self.offer), "created"),
            )

        expired = self.application.mark_offer_expired(
            user_id="u", expected_offer=self.offer, updated_at="expired"
        )
        replay = self.application.mark_offer_expired(
            user_id="u", expected_offer=self.offer, updated_at="later"
        )

        self.assertEqual((expired.status, expired.offer["status"]), ("applied", 0))
        self.assertEqual((replay.status, replay.offer["status"]), ("state_changed", 0))
        with db_backend.connection(self.database) as conn:
            stored = conn.execute(
                "SELECT snapshot,updated_at FROM work_offer_snapshots WHERE user_id='u'"
            ).fetchone()
        self.assertEqual(json.loads(stored[0])["status"], 0)
        self.assertEqual(stored[1], "expired")

    def test_expiration_conflict_does_not_replace_a_new_offer(self) -> None:
        current = {**self.offer, "refresh_time": "new"}
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO work_offer_snapshots VALUES(?,?,?)",
                ("u", json.dumps(current), "new"),
            )

        result = self.application.mark_offer_expired(
            user_id="u", expected_offer=self.offer, updated_at="expired"
        )

        self.assertEqual(result.status, "state_changed")
        self.assertEqual(result.offer, current)
        with db_backend.connection(self.database) as conn:
            stored = conn.execute(
                "SELECT snapshot FROM work_offer_snapshots WHERE user_id='u'"
            ).fetchone()[0]
        self.assertEqual(json.loads(stored), current)

    def test_expiration_write_failure_keeps_original_snapshot(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO work_offer_snapshots VALUES(?,?,?)",
                ("u", json.dumps(self.offer), "created"),
            )
            conn.execute(
                "CREATE TRIGGER fail_expiration BEFORE UPDATE ON work_offer_snapshots "
                "BEGIN SELECT RAISE(ABORT,'expiration failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.application.mark_offer_expired(
                user_id="u", expected_offer=self.offer, updated_at="expired"
            )

        with db_backend.connection(self.database) as conn:
            stored = conn.execute(
                "SELECT snapshot,updated_at FROM work_offer_snapshots WHERE user_id='u'"
            ).fetchone()
        self.assertEqual(json.loads(stored[0]), self.offer)
        self.assertEqual(stored[1], "created")

    def test_stale_state_and_unforced_offer_are_rejected(self) -> None:
        changed_cd = {**self.expected_cd, "create_time": "changed"}
        stale = self.refresh("stale", expected_cd=changed_cd)
        self.assertEqual(stale.status, "state_changed")
        self.assertEqual(self.refresh().status, "applied")
        existing_offer = self.refresh(
            "existing",
            expected_count=2,
            expected_offer=self.offer,
            new_offer={**self.offer, "refresh_time": "new"},
        )
        stale_offer = self.refresh(
            "stale-offer",
            expected_count=2,
            expected_offer={**self.offer, "refresh_time": "other"},
            new_offer={**self.offer, "refresh_time": "another"},
            force=True,
        )
        self.assertEqual((existing_offer.status, stale_offer.status), ("offer_exists", "state_changed"))

    def test_missing_startup_schema_is_not_created_by_request(self) -> None:
        database = Path(self.temp.name) / "unmigrated.sqlite3"
        with db_backend.transaction(database) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT,work_num INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',3)")
            conn.execute(
                "CREATE TABLE user_cd(user_id TEXT,type INTEGER,create_time,scheduled_time)"
            )
            conn.execute("INSERT INTO user_cd VALUES('u',0,'0',NULL)")

        result = WorkRefreshApplication(database).refresh(
            operation_id="unmigrated",
            user_id="u",
            expected_count=3,
            expected_cd=self.expected_cd,
            expected_offer=None,
            new_offer=self.offer,
        )
        expiration = WorkRefreshApplication(database).mark_offer_expired(
            user_id="u", expected_offer=self.offer, updated_at="expired"
        )

        self.assertEqual(result.status, "schema_missing")
        self.assertEqual(expiration.status, "schema_missing")
        with db_backend.connection(database) as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        self.assertNotIn("work_refresh_operations", tables)
        self.assertNotIn("work_offer_snapshots", tables)

    def test_startup_migration_preserves_existing_refresh_receipts(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO work_refresh_operations "
                "(operation_id,payload,remaining_count,offer_snapshot) VALUES(?,?,?,?)",
                ("historic", '["u",false]', 2, json.dumps(self.offer)),
            )
            apply_work_refresh_operations(uow)

        result = self.application.get_result("historic")
        self.assertEqual((result.status, result.remaining_count, result.offer), ("duplicate", 2, self.offer))

    def test_operation_insert_failure_rolls_back_refresh_state(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_refresh BEFORE INSERT ON work_refresh_operations "
                "BEGIN SELECT RAISE(ABORT,'failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.refresh("failed")

        with db_backend.connection(self.database) as conn:
            count = conn.execute("SELECT work_num FROM user_xiuxian WHERE user_id='u'").fetchone()[0]
            snapshots = conn.execute("SELECT COUNT(*) FROM work_offer_snapshots").fetchone()[0]
            operations = conn.execute("SELECT COUNT(*) FROM work_refresh_operations").fetchone()[0]
        self.assertEqual((int(count), snapshots, operations), (3, 0, 0))


if __name__ == "__main__":
    unittest.main()
