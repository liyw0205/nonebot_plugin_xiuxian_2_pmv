from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

from ..status_application import WorkStatusApplication
from ..refresh_repository import WorkRefreshResult
from ..status_repository import WorkStatusState
from ....infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class WorkStatusApplicationTests(unittest.TestCase):
    @staticmethod
    def _database(path: Path, *, offer: dict | None = None, cooldown: tuple | None = None):
        with db_backend.transaction(path) as conn:
            conn.execute(
                "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)"
            )
            conn.execute(
                "CREATE TABLE work_offer_snapshots(user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)"
            )
            if cooldown is not None:
                conn.execute("INSERT INTO user_cd VALUES(?,?,?,?)", cooldown)
            if offer is not None:
                conn.execute(
                    "INSERT INTO work_offer_snapshots VALUES(?,?,?)",
                    ("u", json.dumps(offer, ensure_ascii=False), "updated"),
                )

    def test_sql_offer_wins_over_legacy_json_and_classifies_status(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            sql_offer = {
                "tasks": {"SQL task": {"time": 5}},
                "status": 1,
                "refresh_time": "2026-10-08 09:50:00",
            }
            self._database(db, offer=sql_offer, cooldown=("u", 0, "0", None))
            reader = Mock(return_value={"status": 0})
            app = WorkStatusApplication(db, legacy_offer_reader=reader)

            status, offer = app.get_user_work_status(
                "u", now=datetime(2026, 10, 8, 10, 0, 0)
            )

        self.assertEqual(status, 3)
        self.assertEqual(offer, sql_offer)
        reader.assert_not_called()

    def test_get_offer_prefers_sql_snapshot(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            sql_offer = {"status": 2, "tasks": {"SQL task": {"time": 5}}}
            self._database(db, offer=sql_offer)
            reader = Mock(return_value={"status": 1, "tasks": {"legacy": {}}})
            app = WorkStatusApplication(db, legacy_offer_reader=reader)

            offer = app.get_offer("u")

        self.assertEqual(offer, sql_offer)
        reader.assert_not_called()

    def test_get_offer_falls_back_to_legacy_json_when_sql_row_is_missing(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            self._database(db)
            legacy_offer = {"status": 1, "tasks": {"legacy": {}}}
            reader = Mock(return_value=legacy_offer)
            app = WorkStatusApplication(db, legacy_offer_reader=reader)

            offer = app.get_offer("u")

        self.assertEqual(offer, legacy_offer)
        reader.assert_called_once_with("u")

    def test_get_offer_falls_back_without_creating_feature_schema(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE unrelated(value TEXT)")
            legacy_offer = {"status": 1, "tasks": {"legacy": {}}}
            reader = Mock(return_value=legacy_offer)
            app = WorkStatusApplication(db, legacy_offer_reader=reader)

            offer = app.get_offer("u")

            with db_backend.connection(db) as conn:
                tables = {
                    row[0]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }

        self.assertEqual(offer, legacy_offer)
        self.assertEqual(tables, {"unrelated"})
        reader.assert_called_once_with("u")

    def test_active_cooldown_status_uses_sql_active_snapshot(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            self._database(db, cooldown=("u", 2, "2026-10-08 09:58:00", "SQL task"))
            with DatabaseUnitOfWork(db) as uow:
                uow.execute(
                    "CREATE TABLE work_active_snapshots(user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)"
                )
                uow.execute(
                    "INSERT INTO work_active_snapshots VALUES(?,?,?)",
                    ("u", json.dumps({"tasks": {"SQL task": {"time": 10}}}), "start"),
                )
            reader = Mock(side_effect=AssertionError("legacy JSON read used"))
            app = WorkStatusApplication(db, legacy_offer_reader=reader)

            status, work = app.get_user_work_status(
                "u", now=datetime(2026, 10, 8, 10, 0, 0)
            )

        self.assertEqual(status, 1)
        self.assertEqual(work["scheduled_time"], "SQL task")
        reader.assert_not_called()

    def test_completed_active_cooldown_returns_settleable_status(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            self._database(db, cooldown=("u", 2, "2026-10-08 09:58:00", "SQL task"))
            with DatabaseUnitOfWork(db) as uow:
                uow.execute(
                    "CREATE TABLE work_active_snapshots(user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)"
                )
                uow.execute(
                    "INSERT INTO work_active_snapshots VALUES(?,?,?)",
                    ("u", json.dumps({"tasks": {"SQL task": {"time": 1}}}), "start"),
                )

            status, work = WorkStatusApplication(db).get_user_work_status(
                "u", now=datetime(2026, 10, 8, 10, 0, 0)
            )

        self.assertEqual(status, 2)
        self.assertEqual(work["scheduled_time"], "SQL task")

    def test_no_sql_or_legacy_offer_returns_status_zero(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE unrelated(value TEXT)")

            status, offer = WorkStatusApplication(db).get_user_work_status("u")

        self.assertEqual((status, offer), (0, None))

    def test_expired_sql_offer_uses_cas_then_projects_legacy_json(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            offer = {
                "tasks": {"SQL task": {"time": 5}},
                "status": 1,
                "refresh_time": "2026-10-08 09:00:00",
            }
            self._database(db, offer=offer, cooldown=("u", 0, "0", None))
            writer = Mock()
            app = WorkStatusApplication(db, legacy_projection_writer=writer)

            status, current = app.get_user_work_status(
                "u", now=datetime(2026, 10, 8, 10, 0, 0)
            )

            with db_backend.connection(db) as conn:
                persisted = json.loads(
                    conn.execute(
                        "SELECT snapshot FROM work_offer_snapshots WHERE user_id='u'"
                    ).fetchone()[0]
                )

        self.assertEqual(status, 4)
        self.assertEqual(current["status"], 0)
        self.assertEqual(persisted["status"], 0)
        writer.assert_called_once_with("u", persisted)

    def test_expiration_cas_missing_preserves_expired_status_contract(self):
        offer = {
            "tasks": {"SQL task": {"time": 5}},
            "status": 1,
            "refresh_time": "2026-10-08 09:00:00",
        }

        class Repository:
            def get_state(self, user_id):
                return WorkStatusState(offer=offer, has_sql_offer=True)

            def mark_offer_expired(self, user_id, expected_offer, updated_at):
                return WorkRefreshResult("missing")

        writer = Mock()
        app = WorkStatusApplication(
            "unused.db", repository=Repository(), legacy_projection_writer=writer
        )

        status, current = app.get_user_work_status(
            "u", now=datetime(2026, 10, 8, 10, 0, 0)
        )

        self.assertEqual((status, current["status"]), (4, 0))
        writer.assert_called_once_with("u", current)

    def test_expiration_cas_conflict_projects_the_new_current_offer(self):
        stale = {
            "tasks": {"Old task": {"time": 5}},
            "status": 1,
            "refresh_time": "2026-10-08 09:00:00",
        }
        current = {
            "tasks": {"New task": {"time": 8}},
            "status": 1,
            "refresh_time": "2026-10-08 09:59:00",
        }

        class Repository:
            def get_state(self, user_id):
                return WorkStatusState(offer=stale, has_sql_offer=True)

            def mark_offer_expired(self, user_id, expected_offer, updated_at):
                return WorkRefreshResult("state_changed", offer=current)

        writer = Mock()
        app = WorkStatusApplication(
            "unused.db", repository=Repository(), legacy_projection_writer=writer
        )

        status, result = app.get_user_work_status(
            "u", now=datetime(2026, 10, 8, 10, 0, 0)
        )

        self.assertEqual((status, result), (3, current))
        writer.assert_called_once_with("u", current)

    def test_historical_json_offer_is_expired_only_through_compatibility_adapter(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            self._database(db, cooldown=("u", 0, "0", None))
            historical = {
                "tasks": {"Old task": {"time": 5}},
                "status": 1,
                "refresh_time": "2026-10-08 09:00:00",
            }
            reader = Mock(return_value=historical)
            writer = Mock()
            app = WorkStatusApplication(
                db,
                legacy_offer_reader=reader,
                legacy_projection_writer=writer,
            )

            status, current = app.get_user_work_status(
                "u", now=datetime(2026, 10, 8, 10, 0, 0)
            )

            with db_backend.connection(db) as conn:
                persisted = conn.execute(
                    "SELECT snapshot FROM work_offer_snapshots"
                ).fetchall()

        self.assertEqual((status, current["status"]), (4, 0))
        reader.assert_called_once_with("u")
        writer.assert_called_once_with("u", current)
        self.assertEqual(persisted, [])

    def test_invalid_legacy_refresh_time_is_status_four_without_projection(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE unrelated(value TEXT)")
            historical = {"tasks": {}, "status": 1}
            writer = Mock()
            app = WorkStatusApplication(
                db,
                legacy_offer_reader=Mock(return_value=historical),
                legacy_projection_writer=writer,
            )

            status, offer = app.get_user_work_status(
                "u", now=datetime(2026, 10, 8, 10, 0, 0)
            )

        self.assertEqual((status, offer), (4, historical))
        writer.assert_not_called()

    def test_missing_feature_schema_uses_legacy_reader_without_request_ddl(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE unrelated(value TEXT)")
            historical = {
                "tasks": {"Old task": {"time": 5}},
                "status": 1,
                "refresh_time": "2026-10-08 09:50:00",
            }
            reader = Mock(return_value=historical)
            app = WorkStatusApplication(db, legacy_offer_reader=reader)

            status, offer = app.get_user_work_status(
                "u", now=datetime(2026, 10, 8, 10, 0, 0)
            )

            with db_backend.connection(db) as conn:
                tables = {
                    row[0]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }

        self.assertEqual((status, offer), (3, historical))
        self.assertEqual(tables, {"unrelated"})
        reader.assert_called_once_with("u")


if __name__ == "__main__":
    unittest.main()
