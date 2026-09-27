from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..application import SectApplication
from ..fairyland_repository import SectFairylandSqlRepository
from ..migrations import apply_sect_fairyland_upgrade


class SectFairylandRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect.db"
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian ("
                "user_id TEXT PRIMARY KEY, sect_id INTEGER, sect_position INTEGER)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES ('owner', 1, 0)")
            conn.execute(
                "CREATE TABLE sects (sect_id INTEGER PRIMARY KEY, sect_owner TEXT, "
                "sect_fairyland INTEGER, sect_used_stone INTEGER, sect_materials INTEGER)"
            )
            conn.execute("INSERT INTO sects VALUES (1, 'owner', 1, 1000, 2000)")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def migrate(self) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            apply_sect_fairyland_upgrade(uow)

    def row(self) -> tuple[int, int, int]:
        with sqlite3.connect(self.database) as conn:
            return tuple(
                conn.execute(
                    "SELECT sect_fairyland, sect_used_stone, sect_materials FROM sects WHERE sect_id=1"
                ).fetchone()
            )

    def test_request_without_migration_does_not_create_schema(self) -> None:
        result = SectFairylandSqlRepository(self.database).upgrade(
            "op", "owner", 1, 1, 2, 100, 200
        )

        self.assertEqual(result["status"], "schema_missing")
        with sqlite3.connect(self.database) as conn:
            self.assertIsNone(
                conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sect_fairyland_operations'"
                ).fetchone()
            )
        self.assertEqual(self.row(), (1, 1000, 2000))

    def test_upgrade_replays_and_rolls_assets_atomically(self) -> None:
        self.migrate()
        repo = SectFairylandSqlRepository(self.database)

        first = repo.upgrade("op", "owner", 1, 1, 2, 100, 200)
        duplicate = repo.upgrade("op", "owner", 1, 1, 2, 100, 200)

        self.assertEqual((first["status"], duplicate["status"]), ("upgraded", "duplicate"))
        self.assertEqual(self.row(), (2, 900, 1800))

    def test_application_marks_upgrade_and_replay_as_applied(self) -> None:
        self.migrate()
        application = SectApplication(self.database)

        result = application.upgrade_fairyland("op", "owner", 1, 1, 2, 100, 200)
        replay = application.upgrade_fairyland("op", "owner", 1, 1, 2, 100, 200)

        self.assertEqual(result.status, "upgraded")
        self.assertTrue(result.applied)
        self.assertEqual(replay.status, "duplicate")
        self.assertTrue(replay.applied)
        self.assertEqual(self.row(), (2, 900, 1800))

    def test_level_must_advance_exactly_one_step(self) -> None:
        self.migrate()
        result = SectFairylandSqlRepository(self.database).upgrade(
            "skip-level", "owner", 1, 1, 3, 100, 200
        )

        self.assertEqual(result["status"], "level_changed")
        self.assertEqual(self.row(), (1, 1000, 2000))

    def test_permission_level_and_balance_rejections_keep_state(self) -> None:
        self.migrate()
        with sqlite3.connect(self.database) as conn:
            conn.execute("INSERT INTO user_xiuxian VALUES ('member', 1, 3)")
        repo = SectFairylandSqlRepository(self.database)

        self.assertEqual(repo.upgrade("member", "member", 1, 1, 2, 100, 200)["status"], "not_owner")
        self.assertEqual(repo.upgrade("old-level", "owner", 1, 0, 1, 100, 200)["status"], "level_changed")
        self.assertEqual(repo.upgrade("stone", "owner", 1, 1, 2, 1001, 200)["status"], "stone_insufficient")
        self.assertEqual(repo.upgrade("materials", "owner", 1, 1, 2, 100, 2001)["status"], "materials_insufficient")
        self.assertEqual(self.row(), (1, 1000, 2000))

    def test_migration_preserves_existing_operation_receipts(self) -> None:
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                "CREATE TABLE sect_fairyland_operations("
                "operation_id TEXT PRIMARY KEY,actor_id TEXT NOT NULL,sect_id INTEGER NOT NULL,"
                "from_level INTEGER NOT NULL,to_level INTEGER NOT NULL,stone_cost INTEGER NOT NULL,"
                "materials_cost INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            conn.execute(
                "INSERT INTO sect_fairyland_operations"
                "(operation_id,actor_id,sect_id,from_level,to_level,stone_cost,materials_cost) "
                "VALUES('legacy','owner',1,1,2,100,200)"
            )

        self.migrate()
        result = SectFairylandSqlRepository(self.database).upgrade(
            "legacy", "owner", 1, 1, 2, 100, 200
        )

        self.assertEqual(result["status"], "duplicate")
        self.assertEqual(self.row(), (1, 1000, 2000))

    def test_database_failure_rolls_back_all_fields(self) -> None:
        self.migrate()
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_operation BEFORE INSERT ON sect_fairyland_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(sqlite3.IntegrityError):
            SectFairylandSqlRepository(self.database).upgrade("fail", "owner", 1, 1, 2, 100, 200)
        self.assertEqual(self.row(), (1, 1000, 2000))


if __name__ == "__main__":
    unittest.main()
