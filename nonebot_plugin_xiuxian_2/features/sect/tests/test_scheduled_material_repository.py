from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..application import SectApplication
from ..migrations import apply_sect_scheduled_materials
from ..scheduled_material_repository import SectScheduledMaterialSqlRepository
from tests.test_db_backend import db_backend


class SectScheduledMaterialRepositoryTests(unittest.TestCase):
    def test_startup_migration_preserves_existing_grant_receipts(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "sect.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE sect_scheduled_material_grants("
                    "grant_key TEXT NOT NULL,sect_id INTEGER NOT NULL,materials INTEGER NOT NULL,"
                    "combat_power INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
                    "PRIMARY KEY(grant_key,sect_id))"
                )
                uow.execute(
                    "INSERT INTO sect_scheduled_material_grants "
                    "(grant_key,sect_id,materials,combat_power) VALUES('grant:old',1,50,100)"
                )
                apply_sect_scheduled_materials(uow)
                apply_sect_scheduled_materials(uow)
                receipt = uow.query_one(
                    "SELECT grant_key,materials,combat_power "
                    "FROM sect_scheduled_material_grants WHERE sect_id=1"
                )
            self.assertEqual(
                {"grant_key": "grant:old", "materials": 50, "combat_power": 100},
                receipt,
            )

    def test_grant_is_idempotent_and_updates_power(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "sect.db"
            with db_backend.transaction(database) as connection:
                connection.execute(
                    "CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_scale INTEGER,"
                    "sect_owner TEXT,sect_materials INTEGER,combat_power INTEGER)"
                )
                connection.execute("INSERT INTO sects VALUES(1,120,'owner',50,0)")
                connection.execute(
                    "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,power INTEGER)"
                )
                connection.execute("INSERT INTO user_xiuxian VALUES('owner',1,300)")
            with DatabaseUnitOfWork(database) as uow:
                apply_sect_scheduled_materials(uow)

            application = SectApplication(database)
            first = application.grant_scheduled_materials("grant:1", 1, 2)
            replay = application.grant_scheduled_materials("grant:1", 1, 99)
            self.assertTrue(first.applied)
            self.assertEqual(("granted", "duplicate"), (first.status, replay.status))
            with db_backend.connection(database) as connection:
                row = connection.execute(
                    "SELECT sect_materials,combat_power FROM sects WHERE sect_id=1"
                ).fetchone()
            self.assertEqual((290, 300), tuple(row))

    def test_list_targets_matches_legacy_filter_and_order(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "sect.db"
            with db_backend.transaction(database) as connection:
                connection.execute(
                    "CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_scale INTEGER,"
                    "sect_owner TEXT,elixir_room_level INTEGER)"
                )
                connection.executemany(
                    "INSERT INTO sects VALUES(?,?,?,?)",
                    [(1, 10, "a", 1), (2, 30, "b", 2), (3, 99, None, 3)],
                )
            self.assertEqual(
                [(2, 30, 2), (1, 10, 1)],
                SectScheduledMaterialSqlRepository(database).list_targets(),
            )

    def test_missing_receipt_migration_returns_schema_missing_without_ddl(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "sect.db"
            with db_backend.transaction(database) as connection:
                connection.execute(
                    "CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_scale INTEGER,"
                    "sect_owner TEXT,sect_materials INTEGER,combat_power INTEGER)"
                )
                connection.execute("INSERT INTO sects VALUES(1,120,'owner',50,0)")
            result = SectScheduledMaterialSqlRepository(database).grant("grant:1", 1, 2)
            self.assertEqual("schema_missing", result["status"])
            with db_backend.connection(database) as connection:
                table = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE name='sect_scheduled_material_grants'"
                ).fetchone()
            self.assertIsNone(table)


if __name__ == "__main__":
    unittest.main()
