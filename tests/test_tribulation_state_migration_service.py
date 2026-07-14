import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base.tribulation_state_migration_service import (
    TribulationStateMigrationService,
)
from tests.test_db_backend import db_backend


class TribulationStateMigrationServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.database = Path(self.tmp.name) / "game.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_tribulation("
                "user_id TEXT PRIMARY KEY,current_rate INTEGER,"
                "heart_devil_count INTEGER,last_time TEXT,next_level TEXT)"
            )
        self.service = TribulationStateMigrationService(self.database)

    def tearDown(self):
        self.tmp.cleanup()

    def test_import_is_atomic_and_idempotent(self):
        legacy = {
            "current_rate": "55",
            "heart_devil_count": "2",
            "last_time": "2026-07-14 10:00:00.000000",
            "next_level": "化神境初期",
        }
        first = self.service.migrate("migration:u", "u", legacy)
        replay = self.service.migrate("migration:u", "u", legacy)

        self.assertEqual("applied", first.status)
        self.assertEqual("duplicate", replay.status)
        self.assertEqual(first.state, replay.state)
        with db_backend.connection(self.database) as conn:
            row = conn.execute(
                "SELECT current_rate,heart_devil_count,last_time,next_level "
                "FROM user_tribulation WHERE user_id='u'"
            ).fetchone()
            self.assertEqual((55, 2, legacy["last_time"], "化神境初期"), tuple(row))
            count = conn.execute(
                "SELECT COUNT(*) FROM tribulation_state_migration_operations"
            ).fetchone()[0]
            self.assertEqual(1, count)

    def test_database_state_is_never_overwritten(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO user_tribulation VALUES('u',70,4,'database-time','数据库境界')"
            )

        result = self.service.migrate(
            "migration:u", "u", {"current_rate": 35, "heart_devil_count": 1}
        )

        self.assertEqual("database_authoritative", result.status)
        self.assertEqual(70, result.state["current_rate"])
        with db_backend.connection(self.database) as conn:
            row = conn.execute(
                "SELECT current_rate,heart_devil_count,last_time,next_level "
                "FROM user_tribulation WHERE user_id='u'"
            ).fetchone()
            self.assertEqual((70, 4, "database-time", "数据库境界"), tuple(row))

    def test_null_database_fields_use_legacy_reader_defaults(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO user_tribulation VALUES('u',NULL,NULL,NULL,NULL)"
            )

        result = self.service.migrate(
            "migration:u", "u", {"current_rate": 80}, base_rate=35
        )

        self.assertEqual("database_authoritative", result.status)
        self.assertEqual(
            {
                "current_rate": 35,
                "heart_devil_count": 0,
                "last_time": None,
                "next_level": None,
            },
            result.state,
        )

    def test_changed_replay_conflicts_without_overwrite(self):
        self.service.migrate("migration:u", "u", {"current_rate": 45})

        result = self.service.migrate(
            "migration:u", "u", {"current_rate": 80}
        )

        self.assertEqual("operation_conflict", result.status)
        self.assertEqual(45, result.state["current_rate"])
        with db_backend.connection(self.database) as conn:
            rate = conn.execute(
                "SELECT current_rate FROM user_tribulation WHERE user_id='u'"
            ).fetchone()[0]
            self.assertEqual(45, rate)

    def test_operation_failure_rolls_back_imported_state(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE tribulation_state_migration_operations("
                "operation_id TEXT PRIMARY KEY,user_id TEXT UNIQUE,payload TEXT)"
            )
            conn.execute(
                "CREATE TRIGGER fail_migration_operation BEFORE INSERT ON "
                "tribulation_state_migration_operations BEGIN "
                "SELECT RAISE(ABORT,'operation failed'); END"
            )

        with self.assertRaises(Exception):
            self.service.migrate("migration:u", "u", {"current_rate": 50})

        with db_backend.connection(self.database) as conn:
            row = conn.execute(
                "SELECT 1 FROM user_tribulation WHERE user_id='u'"
            ).fetchone()
            self.assertIsNone(row)


if __name__ == "__main__":
    unittest.main()
