import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..level_repository import AdminLevelChangeSqlRepository
from ..migrations import apply_admin_realm_changes
from ..root_repository import AdminRootChangeSqlRepository


class AdminRealmChangeRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "CREATE TABLE user_xiuxian("
                "user_id TEXT PRIMARY KEY,level TEXT,exp INTEGER,hp INTEGER,mp INTEGER,"
                "atk INTEGER,power INTEGER,root_type TEXT,root_level INTEGER,root TEXT,user_name TEXT)"
            )
            connection.execute(
                "INSERT INTO user_xiuxian VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                ("u", "练气境初期", 6000, 3000, 6000, 600, 22800, "混沌灵根", 0, "混沌灵根", "青云"),
            )

    def tearDown(self):
        self.temp.cleanup()

    def migrate(self):
        with DatabaseUnitOfWork(self.database) as uow:
            apply_admin_realm_changes(uow)

    def test_level_and_root_change_replay_and_snapshot_conflict(self):
        self.migrate()
        level = AdminLevelChangeSqlRepository(self.database)
        original_level = ("练气境初期", 6000, 3000, 6000, 600, 22800, "混沌灵根", 0)
        applied = level.change("level-op", "admin", "u", original_level, "练气境圆满", 10000, 2.6, 1.9)
        self.assertEqual(applied.status, "applied")
        self.assertEqual(level.change("level-op", "admin", "u", original_level, "练气境圆满", 10000, 2.6, 1.9).status, "duplicate")
        self.assertEqual(level.change("stale-level", "admin", "u", original_level, "练气境圆满", 10000, 2.6, 1.9).status, "state_changed")

        root = AdminRootChangeSqlRepository(self.database)
        expected_root = ("混沌灵根", "混沌灵根", 0, "练气境圆满", 10000, applied.power, "青云")
        changed = root.change("root-op", "admin", "u", expected_root, 8, 2.6, 7.0)
        self.assertEqual(changed.status, "applied")
        self.assertEqual(changed.root_type, "永恒道果")
        self.assertEqual(root.change("root-op", "admin", "u", expected_root, 8, 2.6, 7.0).status, "duplicate")

    def test_missing_startup_migration_fails_closed_without_request_ddl(self):
        original = ("练气境初期", 6000, 3000, 6000, 600, 22800, "混沌灵根", 0)
        level = AdminLevelChangeSqlRepository(self.database).change(
            "level-op", "admin", "u", original, "练气境圆满", 10000, 2.6, 1.9
        )
        root = AdminRootChangeSqlRepository(self.database).change(
            "root-op", "admin", "u",
            ("混沌灵根", "混沌灵根", 0, "练气境初期", 6000, 22800, "青云"), 8, 2.6, 7.0,
        )
        self.assertEqual((level.status, root.status), ("schema_missing", "schema_missing"))
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(
                connection.execute("SELECT level,exp,power FROM user_xiuxian WHERE user_id='u'").fetchone(),
                ("练气境初期", 6000, 22800),
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
                    "AND name IN ('admin_level_change_operations','admin_root_change_operations')"
                ).fetchone()[0],
                0,
            )

    def test_startup_migration_preserves_receipts_from_legacy_request_ddl(self):
        with sqlite3.connect(self.database) as connection:
            for table in ("admin_level_change_operations", "admin_root_change_operations"):
                connection.execute(
                    f"CREATE TABLE {table}(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                    "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                connection.execute(
                    f"INSERT INTO {table}(operation_id,payload,result_json) VALUES(?,?,?)",
                    ("prior-op", "prior-payload", "{}"),
                )
        self.migrate()
        with sqlite3.connect(self.database) as connection:
            for table in ("admin_level_change_operations", "admin_root_change_operations"):
                self.assertEqual(
                    connection.execute(f"SELECT operation_id,payload,result_json FROM {table}").fetchone(),
                    ("prior-op", "prior-payload", "{}"),
                )

    def test_missing_database_is_not_created(self):
        missing = Path(self.temp.name) / "missing.db"
        level = AdminLevelChangeSqlRepository(missing).change(
            "level-op", "admin", "u", ("L", 1, 1, 1, 1, 1, "R", 0), "L2", 2, 1.0, 1.0
        )
        root = AdminRootChangeSqlRepository(missing).change(
            "root-op", "admin", "u", ("R", "R", 0, "L", 1, 1, "name"), 1, 1.0, 1.0
        )
        self.assertEqual((level.status, root.status), ("schema_missing", "schema_missing"))
        self.assertFalse(missing.exists())

    def test_late_receipt_failure_rolls_back_level_fields(self):
        self.migrate()
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "CREATE TRIGGER fail_level_receipt BEFORE INSERT ON admin_level_change_operations "
                "BEGIN SELECT RAISE(ABORT,'receipt failure'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            AdminLevelChangeSqlRepository(self.database).change(
                "level-op", "admin", "u",
                ("练气境初期", 6000, 3000, 6000, 600, 22800, "混沌灵根", 0),
                "练气境圆满", 10000, 2.6, 1.9,
            )
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(
                connection.execute("SELECT level,exp,hp,mp,atk,power FROM user_xiuxian WHERE user_id='u'").fetchone(),
                ("练气境初期", 6000, 3000, 6000, 600, 22800),
            )
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM admin_level_change_operations").fetchone()[0], 0)

    def test_late_receipt_failure_rolls_back_root_fields(self):
        self.migrate()
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "CREATE TRIGGER fail_root_receipt BEFORE INSERT ON admin_root_change_operations "
                "BEGIN SELECT RAISE(ABORT,'receipt failure'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            AdminRootChangeSqlRepository(self.database).change(
                "root-op", "admin", "u",
                ("混沌灵根", "混沌灵根", 0, "练气境初期", 6000, 22800, "青云"), 8, 2.6, 7.0,
            )
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(
                connection.execute("SELECT root,root_type,power FROM user_xiuxian WHERE user_id='u'").fetchone(),
                ("混沌灵根", "混沌灵根", 22800),
            )
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM admin_root_change_operations").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
