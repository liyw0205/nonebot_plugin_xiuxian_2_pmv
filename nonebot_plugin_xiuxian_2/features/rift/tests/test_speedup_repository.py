import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema, build_migrations, migrations_for_database
from ..application import RiftApplication
from ..migrations import apply_rift_speedup_operations
from ..speedup_repository import RiftSpeedupSqlRepository
from tests.test_db_backend import db_backend


class RiftSpeedupRepositoryTests(unittest.TestCase):
    @staticmethod
    def _create_state(
        database: Path, *, migration: bool = True, include_bind_num: bool = True
    ) -> None:
        with db_backend.transaction(database) as conn:
            conn.execute("CREATE TABLE rift_entries(user_id TEXT PRIMARY KEY,rift_data TEXT,status TEXT,duration INTEGER)")
            conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time INTEGER)")
            if include_bind_num:
                conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))")
            else:
                conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,PRIMARY KEY(user_id,goods_id))")
            conn.execute("INSERT INTO rift_entries VALUES('u','{\"name\":\"test\",\"time\":100}','active',100)")
            conn.execute("INSERT INTO user_cd VALUES('u',3,'now',100)")
            if include_bind_num:
                conn.execute("INSERT INTO back VALUES('u',9,1,1)")
            else:
                conn.execute("INSERT INTO back VALUES('u',9,1)")
        if migration:
            with DatabaseUnitOfWork(database) as uow:
                apply_rift_speedup_operations(uow)

    def test_applied_duplicate_and_item_missing(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._create_state(database)
            repo = RiftSpeedupSqlRepository(database)
            first = repo.apply("r1", "u", 9, None, None, 50)
            duplicate = repo.apply("r1", "u", 9, None, None, 50)
            missing = repo.apply("r2", "u", 9, None, None, 50)
            self.assertEqual((first.status, duplicate.status, missing.status), ("applied", "duplicate", "item_missing"))

    def test_default_application_uses_sql_repository_and_legacy_payload(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._create_state(database)
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)

            application = RiftApplication(database, Path(temp) / "player.db")

            def fail_legacy_speedup(*args, **kwargs):
                raise AssertionError("legacy speedup used")

            application.legacy_repository.invoke = fail_legacy_speedup
            outcome = application.speedup(
                operation_id="default-speedup", user_id="u", item_id=9, remaining_ratio=50
            )

            self.assertIsNone(application.repository)
            self.assertTrue(outcome.ok)
            self.assertEqual(outcome.data["status"], "applied")
            with db_backend.connection(database) as conn:
                operation = conn.execute(
                    "SELECT payload FROM rift_speedup_operations WHERE operation_id='default-speedup'"
                ).fetchone()
                self.assertEqual(operation[0], json.dumps(["u", 9, 50], ensure_ascii=True))
                self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE goods_id=9").fetchone()[0], 0)

    def test_missing_startup_migration_is_rejected_without_request_ddl(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._create_state(database, migration=False)
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)

            outcome = RiftApplication(database, Path(temp) / "player.db").speedup(
                operation_id="missing-migration", user_id="u", item_id=9, remaining_ratio=50
            )

            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "schema_missing")
            with db_backend.connection(database) as conn:
                self.assertIsNone(conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='rift_speedup_operations'"
                ).fetchone())
                self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE goods_id=9").fetchone()[0], 1)

    def test_replays_existing_legacy_payload(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._create_state(database)
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "INSERT INTO rift_speedup_operations(operation_id,payload,new_time,rift_data,create_time) "
                    "VALUES('old-speedup',%s,50,%s,'now')",
                    (json.dumps(["u", 9, 50], ensure_ascii=True), '{"name":"test","time":50}'),
                )

            result = RiftSpeedupSqlRepository(database).apply("old-speedup", "u", 9, None, None, 50)

            self.assertEqual((result.status, result.new_time, result.rift_data["time"]), ("duplicate", 50, 50))
            with db_backend.connection(database) as conn:
                self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE goods_id=9").fetchone()[0], 1)

    def test_replays_existing_legacy_snapshot_payload(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._create_state(database)
            expected_rift = {"name": "test", "time": 100}
            expected_cd = {"type": 3, "create_time": "now", "scheduled_time": 100}
            expected_snapshot = json.dumps(expected_rift, ensure_ascii=False, sort_keys=True)
            payload = json.dumps(
                ["u", 9, expected_snapshot, expected_cd, 50],
                ensure_ascii=True,
                sort_keys=True,
            )
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "INSERT INTO rift_speedup_operations(operation_id,payload,new_time,rift_data,create_time) "
                    "VALUES('old-snapshot-speedup',%s,50,%s,'now')",
                    (payload, '{"name":"test","time":50}'),
                )

            result = RiftSpeedupSqlRepository(database).apply(
                "old-snapshot-speedup", "u", 9, expected_rift, expected_cd, 50
            )

            self.assertEqual((result.status, result.new_time), ("duplicate", 50))
            with db_backend.connection(database) as conn:
                self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE goods_id=9").fetchone()[0], 1)

    def test_late_failure_rolls_back_item_rift_cooldown_and_operation(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._create_state(database)
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TRIGGER fail_speedup_cd BEFORE UPDATE OF scheduled_time ON user_cd "
                    "BEGIN SELECT RAISE(ABORT,'injected cooldown failure'); END"
                )

            with self.assertRaises(sqlite3.IntegrityError):
                RiftSpeedupSqlRepository(database).apply("rollback", "u", 9, None, None, 50)

            with db_backend.connection(database) as conn:
                self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE goods_id=9").fetchone()[0], 1)
                self.assertEqual(tuple(conn.execute("SELECT duration,rift_data FROM rift_entries").fetchone()), (100, '{"name":"test","time":100}'))
                self.assertEqual(conn.execute("SELECT scheduled_time FROM user_cd WHERE user_id='u'").fetchone()[0], 100)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM rift_speedup_operations").fetchone()[0], 0)

    def test_bound_item_decrement_preserves_inventory_invariant(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._create_state(database)
            with db_backend.transaction(database) as conn:
                conn.execute("UPDATE back SET goods_num=2,bind_num=5 WHERE goods_id=9")

            result = RiftSpeedupSqlRepository(database).apply("bound", "u", 9, None, None, 50)

            self.assertEqual(result.status, "applied")
            with db_backend.connection(database) as conn:
                self.assertEqual(tuple(conn.execute("SELECT goods_num,bind_num FROM back").fetchone()), (1, 1))

    def test_consumes_from_legacy_back_table_without_bind_column(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._create_state(database, include_bind_num=False)

            result = RiftSpeedupSqlRepository(database).apply("no-bind", "u", 9, None, None, 50)

            self.assertEqual(result.status, "applied")
            with db_backend.connection(database) as conn:
                self.assertEqual(conn.execute("SELECT goods_num FROM back").fetchone()[0], 0)

    def test_speedup_migration_is_game_only(self):
        migrations = build_migrations()
        routed = {
            key: {migration.version for migration in migrations_for_database(migrations, key)}
            for key in ("game_db", "player_db", "trade_db", "impart_db", "message_db")
        }

        self.assertIn("rift.004", routed["game_db"])
        for database_key in ("player_db", "trade_db", "impart_db", "message_db"):
            self.assertNotIn("rift.004", routed[database_key])

    def test_migration_upgrades_legacy_replay_table_without_losing_rows(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE rift_speedup_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,new_time INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                conn.execute(
                    "INSERT INTO rift_speedup_operations(operation_id,payload,new_time) "
                    "VALUES('legacy','payload',25)"
                )

            with DatabaseUnitOfWork(database) as uow:
                apply_rift_speedup_operations(uow)

            with db_backend.connection(database) as conn:
                columns = {row[1] for row in conn.execute("PRAGMA table_info(rift_speedup_operations)")}
                self.assertTrue({"rift_data", "create_time"}.issubset(columns))
                self.assertEqual(
                    tuple(conn.execute("SELECT operation_id,payload,new_time FROM rift_speedup_operations").fetchone()),
                    ("legacy", "payload", 25),
                )
