from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from ....core.numeric import as_int_like

from ....infrastructure.database import DatabaseUnitOfWork
from ..application import BaseApplication
from ..breakthrough_repository import BaseDirectBreakthroughSqlRepository
from ..migrations import apply_base_direct_breakthrough_operations


class BaseDirectBreakthroughRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.db"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE user_xiuxian ("
                "user_id TEXT PRIMARY KEY,level TEXT,exp INTEGER,hp INTEGER,mp INTEGER,"
                "atk INTEGER,power INTEGER,level_up_rate INTEGER,level_up_cd TIMESTAMP)"
            )
            uow.execute(
                "INSERT INTO user_xiuxian VALUES(?,?,?,?,?,?,?,?,?)",
                ("user-1", "筑基境初期", 10000, 5000, 10000, 1000, 10000, 5, None),
            )
            apply_base_direct_breakthrough_operations(uow)
        self.repository = BaseDirectBreakthroughSqlRepository(self.database)
        self.occurred_at = datetime(2026, 10, 2, 12, 0, 0)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def row(self):
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return uow.query_one(
                "SELECT level,exp,hp,mp,atk,power,level_up_rate,level_up_cd "
                "FROM user_xiuxian WHERE user_id=?",
                ("user-1",),
            )

    def test_failure_updates_penalty_rate_and_cooldown_atomically(self) -> None:
        result = self.repository.apply(
            "failure-1", "user-1", "failure", "筑基境初期", "筑基境初期",
            10000, 5000, 10000, 5, exp_loss=1000, new_hp=4500, new_mp=9000,
            new_rate=7, occurred_at=self.occurred_at,
        )
        self.assertTrue(result.applied)
        row = self.row()
        self.assertEqual(tuple(row[key] for key in ("level", "exp", "hp", "mp", "atk", "power", "level_up_rate")),
                         ("筑基境初期", 9000, 4500, 9000, 1000, 10000, 7))
        self.assertEqual(str(row["level_up_cd"]), self.occurred_at.isoformat(sep=" "))

    def test_success_updates_level_power_attributes_and_cooldown_atomically(self) -> None:
        result = self.repository.apply(
            "success-1", "user-1", "success", "筑基境初期", "筑基境中期",
            10000, 5000, 10000, 5, root_rate=1.5, level_spend=2.0,
            occurred_at=self.occurred_at,
        )
        self.assertTrue(result.applied)
        row = self.row()
        self.assertEqual(tuple(row[key] for key in ("level", "exp", "hp", "mp", "atk", "power", "level_up_rate")),
                         ("筑基境中期", 10000, 5000, 10000, 1000, 30000, 0))
        self.assertEqual(str(row["level_up_cd"]), self.occurred_at.isoformat(sep=" "))

    def test_duplicate_operation_is_not_applied_twice(self) -> None:
        args = (
            "failure-repeat", "user-1", "failure", "筑基境初期", "筑基境初期",
            10000, 5000, 10000, 5,
        )
        self.repository.apply(*args, exp_loss=1000, new_hp=4500, new_mp=9000, new_rate=7)
        duplicate = self.repository.apply(
            *args, exp_loss=1000, new_hp=4500, new_mp=9000, new_rate=7,
        )
        self.assertEqual(duplicate.status, "duplicate")
        self.assertEqual(self.row()["exp"], 9000)

    def test_changed_operation_payload_is_rejected(self) -> None:
        args = (
            "failure-conflict", "user-1", "failure", "筑基境初期", "筑基境初期",
            10000, 5000, 10000, 5,
        )
        self.repository.apply(*args, exp_loss=1000, new_hp=4500, new_mp=9000, new_rate=7)
        conflict = self.repository.apply(
            *args, exp_loss=2000, new_hp=4000, new_mp=8000, new_rate=7,
        )
        self.assertEqual(conflict.status, "operation_conflict")
        self.assertEqual(self.row()["exp"], 9000)

    def test_stale_state_is_rejected(self) -> None:
        result = self.repository.apply(
            "stale", "user-1", "failure", "筑基境初期", "筑基境初期",
            9999, 5000, 10000, 5, exp_loss=1000, new_hp=4500, new_mp=9000,
            new_rate=7,
        )
        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self.row()["exp"], 10000)

    def test_operation_failure_rolls_back_character_state(self) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "CREATE TRIGGER fail_direct_breakthrough_receipt "
                "BEFORE INSERT ON direct_breakthrough_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.repository.apply(
                "failure-rollback", "user-1", "failure", "筑基境初期", "筑基境初期",
                10000, 5000, 10000, 5, exp_loss=1000, new_hp=4500, new_mp=9000,
                new_rate=7,
            )
        self.assertEqual(self.row()["exp"], 10000)

    def test_missing_schema_fails_closed_without_request_ddl(self) -> None:
        missing = Path(self.temp_dir.name) / "missing.db"
        with DatabaseUnitOfWork(missing, immediate=True):
            pass
        app = BaseApplication(missing, Path(self.temp_dir.name) / "player.db")
        result = app.settle_direct_breakthrough(
            operation_id="no-schema", user_id="user-1", outcome="failure",
            expected_level="筑基境初期", target_level="筑基境初期", expected_exp=10000,
            expected_hp=5000, expected_mp=10000, expected_rate=5,
            exp_loss=1000, new_hp=4500, new_mp=9000, new_rate=7,
        )
        self.assertEqual(result.status, "schema_missing")
        with DatabaseUnitOfWork(missing, read_only=True) as uow:
            table = uow.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE type='table' "
                "AND name='direct_breakthrough_operations'"
            )
        self.assertIsNone(table)

    def test_base_application_uses_feature_repository_by_default(self) -> None:
        app = BaseApplication(self.database, Path(self.temp_dir.name) / "player.db")
        result = app.settle_direct_breakthrough(
            operation_id="application-failure", user_id="user-1", outcome="failure",
            expected_level="筑基境初期", target_level="筑基境初期", expected_exp=10000,
            expected_hp=5000, expected_mp=10000, expected_rate=5,
            exp_loss=1000, new_hp=4500, new_mp=9000, new_rate=7,
        )
        self.assertTrue(result.applied)
        self.assertEqual(self.row()["exp"], 9000)

    def test_missing_database_is_not_created(self) -> None:
        missing = Path(self.temp_dir.name) / "absent" / "game.db"
        result = BaseDirectBreakthroughSqlRepository(missing).apply(
            "missing", "user-1", "failure", "before", "before", 10000, 5000, 10000, 5,
        )
        self.assertEqual(result.status, "schema_missing")
        self.assertFalse(missing.parent.exists())

    def test_migration_preserves_legacy_receipts_and_is_repeatable(self) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("DROP TABLE direct_breakthrough_operations")
            uow.execute(
                "CREATE TABLE direct_breakthrough_operations ("
                "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,outcome TEXT NOT NULL,"
                "from_level TEXT NOT NULL,to_level TEXT NOT NULL,exp_loss INTEGER NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            uow.execute(
                "INSERT INTO direct_breakthrough_operations "
                "(operation_id,user_id,outcome,from_level,to_level,exp_loss) "
                "VALUES('old','user-1','failure','before','before',1000)"
            )
            apply_base_direct_breakthrough_operations(uow)
            apply_base_direct_breakthrough_operations(uow)
            receipt = uow.query_one("SELECT * FROM direct_breakthrough_operations")
        self.assertEqual(receipt["payload"], "")
        self.assertEqual(receipt["exp_loss"], 1000)
        for user, status in (("user-1", "duplicate"), ("other", "operation_conflict")):
            result = self.repository.apply("old", user, "failure", "before", "before", 0, 0, 0, 0)
            self.assertEqual(result.status, status)
        self.assertEqual(self.row()["exp"], 10000)

    def test_unsupported_legacy_schema_is_rejected(self) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("DROP TABLE direct_breakthrough_operations")
            uow.execute("CREATE TABLE direct_breakthrough_operations(operation_id TEXT)")
        with self.assertRaisesRegex(RuntimeError, "unsupported legacy schema"):
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                apply_base_direct_breakthrough_operations(uow)

    def test_high_realm_failure_uses_sqlite_safe_numeric_binding(self) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("UPDATE user_xiuxian SET exp=1e25,hp=5e24,mp=1e25")
        before = self.row()
        exp, hp, mp = (as_int_like(before[key]) for key in ("exp", "hp", "mp"))
        loss = int(exp * 0.1)
        args = ("large", "user-1", "failure", "筑基境初期", "筑基境初期", exp, hp, mp, 5)
        changes = dict(exp_loss=loss, new_hp=hp - loss // 2, new_mp=mp - loss, new_rate=7)
        self.assertTrue(self.repository.apply(*args, **changes).applied)
        self.assertEqual(self.repository.apply(*args, **changes).status, "duplicate")
        self.assertAlmostEqual(float(self.row()["exp"]) / exp, 0.9)

    def test_default_clock_preserves_local_naive_cooldown(self) -> None:
        from datetime import timezone

        now = datetime(2026, 10, 2, 4, 0, tzinfo=timezone.utc)
        repository = BaseDirectBreakthroughSqlRepository(
            self.database, clock=SimpleNamespace(now=lambda: now),
        )
        result = repository.apply(
            "clock", "user-1", "failure", "筑基境初期", "筑基境初期",
            10000, 5000, 10000, 5, exp_loss=1000, new_hp=4500, new_mp=9000, new_rate=7,
        )
        self.assertTrue(result.applied)
        self.assertEqual(
            str(self.row()["level_up_cd"]), str(now.astimezone().replace(tzinfo=None)),
        )

    def test_migration_is_routed_only_to_game_database(self) -> None:
        from ....plugin import build_migrations, migrations_for_database

        migrations = build_migrations()
        for database in ("game_db", "player_db", "trade_db", "impart_db", "message_db"):
            versions = {migration.version for migration in migrations_for_database(migrations, database)}
            self.assertEqual("base.009" in versions, database == "game_db")


if __name__ == "__main__":
    unittest.main()
