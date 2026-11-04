"""`economy_operations` 转账幂等回执的 schema 归属与 fail-closed 合同。

`EconomyRepository.transfer_stones` 原先在请求事务内 `CREATE TABLE`。本切片把该表
交给启动迁移 `economy_ledger.003`（game DB），请求路径只校验形状；未迁移时返回
`False` 且零扣款/零入账/零回执。测试同时覆盖 `repositories.py` 的文件路径独立加载
路径（`tests/test_repositories.py` 的加载方式），确保 fallback 读取同一契约。
"""

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.economy_ledger import migrations as ledger_migrations
from nonebot_plugin_xiuxian_2.infrastructure.database import (
    DatabaseUnitOfWork,
)
from nonebot_plugin_xiuxian_2.infrastructure.database.economy_operations import (
    ECONOMY_OPERATIONS_COLUMNS,
    operations_ready,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import repositories
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_PATH = (
    ROOT
    / "nonebot_plugin_xiuxian_2"
    / "xiuxian"
    / "xiuxian_utils"
    / "repositories.py"
)


def load_standalone_repository_module():
    """Mirror tests/test_repositories.py: load the module without package context."""
    spec = importlib.util.spec_from_file_location(
        "xiuxian_repositories_standalone", REPOSITORY_PATH
    )
    if spec is None or spec.loader is None:  # pragma: no cover
        raise RuntimeError(f"Unable to load {REPOSITORY_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@contextmanager
def connection(database: Path):
    conn = db_backend.connect(database)
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def make_repository(module, database: Path):
    return module.EconomyRepository(
        lambda: connection(database),
        int,
        lambda *args, **kwargs: None,
    )


def seed_users(database: Path, *, sender_stone: int = 100, recipient_stone: int = 20) -> None:
    with db_backend.transaction(database) as conn:
        conn.execute(
            "CREATE TABLE user_xiuxian ("
            "user_id TEXT PRIMARY KEY, user_name TEXT, stone INTEGER, exp INTEGER)"
        )
        conn.execute(
            "INSERT INTO user_xiuxian VALUES (%s,%s,%s,%s)",
            ("1001", "甲", sender_stone, 0),
        )
        conn.execute(
            "INSERT INTO user_xiuxian VALUES (%s,%s,%s,%s)",
            ("1002", "乙", recipient_stone, 0),
        )


def stones(database: Path, user_id: str) -> int:
    with db_backend.connection(database) as conn:
        return int(
            conn.execute(
                "SELECT stone FROM user_xiuxian WHERE user_id=%s", (user_id,)
            ).fetchone()[0]
        )


def table_names(database: Path) -> set[str]:
    with db_backend.connection(database) as conn:
        return {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }


class EconomyOperationsSchemaOwnershipTests(unittest.TestCase):
    def test_migration_creates_the_receipt_shape_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            rows: list = []
            for _ in range(2):
                with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                    ledger_migrations.apply_economy_operations(uow)
                    rows = uow.query_all('PRAGMA table_info("economy_operations")')
            columns = {str(row["name"]) for row in rows}
            self.assertEqual(columns, set(ECONOMY_OPERATIONS_COLUMNS))
            self.assertTrue(
                any(
                    str(row["name"]) == "operation_id" and int(row["pk"] or 0) == 1
                    for row in rows
                )
            )

    def test_migration_is_routed_to_the_game_database_only(self) -> None:
        from nonebot_plugin_xiuxian_2.plugin import (
            build_migrations,
            migrations_for_database,
        )

        migrations = build_migrations()
        versions = [migration.version for migration in migrations]
        self.assertEqual(versions, sorted(set(versions)))
        self.assertIn("economy_ledger.003", versions)
        game = [m.version for m in migrations_for_database(migrations, "game_db")]
        self.assertIn("economy_ledger.003", game)
        for key in ("player_db", "trade_db", "impart_db", "message_db"):
            routed = [m.version for m in migrations_for_database(migrations, key)]
            self.assertNotIn("economy_ledger.003", routed, key)

    def test_legacy_receipt_rows_survive_the_startup_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE economy_operations (operation_id TEXT PRIMARY KEY,"
                    "operation_type TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                conn.execute(
                    "INSERT INTO economy_operations(operation_id, operation_type) "
                    "VALUES ('old', 'stone_transfer')"
                )
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                ledger_migrations.apply_economy_operations(uow)
            with db_backend.connection(database) as conn:
                rows = conn.execute(
                    "SELECT operation_id, operation_type FROM economy_operations"
                ).fetchall()
            self.assertEqual([tuple(row) for row in rows], [("old", "stone_transfer")])

    def test_transfer_fails_closed_without_the_receipt_table(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            seed_users(database)
            repository = make_repository(repositories, database)
            self.assertFalse(repository.transfer_stones("t1", "1001", "1002", 40))
            self.assertEqual((stones(database, "1001"), stones(database, "1002")), (100, 20))
            self.assertNotIn("economy_operations", table_names(database))

    def test_transfer_reads_and_writes_after_the_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            seed_users(database)
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                ledger_migrations.apply_economy_operations(uow)
                self.assertTrue(operations_ready(uow.connection))

            repository = make_repository(repositories, database)
            self.assertTrue(repository.transfer_stones("t1", "1001", "1002", 40))
            self.assertEqual((stones(database, "1001"), stones(database, "1002")), (60, 60))
            with db_backend.connection(database) as conn:
                receipt = conn.execute(
                    "SELECT operation_type FROM economy_operations "
                    "WHERE operation_id='t1'"
                ).fetchone()
            self.assertEqual(receipt[0], "stone_transfer")

            self.assertFalse(repository.transfer_stones("t1", "1001", "1002", 40))
            self.assertEqual((stones(database, "1001"), stones(database, "1002")), (60, 60))

            self.assertFalse(repository.transfer_stones("t2", "1001", "1002", 1000))
            self.assertEqual((stones(database, "1001"), stones(database, "1002")), (60, 60))

            with self.assertRaises(ValueError):
                repository.transfer_stones("t3", "1001", "missing", 40)
            self.assertEqual(stones(database, "1001"), 60)

    def test_standalone_module_loader_uses_the_same_contract(self) -> None:
        standalone = load_standalone_repository_module()
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            seed_users(database)
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                ledger_migrations.apply_economy_operations(uow)
            repository = make_repository(standalone, database)
            self.assertTrue(repository.transfer_stones("standalone", "1001", "1002", 40))
            self.assertEqual((stones(database, "1001"), stones(database, "1002")), (60, 60))

    def test_repository_source_has_no_ddl_and_uses_the_contract(self) -> None:
        source = REPOSITORY_PATH.read_text(encoding="utf-8")
        self.assertNotIn("CREATE TABLE", source)
        self.assertNotIn("ALTER TABLE", source)
        self.assertIn("_economy_operations_bind", source)
        self.assertIn("operations_ready", source)


if __name__ == "__main__":
    unittest.main()
