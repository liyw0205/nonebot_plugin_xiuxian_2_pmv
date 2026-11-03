"""世界首领惩罚/状态表的 schema 归属与 fail-closed 合同。

审计结论：`WorldBossPunishmentSqlRepository` 原先在 `snapshot`、`get_result`、`punish`
共四处 `CREATE TABLE IF NOT EXISTS`（`world_boss_state`、`world_boss_punishment_operations`）。
现在 `boss.007` 是唯一创建者，请求路径只校验：

- 写路径 `punish` 缺表/缺列返回 `schema_missing`，不改变首领状态、不写回执；
- 读路径 `snapshot` 降级为 `([], 0)`，`get_result` 返回 `None`。
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.boss.migrations import apply_boss_punishment_operations
from nonebot_plugin_xiuxian_2.features.boss.punishment_repository import (
    WorldBossPunishmentSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.boss.schema import (
    PUNISHMENT_OPERATION_COLUMNS,
    SCHEMA_MISSING,
    STATE_COLUMNS,
    TABLE_COLUMNS,
    columns,
    table_ready,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = ROOT / "nonebot_plugin_xiuxian_2" / "features" / "boss" / "punishment_repository.py"


class MigrationOwnershipTests(unittest.TestCase):
    def test_migration_builds_both_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_boss_punishment_operations(uow)
            with DatabaseUnitOfWork(str(database), read_only=True) as uow:
                for table, required in TABLE_COLUMNS.items():
                    self.assertTrue(table_ready(uow, table, required), table)

    def test_migration_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_boss_punishment_operations(uow)
            with DatabaseUnitOfWork(str(database), read_only=True) as uow:
                before = {table: columns(uow, table) for table in TABLE_COLUMNS}
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_boss_punishment_operations(uow)
            with DatabaseUnitOfWork(str(database), read_only=True) as uow:
                after = {table: columns(uow, table) for table in TABLE_COLUMNS}
            self.assertEqual(before, after)


class NoRequestPathDdlTests(unittest.TestCase):
    def test_repository_never_creates_or_alters_tables(self) -> None:
        source = REPOSITORY.read_text(encoding="utf-8")
        self.assertNotIn("CREATE TABLE", source)
        self.assertNotIn("ALTER TABLE", source)
        self.assertIn("table_ready", source)
        self.assertIn("SCHEMA_MISSING", source)


class FailClosedTests(unittest.TestCase):
    def test_unmigrated_database_writes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with db_backend.transaction(database):
                pass
            repository = WorldBossPunishmentSqlRepository(database)
            self.assertEqual(repository.snapshot(), ([], 0))
            self.assertIsNone(repository.get_result("op1"))
            result = repository.punish("op1", "all", 0, [])
            self.assertEqual(result.status, SCHEMA_MISSING)
            self.assertFalse(result.succeeded)
            with db_backend.connection(database) as conn:
                tables = {
                    str(row[0])
                    for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                }
            self.assertEqual(tables, set(), "the request path must not create any table")

    def test_half_migrated_state_table_is_rejected_without_alter(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with db_backend.transaction(database) as conn:
                conn.execute("CREATE TABLE world_boss_state(state_key TEXT PRIMARY KEY)")
            result = WorldBossPunishmentSqlRepository(database).punish("op2", "all", 0, [])
            self.assertEqual(result.status, SCHEMA_MISSING)
            with db_backend.connection(database) as conn:
                actual = {
                    str(row[1])
                    for row in conn.execute('PRAGMA table_info("world_boss_state")')
                }
            self.assertEqual(actual, {"state_key"}, "no column may be added")
            self.assertEqual(WorldBossPunishmentSqlRepository(database).snapshot(), ([], 0))


class PostMigrationBehaviourTests(unittest.TestCase):
    def test_punish_applies_and_replays_after_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_boss_punishment_operations(uow)
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "INSERT INTO world_boss_state VALUES('global','[{\"id\":1},{\"id\":2}]','now',0)"
                )
            repository = WorldBossPunishmentSqlRepository(database)
            bosses, revision = repository.snapshot()
            self.assertEqual((len(bosses), revision), (2, 0))
            first = repository.punish("p1", "single", revision, bosses, 1)
            replay = repository.punish("p1", "single", revision, bosses, 1)
            self.assertEqual((first.status, replay.status), ("punished", "duplicate"))
            self.assertEqual(len(first.bosses), 1)
            self.assertEqual(len(first.deleted_bosses), 1)
            with db_backend.connection(database) as conn:
                self.assertEqual(
                    int(conn.execute("SELECT COUNT(*) FROM world_boss_punishment_operations").fetchone()[0]),
                    1,
                )


if __name__ == "__main__":
    unittest.main()
