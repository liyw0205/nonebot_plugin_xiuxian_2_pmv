"""宗门（sect）操作回执表的 schema 归属与 fail-closed 合同。

审计结论：11 份仓储原先在**业务方法写事务内** `CREATE TABLE IF NOT EXISTS`
建 12 张回执表（其中入宗开关是动态的 `sect_{open,close}_join_operations`）。
现在 `sect.019` 是唯一创建者，请求路径只校验「表存在 + 本次要读的列齐全」，
缺表或缺列返回 `schema_missing` 且不改动宗门资产、成员、贡献与任何回执行。

所有表都在 game 库：这些仓储只持有一个 `database` 连接，不使用附库。
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.sect.migrations import apply_sect_operations
from nonebot_plugin_xiuxian_2.features.sect.schema import (
    OPERATION_TABLES,
    SCHEMA_MISSING,
    columns,
    table_ready,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
FEATURE_DIR = ROOT / "nonebot_plugin_xiuxian_2" / "features" / "sect"
REPOSITORIES = (
    "buff_search_repository.py",
    "close_mountain_repository.py",
    "creation_repository.py",
    "daily_maintenance_repository.py",
    "disband_repository.py",
    "elixir_room_repository.py",
    "join_state_repository.py",
    "name_refresh_repository.py",
    "owner_inherit_repository.py",
    "owner_transfer_repository.py",
    "practice_repository.py",
)


class MigrationOwnershipTests(unittest.TestCase):
    def test_migration_builds_every_receipt_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "sect.db"
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_sect_operations(uow)
            with DatabaseUnitOfWork(str(database), read_only=True) as uow:
                for table, required in OPERATION_TABLES.items():
                    self.assertTrue(table_ready(uow, table, required), table)

    def test_migration_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "sect.db"
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_sect_operations(uow)
            with DatabaseUnitOfWork(str(database), read_only=True) as uow:
                before = {table: columns(uow, table) for table in OPERATION_TABLES}
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_sect_operations(uow)
            with DatabaseUnitOfWork(str(database), read_only=True) as uow:
                after = {table: columns(uow, table) for table in OPERATION_TABLES}
            self.assertEqual(before, after)


class NoRequestPathDdlTests(unittest.TestCase):
    def test_repositories_never_create_or_alter_tables(self) -> None:
        offenders = [
            name
            for name in REPOSITORIES
            if "CREATE TABLE" in (FEATURE_DIR / name).read_text(encoding="utf-8")
            or "ALTER TABLE" in (FEATURE_DIR / name).read_text(encoding="utf-8")
        ]
        self.assertEqual(offenders, [])

    def test_every_repository_consults_the_shared_contract(self) -> None:
        for name in REPOSITORIES:
            source = (FEATURE_DIR / name).read_text(encoding="utf-8")
            self.assertIn("table_ready", source, name)
            self.assertIn("SCHEMA_MISSING", source, name)


class FailClosedTests(unittest.TestCase):
    def test_write_paths_report_schema_missing_without_writes(self) -> None:
        from nonebot_plugin_xiuxian_2.features.sect.buff_search_repository import (
            SectBuffSearchSqlRepository,
        )
        from nonebot_plugin_xiuxian_2.features.sect.close_mountain_repository import (
            SectCloseMountainSqlRepository,
        )
        from nonebot_plugin_xiuxian_2.features.sect.creation_repository import (
            SectCreationSqlRepository,
        )
        from nonebot_plugin_xiuxian_2.features.sect.disband_repository import (
            SectDisbandSqlRepository,
        )
        from nonebot_plugin_xiuxian_2.features.sect.elixir_room_repository import (
            SectElixirRoomSqlRepository,
        )
        from nonebot_plugin_xiuxian_2.features.sect.join_state_repository import (
            SectJoinStateSqlRepository,
        )
        from nonebot_plugin_xiuxian_2.features.sect.name_refresh_repository import (
            SectNameRefreshSqlRepository,
        )
        from nonebot_plugin_xiuxian_2.features.sect.owner_inherit_repository import (
            SectOwnerInheritSqlRepository,
        )
        from nonebot_plugin_xiuxian_2.features.sect.owner_transfer_repository import (
            SectOwnerTransferSqlRepository,
        )
        from nonebot_plugin_xiuxian_2.features.sect.practice_repository import (
            SectPracticeSqlRepository,
        )

        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "sect.db"
            with db_backend.transaction(database):
                pass

            self.assertEqual(
                SectCreationSqlRepository(database).create("s1", "u", "宗", 100, 0)["status"],
                SCHEMA_MISSING,
            )
            self.assertEqual(
                SectNameRefreshSqlRepository(database).charge("s2", "u", 100)["status"],
                SCHEMA_MISSING,
            )
            self.assertEqual(
                SectJoinStateSqlRepository(database).open("s3", "u")["status"],
                SCHEMA_MISSING,
            )
            self.assertEqual(
                SectCloseMountainSqlRepository(database).close("s4", "u")["status"],
                SCHEMA_MISSING,
            )
            self.assertEqual(
                SectOwnerInheritSqlRepository(database).inherit("s5", "u")["status"],
                SCHEMA_MISSING,
            )
            self.assertEqual(
                SectOwnerTransferSqlRepository(database).transfer("s6", "u", "t")["status"],
                SCHEMA_MISSING,
            )
            self.assertEqual(
                SectPracticeSqlRepository(database).upgrade("s7", "u", 1, "attack", 0, 1, 100, 10)["status"],
                SCHEMA_MISSING,
            )
            self.assertEqual(
                SectElixirRoomSqlRepository(database).upgrade("s8", "u", 1, 1, 2, 100, 10)["status"],
                SCHEMA_MISSING,
            )
            self.assertEqual(
                SectBuffSearchSqlRepository(database).apply("s9", "u", 1, "main", "[]", "[1]", 100, 10)["status"],
                SCHEMA_MISSING,
            )
            self.assertEqual(
                SectDisbandSqlRepository(database).disband_inactive(
                    "s10", 1, "inactive_sole_owner",
                    expected_sect_name="宗", expected_owner_id="u", expected_closed=False,
                    expected_member_ids=("u",), expected_active_candidate_ids=(),
                    checked_at=datetime(2026, 9, 15), inactivity_days=30,
                )["status"],
                SCHEMA_MISSING,
            )

            with db_backend.connection(database) as conn:
                tables = {
                    str(row[0])
                    for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                }
            self.assertEqual(tables, set(), "the request paths must not create any table")

    def test_half_migrated_ledger_is_rejected_without_alter(self) -> None:
        from nonebot_plugin_xiuxian_2.features.sect.creation_repository import (
            SectCreationSqlRepository,
        )

        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "sect.db"
            with db_backend.transaction(database) as conn:
                conn.execute("CREATE TABLE sect_creation_operations(operation_id TEXT PRIMARY KEY)")
            result = SectCreationSqlRepository(database).create("s1", "u", "宗", 100, 0)
            self.assertEqual(result["status"], SCHEMA_MISSING)
            with db_backend.connection(database) as conn:
                actual = {
                    str(row[1])
                    for row in conn.execute('PRAGMA table_info("sect_creation_operations")')
                }
            self.assertEqual(actual, {"operation_id"}, "no column may be added")


if __name__ == "__main__":
    unittest.main()
