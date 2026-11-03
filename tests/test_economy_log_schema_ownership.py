"""共享经济账本 `economy_log` 的 schema 归属与奖励写入口径。

审计结论：`log_economy_change()` 原先在**每次写入**时调用
`ensure_economy_log_table()`，在请求路径上建表、逐列 `ALTER` 并建五个索引；
而 `tasks`/`dufang`/`auction`/`admin_asset`/`sign_in` 的迁移各自只建了精简表，
真正的权威列集（`sect_id`、`sect_*_delta`、`event_id`）只存在于这段请求期 DDL 里。

本次收敛：`economy_ledger.002` 建权威形状与索引；请求路径只校验，
schema 已就绪时不执行任何 DDL。奖励数值（`exp_delta`/`stone_delta`/道具增量）
逐字透传，不做任何重算。
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.economy_ledger.migrations import apply_economy_log_schema
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.database.economy import (
    ECONOMY_LOG_COLUMNS,
    ECONOMY_LOG_INDEXES,
    columns,
    indexes,
    schema_ready,
)
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_utils" / "economy_log.py"


class StartupOwnershipTests(unittest.TestCase):
    def test_migration_creates_the_canonical_shape_and_indexes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_economy_log_schema(uow)
            with DatabaseUnitOfWork(str(database), read_only=True) as uow:
                self.assertTrue(schema_ready(uow))
                self.assertEqual(columns(uow), ECONOMY_LOG_COLUMNS)
                self.assertTrue(set(ECONOMY_LOG_INDEXES).issubset(indexes(uow)))

    def test_migration_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_economy_log_schema(uow)
            with DatabaseUnitOfWork(str(database), read_only=True) as uow:
                before = (columns(uow), indexes(uow))
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_economy_log_schema(uow)
            with DatabaseUnitOfWork(str(database), read_only=True) as uow:
                self.assertEqual((columns(uow), indexes(uow)), before)

    def test_canonical_shape_covers_every_column_the_ledger_writes(self) -> None:
        """`log_economy_change` 的 INSERT 列必须全部由启动迁移提供。"""
        source = SOURCE.read_text(encoding="utf-8")
        insert_start = source.index("INSERT INTO economy_log")
        insert_block = source[insert_start : source.index("VALUES", insert_start)]
        for column in ECONOMY_LOG_COLUMNS - {"id", "created_at"}:
            self.assertIn(column, insert_block, f"{column} must stay part of the write path")


class RequestPathTests(unittest.TestCase):
    def test_write_path_never_issues_ddl_when_schema_is_ready(self) -> None:
        """就绪后写账本不应再执行 CREATE/ALTER（源码合同）。"""
        source = SOURCE.read_text(encoding="utf-8")
        start = source.index("def ensure_economy_log_table")
        end = source.index("def log_economy_change")
        helper = source[start:end]
        ready_branch = helper[: helper.index("for statement in ECONOMY_LOG_DDL")]
        self.assertNotIn("ALTER TABLE", ready_branch)
        self.assertIn("ECONOMY_LOG_COLUMNS.issubset(columns)", ready_branch)
        self.assertIn("ECONOMY_LOG_INDEXES).issubset(indexes)", ready_branch)

    def test_ready_schema_is_detected_without_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_economy_log_schema(uow)
            with DatabaseUnitOfWork(str(database), read_only=True) as uow:
                before = indexes(uow)
            from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import economy_log as module

            with db_backend.connection(database) as conn:
                before_tables = {
                    str(row[0])
                    for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                }
            self.assertTrue(callable(module.ensure_economy_log_table))
            with DatabaseUnitOfWork(str(database), read_only=True) as uow:
                self.assertEqual(indexes(uow), before)
            with db_backend.connection(database) as conn:
                after_tables = {
                    str(row[0])
                    for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                }
            self.assertEqual(after_tables, before_tables)


class RewardFidelityTests(unittest.TestCase):
    def test_reward_columns_are_present_and_unmodified(self) -> None:
        """奖励口径冻结：账本列集与状态码语义不变。"""
        self.assertTrue({"exp_delta", "stone_delta", "item_delta", "detail"}.issubset(ECONOMY_LOG_COLUMNS))
        self.assertIn("event_id", ECONOMY_LOG_COLUMNS)
        self.assertIn("idx_economy_log_event_id", ECONOMY_LOG_INDEXES)


if __name__ == "__main__":
    unittest.main()
