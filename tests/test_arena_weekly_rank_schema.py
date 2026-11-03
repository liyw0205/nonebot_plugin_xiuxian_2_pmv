"""竞技场周降段维护表的启动迁移与请求路径无 DDL（定向合同）。

`ArenaWeeklyRankRepository._ensure_schema()` 原先在降段事务内
`CREATE TABLE IF NOT EXISTS arena(...)`、`ALTER TABLE arena ADD COLUMN ...` 以及
`CREATE TABLE IF NOT EXISTS arena_weekly_rank_reduction_operations/_targets(...)`。
这三张表其实已由启动迁移 `arena.006`（两张降段回执表）与 `arena.009`（`arena` + 列）
持有，请求路径重复 DDL 违反仓库级「请求路径不执行 DDL」合同。

本切片把关卡改成 schema 校验：缺表或缺列返回 `schema_missing` 且零写入。
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.arena.migrations import (
    apply_arena_state,
    apply_arena_weekly_rank_reduction,
)
from nonebot_plugin_xiuxian_2.features.arena.weekly_rank_application import (
    ArenaWeeklyRankApplication,
)
from nonebot_plugin_xiuxian_2.features.arena.weekly_rank_repository import (
    ArenaWeeklyRankRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
FEATURE = ROOT / "nonebot_plugin_xiuxian_2" / "features" / "arena"
FACADE = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_arena" / "__init__.py"
WEEK = "2026-W29"


class Fixture:
    def __init__(self, root: Path) -> None:
        self.database = root / "player.db"

    def build(self, rows=(("u1", 1100, "青铜", 2), ("u2", 2000, "黄金", 3))) -> "Fixture":
        with DatabaseUnitOfWork(str(self.database), immediate=True) as uow:
            apply_arena_state(uow)
            apply_arena_weekly_rank_reduction(uow)
        with db_backend.transaction(self.database) as conn:
            conn.executemany(
                "INSERT INTO arena(user_id,score,rank,win_streak) VALUES(%s,%s,%s,%s)", rows
            )
        return self

    def bare(self, *, table: str | None = None, columns: str = "user_id TEXT PRIMARY KEY") -> "Fixture":
        """Create only a partial `arena` so the gate must reject it."""
        with db_backend.transaction(self.database) as conn:
            if table:
                conn.execute(f'CREATE TABLE "{table}"({columns})')
        return self

    def tables(self) -> set[str]:
        with db_backend.connection(self.database) as conn:
            return {
                str(row[0])
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }

    def rows(self) -> dict[str, tuple[int, str, int]]:
        with db_backend.connection(self.database) as conn:
            return {
                str(row[0]): (int(row[1]), str(row[2]), int(row[3]))
                for row in conn.execute(
                    "SELECT user_id,score,rank,win_streak FROM arena ORDER BY user_id"
                ).fetchall()
            }

    def apply(self, week=WEEK, steps=2, chunk_size=500):
        return ArenaWeeklyRankApplication(self.database).reduce(
            week, steps, chunk_size=chunk_size, updated_at="2026-07-17 20:00:00"
        )


class SchemaGateTests(unittest.TestCase):
    def test_unmigrated_database_fails_closed_without_ddl(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp))
            result = fixture.apply()
            self.assertEqual((result.status, result.business_week), ("schema_missing", WEEK))
            self.assertEqual(fixture.tables(), set())

    def test_partial_arena_table_is_rejected_and_not_patched(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).bare(table="arena", columns="user_id TEXT PRIMARY KEY")
            result = fixture.apply()
            self.assertEqual(result.status, "schema_missing")
            with db_backend.connection(fixture.database) as conn:
                columns = {str(row[1]) for row in conn.execute('PRAGMA table_info("arena")')}
            self.assertEqual(columns, {"user_id"}, "the request path must not ALTER the schema")

    def test_missing_one_ledger_blocks_the_whole_reduction(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            with db_backend.transaction(fixture.database) as conn:
                conn.execute("DROP TABLE arena_weekly_rank_reduction_targets")
            result = fixture.apply()
            self.assertEqual(result.status, "schema_missing")
            self.assertEqual(fixture.rows()["u1"], (1100, "青铜", 2), "no rank was reduced")

    def test_schema_gate_accepts_the_startup_shape(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            with DatabaseUnitOfWork(str(fixture.database), read_only=True) as uow:
                self.assertTrue(ArenaWeeklyRankRepository._schema_ready(uow))


class StartupMigrationShapeTests(unittest.TestCase):
    def test_reduction_ledgers_carry_every_column_the_repository_reads(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            expected = {
                "arena_weekly_rank_reduction_operations": {
                    "business_week", "reduce_steps", "total", "completed", "changed",
                    "skipped", "conflicted", "status", "last_error", "created_at", "updated_at",
                },
                "arena_weekly_rank_reduction_targets": {
                    "business_week", "user_id", "ordinal", "previous_score", "previous_rank",
                    "previous_win_streak", "target_score", "target_rank", "status",
                    "error_text", "updated_at",
                },
            }
            with db_backend.connection(fixture.database) as conn:
                for table, columns in expected.items():
                    actual = {str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")')}
                    self.assertTrue(columns.issubset(actual), table)

    def test_arena_carries_the_columns_the_reduction_reads(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            with db_backend.connection(fixture.database) as conn:
                actual = {str(row[1]) for row in conn.execute('PRAGMA table_info("arena")')}
            self.assertTrue(ArenaWeeklyRankRepository.ARENA_COLUMNS.issubset(actual))

    def test_legacy_receipt_rows_survive_the_migration(self) -> None:
        """A database that already ran the reduction keeps its ledger rows."""
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            first = fixture.apply(chunk_size=1)
            self.assertEqual((first.status, first.changed), ("applied", 1))
            with DatabaseUnitOfWork(str(fixture.database), immediate=True) as uow:
                apply_arena_state(uow)
                apply_arena_weekly_rank_reduction(uow)
            with db_backend.connection(fixture.database) as conn:
                rows = [
                    str(row[0])
                    for row in conn.execute(
                        "SELECT business_week FROM arena_weekly_rank_reduction_operations"
                    )
                ]
            self.assertEqual(rows, [WEEK])


class ReductionBehaviourTests(unittest.TestCase):
    def test_resume_and_replay_work_after_the_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            first = fixture.apply(chunk_size=1)
            self.assertEqual(
                (first.status, first.task_status, first.total, first.changed),
                ("applied", "running", 2, 1),
            )
            completed = fixture.apply(chunk_size=10)
            replay = fixture.apply()
            self.assertEqual(
                (completed.status, completed.task_status, completed.completed, replay.status),
                ("applied", "completed", 2, "duplicate"),
            )
            self.assertEqual(fixture.rows()["u2"], (1000, "青铜", 0))

    def test_empty_arena_completes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            with db_backend.transaction(fixture.database) as conn:
                conn.execute("DELETE FROM arena")
            first = fixture.apply()
            self.assertEqual(
                (first.status, first.task_status, first.total),
                ("applied", "completed", 0),
            )

    def test_targets_are_still_frozen_and_reported(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            fixture.apply(chunk_size=10)
            with db_backend.connection(fixture.database) as conn:
                rows = [
                    tuple(row)
                    for row in conn.execute(
                        "SELECT user_id,previous_score,previous_rank,previous_win_streak,"
                        "target_score,target_rank,status,error_text "
                        "FROM arena_weekly_rank_reduction_targets ORDER BY ordinal"
                    )
                ]
            self.assertEqual(
                rows,
                [
                    ("u1", 1100, "青铜", 2, 1000, "青铜", "applied", ""),
                    ("u2", 2000, "黄金", 3, 1000, "青铜", "applied", ""),
                ],
            )


class FacadeContractTests(unittest.TestCase):
    def test_repository_never_creates_or_alters_tables(self) -> None:
        source = (FEATURE / "weekly_rank_repository.py").read_text(encoding="utf-8")
        self.assertNotIn("CREATE TABLE", source)
        self.assertNotIn("ALTER TABLE", source)
        self.assertIn("_schema_ready", source)
        self.assertIn("table_ready", source, "the gate must come from the shared arena schema contract")
        self.assertIn("SCHEMA_MISSING", source)

    def test_scheduler_entry_surfaces_schema_missing_before_looping(self) -> None:
        source = FACADE.read_text(encoding="utf-8")
        start = source.index("async def reduce_arena_rank")
        handler = source[start : source.index("async def use_arena_challenge_ticket", start)]
        self.assertIn("arena_weekly_rank_application.reduce(", handler)
        self.assertIn('result.status == "schema_missing"', handler)
        self.assertLess(
            handler.index('result.status == "schema_missing"'),
            handler.index("await asyncio.sleep(0)"),
            "the gate must short-circuit before the resume loop",
        )

    def test_reduction_numbers_are_untouched(self) -> None:
        """段位表与初始分数必须与迁移前逐字一致。"""
        self.assertEqual(
            ArenaWeeklyRankRepository.RANKS,
            ("青铜", "白银", "黄金", "铂金", "钻石", "王者"),
        )
        self.assertEqual(
            ArenaWeeklyRankRepository.INITIAL_SCORES,
            {"青铜": 1000, "白银": 1500, "黄金": 1900, "铂金": 2300, "钻石": 2700, "王者": 3200},
        )


if __name__ == "__main__":
    unittest.main()
