"""地图（map）玩家状态表的 schema 归属与 fail-closed 合同。

审计结论：`map_daily_limit`、`map_status`、`map_mission` 此前在仓库里**没有任何启动迁移**
创建过（只有测试自建），运行期靠仓储的 `ALTER TABLE ... ADD COLUMN` 惰性补列；
`map_cooldown`、`map_explore_status` 由 `map.003`/`map.005` 建出但只含部分列。

`map.018` 现在建出这五张表的完整列集；请求路径只校验，缺表或缺列返回 `schema_missing`：

- `save_daily_limit` / `upsert` / explore `save` / mission `save` → `{"status": "schema_missing"}`
- `set_cooldown`（返回 `str | None`）→ `None`
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.map.migrations import apply_map_player_state_schema
from nonebot_plugin_xiuxian_2.features.map.schema import (
    PLAYER_TABLES,
    SCHEMA_MISSING,
    columns,
    table_ready,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = ROOT / "nonebot_plugin_xiuxian_2" / "features" / "map" / "repository.py"


class MigrationOwnershipTests(unittest.TestCase):
    def test_migration_builds_every_player_state_table(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "player.db"
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_map_player_state_schema(uow)
            with DatabaseUnitOfWork(str(database), read_only=True) as uow:
                for table, required in PLAYER_TABLES.items():
                    self.assertTrue(table_ready(uow, table, required), table)

    def test_migration_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "player.db"
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_map_player_state_schema(uow)
            with DatabaseUnitOfWork(str(database), read_only=True) as uow:
                before = {table: columns(uow, table) for table in PLAYER_TABLES}
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_map_player_state_schema(uow)
            with DatabaseUnitOfWork(str(database), read_only=True) as uow:
                after = {table: columns(uow, table) for table in PLAYER_TABLES}
            self.assertEqual(before, after)

    def test_migration_backfills_legacy_partial_tables(self) -> None:
        """历史库只有部分列时，迁移补齐而不是丢弃既有行。"""
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "player.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE map_daily_limit(user_id TEXT PRIMARY KEY,date TEXT,"
                    "gather_count INTEGER,resource_total_count INTEGER)"
                )
                conn.execute("INSERT INTO map_daily_limit VALUES('u','2026-09-22',1,3)")
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_map_player_state_schema(uow)
            with DatabaseUnitOfWork(str(database), read_only=True) as uow:
                self.assertTrue(table_ready(uow, "map_daily_limit", PLAYER_TABLES["map_daily_limit"]))
                row = uow.query_one("SELECT date,gather_count FROM map_daily_limit WHERE user_id='u'")
            self.assertIsNotNone(row)
            assert row is not None
            self.assertEqual((row["date"], int(row["gather_count"])), ("2026-09-22", 1))


class NoRequestPathDdlTests(unittest.TestCase):
    def test_repository_never_creates_or_alters_tables(self) -> None:
        source = REPOSITORY.read_text(encoding="utf-8")
        self.assertNotIn("CREATE TABLE", source)
        self.assertNotIn("ALTER TABLE", source)
        self.assertIn("table_ready", source)
        self.assertIn("SCHEMA_MISSING", source)


class FailClosedTests(unittest.TestCase):
    def test_write_paths_report_schema_missing_without_writes(self) -> None:
        from nonebot_plugin_xiuxian_2.features.map.repository import (
            MapExploreStatusSqlWriteRepository,
            MapMissionSqlWriteRepository,
            MapProjectionSqlWriteRepository,
            MapStatusSqlWriteRepository,
        )

        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "player.db"
            with db_backend.transaction(database):
                pass

            self.assertEqual(
                MapProjectionSqlWriteRepository(database).save_daily_limit(
                    "u",
                    {"date": "2026-09-22", "gather_count": 1, "combat_count": 2,
                     "explore_count": 3, "resource_total_count": 6},
                )["status"],
                SCHEMA_MISSING,
            )
            self.assertIsNone(
                MapProjectionSqlWriteRepository(database).set_cooldown("u", "combat_cd_until", "x")
            )
            self.assertEqual(
                MapStatusSqlWriteRepository(database).upsert("u", "凡界", "一重天", "n1", ["n1"])["status"],
                SCHEMA_MISSING,
            )
            self.assertEqual(
                MapExploreStatusSqlWriteRepository(database).save(
                    "u",
                    {"running": 0, "node_type": "", "node_name": "", "start_time": "",
                     "duration_min": 0, "settlement": "", "max_duration_min": 0, "interval_min": 0},
                )["status"],
                SCHEMA_MISSING,
            )
            self.assertEqual(
                MapMissionSqlWriteRepository(database).save(
                    "u", {"date": "2026-09-22", "mission_type": "", "target": 0,
                          "claimed": 0, "settlement": ""},
                )["status"],
                SCHEMA_MISSING,
            )

            with db_backend.connection(database) as conn:
                tables = {
                    str(row[0])
                    for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                }
            self.assertEqual(tables, set(), "the request paths must not create any table")


class PostMigrationBehaviourTests(unittest.TestCase):
    def test_projection_and_status_write_after_migration(self) -> None:
        from nonebot_plugin_xiuxian_2.features.map.repository import (
            MapProjectionSqlWriteRepository,
            MapStatusSqlWriteRepository,
        )

        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "player.db"
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_map_player_state_schema(uow)

            projection = MapProjectionSqlWriteRepository(database)
            state = {"date": "2026-09-22", "gather_count": 2, "combat_count": 1,
                     "explore_count": 3, "resource_total_count": 6}
            self.assertEqual(projection.save_daily_limit("u", state), state)
            self.assertEqual(
                projection.set_cooldown("u", "combat_cd_until", "2026-09-22 10:00:00"),
                "2026-09-22 10:00:00",
            )

            status = MapStatusSqlWriteRepository(database).upsert("u", "凡界", "一重天", "n1", ["n1"])
            self.assertEqual(status["visited_nodes"], ["n1"])
            self.assertEqual(status["node_id"], "n1")


if __name__ == "__main__":
    unittest.main()
