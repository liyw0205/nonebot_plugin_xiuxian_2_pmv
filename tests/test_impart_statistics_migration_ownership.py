"""虚神界剩余统计列（`impart_pk.006`）的 schema 归属与 fail-closed 合同。

`closing_enter_repository` 与 `project_join_repository` 原先经共享 helper 在请求
事务内建 `statistics` 表并补 `虚神界闭关次数` / `虚神界投影次数`。本切片把两列交给
player DB 启动迁移 `impart_pk.006`，请求路径只读校验并改用不执行 DDL 的累加函数。
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.impart_pk import migrations as impart_migrations
from nonebot_plugin_xiuxian_2.features.impart_pk.closing_enter_repository import (
    ImpartClosingEnterSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.impart_pk.project_join_repository import (
    ImpartProjectJoinSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.impart_pk.statistics_columns import (
    IMPART_STATISTICS_FIELDS,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
FEATURE = ROOT / "nonebot_plugin_xiuxian_2" / "features" / "impart_pk"


class Pair:
    def __init__(self, root: Path) -> None:
        self.game = root / "game.db"
        self.player = root / "player.db"
        for path in (self.game, self.player):
            with db_backend.transaction(path):
                pass

    def seed(self) -> "Pair":
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, root_type TEXT)"
            )
            conn.execute(
                "CREATE TABLE user_cd (user_id TEXT PRIMARY KEY, type INTEGER, "
                "create_time TEXT, scheduled_time TEXT)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES ('u', '天灵根')")
            conn.execute("INSERT INTO user_cd VALUES ('u', 0, '', NULL)")
        return self

    def migrate(self, *, statistics: bool = True) -> "Pair":
        with DatabaseUnitOfWork(str(self.game), immediate=True) as uow:
            impart_migrations.apply_impart_pk_operations(uow)
        with DatabaseUnitOfWork(str(self.player), immediate=True) as uow:
            impart_migrations.apply_impart_pk_player_operations(uow)
            if statistics:
                impart_migrations.apply_impart_pk_impart_statistics(uow)
        return self

    def stat(self, field: str) -> int | None:
        with db_backend.connection(self.player) as conn:
            row = conn.execute(
                f'SELECT "{field}" FROM statistics WHERE user_id=?', ("u",)
            ).fetchone()
        return None if row is None else row[0]

    def tables(self) -> set[str]:
        with db_backend.connection(self.player) as conn:
            return {
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }

    def cd_state(self) -> tuple[int, str]:
        with db_backend.connection(self.game) as conn:
            row = conn.execute(
                "SELECT type, create_time FROM user_cd WHERE user_id='u'"
            ).fetchone()
        return int(row[0]), str(row[1])

    def receipt_count(self, table: str, database: Path) -> int:
        with db_backend.connection(database) as conn:
            return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


class ImpartStatisticsMigrationOwnershipTests(unittest.TestCase):
    def test_migration_adds_both_columns_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp))
            rows: list = []
            for _ in range(2):
                with DatabaseUnitOfWork(str(pair.player), immediate=True) as uow:
                    impart_migrations.apply_impart_pk_impart_statistics(uow)
                    rows = uow.query_all('PRAGMA table_info("statistics")')
            columns = {str(row["name"]) for row in rows}
            self.assertTrue(set(IMPART_STATISTICS_FIELDS).issubset(columns))
            self.assertIn("user_id", columns)

    def test_migration_is_routed_to_the_player_database_only(self) -> None:
        from nonebot_plugin_xiuxian_2.plugin import (
            build_migrations,
            migrations_for_database,
        )

        migrations = build_migrations()
        versions = [migration.version for migration in migrations]
        self.assertEqual(versions, sorted(set(versions)))
        self.assertIn("impart_pk.006", versions)
        player = [m.version for m in migrations_for_database(migrations, "player_db")]
        self.assertIn("impart_pk.006", player)
        for key in ("game_db", "trade_db", "impart_db", "message_db"):
            routed = [m.version for m in migrations_for_database(migrations, key)]
            self.assertNotIn("impart_pk.006", routed, key)

    def test_existing_rows_and_columns_survive(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp))
            with db_backend.transaction(pair.player) as conn:
                conn.execute(
                    'CREATE TABLE statistics (user_id TEXT PRIMARY KEY, "虚神界闭关次数" INTEGER)'
                )
                conn.execute(
                    'INSERT INTO statistics(user_id, "虚神界闭关次数") VALUES (?, 5)',
                    ("u",),
                )
            with DatabaseUnitOfWork(str(pair.player), immediate=True) as uow:
                impart_migrations.apply_impart_pk_impart_statistics(uow)
            self.assertEqual(pair.stat("虚神界闭关次数"), 5)
            self.assertEqual(pair.stat("虚神界投影次数"), 0)

    def test_closing_enter_fails_closed_without_the_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).seed().migrate(statistics=False)
            result = ImpartClosingEnterSqlRepository(pair.game, pair.player).enter(
                "c1", "u", "2026-01-01"
            )
            self.assertEqual(result.status, "schema_missing")
            self.assertEqual(pair.cd_state(), (0, ""))
            self.assertEqual(
                pair.receipt_count("impart_closing_enter_operations", pair.game), 0
            )
            self.assertNotIn("statistics", pair.tables())

    def test_project_join_fails_closed_without_the_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).seed().migrate(statistics=False)
            result = ImpartProjectJoinSqlRepository(pair.player).join(
                "j1", "u", legacy_pk_num=7, legacy_members=[]
            )
            self.assertEqual(result.status, "schema_missing")
            self.assertEqual(
                pair.receipt_count("impart_project_members", pair.player), 0
            )
            self.assertEqual(
                pair.receipt_count("impart_project_join_operations", pair.player), 0
            )
            self.assertNotIn("statistics", pair.tables())

    def test_both_paths_read_and_write_after_the_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).seed().migrate()
            enter = ImpartClosingEnterSqlRepository(pair.game, pair.player)
            first = enter.enter("c1", "u", "2026-01-01")
            self.assertEqual((first.status, first.entry_count), ("applied", 1))
            replay = enter.enter("c1", "u", "2026-01-01")
            self.assertEqual(replay.status, "duplicate")
            self.assertEqual(pair.stat("虚神界闭关次数"), 1)

            join = ImpartProjectJoinSqlRepository(pair.player)
            joined = join.join("j1", "u", legacy_pk_num=7, legacy_members=[])
            self.assertEqual(joined.status, "applied")
            replay_join = join.join("j1", "u", legacy_pk_num=7, legacy_members=[])
            self.assertEqual(replay_join.status, "duplicate")
            self.assertEqual(pair.stat("虚神界投影次数"), 1)

    def test_request_paths_have_no_ddl_and_use_verified_increment(self) -> None:
        for name in ("closing_enter_repository.py", "project_join_repository.py"):
            source = (FEATURE / name).read_text(encoding="utf-8")
            self.assertNotIn("CREATE TABLE", source, name)
            self.assertNotIn("ALTER TABLE", source, name)
            self.assertIn("statistics_columns_ready", source, name)
            self.assertIn("increment_statistics_verified", source, name)


if __name__ == "__main__":
    unittest.main()
