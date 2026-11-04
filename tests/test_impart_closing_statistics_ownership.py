"""虚神界出关统计列的 schema 归属与 fail-closed 合同。

`closing_settlement_repository.settle` 原先在请求事务内对
`player_data.statistics` 执行 `ALTER TABLE ... ADD COLUMN`。本切片把三列交给
player DB 启动迁移 `impart_pk.005`，请求路径只读校验；缺表或缺列时在任何资产
写入之前返回 `schema_missing`。
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.impart_pk import migrations as impart_migrations
from nonebot_plugin_xiuxian_2.features.impart_pk.closing_settlement_repository import (
    ImpartClosingSettlementSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.impart_pk.statistics_columns import (
    CLOSING_SETTLEMENT_FIELDS,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_PATH = (
    ROOT
    / "nonebot_plugin_xiuxian_2"
    / "features"
    / "impart_pk"
    / "closing_settlement_repository.py"
)

SETTLE_ARGS = ("op", "u", "start", 100, 30, 20, 10, 60, 40, 50, 6, 999)


class Triple:
    """Game/impart/player triple shaped by the real startup migrations."""

    def __init__(self, root: Path) -> None:
        self.game = root / "game.db"
        self.impart = root / "impart.db"
        self.player = root / "player.db"
        for path in (self.game, self.impart, self.player):
            with db_backend.transaction(path):
                pass

    def migrate(self) -> "Triple":
        with DatabaseUnitOfWork(str(self.game), immediate=True) as uow:
            impart_migrations.apply_impart_pk_operations(uow)
        with DatabaseUnitOfWork(str(self.player), immediate=True) as uow:
            impart_migrations.apply_impart_pk_closing_statistics(uow)
        return self

    def seed(self) -> "Triple":
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, exp INTEGER, "
                "hp INTEGER, mp INTEGER, atk INTEGER, power INTEGER)"
            )
            conn.execute(
                "CREATE TABLE user_cd (user_id TEXT PRIMARY KEY, type INTEGER, "
                "create_time TEXT, scheduled_time TEXT)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES ('u',100,1,2,3,4)")
            conn.execute("INSERT INTO user_cd VALUES ('u',4,'start',NULL)")
        with db_backend.transaction(self.impart) as conn:
            conn.execute(
                "CREATE TABLE xiuxian_impart (user_id TEXT PRIMARY KEY, exp_day INTEGER)"
            )
            conn.execute("INSERT INTO xiuxian_impart VALUES ('u',30)")
        return self

    def state(self):
        with db_backend.connection(self.game) as conn:
            user = tuple(
                conn.execute(
                    "SELECT exp, hp, mp, atk, power FROM user_xiuxian"
                ).fetchone()
            )
            cd = tuple(
                conn.execute("SELECT type, create_time FROM user_cd").fetchone()
            )
        with db_backend.connection(self.impart) as conn:
            day = conn.execute("SELECT exp_day FROM xiuxian_impart").fetchone()[0]
        return user, cd, int(day)

    def statistics(self):
        with db_backend.connection(self.player) as conn:
            row = conn.execute(
                'SELECT "虚神界闭关时长","虚神界闭关修为","虚神界闭关祝福时长" '
                "FROM statistics WHERE user_id='u'"
            ).fetchone()
        return tuple(row) if row is not None else None

    def player_tables(self) -> set[str]:
        with db_backend.connection(self.player) as conn:
            return {
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }

    def receipts(self) -> int:
        with db_backend.connection(self.game) as conn:
            return int(
                conn.execute(
                    "SELECT COUNT(*) FROM impart_closing_operations"
                ).fetchone()[0]
            )


class ImpartClosingStatisticsOwnershipTests(unittest.TestCase):
    def test_migration_adds_the_columns_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            triple = Triple(Path(temp))
            rows: list = []
            for _ in range(2):
                with DatabaseUnitOfWork(str(triple.player), immediate=True) as uow:
                    impart_migrations.apply_impart_pk_closing_statistics(uow)
                    rows = uow.query_all('PRAGMA table_info("statistics")')
            columns = {str(row["name"]) for row in rows}
            self.assertTrue(set(CLOSING_SETTLEMENT_FIELDS).issubset(columns))
            self.assertIn("user_id", columns)

    def test_migration_is_routed_to_the_player_database_only(self) -> None:
        from nonebot_plugin_xiuxian_2.plugin import (
            build_migrations,
            migrations_for_database,
        )

        migrations = build_migrations()
        versions = [migration.version for migration in migrations]
        self.assertEqual(versions, sorted(set(versions)))
        self.assertIn("impart_pk.005", versions)
        player = [m.version for m in migrations_for_database(migrations, "player_db")]
        self.assertIn("impart_pk.005", player)
        for key in ("game_db", "trade_db", "impart_db", "message_db"):
            routed = [m.version for m in migrations_for_database(migrations, key)]
            self.assertNotIn("impart_pk.005", routed, key)

    def test_existing_statistics_rows_survive_the_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            triple = Triple(Path(temp))
            with db_backend.transaction(triple.player) as conn:
                conn.execute(
                    'CREATE TABLE statistics (user_id TEXT PRIMARY KEY, "虚神界闭关时长" INTEGER)'
                )
                conn.execute(
                    'INSERT INTO statistics(user_id, "虚神界闭关时长") VALUES (?, ?)',
                    ("u", 5),
                )
            with DatabaseUnitOfWork(str(triple.player), immediate=True) as uow:
                impart_migrations.apply_impart_pk_closing_statistics(uow)
            self.assertEqual(triple.statistics(), (5, 0, 0))

    def test_settle_fails_closed_without_the_statistics_table(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            triple = Triple(Path(temp)).seed()
            with DatabaseUnitOfWork(str(triple.game), immediate=True) as uow:
                impart_migrations.apply_impart_pk_operations(uow)
            repository = ImpartClosingSettlementSqlRepository(
                triple.game, triple.impart, triple.player
            )
            before = triple.state()
            result = repository.settle(*SETTLE_ARGS)
            self.assertEqual(result.status, "schema_missing")
            self.assertEqual(triple.state(), before)
            self.assertEqual(triple.receipts(), 0)
            self.assertNotIn("statistics", triple.player_tables())

    def test_half_built_statistics_table_is_rejected_without_alter(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            triple = Triple(Path(temp)).seed()
            with db_backend.transaction(triple.player) as conn:
                conn.execute("CREATE TABLE statistics (user_id TEXT PRIMARY KEY)")
                conn.execute("INSERT INTO statistics(user_id) VALUES ('u')")
            with DatabaseUnitOfWork(str(triple.game), immediate=True) as uow:
                impart_migrations.apply_impart_pk_operations(uow)
            repository = ImpartClosingSettlementSqlRepository(
                triple.game, triple.impart, triple.player
            )
            before = triple.state()
            result = repository.settle(*SETTLE_ARGS)
            self.assertEqual(result.status, "schema_missing")
            self.assertEqual(triple.state(), before)
            self.assertEqual(triple.receipts(), 0)
            with db_backend.connection(triple.player) as conn:
                columns = {
                    str(row[1])
                    for row in conn.execute(
                        "PRAGMA table_info('statistics')"
                    ).fetchall()
                }
            self.assertEqual(columns, {"user_id"})

    def test_settlement_reads_and_writes_after_the_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            triple = Triple(Path(temp)).migrate().seed()
            repository = ImpartClosingSettlementSqlRepository(
                triple.game, triple.impart, triple.player
            )

            first = repository.settle(*SETTLE_ARGS)
            self.assertEqual(first.status, "applied")
            self.assertEqual(triple.state(), ((120, 40, 50, 6, 999), (0, "0"), 20))
            self.assertEqual(triple.statistics(), (60, 20, 10))
            self.assertEqual(triple.receipts(), 1)

            replay = repository.settle(*SETTLE_ARGS)
            self.assertEqual(replay.status, "duplicate")
            self.assertEqual(triple.statistics(), (60, 20, 10))
            self.assertEqual(triple.receipts(), 1)

            stale_args = ("op2", "u", "bad", 100, 30, 20, 10, 60, 40, 50, 6, 999)
            before = triple.state()
            stale = repository.settle(*stale_args)
            self.assertEqual(stale.status, "state_changed")
            self.assertEqual(triple.state(), before)
            self.assertEqual(triple.statistics(), (60, 20, 10))
            self.assertEqual(triple.receipts(), 1)

    def test_request_path_has_no_ddl_and_uses_the_shared_contract(self) -> None:
        source = REPOSITORY_PATH.read_text(encoding="utf-8")
        self.assertNotIn("CREATE TABLE", source)
        self.assertNotIn("ALTER TABLE", source)
        self.assertIn("statistics_columns_ready", source)
        self.assertIn("CLOSING_SETTLEMENT_FIELDS", source)


if __name__ == "__main__":
    unittest.main()
