"""虚神界回执/投影表的启动迁移与请求路径无 DDL（定向合同）。

六条虚神界动作原先都在请求事务内 ``CREATE TABLE IF NOT EXISTS``：
四条回执表写在 game 主库事务、战斗批回执写在 impart 主库事务、
投影命令的四张表写在 player 主库事务。本切片把它们移入启动迁移
（``impart_pk.002`` game 侧、``impart_pk.003`` player 侧、``impart_pk.004`` impart 侧），
请求路径只校验 schema，缺表/缺列返回 ``schema_missing`` 且零变化。
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.impart.migrations import (
    apply_impart_exact_crystal_balances,
)
from nonebot_plugin_xiuxian_2.features.impart_pk.battle_repository import (
    ImpartBattleBatchSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.impart_pk.closing_enter_repository import (
    ImpartClosingEnterSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.impart_pk.closing_settlement_repository import (
    ImpartClosingSettlementSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.impart_pk.explore_repository import (
    ImpartExploreSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.impart_pk.migrations import (
    GAME_OPERATIONS_TABLE_COLUMNS,
    IMPART_OPERATIONS_TABLE_COLUMNS,
    OPERATIONS_TABLE_COLUMNS,
    PLAYER_OPERATIONS_TABLE_COLUMNS,
    apply_impart_pk_closing_statistics,
    apply_impart_pk_impart_operations,
    apply_impart_pk_impart_statistics,
    apply_impart_pk_operations,
    apply_impart_pk_player_operations,
    operations_ready,
)
from nonebot_plugin_xiuxian_2.features.impart_pk.project_join_repository import (
    ImpartProjectJoinSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.impart_pk.training_repository import (
    ImpartTrainingSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
FACADE = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_impart_pk" / "__init__.py"
FEATURE = ROOT / "nonebot_plugin_xiuxian_2" / "features" / "impart_pk"


class Fixture:
    """Genuine-enough schema: 玩家/冷却 on game, 虚神界状态/日限 on player, 传承 on impart."""

    def __init__(self, root: Path) -> None:
        self.game = root / "game.db"
        self.player = root / "player.db"
        self.impart = root / "impart.db"

    def build(self) -> "Fixture":
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,root_type TEXT,exp INTEGER,hp INTEGER,mp INTEGER,atk INTEGER,power INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u','天灵根',100,1,2,3,4)")
            conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            conn.execute("INSERT INTO user_cd VALUES('u',0,'',NULL)")
        with db_backend.transaction(self.impart) as conn:
            conn.execute("CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,exp_day INTEGER,impart_lv INTEGER,stone_num INTEGER)")
            conn.execute("INSERT INTO xiuxian_impart VALUES('u',100,2,0)")
            conn.execute("INSERT INTO xiuxian_impart VALUES('v',1,0,0)")
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE impart_pk_daily(user_id TEXT PRIMARY KEY,impart_num INTEGER,exp_used INTEGER,exp_count INTEGER,exp_load INTEGER,exp_gain INTEGER)")
            conn.execute("INSERT INTO impart_pk_daily VALUES('u',3,0,0,0,0)")
        return self

    def migrate(self, *, game: bool = True, player: bool = True, impart: bool = True) -> "Fixture":
        if game:
            with DatabaseUnitOfWork(self.game, immediate=True) as uow:
                apply_impart_pk_operations(uow)
        if player:
            with DatabaseUnitOfWork(self.player, immediate=True) as uow:
                apply_impart_pk_player_operations(uow)
                apply_impart_pk_closing_statistics(uow)
                apply_impart_pk_impart_statistics(uow)
        if impart:
            with DatabaseUnitOfWork(self.impart, immediate=True) as uow:
                apply_impart_pk_impart_operations(uow)
                apply_impart_exact_crystal_balances(uow)
        return self

    def tables(self, database: Path) -> set[str]:
        with db_backend.connection(database) as conn:
            return {
                str(row[0])
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }

    def game_scalar(self, sql: str):
        with db_backend.connection(self.game) as conn:
            row = conn.execute(sql).fetchone()
        return None if row is None else row[0]

    def statistics_exists(self) -> bool:
        return "statistics" in self.tables(self.player)


class MigrationShapeTests(unittest.TestCase):
    def test_ledgers_are_split_by_the_database_their_writer_opens(self) -> None:
        self.assertEqual(
            set(GAME_OPERATIONS_TABLE_COLUMNS),
            {
                "impart_closing_enter_operations",
                "impart_closing_operations",
                "impart_explore_operations",
                "impart_training_operations",
            },
        )
        # 战斗批回执写在 impart 主库事务里，不能落到 game 库
        self.assertEqual(set(IMPART_OPERATIONS_TABLE_COLUMNS), {"impart_battle_batch_operations"})
        self.assertEqual(
            set(PLAYER_OPERATIONS_TABLE_COLUMNS),
            {
                "impart_pk_state",
                "impart_project_members",
                "impart_project_meta",
                "impart_project_join_operations",
            },
        )
        self.assertEqual(
            set(OPERATIONS_TABLE_COLUMNS),
            set(GAME_OPERATIONS_TABLE_COLUMNS)
            | set(IMPART_OPERATIONS_TABLE_COLUMNS)
            | set(PLAYER_OPERATIONS_TABLE_COLUMNS),
        )

    def test_each_migration_creates_only_its_own_ledgers(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build().migrate()
            game_tables = fixture.tables(fixture.game)
            player_tables = fixture.tables(fixture.player)
            impart_tables = fixture.tables(fixture.impart)
            for table, expected in GAME_OPERATIONS_TABLE_COLUMNS.items():
                self.assertIn(table, game_tables, table)
                with db_backend.connection(fixture.game) as conn:
                    columns = {str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")')}
                self.assertTrue(expected.issubset(columns), table)
            self.assertIn("impart_battle_batch_operations", impart_tables)
            self.assertNotIn("impart_battle_batch_operations", game_tables)
            for table, expected in PLAYER_OPERATIONS_TABLE_COLUMNS.items():
                self.assertIn(table, player_tables, table)
                with db_backend.connection(fixture.player) as conn:
                    columns = {str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")')}
                self.assertTrue(expected.issubset(columns), table)
            for table in ("impart_closing_enter_operations", "impart_closing_operations",
                          "impart_explore_operations", "impart_training_operations"):
                self.assertNotIn(table, player_tables, table)

    def test_versions_are_recorded_where_they_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build().migrate()
            for database, version in (
                (fixture.game, "impart_pk.002"),
                (fixture.player, "impart_pk.003"),
                (fixture.impart, "impart_pk.004"),
            ):
                with db_backend.connection(database) as conn:
                    versions = [
                        str(row[0])
                        for row in conn.execute("SELECT version FROM impart_pk_feature_migrations")
                    ]
                self.assertIn(version, versions, version)

    def test_legacy_lazy_receipt_rows_survive_the_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            with db_backend.transaction(fixture.game) as conn:
                conn.execute(
                    "CREATE TABLE impart_closing_operations(operation_id TEXT PRIMARY KEY,"
                    "payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TIMESTAMP)"
                )
                conn.execute("INSERT INTO impart_closing_operations VALUES('legacy','[\"u\"]','[]','2026-01-01')")
            fixture.migrate()
            with db_backend.connection(fixture.game) as conn:
                rows = [
                    str(row[0])
                    for row in conn.execute("SELECT operation_id FROM impart_closing_operations")
                ]
            self.assertEqual(rows, ["legacy"])

    def test_operations_ready_rejects_a_partial_table(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            with db_backend.transaction(fixture.game) as conn:
                conn.execute("CREATE TABLE impart_explore_operations(operation_id TEXT PRIMARY KEY)")
            with DatabaseUnitOfWork(fixture.game, read_only=True) as uow:
                self.assertFalse(operations_ready(uow, "impart_explore_operations"))
                self.assertFalse(operations_ready(uow, "impart_training_operations"))


class RequestPathNoDdlTests(unittest.TestCase):
    """Every entry must fail closed and leave state untouched without the migration."""

    def test_closing_enter_reports_schema_missing_and_leaves_cooldown(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            result = ImpartClosingEnterSqlRepository(fixture.game, fixture.player).enter("c", "u", "2026-01-01")
            self.assertEqual(result.status, "schema_missing")
            self.assertEqual(int(fixture.game_scalar("SELECT type FROM user_cd WHERE user_id='u'")), 0)
            self.assertNotIn("impart_closing_enter_operations", fixture.tables(fixture.game))
            self.assertFalse(fixture.statistics_exists())

    def test_closing_settlement_reports_schema_missing_and_keeps_exp(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            with db_backend.transaction(fixture.game) as conn:
                conn.execute("UPDATE user_cd SET type=4,create_time='start' WHERE user_id='u'")
            result = ImpartClosingSettlementSqlRepository(fixture.game, fixture.impart, fixture.player).settle(
                "s", "u", "start", 100, 100, 20, 10, 60, 40, 50, 6, 999
            )
            self.assertEqual(result.status, "schema_missing")
            self.assertEqual(int(fixture.game_scalar("SELECT exp FROM user_xiuxian WHERE user_id='u'")), 100)
            self.assertEqual(int(fixture.game_scalar("SELECT type FROM user_cd WHERE user_id='u'")), 4)
            with db_backend.connection(fixture.impart) as conn:
                self.assertEqual(int(conn.execute("SELECT exp_day FROM xiuxian_impart WHERE user_id='u'").fetchone()[0]), 100)

    def test_explore_reports_schema_missing_and_keeps_level(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            result = ImpartExploreSqlRepository(fixture.game, fixture.impart, fixture.player).settle(
                "e", "u", event_type="up", expected_exp_day=100, expected_impart_lv=2,
                expected_impart_num=3, time_cost=5, new_impart_lv=3,
            )
            self.assertEqual(result.status, "schema_missing")
            with db_backend.connection(fixture.impart) as conn:
                row = conn.execute("SELECT exp_day,impart_lv FROM xiuxian_impart WHERE user_id='u'").fetchone()
            self.assertEqual((int(row[0]), int(row[1])), (100, 2))

    def test_training_reports_schema_missing_and_keeps_exp_and_daily(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            result = ImpartTrainingSqlRepository(fixture.game, fixture.impart, fixture.player).settle(
                "t", "u", expected_exp=10, expected_exp_day=20,
                expected_daily={"exp_used": 0, "exp_count": 0, "exp_load": 0, "exp_gain": 0},
                exp_cost=2, exp_gain=5, exp_load_gain=1, power=2,
            )
            self.assertEqual(result.status, "schema_missing")
            self.assertEqual(int(fixture.game_scalar("SELECT exp FROM user_xiuxian WHERE user_id='u'")), 100)
            with db_backend.connection(fixture.player) as conn:
                row = conn.execute("SELECT exp_used,exp_gain FROM impart_pk_daily WHERE user_id='u'").fetchone()
            self.assertEqual((int(row[0]), int(row[1])), (0, 0))

    def test_battle_reports_schema_missing_and_keeps_pk_num(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build().migrate(game=False, impart=False)
            with db_backend.transaction(fixture.player) as conn:
                conn.execute("INSERT INTO impart_pk_state(user_id,pk_num,win_num) VALUES('u',3,0)")
            result = ImpartBattleBatchSqlRepository(fixture.impart, fixture.player).settle(
                "b", "u", 3, 1, 0, 2
            )
            self.assertEqual(result.status, "schema_missing")
            with db_backend.connection(fixture.player) as conn:
                row = conn.execute("SELECT pk_num,win_num FROM impart_pk_state WHERE user_id='u'").fetchone()
            self.assertEqual((int(row[0]), int(row[1])), (3, 0))
            with db_backend.connection(fixture.impart) as conn:
                self.assertEqual(int(conn.execute("SELECT stone_num FROM xiuxian_impart WHERE user_id='u'").fetchone()[0]), 0)

    def test_project_join_reports_schema_missing_and_creates_no_statistics(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            result = ImpartProjectJoinSqlRepository(fixture.player).join(
                "j", "u", legacy_pk_num=7, legacy_members=[]
            )
            self.assertEqual(result.status, "schema_missing")
            self.assertFalse(fixture.statistics_exists())
            self.assertNotIn("impart_project_members", fixture.tables(fixture.player))

    def test_projection_needs_all_four_tables(self) -> None:
        """A single missing projection table must not let the command half-run."""
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build().migrate(player=True)
            with db_backend.transaction(fixture.player) as conn:
                conn.execute("DROP TABLE impart_project_meta")
            result = ImpartProjectJoinSqlRepository(fixture.player).join(
                "j", "u", legacy_pk_num=7, legacy_members=[]
            )
            self.assertEqual(result.status, "schema_missing")
            with db_backend.connection(fixture.player) as conn:
                count = int(conn.execute("SELECT COUNT(*) FROM impart_project_members").fetchone()[0])
            self.assertEqual(count, 0)


class PostMigrationBehaviourTests(unittest.TestCase):
    def test_closing_enter_then_settlement_work_after_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build().migrate()
            enter = ImpartClosingEnterSqlRepository(fixture.game, fixture.player).enter("c", "u", "2026-01-01")
            self.assertEqual((enter.status, enter.entry_count), ("applied", 1))
            replay = ImpartClosingEnterSqlRepository(fixture.game, fixture.player).enter("c", "u", "2026-01-01")
            self.assertEqual(replay.status, "duplicate")
            settle = ImpartClosingSettlementSqlRepository(fixture.game, fixture.impart, fixture.player).settle(
                "s", "u", "2026-01-01", 100, 100, 20, 10, 60, 40, 50, 6, 999
            )
            self.assertEqual((settle.status, settle.exp_gain), ("applied", 20))
            replay_settle = ImpartClosingSettlementSqlRepository(fixture.game, fixture.impart, fixture.player).settle(
                "s", "u", "2026-01-01", 100, 100, 999, 10, 60, 40, 50, 6, 999
            )
            self.assertEqual(replay_settle.status, "duplicate")

    def test_project_join_works_and_counts_after_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build().migrate(player=True)
            repo = ImpartProjectJoinSqlRepository(fixture.player, capacity=1)
            first = repo.join("j", "u", legacy_pk_num=7, legacy_members=[])
            duplicate = repo.join("j", "u", legacy_pk_num=7, legacy_members=[])
            other = repo.join("k", "v", legacy_pk_num=7, legacy_members=[])
            self.assertEqual(
                (first.status, duplicate.status, other.status),
                ("applied", "duplicate", "capacity_full"),
            )
            with db_backend.connection(fixture.player) as conn:
                value = int(
                    conn.execute(
                        'SELECT "虚神界投影次数" FROM statistics WHERE user_id=\'u\''
                    ).fetchone()[0]
                )
            self.assertEqual(value, 1)

    def test_battle_batch_works_after_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build().migrate()
            with db_backend.transaction(fixture.player) as conn:
                conn.execute("INSERT INTO impart_pk_state(user_id,pk_num,win_num) VALUES('u',3,0)")
            repo = ImpartBattleBatchSqlRepository(fixture.impart, fixture.player)
            first = repo.settle("b", "u", 3, 1, 1, 2)
            duplicate = repo.settle("b", "u", 3, 1, 1, 2)
            self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
            with db_backend.connection(fixture.impart) as conn:
                self.assertEqual(int(conn.execute("SELECT stone_num FROM xiuxian_impart WHERE user_id='u'").fetchone()[0]), 2)


class FacadeContractTests(unittest.TestCase):
    def test_repositories_never_create_tables(self) -> None:
        for name in (
            "battle_repository.py",
            "closing_enter_repository.py",
            "closing_settlement_repository.py",
            "explore_repository.py",
            "training_repository.py",
            "project_join_repository.py",
        ):
            text = (FEATURE / name).read_text(encoding="utf-8")
            self.assertNotIn("CREATE TABLE", text, name)
            self.assertIn("operations_ready", text, name)
            self.assertIn("'schema_missing'", text, name)

    def test_facade_surfaces_schema_missing_to_the_player(self) -> None:
        source = FACADE.read_text(encoding="utf-8")
        self.assertEqual(source.count("schema_missing"), 6)
        for handler in (
            "impart_pk_project_",
            "impart_pk_go_",
            "impart_pk_exp_",
            "impart_pk_in_closing_",
            "impart_pk_out_closing_",
        ):
            self.assertIn(f"async def {handler}", source)

    def test_statistics_column_helper_is_still_the_only_column_writer(self) -> None:
        """The documented `statistics` column contract must survive this slice."""
        helper = (FEATURE / "statistics_columns.py").read_text(encoding="utf-8")
        self.assertIn("ALTER TABLE", helper)
        for name in ("project_join_repository.py", "closing_settlement_repository.py"):
            text = (FEATURE / name).read_text(encoding="utf-8")
            # 两种情况都不得再自建 statistics 表（no-op 建表会掩盖缺列）
            self.assertNotIn("CREATE TABLE IF NOT EXISTS statistics", text, name)
        project_join = (FEATURE / "project_join_repository.py").read_text(encoding="utf-8")
        self.assertNotIn("ALTER TABLE", project_join, "project_join must delegate column work")

    def test_json_payload_shape_is_unchanged(self) -> None:
        """Receipt payloads stay the same compact JSON lists the replay path reads."""
        text = (FEATURE / "closing_enter_repository.py").read_text(encoding="utf-8")
        self.assertIn("json.dumps([user_id],separators=(',',':'))", text)
        self.assertIn("json.loads(old['result_json'])", text)


if __name__ == "__main__":
    unittest.main()
