"""本命法宝回执表的启动迁移与请求路径无 DDL（定向合同）。

六张回执表（觉醒/重塑/养成/升阶/铭刻/遗忘）此前都在请求事务里
``CREATE TABLE IF NOT EXISTS``：觉醒写在 player DB，另五张写在附库后的 game DB。
本切片把它们移入启动迁移（``natal_treasure.002`` game 侧、``natal_treasure.003``
player 侧），请求路径只校验 schema，缺表/缺列返回 ``schema_missing`` 且零变化。
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.natal_treasure.awaken_repository import (
    NatalAwakenSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.natal_treasure.engraving_repository import (
    NatalEngravingSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.natal_treasure.effect_upgrade_repository import (
    NatalEffectUpgradeSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.natal_treasure.forget_repository import (
    NatalForgetSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.natal_treasure.migrations import (
    GAME_OPERATIONS_TABLE_COLUMNS,
    GAME_OPERATIONS_TABLE_DDL,
    OPERATIONS_TABLE_COLUMNS,
    PLAYER_OPERATIONS_TABLE_COLUMNS,
    apply_natal_treasure_operations,
    apply_natal_treasure_player_operations,
)
from nonebot_plugin_xiuxian_2.features.natal_treasure.reawaken_repository import (
    NatalReawakenSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.natal_treasure.training_repository import (
    NatalTrainingSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
FACADE = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_natal_treasure" / "__init__.py"
FEATURE = ROOT / "nonebot_plugin_xiuxian_2" / "features" / "natal_treasure"

TREASURE_COLUMNS = (
    '"form" INTEGER, "name" TEXT, "level" INTEGER, "exp" INTEGER, "max_exp" INTEGER, '
    '"effect1_type" INTEGER, "effect1_base_value" REAL, "effect1_level" INTEGER, '
    '"effect2_type" INTEGER, "effect2_base_value" REAL, "effect2_level" INTEGER, '
    '"effect3_type" INTEGER, "effect3_base_value" REAL, "effect3_level" INTEGER'
)


class Fixture:
    """Minimal genuine schema: back + user_xiuxian on game, natal_treasure on player."""

    def __init__(self, root: Path) -> None:
        self.game = root / "game.db"
        self.player = root / "player.db"

    def build(self) -> "Fixture":
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))"
            )
            uow.execute("INSERT INTO back VALUES('u',20009,'神秘经书','神物',3,0)")
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER NOT NULL)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u',10000000)")
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(f"CREATE TABLE natal_treasure(user_id TEXT PRIMARY KEY,{TREASURE_COLUMNS})")
            uow.execute(
                "INSERT INTO natal_treasure(user_id,form,name,level,exp,max_exp,"
                "effect1_type,effect1_base_value,effect1_level) VALUES('u',1,'旧法宝',3,0,100,11,0.1,1)"
            )
        return self

    def migrate(self, *, game: bool = True, player: bool = True) -> "Fixture":
        if game:
            with DatabaseUnitOfWork(self.game, immediate=True) as uow:
                apply_natal_treasure_operations(uow)
        if player:
            with DatabaseUnitOfWork(self.player, immediate=True) as uow:
                apply_natal_treasure_player_operations(uow)
        return self

    def tables(self, database: Path) -> set[str]:
        with db_backend.connection(database) as conn:
            return {
                str(row[0])
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }

    def back_quantity(self) -> int:
        with db_backend.connection(self.game) as conn:
            return int(conn.execute("SELECT goods_num FROM back WHERE goods_id=20009").fetchone()[0])


class MigrationShapeTests(unittest.TestCase):
    def test_game_side_covers_five_ledgers_player_side_covers_awaken(self) -> None:
        self.assertEqual(
            set(GAME_OPERATIONS_TABLE_COLUMNS),
            {
                "natal_reawaken_operations",
                "natal_training_operations",
                "natal_effect_upgrade_operations",
                "natal_engraving_operations",
                "natal_forget_operations",
            },
        )
        self.assertEqual(set(PLAYER_OPERATIONS_TABLE_COLUMNS), {"natal_awaken_operations"})
        self.assertEqual(set(OPERATIONS_TABLE_COLUMNS), set(GAME_OPERATIONS_TABLE_COLUMNS) | set(PLAYER_OPERATIONS_TABLE_COLUMNS))
        self.assertEqual(len(GAME_OPERATIONS_TABLE_DDL), len(GAME_OPERATIONS_TABLE_COLUMNS))

    def test_migration_creates_the_game_ledgers_with_every_read_column(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build().migrate(player=False)
            tables = fixture.tables(fixture.game)
            for table, expected in GAME_OPERATIONS_TABLE_COLUMNS.items():
                self.assertIn(table, tables, table)
                with db_backend.connection(fixture.game) as conn:
                    columns = {str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")')}
                self.assertTrue(expected.issubset(columns), table)
            self.assertNotIn("natal_awaken_operations", tables)
            with db_backend.connection(fixture.game) as conn:
                versions = [
                    str(row[0])
                    for row in conn.execute("SELECT version FROM natal_treasure_feature_migrations")
                ]
            self.assertIn("natal_treasure.002", versions)

    def test_migration_creates_the_player_awaken_ledger_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build().migrate(game=False)
            tables = fixture.tables(fixture.player)
            self.assertIn("natal_awaken_operations", tables)
            for table in GAME_OPERATIONS_TABLE_COLUMNS:
                self.assertNotIn(table, tables, table)
            with db_backend.connection(fixture.player) as conn:
                versions = [
                    str(row[0])
                    for row in conn.execute("SELECT version FROM natal_treasure_feature_migrations")
                ]
            self.assertIn("natal_treasure.003", versions)

    def test_legacy_lazy_receipt_rows_survive_the_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            with db_backend.transaction(fixture.game) as conn:
                conn.execute(
                    "CREATE TABLE natal_training_operations(operation_id TEXT PRIMARY KEY,"
                    "user_id TEXT,requested_exp INTEGER,base_cost INTEGER,growth_rate REAL,"
                    "max_level INTEGER,max_exp_base INTEGER,max_exp_growth INTEGER,"
                    "exp_added INTEGER,stone_cost INTEGER,level INTEGER,exp INTEGER,max_exp INTEGER)"
                )
                conn.execute("INSERT INTO natal_training_operations VALUES('legacy','u',5,1,0.5,10,100,50,5,1,0,5,100)")
            fixture.migrate()
            with db_backend.connection(fixture.game) as conn:
                rows = [str(row[0]) for row in conn.execute("SELECT operation_id FROM natal_training_operations")]
            self.assertEqual(rows, ["legacy"])


class RequestPathNoDdlTests(unittest.TestCase):
    """Every entry must fail closed and leave state untouched without the migration."""

    def test_awaken_reports_schema_missing_and_writes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            repo = NatalAwakenSqlRepository(fixture.player)
            result = repo.awaken(
                "h1", "u", 3, {1: (0.1, 0.2), 2: (0.3, 0.4)},
                {1: ("一号",), 2: ("二号",)}, {2}, 7,
            )
            self.assertEqual(result["status"], "schema_missing")
            self.assertNotIn("natal_awaken_operations", fixture.tables(fixture.player))
            with db_backend.connection(fixture.player) as conn:
                form = conn.execute("SELECT form FROM natal_treasure WHERE user_id='u'").fetchone()[0]
            self.assertEqual(int(form), 1, "an unawakened guard must not overwrite live state")

    def test_reawaken_reports_schema_missing_and_keeps_scripture(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            before = fixture.back_quantity()
            result = NatalReawakenSqlRepository(fixture.game, fixture.player).reawaken(
                "h1", "u", 20009, "神秘经书", "神物", 1, 3, 1000,
                {1: (0.1, 0.2), 2: (0.3, 0.4)}, {1: ("一号",), 2: ("二号",)}, {2}, 7,
            )
            self.assertEqual(result["status"], "schema_missing")
            self.assertEqual(fixture.back_quantity(), before)
            self.assertNotIn("natal_reawaken_operations", fixture.tables(fixture.game))

    def test_training_reports_schema_missing_and_keeps_stone(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            result = NatalTrainingSqlRepository(fixture.game, fixture.player).train(
                "h1", "u", 5, base_cost=1000000, growth_rate=0.5,
                max_level=10, max_exp_base=100, max_exp_growth=50,
            )
            self.assertEqual(result["status"], "schema_missing")
            with db_backend.connection(fixture.game) as conn:
                stone = int(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id='u'").fetchone()[0])
            self.assertEqual(stone, 10000000)

    def test_effect_upgrade_reports_schema_missing_and_keeps_scripture(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            before = fixture.back_quantity()
            result = NatalEffectUpgradeSqlRepository(fixture.game, fixture.player).upgrade(
                "h1", "u", 20009, 1, 3, 5, 7
            )
            self.assertEqual(result["status"], "schema_missing")
            self.assertEqual(fixture.back_quantity(), before)

    def test_engraving_reports_schema_missing_and_keeps_scripture(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            before = fixture.back_quantity()
            result = NatalEngravingSqlRepository(fixture.game, fixture.player).engrave(
                "h1", "u", 20009, 1, 3, {1: (0.1, 0.2), 2: (0.3, 0.4)}, {2}, 7
            )
            self.assertEqual(result["status"], "schema_missing")
            self.assertEqual(fixture.back_quantity(), before)

    def test_forget_reports_schema_missing_and_keeps_scripture(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            before = fixture.back_quantity()
            result = NatalForgetSqlRepository(fixture.game, fixture.player).forget(
                "h1", "u", 1, 20009, "神秘经书", "神物", 1, 3, 1000
            )
            self.assertEqual(result["status"], "schema_missing")
            self.assertEqual(fixture.back_quantity(), before)

    def test_partial_legacy_ledger_is_rejected(self) -> None:
        """A half-built table must not be trusted just because it exists."""
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build()
            with db_backend.transaction(fixture.game) as conn:
                conn.execute("CREATE TABLE natal_training_operations(operation_id TEXT PRIMARY KEY)")
            result = NatalTrainingSqlRepository(fixture.game, fixture.player).train(
                "h1", "u", 5, base_cost=1000000, growth_rate=0.5,
                max_level=10, max_exp_base=100, max_exp_growth=50,
            )
            self.assertEqual(result["status"], "schema_missing")

    def test_after_migration_the_same_calls_work(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Fixture(Path(temp)).build().migrate()
            with db_backend.transaction(fixture.player) as conn:
                conn.execute("UPDATE natal_treasure SET level=0 WHERE user_id='u'")
            trained = NatalTrainingSqlRepository(fixture.game, fixture.player).train(
                "t1", "u", 5, base_cost=1000000, growth_rate=0.5,
                max_level=10, max_exp_base=100, max_exp_growth=50,
            )
            self.assertEqual(trained["status"], "trained")
            replay = NatalTrainingSqlRepository(fixture.game, fixture.player).train(
                "t1", "u", 5, base_cost=1000000, growth_rate=0.5,
                max_level=10, max_exp_base=100, max_exp_growth=50,
            )
            self.assertEqual(replay["status"], "duplicate")
            upgraded = NatalEffectUpgradeSqlRepository(fixture.game, fixture.player).upgrade(
                "u1", "u", 20009, 1, 3, 5, 7
            )
            self.assertEqual(upgraded["status"], "upgraded")


class FacadeContractTests(unittest.TestCase):
    def test_repositories_never_create_tables(self) -> None:
        for name in (
            "awaken_repository.py",
            "reawaken_repository.py",
            "training_repository.py",
            "effect_upgrade_repository.py",
            "engraving_repository.py",
            "forget_repository.py",
        ):
            text = (FEATURE / name).read_text(encoding="utf-8")
            self.assertNotIn("CREATE TABLE", text, name)
            self.assertIn("_operations_ready", text, name)
            self.assertIn("schema_missing", text, name)

    def test_facade_reports_schema_missing_to_the_player(self) -> None:
        source = FACADE.read_text(encoding="utf-8")
        self.assertEqual(source.count('"schema_missing": "本命法宝数据尚未就绪，请稍后重试"'), 4)
        for handler in ("natal_awaken_handler", "natal_reawaken_handler", "natal_engrave_handler", "natal_forget_handler"):
            self.assertIn(handler, source)

    def test_repository_declares_the_migration_it_depends_on(self) -> None:
        for name, migration in (
            ("awaken_repository.py", "natal_treasure.003"),
            ("reawaken_repository.py", "natal_treasure.002"),
            ("training_repository.py", "natal_treasure.002"),
            ("effect_upgrade_repository.py", "natal_treasure.002"),
            ("engraving_repository.py", "natal_treasure.002"),
            ("forget_repository.py", "natal_treasure.002"),
        ):
            text = (FEATURE / name).read_text(encoding="utf-8")
            self.assertIn(f"``{migration}``", text, name)


if __name__ == "__main__":
    unittest.main()
