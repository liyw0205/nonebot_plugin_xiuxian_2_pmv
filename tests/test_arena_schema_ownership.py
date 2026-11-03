"""竞技场（arena）schema 归属与 fail-closed 合同。

审计结论与本次收敛：

1. `features/arena/` 与 legacy `xiuxian/xiuxian_arena/` 的**请求路径已不含任何 DDL**
   （建表/补列只在 `features/arena/migrations.py`）。本套件用源码合同钉住这一点，
   防止后续有人在命令/调度/回执读取路径重新加 `CREATE TABLE`/`ALTER TABLE`。
2. 迁移是唯一写入者：`arena.002`~`arena.010` 覆盖 `GAME_TABLES`/`PLAYER_TABLES` 的全部表与列。
3. 五个仓储在未迁移库或半成品表上 fail-closed：写路径返回 `schema_missing` 且零写入，
   读路径返回空结果，状态初始化抛 `ArenaSchemaMissing`（DomainError）。
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.features.arena import migrations as arena_migrations
from nonebot_plugin_xiuxian_2.features.arena.opponent_repository import ArenaOpponentRepository
from nonebot_plugin_xiuxian_2.features.arena.repository import ArenaChallengePurchaseSqlRepository
from nonebot_plugin_xiuxian_2.features.arena.schema import (
    GAME_TABLES,
    PLAYER_TABLES,
    SCHEMA_MISSING,
    ArenaSchemaMissing,
    columns,
    table_ready,
)
from nonebot_plugin_xiuxian_2.features.arena.season_reward_application import (
    ArenaSeasonRewardApplication,
)
from nonebot_plugin_xiuxian_2.features.arena.season_reward_repository import (
    ArenaSeasonRewardRepository,
)
from nonebot_plugin_xiuxian_2.features.arena.state_application import ArenaStateApplication
from nonebot_plugin_xiuxian_2.features.arena.state_repository import (
    ARENA_FIELDS,
    ArenaStateRepository,
)
from nonebot_plugin_xiuxian_2.features.arena.weekly_rank_repository import (
    ArenaWeeklyRankRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "nonebot_plugin_xiuxian_2"
FEATURE_DIR = PACKAGE / "features" / "arena"
LEGACY_DIR = PACKAGE / "xiuxian" / "xiuxian_arena"

TODAY = date(2026, 9, 15)

GAME_MIGRATIONS = (
    arena_migrations.apply_arena,
    arena_migrations.apply_arena_challenge_purchase,
    arena_migrations.apply_arena_purchase,
    arena_migrations.apply_arena_challenge_ticket,
    arena_migrations.apply_arena_settlement,
    arena_migrations.apply_arena_season_reward,
    arena_migrations.apply_arena_purchase_receipt,
)
PLAYER_MIGRATIONS = (
    arena_migrations.apply_arena_weekly_rank_reduction,
    arena_migrations.apply_arena_daily_reward_player,
    arena_migrations.apply_arena_state,
)


class Pair:
    """A game/player database pair shaped exactly like the startup migrations."""

    def __init__(self, root: Path) -> None:
        self.game = root / "game.db"
        self.player = root / "player.db"
        # Real deployments always have both files; an unmigrated pair simply has no
        # arena tables inside them.
        for path in (self.game, self.player):
            with db_backend.transaction(path):
                pass

    def migrate(self) -> "Pair":
        with DatabaseUnitOfWork(str(self.game), immediate=True) as uow:
            for migration in GAME_MIGRATIONS:
                migration(uow)
        with DatabaseUnitOfWork(str(self.player), immediate=True) as uow:
            for migration in PLAYER_MIGRATIONS:
                migration(uow)
        return self

    def seed(self, *, user: str = "u1", opponent: str = "u2") -> "Pair":
        columns = "user_id TEXT PRIMARY KEY,hp INTEGER,mp INTEGER,user_stamina INTEGER,stone INTEGER,user_name TEXT"
        with db_backend.transaction(self.game) as conn:
            conn.execute(f"CREATE TABLE user_xiuxian({columns})")
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,"
                "UNIQUE(user_id,goods_id))"
            )
            conn.execute(
                f"INSERT INTO user_xiuxian(user_id,hp,mp,user_stamina,stone,user_name) VALUES(?,100,100,500,10000,'甲')",
                (user,),
            )
            conn.execute(
                f"INSERT INTO user_xiuxian(user_id,hp,mp,user_stamina,stone,user_name) VALUES(?,100,100,500,10000,'乙')",
                (opponent,),
            )
        with db_backend.transaction(self.player) as conn:
            conn.execute(f"CREATE TABLE user_xiuxian({columns})")
            conn.execute(
                f"INSERT INTO user_xiuxian(user_id,hp,mp,user_stamina,stone,user_name) VALUES(?,100,100,500,10000,'甲')",
                (user,),
            )
            conn.execute(
                f"INSERT INTO user_xiuxian(user_id,hp,mp,user_stamina,stone,user_name) VALUES(?,100,100,500,10000,'乙')",
                (opponent,),
            )
            conn.execute("INSERT INTO arena(user_id,score,rank) VALUES(?,1100,'青铜')", (user,))
            conn.execute("INSERT INTO arena(user_id,score,rank) VALUES(?,2000,'黄金')", (opponent,))
            conn.execute(
                "UPDATE arena SET honor_points=120,weekly_purchases=? WHERE user_id=?",
                (json.dumps({"_last_reset": "2026-09-15", "1": 1}), user),
            )
        return self

    def tables(self, database: Path) -> set[str]:
        with db_backend.connection(database) as conn:
            return {str(row[0]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    def arena_row(self, user: str = "u1") -> tuple:
        with db_backend.connection(self.player) as conn:
            return tuple(conn.execute("SELECT score,rank,honor_points FROM arena WHERE user_id=?", (user,)).fetchone())

    def count(self, database: Path, table: str) -> int:
        with db_backend.connection(database) as conn:
            return int(conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])


class MigrationOwnershipTests(unittest.TestCase):
    def test_startup_migrations_create_every_game_table_and_column(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate()
            with DatabaseUnitOfWork(str(pair.game), read_only=True) as uow:
                for table, required in GAME_TABLES.items():
                    self.assertTrue(table_ready(uow, table, required), table)

    def test_startup_migrations_create_every_player_table_and_column(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate()
            with DatabaseUnitOfWork(str(pair.player), read_only=True) as uow:
                for table, required in PLAYER_TABLES.items():
                    self.assertTrue(table_ready(uow, table, required), table)

    def test_migrations_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate()
            before = {
                table: columns_of(pair, table) for table in (*GAME_TABLES, *PLAYER_TABLES)
            }
            pair.migrate()
            after = {table: columns_of(pair, table) for table in (*GAME_TABLES, *PLAYER_TABLES)}
            self.assertEqual(before, after)

    def test_arena_state_operations_lives_with_the_player_tables(self) -> None:
        """`arena_state_operations` 与 `arena` 同库，不能在 game 库重复建。"""
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate()
            self.assertIn("arena_state_operations", pair.tables(pair.player))
            self.assertNotIn("arena", pair.tables(pair.game))
            self.assertNotIn("arena_state_operations", pair.tables(pair.game))


def columns_of(pair: Pair, table: str) -> frozenset[str]:
    database = pair.player if table in PLAYER_TABLES else pair.game
    with DatabaseUnitOfWork(str(database), read_only=True) as uow:
        return columns(uow, table)


class NoRequestPathDdlTests(unittest.TestCase):
    def _sources(self, directory: Path):
        for path in sorted(directory.glob("*.py")):
            if path.name == "migrations.py":
                continue
            yield path

    def test_feature_request_paths_never_create_or_alter_tables(self) -> None:
        offenders = [
            path.name
            for path in self._sources(FEATURE_DIR)
            if "CREATE TABLE" in path.read_text(encoding="utf-8")
            or "ALTER TABLE" in path.read_text(encoding="utf-8")
        ]
        self.assertEqual(offenders, [])

    def test_legacy_facade_never_creates_or_alters_tables(self) -> None:
        offenders = [
            path.name
            for path in self._sources(LEGACY_DIR)
            if "CREATE TABLE" in path.read_text(encoding="utf-8")
            or "ALTER TABLE" in path.read_text(encoding="utf-8")
        ]
        self.assertEqual(offenders, [])

    def test_every_repository_consults_the_shared_schema_contract(self) -> None:
        repositories = (
            "repository.py",
            "state_repository.py",
            "season_reward_repository.py",
            "opponent_repository.py",
            "weekly_rank_repository.py",
        )
        for name in repositories:
            source = (FEATURE_DIR / name).read_text(encoding="utf-8")
            self.assertIn("table_ready", source, f"{name} must gate on the startup-migrated schema")


class PurchaseFailClosedTests(unittest.TestCase):
    def test_unmigrated_databases_write_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp))
            repository = ArenaChallengePurchaseSqlRepository(pair.game, pair.player)
            result = repository.purchase(
                "op1", "u1", 1, "物品", "type", 2, 10, 5, 0, {}, 999, 1, TODAY
            )
            self.assertEqual(result["status"], SCHEMA_MISSING)
            self.assertEqual((pair.game.exists(), pair.player.exists()), (True, True))
            self.assertEqual(pair.tables(pair.game), set())
            self.assertEqual(pair.tables(pair.player), set())

    def test_half_migrated_arena_is_rejected_without_alter(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate().seed()
            with db_backend.transaction(pair.player) as conn:
                conn.execute("DROP TABLE arena")
                conn.execute(
                    "CREATE TABLE arena(user_id TEXT PRIMARY KEY,score INTEGER,rank TEXT,honor_points INTEGER)"
                )
            result = ArenaChallengePurchaseSqlRepository(pair.game, pair.player).purchase(
                "op1", "u1", 1, "物品", "type", 2, 10, 5, 120, {}, 999, 1, TODAY
            )
            self.assertEqual(result["status"], SCHEMA_MISSING)
            with db_backend.connection(pair.player) as conn:
                actual = {str(row[1]) for row in conn.execute('PRAGMA table_info("arena")')}
            self.assertNotIn("weekly_purchases", actual, "the request path must not add the column")
            self.assertEqual(pair.count(pair.game, "arena_purchase_operations"), 0)

    def test_challenge_purchase_and_ticket_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp))
            repository = ArenaChallengePurchaseSqlRepository(pair.game, pair.player)
            self.assertEqual(
                repository.purchase_challenges("op2", "u1", 1, 10, 3, 0, 0, 0, "2026-09-15")["status"],
                SCHEMA_MISSING,
            )
            self.assertEqual(
                repository.use_challenge_ticket("op3", "u1", 1, 1, 1, 1, 0, 10)["status"],
                SCHEMA_MISSING,
            )
            self.assertEqual(pair.tables(pair.game), set())

    def test_settlement_fails_closed_and_keeps_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp))
            repository = ArenaChallengePurchaseSqlRepository(pair.game, pair.player)
            result = repository.settle(
                "op4", "u1", None, "no_match", 10, 5, "2026-09-15 10:00:00",
                {"score": 1100, "rank": "青铜", "daily_challenges_used": 0}, None,
                {"hp": 100, "mp": 100, "user_stamina": 500}, None,
                100, 100, None, None, 20, 10, 10,
            )
            self.assertEqual(result["status"], SCHEMA_MISSING)
            self.assertEqual(pair.tables(pair.game), set())

    def test_receipt_reads_degrade_to_none(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp))
            repository = ArenaChallengePurchaseSqlRepository(pair.game, pair.player)
            self.assertIsNone(repository.purchase_result("op5", "u1", 1, 1))
            self.assertIsNone(repository.settlement_result("op5", "u1"))


class StateAndReadFailClosedTests(unittest.TestCase):
    def test_state_initialization_raises_typed_error_without_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp))
            with self.assertRaises(ArenaSchemaMissing) as raised:
                ArenaStateRepository(pair.player).initialize("u1", TODAY)
            self.assertIsInstance(raised.exception, DomainError)
            self.assertEqual(raised.exception.code, SCHEMA_MISSING)
            self.assertEqual(pair.tables(pair.player), set())

    def test_partial_arena_state_columns_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp))
            with db_backend.transaction(pair.player) as conn:
                conn.execute("CREATE TABLE arena(user_id TEXT PRIMARY KEY,score INTEGER,rank TEXT)")
                conn.execute("CREATE TABLE arena_state_operations(operation_id TEXT PRIMARY KEY)")
            with self.assertRaises(ArenaSchemaMissing):
                ArenaStateRepository(pair.player).initialize("u1", TODAY)
            with db_backend.connection(pair.player) as conn:
                self.assertEqual(int(conn.execute("SELECT COUNT(*) FROM arena").fetchone()[0]), 0)

    def test_ranking_and_candidates_return_empty_instead_of_raising(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp))
            self.assertEqual(ArenaStateRepository(pair.player).ranking(10), ())
            opponents = ArenaOpponentRepository(pair.game, pair.player)
            self.assertEqual(opponents.candidates("u1"), [])

    def test_state_application_surfaces_the_schema_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp))
            with self.assertRaises(ArenaSchemaMissing):
                ArenaStateApplication(pair.player).get("u1")


class SeasonRewardFailClosedTests(unittest.TestCase):
    def test_claim_fails_closed_and_writes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp))
            result = ArenaSeasonRewardRepository(pair.game, pair.player).claim(
                "op6", "u1", "2026-09-15", 1100, "青铜", 1, 0, 0, 100, 0,
            )
            self.assertEqual(result.status, SCHEMA_MISSING)
            self.assertFalse(result.succeeded)
            self.assertEqual(pair.tables(pair.game), set())
            self.assertEqual(pair.tables(pair.player), set())

    def test_daily_candidates_and_reset_are_empty_without_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp))
            repository = ArenaSeasonRewardRepository(pair.game, pair.player)
            self.assertEqual(repository.daily_candidates(), ())
            self.assertEqual(repository.recent_settlements("u1"), ())
            self.assertEqual(ArenaSeasonRewardApplication(pair.game, pair.player).reset_daily(), ())


class WeeklyRankSharesTheContractTests(unittest.TestCase):
    def test_weekly_reduction_reports_schema_missing_with_zero_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp))
            result = ArenaWeeklyRankRepository(pair.player).reduce(
                "2026-W38", 2, chunk_size=10, updated_at="2026-09-15 20:00:00"
            )
            self.assertEqual(result.status, SCHEMA_MISSING)
            self.assertEqual(pair.tables(pair.player), set())

    def test_migrated_repository_still_reduces(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate().seed()
            result = ArenaWeeklyRankRepository(pair.player).reduce(
                "2026-W38", 2, chunk_size=10, updated_at="2026-09-15 20:00:00"
            )
            self.assertEqual((result.status, result.task_status), ("applied", "completed"))
            self.assertEqual(pair.arena_row("u1"), (1000, "青铜", 120))


class ApplicationContractTests(unittest.TestCase):
    def test_purchase_rejection_maps_to_a_rejected_outcome(self) -> None:
        from nonebot_plugin_xiuxian_2.features.arena.application import ArenaApplication

        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp))
            with DatabaseUnitOfWork(str(pair.game), immediate=True) as uow:
                from nonebot_plugin_xiuxian_2.infrastructure.database import OperationLedger

                OperationLedger().ensure_schema(uow)
            outcome = ArenaApplication(pair.game, pair.player).purchase(
                operation_id="op7", user_id="u1", item_id=1, item_name="物品", item_type="type",
                quantity=1, unit_cost=10, weekly_limit=5, expected_honor=0,
                expected_weekly_purchases={}, max_goods_num=999, bind_flag=1, today=TODAY,
            )
            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, SCHEMA_MISSING)
            self.assertEqual(outcome.data["status"], SCHEMA_MISSING)
            self.assertEqual(pair.tables(pair.player), set(), "no arena/player rows were written")


if __name__ == "__main__":
    unittest.main()
