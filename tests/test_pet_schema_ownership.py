"""宠物（pet）回执表 schema 归属与 fail-closed 合同。

审计结论与本次收敛：`features/pet/repository.py` 原先在六个写路径里各自
`CREATE TABLE IF NOT EXISTS <回执行>` 惰性建表。现在启动迁移是唯一写入者：

- `pet.005`（player 库）：`pet_active_switch_operations`、`pet_travel_start_operations`、
  `pet_fusion_breakthrough_operations` —— 三者的仓储只接 player 库。
- `pet.006`（game 库）：`pet_feed_operations`、`pet_release_operations`、
  `pet_skill_reroll_operations` —— 三者的仓储以 game 为主库、player 作附库。

请求路径只校验「表存在 + 本次要读的列齐全」，缺表或缺列返回 `schema_missing` 且零写入。
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.pet.migrations import (
    apply_pet_game_operations,
    apply_pet_player_operations,
)
from nonebot_plugin_xiuxian_2.features.pet.repository import (
    PetActiveSwitchSqlRepository,
    PetFeedSqlRepository,
    PetFusionBreakthroughSqlRepository,
    PetReleaseSqlRepository,
    PetSkillRerollSqlRepository,
    PetTravelStartSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.pet.schema import (
    GAME_OPERATION_TABLES,
    PLAYER_OPERATION_TABLES,
    SCHEMA_MISSING,
    columns,
    operations_ready,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
FEATURE_DIR = ROOT / "nonebot_plugin_xiuxian_2" / "features" / "pet"


class Pair:
    def __init__(self, root: Path) -> None:
        self.game = root / "game.db"
        self.player = root / "player.db"
        for path in (self.game, self.player):
            with db_backend.transaction(path):
                pass

    def migrate(self) -> "Pair":
        with DatabaseUnitOfWork(str(self.player), immediate=True) as uow:
            apply_pet_player_operations(uow)
        with DatabaseUnitOfWork(str(self.game), immediate=True) as uow:
            apply_pet_game_operations(uow)
        return self

    def tables(self, database: Path) -> set[str]:
        with db_backend.connection(database) as conn:
            return {str(row[0]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


class MigrationOwnershipTests(unittest.TestCase):
    def test_player_migration_builds_the_three_player_ledgers(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate()
            with DatabaseUnitOfWork(str(pair.player), read_only=True) as uow:
                for table, required in PLAYER_OPERATION_TABLES.items():
                    self.assertTrue(operations_ready(uow, table, required), table)

    def test_game_migration_builds_the_three_game_ledgers(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate()
            with DatabaseUnitOfWork(str(pair.game), read_only=True) as uow:
                for table, required in GAME_OPERATION_TABLES.items():
                    self.assertTrue(operations_ready(uow, table, required), table)

    def test_player_ledgers_are_not_created_in_the_game_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate()
            game_tables = pair.tables(pair.game)
            for table in PLAYER_OPERATION_TABLES:
                self.assertNotIn(table, game_tables)
            for table in GAME_OPERATION_TABLES:
                self.assertNotIn(table, pair.tables(pair.player))

    def test_migrations_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate()
            with DatabaseUnitOfWork(str(pair.player), read_only=True) as uow:
                before = columns(uow, "pet_active_switch_operations")
            pair.migrate()
            with DatabaseUnitOfWork(str(pair.player), read_only=True) as uow:
                self.assertEqual(before, columns(uow, "pet_active_switch_operations"))


class NoRequestPathDdlTests(unittest.TestCase):
    def test_repository_never_creates_or_alters_tables(self) -> None:
        source = (FEATURE_DIR / "repository.py").read_text(encoding="utf-8")
        self.assertNotIn("CREATE TABLE", source)
        self.assertNotIn("ALTER TABLE", source)
        self.assertIn("operations_ready", source)
        self.assertIn("SCHEMA_MISSING", source)


class FailClosedTests(unittest.TestCase):
    def test_all_six_write_paths_report_schema_missing_without_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp))
            self.assertEqual(
                PetActiveSwitchSqlRepository(pair.player).switch("o1", "u", "", "p").status,
                SCHEMA_MISSING,
            )
            self.assertEqual(
                PetTravelStartSqlRepository(pair.player).start(
                    "o2", "u", "p", None, {"pet_uid": "p", "start_at": 1}
                ).status,
                SCHEMA_MISSING,
            )
            self.assertEqual(
                PetFusionBreakthroughSqlRepository(pair.player).breakthrough(
                    "o3", "u", ("main", "fox", 4, 100, 500, "", 1), (("body", "fox", 1, 0, 0, "", 0),), 5, 0
                ).status,
                SCHEMA_MISSING,
            )
            self.assertEqual(
                PetFeedSqlRepository(pair.game, pair.player).feed(
                    "o4", "u", "p", 1, 1, (1, 2, 3), (2, 4, 6)
                ).status,
                SCHEMA_MISSING,
            )
            self.assertEqual(
                PetReleaseSqlRepository(pair.game, pair.player).release(
                    "o5", "u", "p", 0, 9, "灵髓", "特殊道具", 3, 99
                ).status,
                SCHEMA_MISSING,
            )
            self.assertEqual(
                PetSkillRerollSqlRepository(pair.game, pair.player).reroll(
                    "o6", "u", ("p", "fox", 1, 0, 0, "", 0), "new-skill", 9
                ).status,
                SCHEMA_MISSING,
            )
            self.assertEqual(pair.tables(pair.player), set())
            self.assertEqual(pair.tables(pair.game), set())

    def test_half_migrated_ledger_is_rejected_without_alter(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate()
            with db_backend.transaction(pair.game) as conn:
                conn.execute("DROP TABLE pet_feed_operations")
                conn.execute("CREATE TABLE pet_feed_operations(operation_id TEXT PRIMARY KEY)")
            result = PetFeedSqlRepository(pair.game, pair.player).feed(
                "o7", "u", "p", 1, 1, (1, 2, 3), (2, 4, 6)
            )
            self.assertEqual(result.status, SCHEMA_MISSING)
            with db_backend.connection(pair.game) as conn:
                actual = {str(row[1]) for row in conn.execute('PRAGMA table_info("pet_feed_operations")')}
            self.assertEqual(actual, {"operation_id"}, "no column may be added")


class PostMigrationBehaviourTests(unittest.TestCase):
    def test_active_switch_applies_and_replays(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate()
            with db_backend.transaction(pair.player) as conn:
                conn.execute("CREATE TABLE player_pet(user_id TEXT PRIMARY KEY, active_uid TEXT, active TEXT)")
                conn.execute("INSERT INTO player_pet VALUES('u','old','old')")
                conn.execute("CREATE TABLE player_pet_item(user_id TEXT, uid TEXT, is_active INTEGER, updated_at INTEGER)")
                conn.execute("INSERT INTO player_pet_item VALUES('u','old',1,0)")
                conn.execute("INSERT INTO player_pet_item VALUES('u','new',0,0)")
            repository = PetActiveSwitchSqlRepository(pair.player)
            first = repository.switch("o8", "u", "old", "new")
            replay = repository.switch("o8", "u", "old", "new")
            self.assertEqual((first.status, replay.status), ("applied", "duplicate"))
            with db_backend.connection(pair.player) as conn:
                self.assertEqual(conn.execute("SELECT active_uid FROM player_pet WHERE user_id='u'").fetchone()[0], "new")

    def test_travel_start_applies_and_replays(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate()
            with db_backend.transaction(pair.player) as conn:
                conn.execute("CREATE TABLE player_pet(user_id TEXT PRIMARY KEY, travel TEXT)")
                conn.execute("INSERT INTO player_pet VALUES('u',NULL)")
                conn.execute("CREATE TABLE player_pet_item(user_id TEXT, uid TEXT, is_active INTEGER)")
                conn.execute("INSERT INTO player_pet_item VALUES('u','p',1)")
            travel = {"pet_uid": "p", "start_at": 1}
            repository = PetTravelStartSqlRepository(pair.player)
            first = repository.start("o9", "u", "p", None, travel)
            replay = repository.start("o9", "u", "p", None, travel)
            self.assertEqual((first.status, replay.status), ("applied", "duplicate"))


if __name__ == "__main__":
    unittest.main()
