"""前世（past_life）schema 归属与 fail-closed 合同。

审计结论与本次收敛：

1. `features/past_life/` 的四份仓储原先在请求事务内建表、补列（`past_life`、
   `statistics."前尘往事次数"`、四张回执表与 `past_life_reset_targets`、以及共享 `economy_log`），
   而 `features/past_life/migrations.py` 只有一个 `legacy.past_life.001` 标记。
2. 现在 `past_life.002`（player 库：`past_life` + `statistics` 列）与
   `past_life.003`（game 库：五张回执表）是唯一写入者；`economy_log` 仍由
   `tasks`/`dufang`/`auction`/`admin_asset`/`sign_in` 的启动迁移持有，前世只校验。
3. 请求路径缺表或缺列时返回 `schema_missing` 且零写入；兼容
   `xiuxian_past_life/transaction_service.py` 保留为显式回滚入口。
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.past_life.choice_repository import PastLifeChoiceSqlRepository
from nonebot_plugin_xiuxian_2.features.past_life.final_repository import (
    PastLifeFinalSettlementSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.past_life.migrations import (
    apply_past_life_game,
    apply_past_life_player,
)
from nonebot_plugin_xiuxian_2.features.past_life.reset_repository import PastLifeResetSqlRepository
from nonebot_plugin_xiuxian_2.features.past_life.schema import (
    GAME_TABLES,
    PAST_LIFE_COLUMNS,
    PAST_LIFE_FIELDS,
    PLAYER_TABLES,
    SCHEMA_MISSING,
    STATISTICS_COLUMN,
    columns,
    table_ready,
)
from nonebot_plugin_xiuxian_2.features.past_life.start_repository import (
    PastLifeStartSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "nonebot_plugin_xiuxian_2"
FEATURE_DIR = PACKAGE / "features" / "past_life"
LEGACY_DIR = PACKAGE / "xiuxian" / "xiuxian_past_life"

STATE = {
    "state": 2, "stage": 3, "revision": 1, "alloc": {}, "accumulated": {},
    "talent": "", "birth_scenario": "", "total_score": 0, "score_breakdown": {},
    "event_indices": [], "event_snapshots": [], "early_death_rolls": {}, "history": [],
    "last_run_time": None, "total_runs": 0, "best_ending": "", "best_score": 0,
    "endings_log": [], "achievement_points": 0,
}
STATE_COLUMNS = ",".join(["user_id", *PAST_LIFE_FIELDS])
STATE_VALUES = (
    "u", 2, 3, 1, "{}", "{}", "", "", 0, "{}", "[]", "[]", "{}", "[]", None, 0, "", 0, "[]", 0,
)


class Pair:
    """A game/player pair. `migrate()` reproduces the startup migration shape."""

    def __init__(self, root: Path) -> None:
        self.game = root / "game.db"
        self.player = root / "player.db"
        for path in (self.game, self.player):
            with db_backend.transaction(path):
                pass

    def migrate(self) -> "Pair":
        with DatabaseUnitOfWork(str(self.player), immediate=True) as uow:
            apply_past_life_player(uow)
        with DatabaseUnitOfWork(str(self.game), immediate=True) as uow:
            apply_past_life_game(uow)
        return self

    def seed(self) -> "Pair":
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',100,50)")
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,create_time TEXT,update_time TEXT,UNIQUE(user_id,goods_id))"
            )
            conn.execute(
                "CREATE TABLE economy_log(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,"
                "source TEXT NOT NULL,action TEXT NOT NULL,stone_delta INTEGER NOT NULL DEFAULT 0,"
                "exp_delta INTEGER NOT NULL DEFAULT 0,item_delta TEXT NOT NULL DEFAULT '[]',"
                "detail TEXT NOT NULL DEFAULT '{}',trace_id TEXT,created_at TEXT NOT NULL)"
            )
        with db_backend.transaction(self.player) as conn:
            conn.execute(f"INSERT INTO past_life({STATE_COLUMNS}) VALUES({','.join('?' * 20)})", STATE_VALUES)
        return self

    def tables(self, database: Path) -> set[str]:
        with db_backend.connection(database) as conn:
            return {str(row[0]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    def count(self, database: Path, table: str) -> int:
        with db_backend.connection(database) as conn:
            return int(conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])

    def state_row(self) -> tuple:
        with db_backend.connection(self.player) as conn:
            return tuple(conn.execute("SELECT state,stage,revision FROM past_life WHERE user_id='u'").fetchone())


class MigrationOwnershipTests(unittest.TestCase):
    def test_player_migration_builds_state_and_statistics(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate()
            with DatabaseUnitOfWork(str(pair.player), read_only=True) as uow:
                for table, required in PLAYER_TABLES.items():
                    self.assertTrue(table_ready(uow, table, required, schema=""), table)
                self.assertIn(STATISTICS_COLUMN, columns(uow, "statistics", schema=""))

    def test_game_migration_builds_every_receipt_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate()
            with DatabaseUnitOfWork(str(pair.game), read_only=True) as uow:
                for table, required in GAME_TABLES.items():
                    self.assertTrue(table_ready(uow, table, required, schema=""), table)
                self.assertNotIn("past_life", {
                    row["name"] for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
                })

    def test_migrations_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate()
            with DatabaseUnitOfWork(str(pair.player), read_only=True) as uow:
                before = columns(uow, "past_life", schema="")
            pair.migrate()
            with DatabaseUnitOfWork(str(pair.player), read_only=True) as uow:
                self.assertEqual(before, columns(uow, "past_life", schema=""))

    def test_past_life_does_not_own_economy_log(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate()
            self.assertNotIn("economy_log", pair.tables(pair.game))


class NoRequestPathDdlTests(unittest.TestCase):
    def test_feature_sources_never_create_or_alter_tables(self) -> None:
        offenders = [
            path.name
            for path in sorted(FEATURE_DIR.glob("*.py"))
            if path.name not in {"migrations.py", "schema.py"}
            and (
                "CREATE TABLE" in path.read_text(encoding="utf-8")
                or "ALTER TABLE" in path.read_text(encoding="utf-8")
            )
        ]
        self.assertEqual(offenders, [])

    def test_legacy_compat_service_is_the_only_remaining_ddl_owner(self) -> None:
        """`transaction_service` 保留为显式回滚入口，其余 legacy 文件不得有 DDL。"""
        offenders = [
            path.name
            for path in sorted(LEGACY_DIR.glob("*.py"))
            if path.name != "transaction_service.py"
            and (
                "CREATE TABLE" in path.read_text(encoding="utf-8")
                or "ALTER TABLE" in path.read_text(encoding="utf-8")
            )
        ]
        self.assertEqual(offenders, [])

    def test_every_repository_consults_the_shared_contract(self) -> None:
        for name in ("start_repository.py", "choice_repository.py", "final_repository.py", "reset_repository.py"):
            source = (FEATURE_DIR / name).read_text(encoding="utf-8")
            self.assertIn("table_ready", source, name)
            self.assertIn("SCHEMA_MISSING", source, name)


class WritePathFailClosedTests(unittest.TestCase):
    def test_start_writes_nothing_without_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp))
            result = PastLifeStartSqlRepository(pair.game, pair.player).start(
                "s1", "u", expected_state={}, alloc={}, accumulated={}, talent="t",
                birth_scenario="b", event_indices=[1], event_snapshots=[{}],
                first_stage_message="m", choices_count=1, refresh_slot_start="2026-09-15 10:00:00",
            )
            self.assertEqual(result.status, SCHEMA_MISSING)
            self.assertEqual(pair.tables(pair.game), set())
            self.assertEqual(pair.tables(pair.player), set())

    def test_choice_and_final_and_reset_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp))
            choice = PastLifeChoiceSqlRepository(pair.game, pair.player)
            final = PastLifeFinalSettlementSqlRepository(pair.game, pair.player)
            reset = PastLifeResetSqlRepository(pair.game, pair.player)
            expected = dict(STATE)
            final_state = dict(STATE, stage=4, revision=2)
            self.assertEqual(
                choice.advance("c1", "u", 1, expected_state=expected, final_state=final_state,
                               response={"message": "ok", "is_end": False, "ending": None}).status,
                SCHEMA_MISSING,
            )
            self.assertEqual(
                final.settle("f1", "u", expected, dict(expected), "e", 10, 1, 1, 1).status,
                SCHEMA_MISSING,
            )
            self.assertEqual(reset.reset_one("r1", "u", False).status, SCHEMA_MISSING)
            self.assertEqual(reset.reset_all_create("a1", False).status, SCHEMA_MISSING)
            self.assertEqual(pair.tables(pair.game), set())
            self.assertEqual(pair.tables(pair.player), set())

    def test_half_migrated_state_table_is_rejected_without_alter(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate().seed()
            with db_backend.transaction(pair.player) as conn:
                conn.execute("DROP TABLE past_life")
                conn.execute("CREATE TABLE past_life(user_id TEXT PRIMARY KEY,state INTEGER,revision INTEGER)")
            result = PastLifeResetSqlRepository(pair.game, pair.player).reset_one("r2", "u", False)
            self.assertEqual(result.status, SCHEMA_MISSING)
            with db_backend.connection(pair.player) as conn:
                actual = {str(row[1]) for row in conn.execute('PRAGMA table_info("past_life")')}
            self.assertEqual(actual, {"user_id", "state", "revision"}, "no column may be added")
            self.assertEqual(pair.count(pair.game, "past_life_reset_operations"), 0)

    def test_missing_economy_log_blocks_final_settlement(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate().seed()
            with db_backend.transaction(pair.game) as conn:
                conn.execute("DROP TABLE economy_log")
            result = PastLifeFinalSettlementSqlRepository(pair.game, pair.player).settle(
                "f2", "u", dict(STATE), dict(STATE), "e", 10, 1, 1, 1,
            )
            self.assertEqual(result.status, SCHEMA_MISSING)
            self.assertEqual(pair.count(pair.game, "past_life_final_operations"), 0)


class PostMigrationBehaviourTests(unittest.TestCase):
    def test_reset_one_applies_and_replays_after_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate().seed()
            repository = PastLifeResetSqlRepository(pair.game, pair.player)
            first = repository.reset_one("r3", "u", False)
            replay = repository.reset_one("r3", "u", False)
            self.assertEqual((first.status, replay.status), ("applied", "duplicate"))
            self.assertEqual(pair.count(pair.game, "past_life_reset_operations"), 1)
            self.assertEqual(pair.state_row(), (0, 0, 2))

    def test_final_settlement_applies_and_replays_after_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate().seed()
            repository = PastLifeFinalSettlementSqlRepository(pair.game, pair.player)
            first = repository.settle(
                "f3", "u", dict(STATE), dict(STATE), "ending", 88, 10, 20, 3,
                {"id": 7, "name": "item", "type": "道具", "num": 2}, None, {"message": "done"},
            )
            replay = repository.settle(
                "f3", "u", dict(STATE), dict(STATE), "ending", 88, 10, 20, 3,
                {"id": 7, "name": "item", "type": "道具", "num": 2}, None, {"message": "done"},
            )
            self.assertEqual((first.status, replay.status), ("applied", "duplicate"))
            self.assertEqual(pair.count(pair.game, "past_life_final_operations"), 1)
            self.assertEqual(pair.count(pair.game, "economy_log"), 1)

    def test_choice_applies_after_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate().seed()
            repository = PastLifeChoiceSqlRepository(pair.game, pair.player)
            expected = json.loads(json.dumps(STATE))
            expected["state"] = 2
            final = dict(expected, stage=4, revision=2)
            result = repository.advance(
                "c2", "u", 1, expected_state=expected, final_state=final,
                response={"message": "ok", "is_end": False, "ending": None},
            )
            self.assertEqual(result.status, "applied")
            self.assertEqual(pair.count(pair.game, "past_life_choice_operations"), 1)


if __name__ == "__main__":
    unittest.main()
