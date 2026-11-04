"""洞天福地回执表 `blessed_spot_operations` 的 schema 归属与写路径合同。

本切片收敛三件事：

1. `buff.014`（game DB）成为 `blessed_spot_operations` 的唯一启动创建者；
   两条旧请求路径（`rename_repository` / `upgrade_repository`）只校验形状。
2. 未迁移或半成品表上 fail-closed：返回 `schema_missing`，零资产/零回执写入，
   且请求路径不建表、不补列。
3. `灵田开垦` handler 自 `08eeef62` 起误传 `cost=`（应用只接受 `stone_cost=`），
   导致真实命令在参数校验阶段必然失败；这里用真实 `BuffApplication` 钉住修复后的
   端到端读写与重放语义。
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.buff import migrations as buff_migrations
from nonebot_plugin_xiuxian_2.features.buff.application import BuffApplication
from nonebot_plugin_xiuxian_2.features.buff.operation_schema import (
    BLESSED_SPOT_OPERATION_COLUMNS,
    SCHEMA_MISSING,
    operations_ready,
)
from nonebot_plugin_xiuxian_2.features.buff.rename_repository import (
    BlessedSpotRenameSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.buff.stone_training_repository import (
    StoneTrainingSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.buff.upgrade_repository import (
    BlessedSpotUpgradeSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import (
    DatabaseUnitOfWork,
    OperationLedger,
)
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "nonebot_plugin_xiuxian_2"
FEATURE_DIR = PACKAGE / "features" / "buff"
LEGACY_SERVICE = PACKAGE / "xiuxian" / "xiuxian_buff" / "transaction_service.py"
HANDLERS = PACKAGE / "xiuxian" / "xiuxian_buff" / "__init__.py"


class Pair:
    """A game/player pair shaped by the real startup migrations."""

    def __init__(self, root: Path) -> None:
        self.game = root / "game.db"
        self.player = root / "player.db"
        for path in (self.game, self.player):
            with db_backend.transaction(path):
                pass

    def migrate(self) -> "Pair":
        with DatabaseUnitOfWork(str(self.game), immediate=True) as uow:
            OperationLedger().ensure_schema(uow)
            buff_migrations.apply_blessed_spot_operations(uow)
            buff_migrations.apply_stone_training_operations(uow)
        return self

    def seed(self, *, stone: int = 500, level: str = "1") -> "Pair":
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER, "
                "exp INTEGER, power INTEGER, blessed_spot_flag INTEGER, "
                "blessed_spot_name TEXT)"
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s,%s,%s,%s,%s,%s)",
                ("u", stone, 100, 10, 1, "旧名"),
            )
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TABLE mix_elixir_info (user_id TEXT PRIMARY KEY, 灵田数量 TEXT)"
            )
            conn.execute("INSERT INTO mix_elixir_info VALUES (%s,%s)", ("u", level))
        return self

    def state(self) -> tuple[tuple[int, str], str]:
        with db_backend.connection(self.game) as conn:
            row = conn.execute(
                "SELECT stone, blessed_spot_name FROM user_xiuxian WHERE user_id='u'"
            ).fetchone()
        with db_backend.connection(self.player) as conn:
            level = conn.execute(
                "SELECT 灵田数量 FROM mix_elixir_info WHERE user_id='u'"
            ).fetchone()[0]
        return tuple(row), str(level)

    def training_state(self) -> tuple[int, int, int]:
        with db_backend.connection(self.game) as conn:
            row = conn.execute(
                "SELECT exp, stone, power FROM user_xiuxian WHERE user_id='u'"
            ).fetchone()
        return int(row[0]), int(row[1]), int(row[2])

    def receipt_actions(self) -> list[tuple[str, str]]:
        with db_backend.connection(self.game) as conn:
            return [
                (str(row[0]), str(row[1]))
                for row in conn.execute(
                    "SELECT operation_id, action FROM blessed_spot_operations "
                    "ORDER BY operation_id"
                ).fetchall()
            ]

    def tables(self, path: Path) -> set[str]:
        with db_backend.connection(path) as conn:
            return {
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }


class BlessedSpotSchemaOwnershipTests(unittest.TestCase):
    def test_migration_creates_the_receipt_shape_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp))
            rows: list = []
            for _ in range(2):
                with DatabaseUnitOfWork(str(pair.game), immediate=True) as uow:
                    buff_migrations.apply_blessed_spot_operations(uow)
                    self.assertTrue(operations_ready(uow))
                    rows = uow.query_all(
                        'PRAGMA table_info("blessed_spot_operations")'
                    )
            columns = {str(row["name"]) for row in rows}
            self.assertEqual(columns, set(BLESSED_SPOT_OPERATION_COLUMNS))
            self.assertTrue(
                any(
                    str(row["name"]) == "operation_id" and int(row["pk"] or 0) == 1
                    for row in rows
                )
            )

    def test_migration_is_routed_to_the_game_database_only(self) -> None:
        from nonebot_plugin_xiuxian_2.plugin import (
            build_migrations,
            migrations_for_database,
        )

        migrations = build_migrations()
        versions = [migration.version for migration in migrations]
        self.assertEqual(versions, sorted(set(versions)))
        self.assertIn("buff.014", versions)
        self.assertIn("buff.015", versions)
        game = [m.version for m in migrations_for_database(migrations, "game_db")]
        self.assertIn("buff.014", game)
        self.assertIn("buff.015", game)
        for key in ("player_db", "trade_db", "impart_db", "message_db"):
            routed = [m.version for m in migrations_for_database(migrations, key)]
            self.assertNotIn("buff.014", routed, key)
            self.assertNotIn("buff.015", routed, key)

    def test_legacy_receipt_rows_survive_the_startup_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp))
            with db_backend.transaction(pair.game) as conn:
                conn.execute(
                    "CREATE TABLE blessed_spot_operations (operation_id TEXT PRIMARY KEY,"
                    "action TEXT NOT NULL,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                conn.execute(
                    "INSERT INTO blessed_spot_operations(operation_id, action, payload, result_json) "
                    "VALUES ('old', 'rename', '[]', '{}')"
                )
            with DatabaseUnitOfWork(str(pair.game), immediate=True) as uow:
                buff_migrations.apply_blessed_spot_operations(uow)
            self.assertEqual(pair.receipt_actions(), [("old", "rename")])

    def test_stone_training_fails_closed_without_its_receipt_table(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).seed()
            with DatabaseUnitOfWork(str(pair.game), immediate=True) as uow:
                buff_migrations.apply_blessed_spot_operations(uow)
            result = StoneTrainingSqlRepository(pair.game, pair.player).settle(
                "s1",
                "u",
                requested_stone=200,
                expected_exp=100,
                expected_stone=500,
                exp_cap=1000,
                power_multiplier=1.0,
            )
            self.assertEqual(result.status, SCHEMA_MISSING)
            self.assertEqual(pair.training_state(), (100, 500, 10))
            self.assertNotIn("stone_training_operations", pair.tables(pair.game))

    def test_stone_training_receipts_are_idempotent_after_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate().seed()
            repository = StoneTrainingSqlRepository(pair.game, pair.player)
            first = repository.settle(
                "s1",
                "u",
                requested_stone=200,
                expected_exp=100,
                expected_stone=500,
                exp_cap=1000,
                power_multiplier=1.0,
            )
            self.assertEqual(first.status, "applied")
            self.assertEqual((first.exp_gain, first.stone_cost, first.power), (20, 200, 120))
            self.assertEqual(pair.training_state(), (120, 300, 120))

            replay = repository.settle(
                "s1",
                "u",
                requested_stone=200,
                expected_exp=100,
                expected_stone=500,
                exp_cap=1000,
                power_multiplier=1.0,
            )
            self.assertEqual(replay.status, "duplicate")
            self.assertEqual(pair.training_state(), (120, 300, 120))

    def test_unmigrated_receipt_table_fails_closed_without_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).seed()
            with DatabaseUnitOfWork(str(pair.game), immediate=True) as uow:
                OperationLedger().ensure_schema(uow)
            rename = BlessedSpotRenameSqlRepository(pair.game).rename(
                "r1", "u", "旧名", "新名"
            )
            upgrade = BlessedSpotUpgradeSqlRepository(pair.game, pair.player).upgrade(
                "u1", "u", 1, 200, 10
            )
            self.assertEqual((rename.status, upgrade.status), (SCHEMA_MISSING, SCHEMA_MISSING))
            self.assertEqual(pair.state(), ((500, "旧名"), "1"))
            self.assertNotIn("blessed_spot_operations", pair.tables(pair.game))

    def test_half_built_receipt_table_is_rejected_and_not_altered(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).seed()
            with db_backend.transaction(pair.game) as conn:
                conn.execute(
                    "CREATE TABLE blessed_spot_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL)"
                )
            rename = BlessedSpotRenameSqlRepository(pair.game).rename(
                "r1", "u", "旧名", "新名"
            )
            upgrade = BlessedSpotUpgradeSqlRepository(pair.game, pair.player).upgrade(
                "u1", "u", 1, 200, 10
            )
            self.assertEqual((rename.status, upgrade.status), (SCHEMA_MISSING, SCHEMA_MISSING))
            with db_backend.connection(pair.game) as conn:
                columns = {
                    str(row[1])
                    for row in conn.execute(
                        "PRAGMA table_info('blessed_spot_operations')"
                    ).fetchall()
                }
            self.assertEqual(columns, {"operation_id", "payload"})
            self.assertEqual(pair.state(), ((500, "旧名"), "1"))

    def test_application_reads_and_writes_after_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).migrate().seed()
            app = BuffApplication(pair.game, pair.player)

            upgrade = app.upgrade_field(
                operation_id="u1",
                user_id="u",
                expected_level=1,
                stone_cost=200,
                max_level=10,
            )
            self.assertTrue(upgrade.ok, upgrade.message)
            self.assertEqual(pair.state(), ((300, "旧名"), "2"))

            replay = app.upgrade_field(
                operation_id="u1",
                user_id="u",
                expected_level=1,
                stone_cost=200,
                max_level=10,
            )
            self.assertTrue(replay.ok)
            self.assertTrue(replay.replayed)
            self.assertEqual(pair.state(), ((300, "旧名"), "2"))

            stale = app.upgrade_field(
                operation_id="u2",
                user_id="u",
                expected_level=1,
                stone_cost=200,
                max_level=10,
            )
            self.assertFalse(stale.ok)
            self.assertEqual(stale.code, "state_changed")
            self.assertEqual(pair.state(), ((300, "旧名"), "2"))

            rename = app.rename(
                operation_id="r1",
                user_id="u",
                expected_name="旧名",
                new_name="新名",
            )
            self.assertTrue(rename.ok, rename.message)
            self.assertEqual(pair.state(), ((300, "新名"), "2"))

            stale_rename = app.rename(
                operation_id="r2",
                user_id="u",
                expected_name="旧名",
                new_name="再改",
            )
            self.assertFalse(stale_rename.ok)
            self.assertEqual(stale_rename.code, "state_changed")
            self.assertEqual(pair.state(), ((300, "新名"), "2"))

            self.assertEqual(
                pair.receipt_actions(), [("r1", "rename"), ("u1", "upgrade")]
            )

    def test_application_surfaces_schema_missing_without_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            pair = Pair(Path(temp)).seed()
            with DatabaseUnitOfWork(str(pair.game), immediate=True) as uow:
                OperationLedger().ensure_schema(uow)
            app = BuffApplication(pair.game, pair.player)
            outcome = app.upgrade_field(
                operation_id="u1",
                user_id="u",
                expected_level=1,
                stone_cost=200,
                max_level=10,
            )
            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, SCHEMA_MISSING)
            self.assertEqual(pair.state(), ((500, "旧名"), "1"))
            self.assertNotIn("blessed_spot_operations", pair.tables(pair.game))

    def test_request_paths_have_no_ddl_and_share_the_contract(self) -> None:
        allowed = {"migrations.py", "operation_schema.py"}
        for path in sorted(FEATURE_DIR.glob("*.py")):
            if path.name in allowed:
                continue
            text = path.read_text(encoding="utf-8")
            self.assertNotIn("CREATE TABLE", text, path.name)
            self.assertNotIn("ALTER TABLE", text, path.name)
        for name in ("rename_repository.py", "upgrade_repository.py"):
            text = (FEATURE_DIR / name).read_text(encoding="utf-8")
            self.assertIn("operations_ready", text, name)
            self.assertIn("SCHEMA_MISSING", text, name)
        legacy = LEGACY_SERVICE.read_text(encoding="utf-8")
        self.assertIn("_ensure_operation_table", legacy)
        self.assertIn("stone_training_operations", legacy)

    def test_handler_passes_stone_cost_and_reports_schema_missing(self) -> None:
        text = HANDLERS.read_text(encoding="utf-8")
        self.assertIn("stone_cost=cost,", text)
        self.assertNotIn("                cost=cost,", text)
        self.assertIn('outcome.code == "schema_missing"', text)
        self.assertIn("洞天福地数据尚未就绪，请检查启动迁移。", text)


if __name__ == "__main__":
    unittest.main()
