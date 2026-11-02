"""灵田读模型与傀儡回执迁移的定向合同。

覆盖：
- ``puppet.003`` 启动迁移幂等、保留既有回执行；
- 购买/收取仓储缺表时 ``schema_missing`` 且请求期零 DDL；
- ``PuppetFieldApplication`` 用真实列集给出成熟状态、最近收取与降级文案；
- ``format_puppet_field`` 渲染与 ``我的灵田`` handler 源合同。
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.puppet.field_view import (
    PuppetFieldApplication,
    format_puppet_field,
)
from nonebot_plugin_xiuxian_2.features.puppet.harvest_repository import (
    HarvestReward,
    PuppetHarvestSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.puppet.migrations import apply_puppet_operations
from nonebot_plugin_xiuxian_2.features.puppet.purchase_repository import (
    PuppetPurchaseSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

REAL_PLAYER_FIELDS = (
    "收取时间",
    "收取等级",
    "灵田数量",
    "药材速度",
    "灵田傀儡",
)


class FixedClock:
    def __init__(self, moment: datetime) -> None:
        self.moment = moment

    def now(self) -> datetime:
        return self.moment


def create_game(database: Path, *, stone: int = 5_000_000, puppet_status: int = 1) -> None:
    with db_backend.transaction(database) as conn:
        conn.execute(
            "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level TEXT,stone INTEGER,"
            "blessed_spot_flag INTEGER,puppet_status INTEGER)"
        )
        conn.execute(
            "INSERT INTO user_xiuxian VALUES('u1','化神境初期',%s,1,%s)",
            (stone, puppet_status),
        )
        conn.execute(
            "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
            "goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))"
        )


def create_player(
    database: Path,
    *,
    last: str = "2026-01-01 12:00:00",
    puppet_level: int = 2,
    herb_speed: int = 0,
    fields: str = "3",
    harvest_level: str = "2",
) -> None:
    columns = ", ".join(f'"{name}" TEXT' for name in REAL_PLAYER_FIELDS)
    with db_backend.transaction(database) as conn:
        conn.execute(f"CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,{columns})")
        conn.execute(
            'INSERT INTO mix_elixir_info(user_id,"收取时间","收取等级","灵田数量","药材速度","灵田傀儡") '
            "VALUES('u1',%s,%s,%s,%s,%s)",
            (last, harvest_level, fields, str(herb_speed), str(puppet_level)),
        )


def scalar(database: Path, sql: str, params=()):
    with db_backend.connection(database) as conn:
        row = conn.execute(sql, params).fetchone()
        return row[0] if row else None


def table_exists(database: Path, table: str) -> bool:
    return scalar(
        database,
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ) is not None


class PuppetMigrationTests(unittest.TestCase):
    def test_migration_is_idempotent_and_preserves_receipts(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            create_game(game)
            with DatabaseUnitOfWork(game, immediate=True) as uow:
                apply_puppet_operations(uow)
                uow.execute(
                    "INSERT INTO puppet_harvest_operations"
                    "(operation_id,user_id,payload,rewards_json,stone_cost) VALUES(?,?,?,?,?)",
                    ("h1", "u1", "[]", "[]", 500),
                )
                uow.execute(
                    "INSERT INTO puppet_operations"
                    "(operation_id,user_id,action,previous_level,current_level,stone_cost) "
                    "VALUES(?,?,?,?,?,?)",
                    ("p1", "u1", "purchase", 0, 1, 50),
                )
            with DatabaseUnitOfWork(game, immediate=True) as uow:
                apply_puppet_operations(uow)

            self.assertTrue(table_exists(game, "puppet_operations"))
            self.assertTrue(table_exists(game, "puppet_harvest_operations"))
            self.assertEqual(
                scalar(game, "SELECT COUNT(*) FROM puppet_harvest_operations"), 1
            )
            self.assertEqual(scalar(game, "SELECT COUNT(*) FROM puppet_operations"), 1)
            self.assertEqual(
                scalar(
                    game,
                    "SELECT COUNT(*) FROM puppet_feature_migrations WHERE version=?",
                    ("puppet.003",),
                ),
                1,
            )


class PuppetRequestPathSchemaTests(unittest.TestCase):
    def test_purchase_returns_schema_missing_without_creating_table(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            create_game(game)
            create_player(player, puppet_level=0)
            repository = PuppetPurchaseSqlRepository(game, player)

            first = repository.purchase("p1", "u1", 50)
            second = repository.upgrade("p2", "u1", {1: 20}, max_level=3)

            self.assertEqual((first.status, second.status), ("schema_missing", "schema_missing"))
            self.assertFalse(table_exists(game, "puppet_operations"))
            self.assertEqual(scalar(game, "SELECT stone FROM user_xiuxian WHERE user_id='u1'"), 5_000_000)

    def test_harvest_returns_schema_missing_without_creating_table(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            create_game(game)
            create_player(player, last="2026-01-01 00:00:00")
            repository = PuppetHarvestSqlRepository(game, player, max_goods_num=99)

            result = repository.harvest(
                "u1",
                operation_id="h1",
                now=datetime(2026, 1, 3, 0, 0),
                time_cost_hours=23,
                speed_base=0.05,
                harvest_costs={2: 500},
                harvest_bonus=0,
                reward_factory=lambda level, quantity: [
                    HarvestReward(3001, "恒心草", "药材", quantity)
                ],
            )

            self.assertEqual(result.status, "schema_missing")
            self.assertFalse(table_exists(game, "puppet_harvest_operations"))
            self.assertEqual(scalar(game, "SELECT stone FROM user_xiuxian WHERE user_id='u1'"), 5_000_000)


class PuppetFieldViewTests(unittest.TestCase):
    def build(self, temp: str, *, now: datetime | None = None, recent_rows=()):
        game = Path(temp) / "game.db"
        player = Path(temp) / "player.db"
        create_game(game)
        create_player(player)
        with DatabaseUnitOfWork(game, immediate=True) as uow:
            apply_puppet_operations(uow)
            for index, row in enumerate(recent_rows):
                uow.execute(
                    "INSERT INTO puppet_harvest_operations"
                    "(operation_id,user_id,payload,rewards_json,stone_cost,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (
                        f"h{index}",
                        row.get("user_id", "u1"),
                        "[]",
                        json.dumps(row.get("rewards", []), ensure_ascii=False),
                        row.get("stone_cost", 0),
                        row["created_at"],
                    ),
                )
        clock = FixedClock(now or datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc))
        return PuppetFieldApplication(game, player, clock=clock)

    def test_ready_snapshot_uses_real_columns(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            application = self.build(temp)
            snapshot = application.snapshot("u1")

            self.assertTrue(snapshot.ok)
            self.assertEqual(
                (
                    snapshot.field_count,
                    snapshot.harvest_level,
                    snapshot.puppet_level,
                    snapshot.puppet_enabled,
                ),
                (3, 2, 2, 1),
            )
            self.assertEqual(snapshot.maturity, "ready")
            self.assertEqual(snapshot.remaining_hours, 0.0)

    def test_waiting_snapshot_reports_remaining_hours(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            create_game(game)
            create_player(player, last="2026-01-02 11:00:00")
            clock = FixedClock(datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc))
            snapshot = PuppetFieldApplication(game, player, clock=clock).snapshot("u1")

            self.assertEqual(snapshot.maturity, "waiting")
            self.assertAlmostEqual(snapshot.remaining_hours, 22.0, places=2)
            self.assertIn("灵药成熟还需22小时0分钟", format_puppet_field(snapshot))

    def test_dirty_time_degrades_without_raising(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            create_game(game)
            create_player(player, last="not-a-time")
            snapshot = PuppetFieldApplication(game, player).snapshot("u1")

            self.assertTrue(snapshot.ok)
            self.assertEqual(snapshot.maturity, "invalid")
            self.assertIn("暂时无法判断（收取时间异常）", format_puppet_field(snapshot))

    def test_recent_window_uses_lower_bound_and_ignores_other_players(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            rows = (
                {
                    "created_at": "2026-01-02 08:00:00",
                    "rewards": [
                        {"goods_name": "恒心草", "quantity": 2},
                        {"goods_name": "恒心草", "quantity": 1},
                        {"goods_name": "灵芝", "quantity": 1},
                    ],
                    "stone_cost": 500,
                },
                {
                    "created_at": "2026-01-01 09:00:00",
                    "rewards": [{"goods_name": "灵芝", "quantity": 3}],
                    "stone_cost": 500,
                },
                {  # other player: must not leak
                    "created_at": "2026-01-02 09:00:00",
                    "user_id": "other",
                    "rewards": [{"goods_name": "灵芝", "quantity": 99}],
                    "stone_cost": 999,
                },
                {  # older than 7 days: outside the lower-bound window
                    "created_at": "2025-12-20 09:00:00",
                    "rewards": [{"goods_name": "旧药", "quantity": 9}],
                    "stone_cost": 900,
                },
            )
            application = self.build(temp, recent_rows=rows)
            snapshot = application.snapshot("u1")

            self.assertIsNotNone(snapshot.recent)
            recent = snapshot.recent
            assert recent is not None
            self.assertEqual(recent.count, 2)
            self.assertEqual(recent.stone_cost, 1000)
            self.assertEqual(recent.last_at, "2026-01-02 08:00:00")
            self.assertEqual(
                recent.rewards,
                (("恒心草", 3), ("灵芝", 1)),
            )
            rendered = format_puppet_field(snapshot)
            self.assertIn("最近7天收取：2 次，消耗灵石 1,000", rendered)
            self.assertIn("最近一次：01-02 08:00", rendered)
            self.assertIn("上次收获：恒心草 x3、灵芝 x1", rendered)
            self.assertNotIn("旧药", rendered)
            self.assertNotIn("99", rendered)

    def test_missing_operations_table_keeps_base_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            create_game(game)
            create_player(player)
            snapshot = PuppetFieldApplication(game, player).snapshot("u1")

            self.assertTrue(snapshot.ok)
            self.assertIsNone(snapshot.recent)

    def test_missing_player_row_and_optional_columns(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            create_game(game)
            with db_backend.transaction(player) as conn:
                conn.execute(
                    'CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,'
                    '"收取时间" TEXT, "灵田傀儡" INTEGER)'
                )
            application = PuppetFieldApplication(game, player)
            self.assertFalse(application.snapshot("u1").ok)
            self.assertIn("还没有灵田记录", application.snapshot("u1").notice or "")

            with db_backend.transaction(player) as conn:
                conn.execute(
                    'INSERT INTO mix_elixir_info VALUES(\'u1\',%s,%s)',
                    ("2026-01-01 12:00:00", 1),
                )
            snapshot = application.snapshot("u1")
            self.assertTrue(snapshot.ok)
            self.assertEqual((snapshot.field_count, snapshot.harvest_level, snapshot.herb_speed), (0, 0, 0))

    def test_missing_databases_and_identity_degrade(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "missing-game.db"
            player = Path(temp) / "missing-player.db"
            application = PuppetFieldApplication(game, player)
            self.assertFalse(application.snapshot("u1").ok)
            self.assertIn("玩家数据库缺失", application.snapshot("u1").notice or "")
            self.assertIn(
                "缺少玩家身份",
                PuppetFieldApplication(game, player).snapshot("").notice or "",
            )

    def test_status_reader_keeps_enable_switch_behind_application(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            create_game(game, puppet_status=1)
            create_player(player)
            snapshot = PuppetFieldApplication(
                game, player, status_reader=lambda user_id: 0
            ).snapshot("u1")

            self.assertTrue(snapshot.ok)
            self.assertEqual(snapshot.puppet_enabled, 0)
            self.assertIn("傀儡状态：关闭", format_puppet_field(snapshot))

    def test_handler_uses_feature_read_model(self) -> None:
        source = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_puppet"
            / "__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index("async def puppet_info_handler")
        end = source.find("\n\n\n", start)
        handler = source[start : end if end != -1 else len(source)]

        self.assertIn("field_application.snapshot(", handler)
        self.assertIn("format_puppet_field(", handler)
        self.assertNotIn("get_player_info", handler)
        self.assertNotIn("strptime", handler)


if __name__ == "__main__":
    unittest.main()
