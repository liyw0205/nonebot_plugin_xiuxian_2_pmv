"""Production-shaped evidence for 灵田傀儡自动收取.

The defects this suite pins down were only reachable when the REAL player schema
(``灵田傀儡`` is the puppet level; there is no ``傀儡等级`` column), the REAL
reward factory (``PuppetHarvestReward`` dataclasses) and the REAL clock
(``SystemClock``, an AWARE UTC instant) were combined.  Every earlier test used
a hand-built fixture with a ``傀儡等级`` column and a 4-tuple factory, so the
自动收取 path looked green while it raised on production data.
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.puppet.application import PuppetApplication
from nonebot_plugin_xiuxian_2.features.puppet.harvest_repository import (
    HarvestReward,
    PuppetHarvestSqlRepository,
    normalize_now,
    parse_harvest_time,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_puppet.transaction_service import (
    PuppetHarvestReward,
)
from tests.test_db_backend import db_backend

REAL_PLAYER_FIELDS = (
    "收取时间",
    "收取等级",
    "灵田数量",
    "药材速度",
    "灵田傀儡",
    "丹药控火",
    "丹药耐药性",
    "炼丹记录",
    "炼丹经验",
)
COSTS = {1: 5_000_000, 2: 3_500_000, 3: 2_000_000}


def real_player_schema(database: Path) -> None:
    columns = ", ".join(f'"{field}" TEXT' for field in REAL_PLAYER_FIELDS)
    with db_backend.transaction(database) as conn:
        conn.execute(f"CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,{columns})")


def seed_player_row(
    database: Path,
    *,
    puppet_level: str = "1",
    last: str = "2026-10-29 00:00:00",
    harvest_level: str = "2",
    field_count: str = "3",
    herb_speed: str = "0",
) -> None:
    with db_backend.transaction(database) as conn:
        conn.execute(
            'INSERT INTO mix_elixir_info(user_id,"收取时间","收取等级","灵田数量","药材速度","灵田傀儡") '
            "VALUES(?,?,?,?,?,?)",
            ("u1", last, harvest_level, field_count, herb_speed, puppet_level),
        )


class ProductionSchemaHarvestTests(unittest.TestCase):
    """Drive the repository the way the hourly job drives it."""

    def build(
        self,
        temp: str,
        *,
        puppet_level: str = "1",
        puppet_status: int = 1,
        blessed_spot_flag: int = 1,
        stone: int = 999_999_999,
        last: str = "2026-10-29 00:00:00",
        field_count: str = "3",
        herb_speed: str = "0",
    ) -> tuple[Path, Path]:
        game = Path(temp) / "game.db"
        player = Path(temp) / "player.db"
        with db_backend.transaction(game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level TEXT,stone INTEGER,"
                "blessed_spot_flag INTEGER,puppet_status INTEGER)"
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES('u1','化神境初期',%s,%s,%s)",
                (stone, blessed_spot_flag, puppet_status),
            )
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))"
            )
        real_player_schema(player)
        seed_player_row(
            player,
            puppet_level=puppet_level,
            last=last,
            field_count=field_count,
            herb_speed=herb_speed,
        )
        return game, player

    def scalar(self, database: Path, sql: str, params=()):
        with db_backend.connection(database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    @staticmethod
    def production_factory(level: str, quantity: int):
        """Exactly what ``check_and_harvest`` builds: dataclasses, not tuples."""
        return [PuppetHarvestReward(3001, "恒心草", "药材", quantity)]

    def aware_now(self) -> datetime:
        """Exactly what ``SystemClock.now()`` returns: an aware UTC instant."""
        return datetime.now(timezone.utc)

    def mature_stamp(self) -> str:
        """A 收取时间 the aware clock is guaranteed to consider mature.

        The window is 23h, so one day back is always ready regardless of where
        the current instant falls inside its day.
        """
        return (self.aware_now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")

    def harvest(self, application: PuppetApplication, *, now: datetime | None = None, **overrides):
        params = {
            "operation_id": "puppet:harvest:u1:1",
            "user_id": "u1",
            "now": now if now is not None else self.aware_now(),
            "time_cost_hours": 23,
            "speed_base": 0.05,
            "harvest_costs": COSTS,
            "harvest_bonus": 0,
            "reward_factory": self.production_factory,
            "max_goods_num": 1000,
        }
        params.update(overrides)
        return application.harvest(**params)

    def test_production_shape_harvest_succeeds_with_aware_clock(self) -> None:
        """Regression: the old SELECT read a nonexistent 傀儡等级 column."""
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, last=self.mature_stamp())
            summary = self.harvest(PuppetApplication(game, player))
            self.assertEqual(summary.status, "harvested")
            self.assertTrue(summary.harvested)
            self.assertEqual(summary.stone_cost, COSTS[1])
            self.assertEqual([reward.goods_name for reward in summary.rewards], ["恒心草"])
            self.assertEqual(
                self.scalar(game, "SELECT stone FROM user_xiuxian WHERE user_id=%s", ("u1",)),
                999_999_999 - COSTS[1],
            )
            self.assertEqual(
                self.scalar(game, "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s", ("u1", 3001)),
                3 + 2,
            )

    def test_quantity_follows_field_count_plus_harvest_level_plus_bonus(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, field_count="4", last="2026-10-28 00:00:00")
            summary = self.harvest(PuppetApplication(game, player), harvest_bonus=5)
            self.assertEqual(summary.status, "harvested")
            self.assertEqual(summary.rewards[0].quantity, 4 + 2 + 5)

    def test_herb_speed_shortens_the_maturity_window(self) -> None:
        """20% speed means 23h * (1 - 0.05*4) = 18.4h, so a 20h-old field is ready."""
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, herb_speed="4", last="2026-10-29 00:00:00")
            summary = self.harvest(PuppetApplication(game, player), now=datetime(2026, 10, 29, 20, 0))
            self.assertEqual(summary.status, "harvested")

    def test_immature_field_reports_remaining_hours_instead_of_raising(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, last="2026-10-29 00:00:00")
            summary = self.harvest(PuppetApplication(game, player), now=datetime(2026, 10, 29, 1, 0))
            self.assertEqual(summary.status, "not_ready")
            self.assertFalse(summary.harvested)
            self.assertAlmostEqual(summary.remaining_hours, 22.0, places=3)
            self.assertEqual(
                self.scalar(game, "SELECT stone FROM user_xiuxian WHERE user_id=%s", ("u1",)),
                999_999_999,
            )

    def test_naive_clock_is_still_accepted(self) -> None:
        """Normalizing must not break the naive-clock callers the old tests used."""
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            summary = self.harvest(PuppetApplication(game, player), now=datetime(2026, 10, 30, 0, 0))
            self.assertEqual(summary.status, "harvested")

    def test_puppet_switched_off_is_reported_as_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, puppet_status=0)
            summary = self.harvest(PuppetApplication(game, player))
            self.assertEqual(summary.status, "puppet_disabled")
            self.assertEqual(
                self.scalar(game, "SELECT stone FROM user_xiuxian WHERE user_id=%s", ("u1",)),
                999_999_999,
            )

    def test_insufficient_stone_switches_the_puppet_off(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, stone=COSTS[1] - 1)
            summary = self.harvest(PuppetApplication(game, player))
            self.assertEqual(summary.status, "stone_insufficient")
            self.assertEqual(summary.stone_cost, COSTS[1])
            self.assertEqual(
                self.scalar(game, "SELECT puppet_status FROM user_xiuxian WHERE user_id=%s", ("u1",)),
                0,
            )
            self.assertEqual(
                self.scalar(game, "SELECT COUNT(*) FROM back WHERE user_id=%s", ("u1",)),
                0,
            )

    def test_upgraded_puppet_uses_its_own_level_cost(self) -> None:
        """Level 3 lives in 灵田傀儡; the old code read a literal string instead."""
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, puppet_level="3")
            summary = self.harvest(PuppetApplication(game, player))
            self.assertEqual(summary.status, "harvested")
            self.assertEqual(summary.stone_cost, COSTS[3])

    def test_full_backpack_refuses_without_spending_stone(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            with db_backend.transaction(game) as conn:
                conn.execute(
                    "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) "
                    "VALUES('u1',3001,'恒心草','药材',999,0)"
                )
            summary = self.harvest(PuppetApplication(game, player))
            self.assertEqual(summary.status, "inventory_full")
            self.assertEqual(
                self.scalar(game, "SELECT stone FROM user_xiuxian WHERE user_id=%s", ("u1",)),
                999_999_999,
            )
            self.assertEqual(
                self.scalar(
                    player, 'SELECT "收取时间" FROM mix_elixir_info WHERE user_id=%s', ("u1",)
                ),
                "2026-10-29 00:00:00",
            )

    def test_duplicate_operation_does_not_pay_twice(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            application = PuppetApplication(game, player)
            first = self.harvest(application)
            again = self.harvest(application)
            self.assertEqual((first.status, again.status), ("harvested", "duplicate"))
            self.assertTrue(again.harvested)
            self.assertEqual(
                self.scalar(game, "SELECT stone FROM user_xiuxian WHERE user_id=%s", ("u1",)),
                999_999_999 - COSTS[1],
            )
            self.assertEqual(
                self.scalar(game, "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s", ("u1", 3001)),
                5,
            )

    def test_missing_player_row_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            with db_backend.transaction(player) as conn:
                conn.execute("DELETE FROM mix_elixir_info")
            summary = self.harvest(PuppetApplication(game, player))
            self.assertEqual(summary.status, "player_info_missing")

    def test_blessed_spot_is_required(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, blessed_spot_flag=0)
            summary = self.harvest(PuppetApplication(game, player))
            self.assertEqual(summary.status, "blessed_spot_missing")


class HarvestHelperTests(unittest.TestCase):
    def test_normalize_now_converts_aware_to_naive_utc(self) -> None:
        aware = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)
        self.assertEqual(normalize_now(aware), datetime(2026, 10, 29, 12, 0))
        self.assertIsNone(normalize_now(aware).tzinfo)

    def test_normalize_now_passes_naive_through(self) -> None:
        naive = datetime(2026, 10, 29, 12, 0)
        self.assertEqual(normalize_now(naive), naive)

    def test_parse_harvest_time_rejects_unusable_values(self) -> None:
        self.assertEqual(parse_harvest_time("2026-10-29 12:00:00"), datetime(2026, 10, 29, 12, 0))
        for bad in (None, "", 0, "0", "not a date"):
            self.assertIsNone(parse_harvest_time(bad), bad)

    def test_reward_factory_accepts_both_shapes(self) -> None:
        from nonebot_plugin_xiuxian_2.features.puppet.harvest_repository import _coerce_reward

        self.assertEqual(
            _coerce_reward((7, "药材", "药材", 2)),
            HarvestReward(7, "药材", "药材", 2),
        )
        self.assertEqual(
            _coerce_reward(PuppetHarvestReward(7, "药材", "药材", 2)),
            HarvestReward(7, "药材", "药材", 2),
        )

    def test_missing_puppet_level_costs_are_rejected(self) -> None:
        """A level with no configured cost must fail closed, not harvest free."""
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            with db_backend.transaction(game) as conn:
                conn.execute(
                    "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level TEXT,stone INTEGER,"
                    "blessed_spot_flag INTEGER,puppet_status INTEGER)"
                )
                conn.execute("INSERT INTO user_xiuxian VALUES('u1','化神境初期',100,1,1)")
                conn.execute(
                    "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                    "goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))"
                )
            real_player_schema(player)
            seed_player_row(player, puppet_level="9", last="2026-10-28 00:00:00")
            repo = PuppetHarvestSqlRepository(game, player, max_goods_num=1000)
            summary = repo.harvest(
                "u1",
                operation_id="op-9",
                now=datetime(2026, 10, 30, 0, 0),
                time_cost_hours=23,
                speed_base=0.05,
                harvest_costs=COSTS,
                harvest_bonus=0,
                reward_factory=lambda level, quantity: [(3001, "恒心草", "药材", quantity)],
            )
            self.assertEqual(summary.status, "invalid_puppet_level")


class HarvestWiringContractTests(unittest.TestCase):
    """The handler must surface every failure instead of silently returning None."""

    def handler_source(self) -> str:
        source = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_puppet"
            / "__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index("async def check_and_harvest")
        return source[start : source.index("@buy_puppet.handle()", start)]

    def test_handler_keeps_calling_the_feature_application(self) -> None:
        self.assertIn("puppet_application.harvest(", self.handler_source())

    def test_handler_does_not_silently_swallow_unknown_statuses(self) -> None:
        handler = self.handler_source()
        self.assertNotIn("return None\n\n\n@buy_puppet", handler)
        # The generic fallback must be a real message, not a bare None.
        self.assertIn("傀儡收取未完成", handler)

    def test_handler_reports_maturity_cost_and_duplicate_states(self) -> None:
        handler = self.handler_source()
        for token in ('"not_ready"', '"stone_insufficient"', '"inventory_full"', '"duplicate"'):
            self.assertIn(token, handler, token)


if __name__ == "__main__":
    unittest.main()
