from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_puppet.transaction_service import (
    PuppetHarvestReward,
    PuppetHarvestService,
)
from tests.test_db_backend import db_backend


NOW = datetime(2026, 7, 11, 12, 0, 0)
LAST_HARVEST = "2026-07-10 12:00:00"


class PuppetHarvestServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "xiuxian.sqlite3"
        self.player_database = root / "player.sqlite3"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                """
                CREATE TABLE user_xiuxian (
                    user_id TEXT PRIMARY KEY, level TEXT, stone INTEGER,
                    blessed_spot_flag INTEGER, puppet_status INTEGER
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE back (
                    user_id TEXT NOT NULL, goods_id INTEGER NOT NULL,
                    goods_name TEXT, goods_type TEXT, goods_num INTEGER,
                    create_time TEXT, update_time TEXT, bind_num INTEGER DEFAULT 0,
                    UNIQUE (user_id, goods_id)
                )
                """
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s, %s, %s)",
                ("user-1", "化神境初期", 100, 1, 1),
            )
        with db_backend.transaction(self.player_database) as conn:
            conn.execute(
                """
                CREATE TABLE mix_elixir_info (
                    user_id TEXT PRIMARY KEY, "收取时间" TEXT, "收取等级" INTEGER,
                    "灵田数量" INTEGER, "药材速度" INTEGER, "灵田傀儡" INTEGER
                )
                """
            )
            conn.execute(
                "INSERT INTO mix_elixir_info VALUES (%s, %s, %s, %s, %s, %s)",
                ("user-1", LAST_HARVEST, 2, 3, 0, 1),
            )
        self.service = PuppetHarvestService(
            self.game_database, self.player_database, max_goods_num=10
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def scalar(self, database: Path, sql: str, params=()):
        with db_backend.connection(database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    @staticmethod
    def rewards(level: str, quantity: int):
        return [PuppetHarvestReward(3001, "恒心草", "药材", quantity)]

    def harvest(self):
        return self.service.harvest(
            "user-1",
            now=NOW,
            time_cost_hours=23,
            speed_base=0.05,
            harvest_costs={1: 10},
            harvest_bonus=1,
            reward_factory=self.rewards,
        )

    def test_harvest_updates_rewards_stone_and_time_together(self) -> None:
        result = self.harvest()

        self.assertTrue(result.harvested)
        self.assertEqual(result.stone_cost, 10)
        self.assertEqual(result.rewards[0].quantity, 6)
        self.assertEqual(
            self.scalar(self.game_database, "SELECT stone FROM user_xiuxian WHERE user_id=%s", ("user-1",)),
            90,
        )
        self.assertEqual(
            self.scalar(self.game_database, "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s", ("user-1", 3001)),
            6,
        )
        self.assertEqual(
            self.scalar(self.player_database, 'SELECT "收取时间" FROM mix_elixir_info WHERE user_id=%s', ("user-1",)),
            "2026-07-11 12:00:00",
        )

    def test_retry_after_success_does_not_duplicate_rewards(self) -> None:
        self.assertTrue(self.harvest().harvested)
        duplicate = self.harvest()

        self.assertEqual(duplicate.status, "not_ready")
        self.assertEqual(
            self.scalar(self.game_database, "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s", ("user-1", 3001)),
            6,
        )
        self.assertEqual(
            self.scalar(self.game_database, "SELECT stone FROM user_xiuxian WHERE user_id=%s", ("user-1",)),
            90,
        )

    def test_full_inventory_preserves_harvest_state_and_stone(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "INSERT INTO back (user_id, goods_id, goods_num) VALUES (%s, %s, %s)",
                ("user-1", 3001, 5),
            )

        result = self.harvest()

        self.assertEqual(result.status, "inventory_full")
        self.assertEqual(
            self.scalar(self.game_database, "SELECT stone FROM user_xiuxian WHERE user_id=%s", ("user-1",)),
            100,
        )
        self.assertEqual(
            self.scalar(self.player_database, 'SELECT "收取时间" FROM mix_elixir_info WHERE user_id=%s', ("user-1",)),
            LAST_HARVEST,
        )

    def test_insufficient_stone_disables_puppet_without_advancing_harvest(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=%s WHERE user_id=%s", (9, "user-1"))

        result = self.harvest()

        self.assertEqual(result.status, "stone_insufficient")
        self.assertEqual(
            self.scalar(self.game_database, "SELECT puppet_status FROM user_xiuxian WHERE user_id=%s", ("user-1",)),
            0,
        )
        self.assertEqual(
            self.scalar(self.player_database, 'SELECT "收取时间" FROM mix_elixir_info WHERE user_id=%s', ("user-1",)),
            LAST_HARVEST,
        )
        self.assertIsNone(self.scalar(self.game_database, "SELECT goods_num FROM back WHERE user_id=%s", ("user-1",)))

    def test_player_write_failure_rolls_back_game_assets(self) -> None:
        with db_backend.transaction(self.player_database) as conn:
            conn.execute(
                """
                CREATE TRIGGER reject_harvest_time BEFORE UPDATE ON mix_elixir_info
                BEGIN SELECT RAISE(ABORT, 'player update rejected'); END
                """
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.harvest()

        self.assertEqual(
            self.scalar(self.game_database, "SELECT stone FROM user_xiuxian WHERE user_id=%s", ("user-1",)),
            100,
        )
        self.assertIsNone(self.scalar(self.game_database, "SELECT goods_num FROM back WHERE user_id=%s", ("user-1",)))
        self.assertEqual(
            self.scalar(self.player_database, 'SELECT "收取时间" FROM mix_elixir_info WHERE user_id=%s', ("user-1",)),
            LAST_HARVEST,
        )
