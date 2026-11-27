from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.features.package_reward.application import PackageRewardApplication
from nonebot_plugin_xiuxian_2.features.package_reward.domain import PackageReward
from nonebot_plugin_xiuxian_2.features.package_reward.migrations import apply_package_reward
from nonebot_plugin_xiuxian_2.features.package_reward.repository import PackageRewardRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.database.inventory import read_inventory_rows
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


class PackageRewardReadReuseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "game.db"
        with sqlite3.connect(self.database) as connection:
            connection.executescript(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone TEXT NOT NULL);"
                "INSERT INTO user_xiuxian VALUES('u','100');"
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,bind_num INTEGER,UNIQUE(user_id,goods_id));"
                "INSERT INTO back VALUES('u',3001,'package','package',3,3);"
            )
        with DatabaseUnitOfWork(self.database) as uow:
            apply_platform_schema(uow)
            apply_package_reward(uow)
        self.repository = PackageRewardRepository()
        self.application = PackageRewardApplication(self.database, repository=self.repository)

    def open(self, *, operation_id="op", rewards=None, max_goods_num=1000):
        return self.application.open_package(
            operation_id=operation_id,
            user_id="u",
            package_id=3001,
            quantity=2,
            rewards=rewards or (PackageReward(4001, "reward", "item", 2),),
            max_goods_num=max_goods_num,
        )

    def row(self, sql, params=()):
        with sqlite3.connect(self.database) as connection:
            return connection.execute(sql, params).fetchone()

    def test_one_wallet_read_per_stage_with_large_balance_and_replay(self):
        balance = 10**80 + 123
        with sqlite3.connect(self.database) as connection:
            connection.execute("UPDATE user_xiuxian SET stone=?", (str(balance),))
        rewards = (
            PackageReward(None, "灵石", None, 7),
            PackageReward(3001, "returned package", "item", 1),
            PackageReward(4001, "first name", "item", 2),
            PackageReward(4001, "last name", "final type", 3),
            PackageReward(5001, "other", "item", 4),
        )
        with (
            patch.object(self.repository, "snapshot", wraps=self.repository.snapshot) as snapshots,
            patch("nonebot_plugin_xiuxian_2.features.package_reward.repository.read_inventory_rows", wraps=read_inventory_rows) as inventory,
        ):
            result = self.open(rewards=rewards)
            replay = self.open(rewards=rewards)
        self.assertEqual((result.status, replay.status), ("applied", "replayed"))
        self.assertEqual(snapshots.call_count, 2)
        self.assertEqual(inventory.call_count, 1)
        self.assertEqual(tuple(inventory.call_args.args[2]), (3001, 4001, 5001))
        self.assertEqual(result.before, {"rows": 1, "stone": balance})
        self.assertEqual(result.after, {"rows": 1, "stone": balance + 7})
        self.assertEqual(int(self.row("SELECT stone FROM user_xiuxian")[0]), balance + 7)
        self.assertEqual(self.row("SELECT goods_num,bind_num FROM back WHERE goods_id=3001"), (2, 2))
        self.assertEqual(self.row("SELECT goods_name,goods_type,goods_num,bind_num FROM back WHERE goods_id=4001"), ("last name", "final type", 5, 5))

    def test_standalone_apply_reads_a_fresh_wallet_once_per_call(self):
        with patch.object(self.repository, "snapshot", wraps=self.repository.snapshot) as snapshots:
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                status = self.repository.apply(
                    uow, operation_id="first", user_id="u", package_id=3001,
                    quantity=1, rewards=(PackageReward(None, "灵石", None, -100),), max_goods_num=1000,
                )
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                refused = self.repository.apply(
                    uow, operation_id="second", user_id="u", package_id=3001,
                    quantity=1, rewards=(PackageReward(None, "灵石", None, -1),), max_goods_num=1000,
                )
        self.assertEqual((status, refused), ("applied", "stone_insufficient"))
        self.assertEqual(snapshots.call_count, 2)
        self.assertEqual(self.row("SELECT goods_num FROM back WHERE goods_id=3001"), (2,))

    def test_capacity_reads_remain_after_package_and_wallet_writes(self):
        with sqlite3.connect(self.database) as connection:
            connection.execute("INSERT INTO back VALUES('u',4001,'old','item',1,NULL)")
        for source, trigger in (
            ("package", "AFTER UPDATE OF goods_num ON back WHEN NEW.goods_id=3001"),
            ("wallet", "AFTER UPDATE OF stone ON user_xiuxian"),
        ):
            with self.subTest(source=source):
                with sqlite3.connect(self.database) as connection:
                    connection.execute(
                        f"CREATE TRIGGER rewrite_reward {trigger} BEGIN "
                        "UPDATE back SET goods_num=100 WHERE user_id='u' AND goods_id=4001; END"
                    )
                result = self.open(
                    operation_id=source,
                    rewards=(PackageReward(None, "灵石", None, 5), PackageReward(4001, "reward", "item", 2)),
                    max_goods_num=100,
                )
                self.assertEqual((result.status, result.code), ("rejected", "inventory_full"))
                self.assertEqual(self.row("SELECT goods_num,bind_num FROM back WHERE goods_id=3001"), (3, 3))
                self.assertEqual(self.row("SELECT goods_num,bind_num FROM back WHERE goods_id=4001"), (1, None))
                self.assertEqual(self.row("SELECT stone FROM user_xiuxian"), ("100",))
                self.assertEqual(self.row("SELECT COUNT(*) FROM package_reward_operations"), (0,))
                with sqlite3.connect(self.database) as connection:
                    connection.execute("DROP TRIGGER rewrite_reward")

    def test_package_as_reward_capacity_uses_consumed_quantity(self):
        result = self.open(rewards=(PackageReward(3001, "returned", "item", 2),), max_goods_num=3)
        self.assertEqual(result.status, "applied")
        self.assertEqual(self.row("SELECT goods_name,goods_num,bind_num FROM back"), ("returned", 3, 3))

    def test_legacy_duplicate_wallet_rows_keep_exact_sum(self):
        large = 10**70 + 17
        with sqlite3.connect(self.database) as connection:
            connection.executescript("DROP TABLE user_xiuxian;CREATE TABLE user_xiuxian(user_id TEXT,stone TEXT);")
            connection.executemany("INSERT INTO user_xiuxian VALUES('u',?)", ((str(large),), ("5",)))
        with DatabaseUnitOfWork(self.database) as uow:
            self.assertEqual(self.repository.snapshot(uow, "u"), {"rows": 2, "stone": large + 5})

    def test_missing_bind_column_retains_stone_only_compatibility(self):
        with sqlite3.connect(self.database) as connection:
            connection.executescript(
                "DROP TABLE back;CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,UNIQUE(user_id,goods_id));"
                "INSERT INTO back VALUES('u',3001,'package','package',3);"
            )
        result = self.open(rewards=(PackageReward(None, "灵石", None, 5),))
        self.assertEqual(result.status, "applied")
        self.assertEqual(self.row("SELECT goods_num FROM back"), (1,))
        self.assertEqual(self.row("SELECT stone FROM user_xiuxian"), ("105",))

    def test_nonunique_legacy_inventory_does_not_hide_upsert_error(self):
        with sqlite3.connect(self.database) as connection:
            connection.executescript(
                "DROP TABLE back;CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,bind_num INTEGER);"
                "INSERT INTO back VALUES('u',3001,'package','package',3,3);"
                "INSERT INTO back VALUES('u',4001,'first','item',1,NULL);"
                "INSERT INTO back VALUES('u',4001,'second','item',100,100);"
            )
        with self.assertRaisesRegex(sqlite3.OperationalError, "ON CONFLICT"):
            self.open(max_goods_num=10)
        self.assertEqual(self.row("SELECT goods_num FROM back WHERE goods_id=3001"), (3,))
        self.assertEqual(self.row("SELECT COUNT(*) FROM package_reward_operations"), (0,))

    def test_operation_insert_failure_rolls_back_all_assets(self):
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "CREATE TRIGGER reject_receipt BEFORE INSERT ON package_reward_operations "
                "BEGIN SELECT RAISE(ABORT,'receipt unavailable');END"
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "receipt unavailable"):
            self.open(rewards=(PackageReward(None, "灵石", None, 50), PackageReward(4001, "reward", "item", 2)))
        self.assertEqual(self.row("SELECT goods_num,bind_num FROM back WHERE goods_id=3001"), (3, 3))
        self.assertIsNone(self.row("SELECT goods_num FROM back WHERE goods_id=4001"))
        self.assertEqual(self.row("SELECT stone FROM user_xiuxian"), ("100",))
