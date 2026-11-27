from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.features.accessory_package.application import AccessoryPackageApplication
from nonebot_plugin_xiuxian_2.features.accessory_package.attached_migrations import (
    apply_attached_player_accessory,
    apply_attached_player_accessory_operations,
)
from nonebot_plugin_xiuxian_2.features.accessory_package.migrations import apply_accessory_package
from nonebot_plugin_xiuxian_2.features.accessory_package.schemas import AccessoryPackageRequest
from nonebot_plugin_xiuxian_2.features.package_reward.domain import PackageReward
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.database.inventory import read_inventory_rows
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


class AccessoryPackageReadReuseTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.game = Path(self.directory.name) / "game.db"
        self.player = Path(self.directory.name) / "player.db"
        with sqlite3.connect(self.game) as connection:
            connection.executescript(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone TEXT NOT NULL);"
                "INSERT INTO user_xiuxian VALUES('u','100');"
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,bind_num INTEGER,UNIQUE(user_id,goods_id));"
                "INSERT INTO back VALUES('u',3001,'package','package',3,3);"
            )
        with DatabaseUnitOfWork(self.game) as uow:
            apply_platform_schema(uow)
            apply_accessory_package(uow)
        with DatabaseUnitOfWork(self.player) as uow:
            apply_platform_schema(uow)
        with AttachedDatabaseUnitOfWork(self.game, attachments={"player_data": self.player}) as uow:
            apply_attached_player_accessory(uow)
            apply_attached_player_accessory_operations(uow)
        self.application = AccessoryPackageApplication(self.game, self.player)

    def request(self, *, operation_id="op", rewards=None, max_goods_num=1000, accessory_limit=5):
        return AccessoryPackageRequest.build(
            operation_id=operation_id, user_id="u", package_id=3001, quantity=2,
            rewards=(PackageReward(4001, "reward", "item", 2),) if rewards is None else rewards,
            accessories=({"uid": "acc-1", "name": "ring"},),
            max_goods_num=max_goods_num, accessory_limit=accessory_limit,
        )

    def open(self, **kwargs):
        request = self.request(**kwargs)
        return self.application.open_package(
            operation_id=request.operation_id, user_id=request.user_id,
            package_id=request.package_id, quantity=request.quantity,
            rewards=request.rewards, accessories=request.accessories,
            max_goods_num=request.max_goods_num, accessory_limit=request.accessory_limit,
        )

    def row(self, sql, params=(), *, database=None):
        with sqlite3.connect(database or self.game) as connection:
            return connection.execute(sql, params).fetchone()

    def test_pure_accessory_package_success_replay_and_reconciliation(self):
        import json

        request = self.request(rewards=iter(()))
        self.assertEqual(request.rewards, ())
        self.assertIsNone(self.application._prepare(request, request.payload()))
        result = self.application.reconcile({}, operation_id=request.operation_id)
        replay = self.open(rewards=())
        self.assertEqual((result.status, replay.status), ("applied", "replayed"))
        self.assertEqual(self.row("SELECT stone FROM user_xiuxian"), ("100",))
        self.assertEqual(self.row("SELECT goods_num,bind_num FROM back WHERE goods_id=3001"), (1, 1))
        self.assertEqual(self.row("SELECT COUNT(*) FROM back"), (1,))
        bag = json.loads(self.row("SELECT bag FROM player_accessory", database=self.player)[0])
        self.assertEqual([item["uid"] for item in bag], ["acc-1"])
        self.assertEqual(self.row("SELECT rewards_json,status FROM accessory_package_operations"), ("[]", "applied"))
        self.assertEqual(self.row("SELECT status FROM accessory_package_operations", database=self.player), ("applied",))

    def test_pure_accessory_capacity_rejection_compensates_package(self):
        with sqlite3.connect(self.player) as connection:
            connection.execute("INSERT INTO player_accessory VALUES('u','{}','[{\"uid\":\"old\"}]')")
        result = self.open(rewards=(), accessory_limit=1)
        self.assertEqual((result.status, result.code), ("rejected", "accessory_full"))
        self.assertEqual(self.row("SELECT goods_num,bind_num FROM back WHERE goods_id=3001"), (3, 3))
        self.assertEqual(self.row("SELECT stone FROM user_xiuxian"), ("100",))
        self.assertEqual(self.row("SELECT status FROM accessory_package_operations"), ("compensated",))

    def test_optional_ordinary_rewards_still_validate_and_accessories_are_required(self):
        for rewards in ((PackageReward(4001, "bad", "item", 0),), ()):
            with self.subTest(rewards=rewards), self.assertRaises(ValueError):
                AccessoryPackageRequest.build(operation_id="invalid", user_id="u", package_id=3001,
                    quantity=1, rewards=rewards, accessories=(), max_goods_num=1000, accessory_limit=5)
        with self.assertRaises(ValueError):
            self.request(rewards=(PackageReward(4001, "bad", "item", 0),))

    def test_only_prepare_and_finalize_snapshots_with_ordered_duplicate_rewards(self):
        balance = 10**80 + 321
        with sqlite3.connect(self.game) as connection:
            connection.execute("UPDATE user_xiuxian SET stone=?", (str(balance),))
        rewards = (
            PackageReward(None, "灵石", None, 7),
            PackageReward(3001, "returned package", "item", 1),
            PackageReward(4001, "first name", "item", 2),
            PackageReward(4001, "last name", "final type", 3),
            PackageReward(5001, "other", "item", 4),
        )
        with (
            patch.object(self.application.game, "snapshot", wraps=self.application.game.snapshot) as snapshots,
            patch("nonebot_plugin_xiuxian_2.features.accessory_package.repository.read_inventory_rows", wraps=read_inventory_rows) as inventory,
        ):
            result = self.open(rewards=rewards)
            replay = self.open(rewards=rewards)
        self.assertEqual((result.status, replay.status), ("applied", "replayed"))
        self.assertEqual(snapshots.call_count, 2)
        self.assertEqual(inventory.call_count, 2)
        self.assertEqual(tuple(inventory.call_args_list[0].args[2]), (3001, 3001, 4001, 5001))
        self.assertEqual(result.before["rewards"]["3001"], result.before["package"])
        self.assertIsNone(result.before["rewards"]["4001"])
        self.assertEqual(int(result.before["stone"]), balance)
        self.assertEqual(int(result.after["stone"]), balance + 7)
        self.assertEqual(self.row("SELECT goods_name,goods_num,bind_num FROM back WHERE goods_id=3001"), ("returned package", 2, 2))
        self.assertEqual(self.row("SELECT goods_name,goods_type,goods_num,bind_num FROM back WHERE goods_id=4001"), ("last name", "final type", 5, 5))

    def test_finalize_reads_changes_from_the_intervening_player_phase(self):
        apply_player = self.application._apply_player

        def apply_and_change_game(request):
            status = apply_player(request)
            with sqlite3.connect(self.game) as connection:
                connection.execute("UPDATE back SET goods_num=goods_num+7 WHERE goods_id=4001")
                connection.execute("UPDATE user_xiuxian SET stone='111'")
            return status

        with patch.object(self.application, "_apply_player", side_effect=apply_and_change_game):
            result = self.open()
        self.assertEqual(result.status, "applied")
        self.assertEqual(result.after["rewards"]["4001"]["goods_num"], 9)
        self.assertEqual(int(result.after["stone"]), 111)
        self.assertEqual(int(result.before["stone"]), 100)

    def test_multi_item_compensation_restores_null_bind_names_and_missing_rows(self):
        with sqlite3.connect(self.game) as connection:
            connection.execute("INSERT INTO back VALUES('u',4001,'old name','old type',6,NULL)")
        with sqlite3.connect(self.player) as connection:
            connection.execute("INSERT INTO player_accessory VALUES('u','{}','[{\"uid\":\"old\"}]')")
        rewards = (
            PackageReward(None, "灵石", None, 5),
            PackageReward(3001, "returned package", "item", 1),
            PackageReward(4001, "first", "item", 2),
            PackageReward(4001, "last", "item", 3),
            PackageReward(5001, "missing", "item", 4),
        )
        result = self.open(rewards=rewards, accessory_limit=1)
        self.assertEqual((result.status, result.code), ("rejected", "accessory_full"))
        self.assertEqual(self.row("SELECT stone FROM user_xiuxian"), ("100",))
        self.assertEqual(self.row("SELECT goods_name,goods_type,goods_num,bind_num FROM back WHERE goods_id=3001"), ("package", "package", 3, 3))
        self.assertEqual(self.row("SELECT goods_name,goods_type,goods_num,bind_num FROM back WHERE goods_id=4001"), ("old name", "old type", 6, None))
        self.assertIsNone(self.row("SELECT goods_num FROM back WHERE goods_id=5001"))
        self.assertEqual(self.row("SELECT status FROM accessory_package_operations"), ("compensated",))

    def test_package_as_reward_retains_preconsume_capacity_rule(self):
        result = self.open(rewards=(PackageReward(3001, "returned", "item", 2),), max_goods_num=3)
        self.assertEqual((result.status, result.code), ("rejected", "inventory_full"))
        self.assertEqual(self.row("SELECT goods_num FROM back"), (3,))

    def test_missing_bind_column_retains_snapshot_and_reward_compatibility(self):
        with sqlite3.connect(self.game) as connection:
            connection.executescript(
                "DROP TABLE back;CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,UNIQUE(user_id,goods_id));"
                "INSERT INTO back VALUES('u',3001,'package','package',3);"
                "INSERT INTO back VALUES('u',4001,'old','item',1);"
            )
        result = self.open()
        self.assertEqual(result.status, "applied")
        self.assertEqual(result.before["package"]["bind_num"], 0)
        self.assertEqual(result.before["rewards"]["4001"]["bind_num"], 0)
        self.assertEqual(result.after["rewards"]["4001"]["bind_num"], 0)
        self.assertEqual(self.row("SELECT goods_num FROM back WHERE goods_id=4001"), (3,))

    def test_duplicate_legacy_rows_and_numeric_aliases_match_original_point_reads(self):
        with sqlite3.connect(self.game) as connection:
            connection.executescript(
                "DROP TABLE back;CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,bind_num INTEGER);"
                "INSERT INTO back VALUES('u',3001,'package','package',3,3);"
                "INSERT INTO back VALUES('u',4001,'first','item',1,NULL);"
                "INSERT INTO back VALUES('u',4001,'second','item',100,100);"
            )
        request = self.request(rewards=(
            PackageReward(4001, "reward", "item", 2),
            PackageReward("04001", "alias", "item", 1),
            PackageReward(5001, "missing", "item", 1),
        ))
        for indexed in (False, True):
            with self.subTest(indexed=indexed):
                if indexed:
                    with sqlite3.connect(self.game) as connection:
                        connection.execute("CREATE INDEX legacy_back_lookup ON back(user_id,goods_id)")
                with DatabaseUnitOfWork(self.game) as uow:
                    snapshot = self.application.game.snapshot(uow, request)
                    for key, item_id in (("4001", 4001), ("04001", "04001"), ("5001", 5001)):
                        original = uow.query_one(
                            "SELECT goods_name,goods_type,goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?",
                            ("u", item_id),
                        )
                        self.assertEqual(snapshot["rewards"][key], original)
        with self.assertRaisesRegex(sqlite3.OperationalError, "ON CONFLICT"):
            self.open(max_goods_num=10)
        self.assertEqual(self.row("SELECT goods_num FROM back WHERE goods_id=3001"), (3,))
        self.assertEqual(self.row("SELECT COUNT(*) FROM accessory_package_operations"), (0,))

    def test_missing_inventory_table_preserves_empty_snapshot(self):
        with sqlite3.connect(self.game) as connection:
            connection.execute("DROP TABLE back")
        with DatabaseUnitOfWork(self.game) as uow:
            snapshot = self.application.game.snapshot(uow, self.request())
        self.assertEqual(snapshot, {"stone": "100", "package": None, "rewards": {"4001": None}})
