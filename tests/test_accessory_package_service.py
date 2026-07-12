from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.accessory_package_service import (
    AccessoryPackageService,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.package_reward_service import (
    PackageReward,
)
from tests.test_db_backend import db_backend


class AccessoryPackageServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "xiuxian.sqlite3"
        self.player_database = root / "player.sqlite3"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)"
            )
            conn.execute(
                """
                CREATE TABLE back (
                    user_id TEXT NOT NULL, goods_id INTEGER NOT NULL,
                    goods_name TEXT, goods_type TEXT, goods_num INTEGER NOT NULL,
                    bind_num INTEGER DEFAULT 0,
                    UNIQUE (user_id, goods_id)
                )
                """
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("user", 100))
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)",
                ("user", 3001, "饰品礼包", "礼包", 3, 3),
            )
        self.service = AccessoryPackageService(
            self.game_database, self.player_database
        )
        self.rewards = (
            PackageReward(None, "灵石", None, 50),
            PackageReward(4001, "测试丹药", "丹药", 2),
        )
        self.accessories = (
            {"uid": "acc-fixed-1", "item_id": 5001, "name": "测试戒指", "part": "戒指", "set_type": "烈阳", "quality": 2, "affixes": [], "locked_affixes": [], "wash_count": 0},
            {"uid": "acc-fixed-2", "item_id": 5001, "name": "测试戒指", "part": "戒指", "set_type": "烈阳", "quality": 2, "affixes": [], "locked_affixes": [], "wash_count": 0},
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def game_scalar(self, sql, params=()):
        with db_backend.connection(self.game_database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    def accessory_bag(self):
        with db_backend.connection(self.player_database) as conn:
            if not conn.table_exists("player_accessory"):
                return []
            row = conn.execute(
                "SELECT bag FROM player_accessory WHERE user_id=%s", ("user",)
            ).fetchone()
            return json.loads(row[0]) if row else []

    def apply(self, operation_id="accessory-package-1", quantity=2):
        return self.service.apply(
            operation_id, "user", 3001, quantity,
            self.rewards, self.accessories,
            max_goods_num=1000, accessory_limit=1000,
        )

    def test_grants_main_and_accessory_rewards_in_one_transaction(self) -> None:
        result = self.apply()

        self.assertEqual(result.status, "applied")
        self.assertEqual(self.game_scalar("SELECT stone FROM user_xiuxian"), 150)
        self.assertEqual(self.game_scalar("SELECT goods_num FROM back WHERE goods_id=3001"), 1)
        self.assertEqual(self.game_scalar("SELECT goods_num FROM back WHERE goods_id=4001"), 2)
        self.assertEqual([item["uid"] for item in self.accessory_bag()], ["acc-fixed-1", "acc-fixed-2"])

    def test_duplicate_reuses_fixed_instances_without_second_grant(self) -> None:
        first = self.apply("accessory-package-repeat")
        second = self.apply("accessory-package-repeat")

        self.assertEqual((first.status, second.status), ("applied", "duplicate"))
        self.assertEqual(second.accessories, self.accessories)
        self.assertEqual(self.game_scalar("SELECT stone FROM user_xiuxian"), 150)
        self.assertEqual(len(self.accessory_bag()), 2)

    def test_accessory_capacity_failure_changes_neither_database(self) -> None:
        result = self.service.apply(
            "accessory-full", "user", 3001, 2,
            self.rewards, self.accessories,
            max_goods_num=1000, accessory_limit=1,
        )

        self.assertEqual(result.status, "accessory_full")
        self.assertEqual(self.game_scalar("SELECT stone FROM user_xiuxian"), 100)
        self.assertEqual(self.game_scalar("SELECT goods_num FROM back WHERE goods_id=3001"), 3)
        self.assertEqual(self.accessory_bag(), [])

    def test_player_database_write_failure_rolls_back_main_database(self) -> None:
        with db_backend.transaction(self.player_database) as conn:
            conn.execute(
                "CREATE TABLE player_accessory (user_id TEXT PRIMARY KEY, equipped TEXT, bag TEXT)"
            )
            conn.execute(
                "CREATE TRIGGER fail_accessory_write BEFORE INSERT ON player_accessory "
                "BEGIN SELECT RAISE(ABORT, 'player write failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.apply("accessory-package-fail")

        self.assertEqual(self.game_scalar("SELECT stone FROM user_xiuxian"), 100)
        self.assertEqual(self.game_scalar("SELECT goods_num FROM back WHERE goods_id=3001"), 3)
        self.assertIsNone(self.game_scalar("SELECT goods_num FROM back WHERE goods_id=4001"))

    def test_operation_failure_rolls_back_both_databases(self) -> None:
        with db_backend.connection(self.game_database) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self.player_database),))
            self.service._ensure_schema(conn)
            conn.commit()
            conn.execute("DETACH DATABASE player_data")
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_accessory_operation "
                "BEFORE INSERT ON accessory_package_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.apply("accessory-operation-fail")

        self.assertEqual(self.game_scalar("SELECT stone FROM user_xiuxian"), 100)
        self.assertEqual(self.game_scalar("SELECT goods_num FROM back WHERE goods_id=3001"), 3)
        self.assertEqual(self.accessory_bag(), [])


if __name__ == "__main__":
    unittest.main()
