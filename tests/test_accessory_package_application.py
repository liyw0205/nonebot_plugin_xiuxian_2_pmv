from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.accessory_package.application import AccessoryPackageApplication
from nonebot_plugin_xiuxian_2.features.accessory_package.attached_migrations import (
    apply_attached_player_accessory,
    apply_attached_player_accessory_operations,
)
from nonebot_plugin_xiuxian_2.features.accessory_package.migrations import apply_accessory_package
from nonebot_plugin_xiuxian_2.features.package_reward.domain import PackageReward
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


class AccessoryPackageApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        root = Path(self.directory.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        connection = sqlite3.connect(self.game)
        connection.executescript(
            """
            CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL);
            CREATE TABLE back (
                user_id TEXT NOT NULL, goods_id INTEGER NOT NULL, goods_name TEXT,
                goods_type TEXT, goods_num INTEGER NOT NULL, bind_num INTEGER DEFAULT 0,
                UNIQUE(user_id, goods_id)
            );
            INSERT INTO user_xiuxian VALUES ('u', 100);
            INSERT INTO back VALUES ('u', 3001, '礼包', '礼包', 3, 3);
            """
        )
        connection.commit()
        connection.close()
        with DatabaseUnitOfWork(self.game) as uow:
            apply_platform_schema(uow)
            apply_accessory_package(uow)
        with DatabaseUnitOfWork(self.player) as uow:
            apply_platform_schema(uow)
        with AttachedDatabaseUnitOfWork(self.game, attachments={"player_data": self.player}) as uow:
            apply_attached_player_accessory(uow)
            apply_attached_player_accessory_operations(uow)
        self.application = AccessoryPackageApplication(self.game, self.player)

    def tearDown(self) -> None:
        self.directory.cleanup()

    def request(self, operation_id: str = "op", limit: int = 5):
        return self.application.open_package(
            operation_id=operation_id,
            user_id="u",
            package_id=3001,
            quantity=2,
            rewards=(PackageReward(None, "灵石", None, 50), PackageReward(4001, "丹药", "丹药", 2)),
            accessories=({"uid": "acc-1", "name": "戒指"},),
            max_goods_num=1000,
            accessory_limit=limit,
        )

    def scalar(self, database: Path, sql: str):
        connection = sqlite3.connect(database)
        try:
            row = connection.execute(sql).fetchone()
            return row[0] if row else None
        finally:
            connection.close()

    def test_success_and_replay_do_not_duplicate_assets(self) -> None:
        first = self.request()
        replay = self.request()
        self.assertEqual(first.status, "applied")
        self.assertEqual(replay.status, "replayed")
        self.assertEqual(self.scalar(self.game, "SELECT stone FROM user_xiuxian"), 150)
        self.assertEqual(self.scalar(self.game, "SELECT goods_num FROM back WHERE goods_id=3001"), 1)
        self.assertEqual(self.scalar(self.player, "SELECT COUNT(*) FROM player_accessory"), 1)

    def test_accessory_rejection_compensates_game_stage(self) -> None:
        connection = sqlite3.connect(self.player)
        connection.execute("INSERT INTO player_accessory VALUES ('u', '{}', '[{\"uid\":\"old\"}]')")
        connection.commit()
        connection.close()
        result = self.request("full", limit=1)
        self.assertEqual((result.status, result.code), ("rejected", "accessory_full"))
        self.assertEqual(self.scalar(self.game, "SELECT stone FROM user_xiuxian"), 100)
        self.assertEqual(self.scalar(self.game, "SELECT goods_num FROM back WHERE goods_id=3001"), 3)

    def test_player_write_failure_is_recorded_and_compensated(self) -> None:
        connection = sqlite3.connect(self.player)
        connection.execute("CREATE TRIGGER fail_accessory BEFORE INSERT ON player_accessory BEGIN SELECT RAISE(ABORT, 'player unavailable'); END")
        connection.commit()
        connection.close()
        result = self.request("failure")
        self.assertEqual((result.status, result.code), ("failed", "needs_reconcile"))
        self.assertEqual(self.scalar(self.game, "SELECT stone FROM user_xiuxian"), 100)
        self.assertEqual(self.scalar(self.game, "SELECT goods_num FROM back WHERE goods_id=3001"), 3)


if __name__ == "__main__":
    unittest.main()
