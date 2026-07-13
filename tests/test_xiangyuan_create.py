from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base.xiangyuan_settlement_service import (
    XiangyuanSettlementService,
)
from tests.test_db_backend import db_backend


class XiangyuanCreateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game, self.player = root / "game.sqlite3", root / "player.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("giver", 5_000_000))
            conn.execute(
                "CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,state INTEGER DEFAULT 0,"
                "UNIQUE(user_id,goods_id))"
            )
            conn.execute(
                "INSERT INTO back VALUES (%s,%s,%s,%s,%s,'','',%s,%s)",
                ("giver", 101, "符剑", "装备", 4, 1, 0),
            )
        self.service = XiangyuanSettlementService(self.game, self.player)
        self.items = [{"goods_id": 101, "name": "符剑", "type": "装备", "quantity": 2}]

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.game) as conn:
            stone = int(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id='giver'").fetchone()[0])
            goods = int(conn.execute("SELECT goods_num FROM back WHERE user_id='giver' AND goods_id=101").fetchone()[0])
            gifts = int(conn.execute("SELECT COUNT(*) FROM xiangyuan_gifts").fetchone()[0]) if conn.table_exists("xiangyuan_gifts") else 0
        with db_backend.connection(self.player) as conn:
            row = conn.execute("SELECT send_count FROM xiangyuan_limit WHERE user_id='giver'").fetchone() if conn.table_exists("xiangyuan_limit") else None
            count = int(row[0]) if row else 0
        return stone, goods, gifts, count

    def test_create_moves_all_assets_pool_and_daily_count_atomically(self):
        result = self.service.create("create-1", "group", "giver", "道友", 1_000_000, self.items, 3, 3)

        self.assertEqual((result.status, result.gift_id, result.send_count), ("applied", 1, 1))
        self.assertEqual(self.state(), (4_000_000, 2, 1, 1))
        group = self.service.get_group("group")
        self.assertEqual(group["last_id"], 2)
        self.assertEqual(group["gifts"]["1"]["items"][0]["quantity"], 2)

    def test_duplicate_and_limit_or_inventory_rejection_do_not_charge_twice(self):
        first = self.service.create("repeat", "group", "giver", "道友", 1_000_000, self.items, 3, 3)
        duplicate = self.service.create("repeat", "group", "giver", "道友", 1_000_000, self.items, 3, 3)
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual(self.state(), (4_000_000, 2, 1, 1))

        self.assertEqual(
            self.service.create("too-many-items", "group", "giver", "道友", 0, [{**self.items[0], "quantity": 3}], 2, 3).status,
            "item_insufficient",
        )
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE xiangyuan_limit SET send_count=3 WHERE user_id='giver'")
        self.assertEqual(self.service.create("limited", "group", "giver", "道友", 1_000_000, (), 2, 3).status, "limit_reached")
        self.assertEqual(self.state()[:3], (4_000_000, 2, 1))

    def test_legacy_json_is_imported_once_before_new_database_gift(self):
        legacy = {
            "last_id": 4,
            "gifts": {
                "3": {
                    "id": 3, "giver_id": "giver", "giver_name": "旧道友",
                    "stone_amount": 90, "remaining_stone": 30, "items": [],
                    "receiver_count": 3, "received": 2, "receivers": ["a", "b"],
                    "create_time": "2026-07-12 12:00:00",
                }
            },
        }
        result = self.service.create("legacy-create", "group", "giver", "道友", 1_000_000, (), 2, 3, legacy_data=legacy)

        self.assertEqual(result.gift_id, 4)
        group = self.service.get_group("group", legacy_data=legacy)
        self.assertEqual(set(group["gifts"]), {"3", "4"})
        self.assertEqual(group["last_id"], 5)

    def test_operation_insert_failure_rolls_back_resources_pool_and_count(self):
        with db_backend.connection(self.game) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self.player),))
            conn.execute("BEGIN IMMEDIATE")
            self.service._ensure_schema(conn)
            conn.execute(
                "CREATE TRIGGER fail_xiangyuan_create BEFORE INSERT ON xiangyuan_create_operations "
                "BEGIN SELECT RAISE(ABORT, 'failed'); END"
            )
            conn.commit()
            conn.execute("DETACH DATABASE player_data")

        with self.assertRaises(db_backend.IntegrityError):
            self.service.create("failure", "group", "giver", "道友", 1_000_000, self.items, 3, 3)
        self.assertEqual(self.state(), (5_000_000, 4, 0, 0))


if __name__ == "__main__":
    unittest.main()
