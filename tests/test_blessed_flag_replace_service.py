from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.transaction_service import (
    BlessedFlagReplaceService,
)
from tests.test_db_backend import db_backend


class BlessedFlagReplaceServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,blessed_spot_flag INTEGER)")
            conn.execute("CREATE TABLE BuffInfo (user_id TEXT PRIMARY KEY,blessed_spot INTEGER)")
            conn.execute(
                "CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_num INTEGER,"
                "bind_num INTEGER,update_time TEXT,action_time TEXT)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("u", 1))
            conn.execute("INSERT INTO BuffInfo VALUES (%s,%s)", ("u", 2))
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,NULL,NULL)", ("u", 9001, 3, 2))
        with db_backend.transaction(self.player) as conn:
            speed = db_backend.quote_ident("药材速度")
            conn.execute(f"CREATE TABLE mix_elixir_info (user_id TEXT PRIMARY KEY,{speed} TEXT)")
            conn.execute(f"INSERT INTO mix_elixir_info (user_id,{speed}) VALUES (%s,%s)", ("u", "20"))
        self.service = BlessedFlagReplaceService(self.game, self.player)

    def tearDown(self):
        self.tmp.cleanup()

    def state(self):
        with db_backend.connection(self.game) as conn:
            flag = conn.execute("SELECT blessed_spot_flag FROM user_xiuxian WHERE user_id=%s", ("u",)).fetchone()
            level = conn.execute("SELECT blessed_spot FROM BuffInfo WHERE user_id=%s", ("u",)).fetchone()
            item = conn.execute("SELECT goods_num,bind_num FROM back WHERE user_id=%s AND goods_id=%s", ("u", 9001)).fetchone()
        with db_backend.connection(self.player) as conn:
            speed = conn.execute(f"SELECT {db_backend.quote_ident('药材速度')} FROM mix_elixir_info WHERE user_id=%s", ("u",)).fetchone()
        return int(flag[0]), int(level[0]), tuple(item), int(speed[0])

    def replace(self, operation_id="replace-1", **overrides):
        params = dict(
            operation_id=operation_id, user_id="u", item_id=9001,
            target_level=4, herb_speed=50, expected_level=2,
            expected_herb_speed=20, expected_quantity=3,
        )
        params.update(overrides)
        return self.service.replace(**params)

    def test_replace_is_atomic_and_idempotent(self):
        first, second = self.replace(), self.replace()
        self.assertEqual(("applied", 2, 4, 50, 1), (
            first.status, first.previous_level, first.current_level,
            first.herb_speed, first.quantity,
        ))
        self.assertEqual("duplicate", second.status)
        self.assertEqual((1, 4, (2, 1), 50), self.state())

    def test_operation_payload_conflict_does_not_mutate(self):
        self.replace()
        before = self.state()
        self.assertEqual("state_changed", self.replace(target_level=5).status)
        self.assertEqual(before, self.state())

    def test_missing_spot_same_level_and_downgrade_are_rejected(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE user_xiuxian SET blessed_spot_flag=0 WHERE user_id=%s", ("u",))
        self.assertEqual("blessed_spot_missing", self.replace("missing").status)
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE user_xiuxian SET blessed_spot_flag=1 WHERE user_id=%s", ("u",))
        self.assertEqual("same_level", self.replace("same", target_level=2).status)
        self.assertEqual("downgrade", self.replace("down", target_level=1).status)
        self.assertEqual((1, 2, (3, 2), 20), self.state())

    def test_inventory_or_speed_snapshot_change_is_rejected(self):
        self.assertEqual("state_changed", self.replace("inventory", expected_quantity=4).status)
        self.assertEqual("state_changed", self.replace("speed", expected_herb_speed=21).status)
        self.assertEqual((1, 2, (3, 2), 20), self.state())

    def test_missing_inventory_is_rejected(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE back SET goods_num=0,bind_num=0 WHERE user_id=%s AND goods_id=%s", ("u", 9001))
        self.assertEqual("item_missing", self.replace("empty", expected_quantity=0).status)
        self.assertEqual((1, 2, (0, 0), 20), self.state())

    def test_operation_failure_rolls_back_both_databases(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE blessed_flag_replace_operations (operation_id TEXT PRIMARY KEY,payload TEXT,result_json TEXT)")
            conn.execute("CREATE TRIGGER reject_replace BEFORE INSERT ON blessed_flag_replace_operations BEGIN SELECT RAISE(ABORT,'reject'); END")
        before = self.state()
        with self.assertRaises(db_backend.IntegrityError):
            self.replace("rollback")
        self.assertEqual(before, self.state())


if __name__ == "__main__":
    unittest.main()
