from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.item_reward_service import (
    TiantiItemRewardService,
)
from tests.test_db_backend import db_backend


class TiantiItemRewardServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.sqlite3"
        self.player_database = root / "player.sqlite3"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_num INTEGER, "
                "bind_num INTEGER DEFAULT 0, UNIQUE(user_id, goods_id))"
            )
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s)", ("user", 2001, 3, 3))
        self.service = TiantiItemRewardService(self.game_database, self.player_database)
        self.default_data = {
            "tianti_level": "初境", "tianti_hp": 10, "last_settle_time": None,
            "medicine_last_time": None, "medicine_end_time": None,
            "medicine_effect": 0.0, "medicine_name": "", "opened_qiaoxue": [],
            "opened_qiaoxue_detail": [], "qiaoxue_stage_opened": {},
        }
        self.service._manager._default = lambda: dict(self.default_data)
        self.service._manager._clean_user_data = lambda data: {**self.default_data, **data}

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def goods_num(self):
        with db_backend.connection(self.game_database) as conn:
            return conn.execute("SELECT goods_num FROM back").fetchone()[0]

    def tianti_hp(self):
        with db_backend.connection(self.player_database) as conn:
            if not conn.table_exists("tianti_info"):
                return None
            row = conn.execute("SELECT tianti_hp FROM tianti_info WHERE user_id=%s", ("user",)).fetchone()
            return int(row[0]) if row else None

    @staticmethod
    def grant(data, minutes, **kwargs):
        data["tianti_hp"] = int(data["tianti_hp"]) + minutes
        return {"status": "ok", "real_gain": minutes, "new_hp": data["tianti_hp"], "bath": None, "bath_expired": False, "sect_bonus": 0.0}

    def apply(self, operation_id="tianti-item-1", quantity=2):
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.item_reward_service.grant_tianti_settle_minutes",
            side_effect=self.grant,
        ):
            return self.service.apply(operation_id, "user", 2001, quantity, 30)

    def test_consumes_item_and_updates_tianti_atomically(self) -> None:
        result = self.apply()

        self.assertEqual(result.status, "applied")
        self.assertEqual((result.minutes, result.detail["real_gain"]), (60, 60))
        self.assertEqual(self.goods_num(), 1)
        self.assertEqual(self.tianti_hp(), 70)

    def test_duplicate_does_not_consume_or_grant_twice(self) -> None:
        first = self.apply("tianti-repeat")
        second = self.apply("tianti-repeat")

        self.assertEqual((first.status, second.status), ("applied", "duplicate"))
        self.assertEqual(self.goods_num(), 1)
        self.assertEqual(self.tianti_hp(), 70)

    def test_same_operation_with_changed_minutes_is_rejected(self) -> None:
        first = self.apply("tianti-conflict")
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.item_reward_service.grant_tianti_settle_minutes",
            side_effect=self.grant,
        ):
            conflict = self.service.apply(
                "tianti-conflict", "user", 2001, 2, 60
            )

        self.assertEqual((first.status, conflict.status), ("applied", "state_changed"))
        self.assertEqual(self.goods_num(), 1)
        self.assertEqual(self.tianti_hp(), 70)

    def test_insufficient_item_does_not_update_player_database(self) -> None:
        result = self.apply(quantity=4)

        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual(self.goods_num(), 3)
        self.assertIsNone(self.tianti_hp())

    def test_player_write_failure_rolls_back_item_consumption(self) -> None:
        with db_backend.transaction(self.player_database) as conn:
            conn.execute("CREATE TABLE tianti_info (user_id TEXT PRIMARY KEY)")
            conn.execute(
                "CREATE TRIGGER fail_tianti_write BEFORE INSERT ON tianti_info "
                "BEGIN SELECT RAISE(ABORT, 'write failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.apply("tianti-write-fail")

        self.assertEqual(self.goods_num(), 3)


if __name__ == "__main__":
    unittest.main()
