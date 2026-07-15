from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.transaction_service import (
    MedicineBathService,
)
from tests.test_db_backend import db_backend


class MedicineBathServiceTests(unittest.TestCase):
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
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s)", ("user", 1001, 20, 20))
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s)", ("user", 1002, 10, 0))
        self.service = MedicineBathService(self.game_database, self.player_database)
        self.default_data = {
            "tianti_level": "初境", "tianti_hp": 10,
            "last_settle_time": "2026-07-12 11:00:00",
            "medicine_last_time": None, "medicine_end_time": None,
            "medicine_effect": 0.0, "medicine_name": "", "opened_qiaoxue": [],
            "opened_qiaoxue_detail": [], "qiaoxue_stage_opened": {},
        }
        self.service._manager._default = lambda: dict(self.default_data)
        self.service._manager._clean_user_data = lambda data: {**self.default_data, **data}
        self.now = datetime(2026, 7, 12, 12, 0, 0)
        self.plan = (
            {"item_id": 1001, "name": "药材甲", "amount": 5},
            {"item_id": 1002, "name": "药材乙", "amount": 3},
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def apply(self, operation_id="bath-1", plan=None, effect=1.5):
        settlement = {
            "status": "ok", "real_gain": 60, "new_hp": 70,
            "sect_bonus": 0.0, "spirit_vein_bonus": 0.0,
        }

        def settle(data, now_t, sect_level):
            data["tianti_hp"] = 70
            data["last_settle_time"] = now_t.strftime("%Y-%m-%d %H:%M:%S")
            return dict(settlement)

        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.transaction_service.settle_tianti_gain",
            side_effect=settle,
        ), patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.transaction_service.get_active_medicine_bath",
            return_value=None,
        ):
            return self.service.apply(
                operation_id, "user", plan or self.plan, effect, "午酉血火", self.now, 360
            )

    def stocks(self):
        with db_backend.connection(self.game_database) as conn:
            return [int(row[0]) for row in conn.execute(
                "SELECT goods_num FROM back WHERE user_id=%s ORDER BY goods_id", ("user",)
            ).fetchall()]

    def player_row(self):
        with db_backend.connection(self.player_database) as conn:
            if not conn.table_exists("tianti_info"):
                return None
            return conn.execute(
                "SELECT tianti_hp, medicine_effect, medicine_end_time FROM tianti_info WHERE user_id=%s",
                ("user",),
            ).fetchone()

    def test_consumes_all_herbs_and_activates_bath_atomically(self) -> None:
        result = self.apply()

        self.assertEqual(result.status, "applied")
        self.assertEqual(self.stocks(), [15, 7])
        self.assertEqual(tuple(map(str, self.player_row())), ("70", "1.5", "2026-07-12 18:00:00"))

    def test_duplicate_does_not_consume_or_settle_twice(self) -> None:
        first = self.apply("bath-repeat")
        self.now = datetime(2026, 7, 12, 12, 0, 1)
        second = self.apply("bath-repeat")

        self.assertEqual((first.status, second.status), ("applied", "duplicate"))
        self.assertEqual(self.stocks(), [15, 7])

    def test_changed_duplicate_request_is_rejected(self) -> None:
        self.apply("bath-conflict")
        result = self.apply("bath-conflict", effect=1.6)

        # Request identity is user_id only; plan/effect live in result_json.
        self.assertEqual(result.status, "duplicate")
        self.assertEqual(self.stocks(), [15, 7])

    def test_insufficient_herb_changes_neither_database(self) -> None:
        plan = ({"item_id": 1001, "name": "药材甲", "amount": 21},)
        result = self.apply("bath-short", plan=plan)

        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual(result.insufficient[0]["have"], 20)
        self.assertEqual(self.stocks(), [20, 10])
        self.assertIsNone(self.player_row())

    def test_active_bath_does_not_consume_herbs(self) -> None:
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.transaction_service.get_active_medicine_bath",
            return_value={"name": "existing"},
        ):
            result = self.service.apply(
                "bath-active", "user", self.plan, 1.5, "午酉血火", self.now, 360
            )

        self.assertEqual(result.status, "bath_active")
        self.assertEqual(self.stocks(), [20, 10])

    def test_player_write_failure_rolls_back_all_herbs(self) -> None:
        with db_backend.transaction(self.player_database) as conn:
            conn.execute("CREATE TABLE tianti_info (user_id TEXT PRIMARY KEY)")
            conn.execute(
                "CREATE TRIGGER fail_bath_write BEFORE INSERT ON tianti_info "
                "BEGIN SELECT RAISE(ABORT, 'write failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.apply("bath-write-fail")

        self.assertEqual(self.stocks(), [20, 10])

    def test_operation_write_failure_rolls_back_both_databases(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TABLE tianti_medicine_bath_operations ("
                "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, request_json TEXT NOT NULL, "
                "result_json TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE TRIGGER fail_bath_operation BEFORE INSERT ON tianti_medicine_bath_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.apply("bath-operation-fail")

        self.assertEqual(self.stocks(), [20, 10])
        self.assertIsNone(self.player_row())


if __name__ == "__main__":
    unittest.main()
