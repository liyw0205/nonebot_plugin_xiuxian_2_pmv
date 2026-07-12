from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.breakthrough_service import (
    TiantiBreakthroughService,
)
from tests.test_db_backend import db_backend


class TiantiBreakthroughServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "player.sqlite3"
        self.service = TiantiBreakthroughService(self.database)
        self.default_data = {
            "tianti_level": "初境", "tianti_hp": 200,
            "last_settle_time": None, "medicine_last_time": None,
            "medicine_end_time": None, "medicine_effect": 0.0,
            "medicine_name": "", "opened_qiaoxue": [],
            "opened_qiaoxue_detail": [], "qiaoxue_stage_opened": {},
        }
        self.service._manager._default = lambda: dict(self.default_data)
        self.service._manager._clean_user_data = lambda data: {**self.default_data, **data}

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def attempt(self, operation_id="break-1", cultivation_rank=1, success=True):
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.breakthrough_service.get_next_tianti_level_name",
            return_value="次境",
        ), patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.breakthrough_service.get_tianti_level_data",
            return_value={"need_hp": 100, "min_xx_level": "筑基"},
        ), patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.breakthrough_service.get_tianti_level_index",
            return_value=2,
        ):
            return self.service.attempt(
                operation_id, "user", cultivation_rank=cultivation_rank, roll_success=success
            )

    def state(self):
        with db_backend.connection(self.database) as conn:
            if not conn.table_exists("tianti_info"):
                return None
            row = conn.execute(
                "SELECT tianti_level, tianti_hp FROM tianti_info WHERE user_id=%s", ("user",)
            ).fetchone()
            return (str(row[0]), int(row[1])) if row else None

    def test_success_advances_level_and_charges_five_percent(self) -> None:
        result = self.attempt()

        self.assertEqual((result.status, result.success, result.hp_cost), ("completed", True, 10))
        self.assertEqual(self.state(), ("次境", 190))

    def test_failed_roll_still_charges_but_keeps_level(self) -> None:
        result = self.attempt("break-fail", success=False)

        self.assertEqual((result.status, result.success, result.hp_cost), ("completed", False, 10))
        self.assertEqual(self.state(), ("初境", 190))

    def test_duplicate_does_not_charge_twice(self) -> None:
        first = self.attempt("break-repeat", success=False)
        second = self.attempt("break-repeat", success=False)

        self.assertEqual((first.status, second.status), ("completed", "duplicate"))
        self.assertEqual(self.state(), ("初境", 190))

    def test_changed_duplicate_roll_is_rejected(self) -> None:
        self.attempt("break-conflict", success=False)
        result = self.attempt("break-conflict", success=True)

        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self.state(), ("初境", 190))

    def test_ineligible_attempt_does_not_charge(self) -> None:
        result = self.attempt("break-level-short", cultivation_rank=3)

        self.assertEqual(result.status, "cultivation_insufficient")
        self.assertIsNone(self.state())

    def test_insufficient_hp_does_not_write_state(self) -> None:
        self.default_data["tianti_hp"] = 99
        result = self.attempt("break-hp-short")

        self.assertEqual(result.status, "hp_insufficient")
        self.assertIsNone(self.state())

    def test_operation_failure_rolls_back_breakthrough(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE tianti_breakthrough_operations ("
                "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, cultivation_rank INTEGER NOT NULL, "
                "roll_success INTEGER NOT NULL, old_level TEXT NOT NULL, new_level TEXT NOT NULL, "
                "hp_cost INTEGER NOT NULL, new_hp INTEGER NOT NULL, success INTEGER NOT NULL)"
            )
            conn.execute(
                "CREATE TRIGGER fail_break_operation BEFORE INSERT ON tianti_breakthrough_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.attempt("break-write-fail")

        self.assertIsNone(self.state())


if __name__ == "__main__":
    unittest.main()
