from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.settlement_service import TiantiSettlementService
from tests.test_db_backend import db_backend


class TiantiSettlementServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "player.sqlite3"
        self.service = TiantiSettlementService(self.database)
        self.default_data = {
            "tianti_level": "初境", "tianti_hp": 10,
            "last_settle_time": "2026-07-13 09:00:00",
            "medicine_last_time": None, "medicine_end_time": None,
            "medicine_effect": 0.0, "medicine_name": "", "opened_qiaoxue": [],
            "opened_qiaoxue_detail": [], "qiaoxue_stage_opened": {},
        }
        self.service._manager._default = lambda: dict(self.default_data)
        self.service._manager._clean_user_data = lambda data: {**self.default_data, **data}

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def settle(self, operation_id="settle-1", sect_level=0, gain=60):
        def apply(data, now_t, level):
            data["tianti_hp"] = int(data["tianti_hp"]) + gain
            data["last_settle_time"] = now_t.strftime("%Y-%m-%d %H:%M:%S")
            return {"status": "ok", "mins": 60, "real_gain": gain,
                    "new_hp": data["tianti_hp"], "sect_bonus": level / 10,
                    "spirit_vein_bonus": 0.0}

        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.settlement_service.settle_tianti_gain",
            side_effect=apply,
        ):
            return self.service.settle(
                operation_id, "user", datetime(2026, 7, 13, 10, 0, 0),
                sect_fairyland_level=sect_level,
            )

    def state(self):
        with db_backend.connection(self.database) as conn:
            if not conn.table_exists("tianti_info"):
                return None
            row = conn.execute(
                "SELECT tianti_hp, last_settle_time FROM tianti_info WHERE user_id=%s", ("user",)
            ).fetchone()
            return (int(row[0]), str(row[1])) if row else None

    def test_settles_gain_and_timestamp_atomically(self) -> None:
        result = self.settle()
        self.assertEqual((result.status, result.detail["real_gain"]), ("settled", 60))
        self.assertEqual(self.state(), (70, "2026-07-13 10:00:00"))

    def test_duplicate_reuses_first_result_without_second_gain(self) -> None:
        first = self.settle("settle-repeat")
        second = self.settle("settle-repeat", gain=999)
        self.assertEqual((first.status, second.status, second.detail["real_gain"]), ("settled", "duplicate", 60))
        self.assertEqual(self.state()[0], 70)

    def test_changed_sect_level_is_rejected(self) -> None:
        self.settle("settle-conflict", sect_level=1)
        result = self.settle("settle-conflict", sect_level=2)
        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self.state()[0], 70)

    def test_operation_failure_rolls_back_gain_and_timestamp(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE tianti_settlement_operations ("
                "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, sect_level INTEGER NOT NULL, "
                "result_status TEXT NOT NULL, detail_json TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE TRIGGER fail_settlement_operation BEFORE INSERT ON tianti_settlement_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.settle("settle-write-fail")
        self.assertIsNone(self.state())


if __name__ == "__main__":
    unittest.main()
