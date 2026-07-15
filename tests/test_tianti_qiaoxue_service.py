from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.transaction_service import QiaoxueService
from tests.test_db_backend import db_backend


class QiaoxueServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "player.sqlite3"
        self.service = QiaoxueService(self.database)
        self.default_data = {
            "tianti_level": "初境", "tianti_hp": 100,
            "last_settle_time": None, "medicine_last_time": None,
            "medicine_end_time": None, "medicine_effect": 0.0,
            "medicine_name": "", "opened_qiaoxue": [],
            "opened_qiaoxue_detail": [], "qiaoxue_stage_opened": {},
        }
        self.service._manager._default = lambda: dict(self.default_data)
        self.service._manager._clean_user_data = lambda data: {**self.default_data, **data}
        self.pool = [
            {"name": "窍一", "group": "组一", "effect_type": "hp_gain_pct", "effect_value": 0.01},
            {"name": "窍二", "group": "组一", "effect_type": "base_per_min_ratio", "effect_value": 0.02},
        ]

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def open(self, operation_id="qiaoxue-1", roll=0, rank=1):
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.transaction_service.get_qiaoxue_pool",
            return_value=self.pool,
        ), patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.transaction_service.get_tianti_level_data",
            return_value={"rank": rank},
        ):
            return self.service.open(operation_id, "user", roll)

    def state(self):
        with db_backend.connection(self.database) as conn:
            if not conn.table_exists("tianti_info"):
                return None
            row = conn.execute(
                "SELECT tianti_hp, opened_qiaoxue FROM tianti_info WHERE user_id=%s", ("user",)
            ).fetchone()
            return int(row[0]), row[1] if row else None

    def seed(self, hp=100, opened=None):
        opened = opened or []
        data = {**self.default_data, "tianti_hp": hp, "opened_qiaoxue": opened}
        self.service._manager.save_user_tianti_info = lambda *args, **kwargs: None
        fields = tuple(data.keys())
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_schema(conn, fields)
            columns = ", ".join(["user_id", *fields])
            placeholders = ", ".join(["%s"] * (len(fields) + 1))
            values = [json.dumps(data[field], ensure_ascii=False) if isinstance(data[field], (list, dict)) else data[field] for field in fields]
            conn.execute(f"INSERT INTO tianti_info ({columns}) VALUES ({placeholders})", ("user", *values))

    def test_opens_selected_qiaoxue_and_charges_ten_percent(self) -> None:
        result = self.open(roll=1)

        self.assertEqual((result.status, result.qiaoxue["name"], result.hp_cost), ("opened", "窍二", 10))
        self.assertEqual(self.state(), (90, '["窍二"]'))

    def test_duplicate_reuses_first_choice_without_second_charge(self) -> None:
        first = self.open("qiaoxue-repeat", roll=0)
        second = self.open("qiaoxue-repeat", roll=1)

        self.assertEqual((first.status, second.status, second.qiaoxue["name"]), ("opened", "duplicate", "窍一"))
        self.assertEqual(self.state(), (90, '["窍一"]'))

    def test_unlock_limit_prevents_opening(self) -> None:
        self.seed(opened=["窍一", "窍二"])
        result = self.open("qiaoxue-limit", rank=0)

        self.assertEqual(result.status, "limit_reached")
        self.assertEqual(self.state()[0], 100)

    def test_zero_hp_is_rejected_without_write(self) -> None:
        self.default_data["tianti_hp"] = 0
        result = self.open("qiaoxue-hp")

        self.assertEqual(result.status, "hp_insufficient")
        self.assertIsNone(self.state())

    def test_operation_failure_rolls_back_hp_and_qiaoxue(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE tianti_qiaoxue_operations ("
                "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, roll INTEGER NOT NULL, "
                "qiaoxue_json TEXT NOT NULL, hp_cost INTEGER NOT NULL, new_hp INTEGER NOT NULL, "
                "opened_count INTEGER NOT NULL, unlock_limit INTEGER NOT NULL)"
            )
            conn.execute(
                "CREATE TRIGGER fail_qiaoxue_operation BEFORE INSERT ON tianti_qiaoxue_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.open("qiaoxue-write-fail")

        self.assertIsNone(self.state())


if __name__ == "__main__":
    unittest.main()
