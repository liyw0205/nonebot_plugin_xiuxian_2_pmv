from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.fairyland_claim_service import (
    FairylandClaimService,
)
from tests.test_db_backend import db_backend


class FairylandClaimServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "player.sqlite3"
        self.service = FairylandClaimService(self.database)
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

    @staticmethod
    def grant(data, minutes, **kwargs):
        data["tianti_hp"] = int(data["tianti_hp"]) + minutes
        return {"status": "ok", "real_gain": minutes, "new_hp": data["tianti_hp"], "sect_bonus": 0.1}

    def claim(self, operation_id="claim-1", day="2026-07-12", level=2, minutes=45):
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.fairyland_claim_service.grant_tianti_settle_minutes",
            side_effect=self.grant,
        ):
            return self.service.claim(operation_id, "user", "sect", day, level, minutes)

    def scalar(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    def test_claim_updates_tianti_and_daily_marker_atomically(self) -> None:
        result = self.claim()

        self.assertEqual(result.status, "claimed")
        self.assertEqual(self.scalar("SELECT tianti_hp FROM tianti_info"), "55")
        self.assertEqual(self.scalar('SELECT "last_claim_sect" FROM sect_fairyland_claim'), "2026-07-12")

    def test_duplicate_and_second_operation_do_not_grant_twice(self) -> None:
        first = self.claim("claim-repeat")
        duplicate = self.claim("claim-repeat")
        already = self.claim("claim-other")

        self.assertEqual((first.status, duplicate.status, already.status),
                         ("claimed", "duplicate", "already_claimed"))
        self.assertEqual(self.scalar("SELECT tianti_hp FROM tianti_info"), "55")

    def test_changed_level_or_minutes_is_state_conflict(self) -> None:
        self.claim("claim-conflict")
        conflict = self.claim("claim-conflict", level=3, minutes=60)

        self.assertEqual(conflict.status, "state_changed")
        self.assertEqual(self.scalar("SELECT tianti_hp FROM tianti_info"), "55")

    def test_marker_write_failure_rolls_back_tianti_gain(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE sect_fairyland_claim (user_id TEXT PRIMARY KEY)")
            conn.execute(
                "CREATE TRIGGER fail_claim_marker BEFORE INSERT ON sect_fairyland_claim "
                "BEGIN SELECT RAISE(ABORT, 'marker failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.claim("claim-fail")

        with db_backend.connection(self.database) as conn:
            self.assertFalse(conn.table_exists("tianti_info"))


if __name__ == "__main__":
    unittest.main()
