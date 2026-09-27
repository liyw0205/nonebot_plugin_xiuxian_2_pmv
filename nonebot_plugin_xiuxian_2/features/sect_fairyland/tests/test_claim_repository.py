import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ....infrastructure.clock import SystemClock
from ..claim_repository import SectFairylandSqlRepository
from ..migrations import apply_sect_fairyland_player
from ...tianti_training.migrations import apply_tianti_player_info


class _Clock:
    def now(self):
        return datetime(2026, 9, 27, 10, 0, 0)


class _Profile:
    fields = {
        "tianti_level": "初境", "tianti_hp": 0, "last_settle_time": None,
        "medicine_last_time": None, "medicine_end_time": None,
        "medicine_effect": 0.0, "medicine_name": "", "opened_qiaoxue": [],
        "opened_qiaoxue_detail": [], "qiaoxue_stage_opened": {},
    }

    def default_data(self):
        return dict(self.fields)

    def levels(self):
        return {"初境": {"rank": 1, "hp_gain_per_min": 2, "need_hp": 1000}}

    def clean(self, row):
        data = self.default_data()
        data.update({key: value for key, value in (row or {}).items() if value is not None})
        data["tianti_hp"] = int(data["tianti_hp"] or 0)
        return data

    def cap(self, _data):
        return 10000


class SectFairylandSqlRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.player = Path(self.temp.name) / "player.sqlite3"
        with DatabaseUnitOfWork(self.player) as uow:
            apply_tianti_player_info(uow)
            apply_sect_fairyland_player(uow)
            uow.execute(
                "INSERT INTO tianti_info(user_id,tianti_level,tianti_hp,medicine_end_time,"
                "medicine_effect,medicine_name,opened_qiaoxue_detail) VALUES(?,?,?,?,?,?,?)",
                (
                    "user", "初境", "10", "2026-09-27 11:00:00", "2.0", "灵泉浴",
                    json.dumps([
                        {"effect_type": "base_per_min_ratio", "effect_value": 0.5},
                        {"effect_type": "hp_gain_pct", "effect_value": 0.2},
                    ]),
                ),
            )
        self.repository = SectFairylandSqlRepository(
            self.player,
            profile_reader=_Profile(),
            clock=_Clock(),
            spirit_vein_multiplier=lambda: 1.2,
        )

    def tearDown(self):
        self.temp.cleanup()

    def query(self, sql, params=()):
        with DatabaseUnitOfWork(self.player) as uow:
            return uow.query_one(sql, params)

    def test_claim_replay_and_daily_marker_share_tianti_transaction(self):
        first = self.repository.claim("op", "user", "7", "2026-09-27", 2, 30)
        duplicate = self.repository.claim("op", "user", "7", "2026-09-27", 2, 30)
        already = self.repository.claim("another", "user", "7", "2026-09-27", 2, 30)

        self.assertEqual((first.status, duplicate.status, already.status), ("claimed", "duplicate", "already_claimed"))
        self.assertEqual(first.detail["new_hp"], 295)
        self.assertEqual(self.query("SELECT tianti_hp FROM tianti_info WHERE user_id='user'")["tianti_hp"], "295")
        self.assertEqual(self.query("SELECT claim_day FROM sect_fairyland_claim_days WHERE user_id='user' AND sect_id='7'")["claim_day"], "2026-09-27")
        self.assertTrue(first.detail["bath"])
        self.assertEqual(first.detail, duplicate.detail)

    def test_aware_clock_uses_local_wall_time_for_legacy_bath_timestamp(self):
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "UPDATE tianti_info SET medicine_end_time='2099-12-31 23:59:59' WHERE user_id='user'"
            )
        repository = SectFairylandSqlRepository(
            self.player,
            profile_reader=_Profile(),
            clock=SystemClock(),
        )

        result = repository.claim("aware-clock", "user", "8", "2026-09-27", 2, 1)

        self.assertEqual(result.status, "claimed")
        self.assertTrue(result.detail["bath"])

    def test_operation_conflict_does_not_change_profile(self):
        self.repository.claim("op", "user", "7", "2026-09-27", 2, 30)
        conflict = self.repository.claim("op", "user", "7", "2026-09-27", 3, 60)
        self.assertEqual(conflict.status, "state_changed")
        self.assertEqual(self.query("SELECT tianti_hp FROM tianti_info WHERE user_id='user'")["tianti_hp"], "295")

    def test_receipt_failure_rolls_back_hp_and_marker(self):
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "CREATE TRIGGER fail_fairyland_receipt BEFORE INSERT ON sect_fairyland_claim_operations "
                "BEGIN SELECT RAISE(ABORT,'receipt failed'); END"
            )
        with self.assertRaises(Exception):
            self.repository.claim("fail", "user", "7", "2026-09-27", 2, 30)
        self.assertEqual(self.query("SELECT tianti_hp FROM tianti_info WHERE user_id='user'")["tianti_hp"], "10")
        self.assertIsNone(self.query("SELECT 1 FROM sect_fairyland_claim_days WHERE user_id='user' AND sect_id='7'"))
        self.assertIsNone(self.query("SELECT 1 FROM sect_fairyland_claim_operations WHERE operation_id='fail'"))

    def test_request_without_migration_does_not_create_claim_schema(self):
        other = Path(self.temp.name) / "unmigrated.sqlite3"
        with DatabaseUnitOfWork(other) as uow:
            apply_tianti_player_info(uow)
            uow.execute("INSERT INTO tianti_info(user_id,tianti_level,tianti_hp) VALUES('user','初境','10')")
        repo = SectFairylandSqlRepository(other, profile_reader=_Profile(), clock=_Clock())
        result = repo.claim("op", "user", "7", "2026-09-27", 2, 30)
        self.assertEqual(result.status, "schema_missing")
        self.assertIsNone(self.query_on(other, "SELECT 1 FROM sqlite_master WHERE name='sect_fairyland_claim_operations'"))

    def test_player_migration_backfills_dynamic_claim_columns_and_preserves_receipts(self):
        other = Path(self.temp.name) / "legacy.sqlite3"
        with DatabaseUnitOfWork(other) as uow:
            uow.execute("CREATE TABLE sect_fairyland_claim(user_id TEXT PRIMARY KEY, last_claim_7 TEXT)")
            uow.execute("INSERT INTO sect_fairyland_claim VALUES('user','2026-09-26')")
            uow.execute(
                "CREATE TABLE sect_fairyland_claim_operations(operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,"
                "sect_id TEXT NOT NULL,claim_day TEXT NOT NULL,level INTEGER NOT NULL,minutes INTEGER NOT NULL,"
                "detail_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            uow.execute("INSERT INTO sect_fairyland_claim_operations(operation_id,user_id,sect_id,claim_day,level,minutes,detail_json) VALUES('old','user','7','2026-09-25',1,30,'{}')")
            apply_sect_fairyland_player(uow)
        with DatabaseUnitOfWork(other) as uow:
            marker = uow.query_one("SELECT claim_day FROM sect_fairyland_claim_days WHERE user_id='user' AND sect_id='7'")
            receipt = uow.query_one("SELECT claim_day FROM sect_fairyland_claim_operations WHERE operation_id='old'")
        self.assertEqual(marker["claim_day"], "2026-09-26")
        self.assertEqual(receipt["claim_day"], "2026-09-25")

    @staticmethod
    def query_on(database, sql):
        with DatabaseUnitOfWork(database) as uow:
            return uow.query_one(sql)


if __name__ == "__main__":
    unittest.main()
