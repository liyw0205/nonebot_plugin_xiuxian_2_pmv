from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.daily_reset_maintenance_service import (
    SectDailyResetMaintenanceService,
)
from tests.test_db_backend import db_backend


class SectDailyResetMaintenanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect-daily-reset.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, "
                "sect_task INTEGER, sect_elixir_get INTEGER)"
            )
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s)",
                (("u1", 3, 1), ("u2", 5, 1)),
            )
            conn.execute(
                "CREATE TABLE sects (sect_id INTEGER PRIMARY KEY, sect_name TEXT, "
                "sect_owner TEXT, elixir_room_level INTEGER, sect_materials INTEGER)"
            )
            conn.executemany(
                "INSERT INTO sects VALUES (%s, %s, %s, %s, %s)",
                (
                    (1, "富宗", "owner1", 2, 700),
                    (2, "降级宗", "owner2", 2, 500),
                    (3, "失效宗", "owner3", 1, 100),
                    (4, "无丹房", "owner4", 0, 900),
                    (5, "无主宗", None, 2, 900),
                ),
            )
        self.service = SectDailyResetMaintenanceService(self.database)
        self.costs = {1: 200, 2: 600}

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def users(self):
        with db_backend.connection(self.database) as conn:
            return [tuple(row) for row in conn.execute(
                "SELECT sect_task, sect_elixir_get FROM user_xiuxian ORDER BY user_id"
            )]

    def sects(self):
        with db_backend.connection(self.database) as conn:
            return [tuple(row) for row in conn.execute(
                "SELECT sect_id, elixir_room_level, sect_materials FROM sects ORDER BY sect_id"
            )]

    def test_resets_users_and_settles_all_sects_atomically(self) -> None:
        result = self.service.settle("2026-07-14", self.costs)

        self.assertEqual("applied", result.status)
        self.assertEqual(2, result.user_count)
        self.assertEqual([(0, 0), (0, 0)], self.users())
        self.assertEqual(
            [(1, 2, 100), (2, 1, 500), (3, 0, 100), (4, 0, 900), (5, 2, 900)],
            self.sects(),
        )
        self.assertEqual(
            ["charged", "downgraded", "disabled", "no_room", "inactive"],
            [outcome.status for outcome in result.outcomes],
        )

    def test_same_business_date_is_idempotent(self) -> None:
        first = self.service.settle("2026-07-14", self.costs)
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET sect_task=7, sect_elixir_get=1")
        second = self.service.settle("2026-07-14", self.costs)

        self.assertEqual("applied", first.status)
        self.assertEqual("duplicate", second.status)
        self.assertEqual([(7, 1), (7, 1)], self.users())
        self.assertEqual(first.outcomes, second.outcomes)

    def test_new_business_date_runs_again(self) -> None:
        self.service.settle("2026-07-14", self.costs)
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET sect_task=4, sect_elixir_get=1")
        result = self.service.settle("2026-07-15", self.costs)

        self.assertEqual("applied", result.status)
        self.assertEqual([(0, 0), (0, 0)], self.users())
        self.assertEqual((2, 1, 300), self.sects()[1])

    def test_changed_costs_conflict_without_mutation(self) -> None:
        self.service.settle("2026-07-14", self.costs)
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET sect_task=9, sect_elixir_get=1")
        before = self.sects()

        result = self.service.settle("2026-07-14", {1: 300, 2: 700})

        self.assertEqual("operation_conflict", result.status)
        self.assertEqual([(9, 1), (9, 1)], self.users())
        self.assertEqual(before, self.sects())

    def test_operation_failure_rolls_back_resets_and_maintenance(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_operations(conn)
            conn.execute(
                "CREATE TRIGGER reject_daily_reset BEFORE INSERT ON "
                "sect_daily_reset_operations BEGIN "
                "SELECT RAISE(ABORT, 'reject daily reset'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.settle("2026-07-14", self.costs)

        self.assertEqual([(3, 1), (5, 1)], self.users())
        self.assertEqual(
            [(1, 2, 700), (2, 2, 500), (3, 1, 100), (4, 0, 900), (5, 2, 900)],
            self.sects(),
        )

    def test_production_entry_has_no_legacy_partial_commit_path(self) -> None:
        source = Path(
            "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/__init__.py"
        ).read_text(encoding="utf-8")
        handler = source[source.index("async def resetusertask") : source.index(
            "async def auto_handle_inactive_sect_owners"
        )]
        self.assertIn("sect_daily_reset_maintenance_service.settle", handler)
        self.assertNotIn("sect_task_reset()", handler)
        self.assertNotIn("sect_elixir_get_num_reset()", handler)
        self.assertNotIn("charge_elixir_room_maintenance", handler)


if __name__ == "__main__":
    unittest.main()
