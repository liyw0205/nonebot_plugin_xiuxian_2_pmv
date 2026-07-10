from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.membership_service import (
    SectMembershipService,
)
from tests.test_db_backend import db_backend


class SectElixirRoomMaintenanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect-maintenance.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                """
                CREATE TABLE sects (
                    sect_id INTEGER PRIMARY KEY,
                    sect_name TEXT,
                    sect_owner TEXT,
                    elixir_room_level INTEGER,
                    sect_materials INTEGER
                )
                """
            )
            conn.executemany(
                "INSERT INTO sects VALUES (%s, %s, %s, %s, %s)",
                (
                    (1, "青云宗", "owner", 2, 1000),
                    (2, "贫宗", "poor", 2, 100),
                    (3, "空丹房", "empty", 0, 1000),
                ),
            )
        self.service = SectMembershipService(self.database)
        self.costs = {1: 200, 2: 600}

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def materials(self, sect_id: int) -> int:
        with db_backend.connection(self.database) as conn:
            return int(
                conn.execute(
                    "SELECT sect_materials FROM sects WHERE sect_id=%s", (sect_id,)
                ).fetchone()[0]
            )

    def test_maintenance_is_charged_once_per_business_day(self) -> None:
        key = "sect-elixir-maintenance:2026-07-11"
        first = self.service.charge_elixir_room_maintenance(key, 1, self.costs)
        second = self.service.charge_elixir_room_maintenance(key, 1, self.costs)

        self.assertEqual(first.status, "charged")
        self.assertTrue(first.charged)
        self.assertEqual(second.status, "charged")
        self.assertTrue(second.duplicate)
        self.assertFalse(second.charged)
        self.assertEqual(self.materials(1), 400)

    def test_next_business_day_charges_again(self) -> None:
        self.service.charge_elixir_room_maintenance(
            "sect-elixir-maintenance:2026-07-11", 1, self.costs
        )
        result = self.service.charge_elixir_room_maintenance(
            "sect-elixir-maintenance:2026-07-12", 1, self.costs
        )

        self.assertEqual(result.status, "insufficient")
        self.assertEqual(self.materials(1), 400)

    def test_insufficient_result_is_stable_after_materials_change(self) -> None:
        key = "sect-elixir-maintenance:2026-07-11"
        first = self.service.charge_elixir_room_maintenance(key, 2, self.costs)
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE sects SET sect_materials=%s WHERE sect_id=%s", (1000, 2)
            )
        second = self.service.charge_elixir_room_maintenance(key, 2, self.costs)

        self.assertEqual(first.status, "insufficient")
        self.assertEqual(second.status, "insufficient")
        self.assertTrue(second.duplicate)
        self.assertEqual(self.materials(2), 1000)

    def test_room_level_and_cost_are_read_inside_transaction(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE sects SET elixir_room_level=%s WHERE sect_id=%s", (1, 1)
            )

        result = self.service.charge_elixir_room_maintenance(
            "sect-elixir-maintenance:2026-07-11", 1, self.costs
        )

        self.assertEqual(result.room_level, 1)
        self.assertEqual(result.materials_cost, 200)
        self.assertEqual(self.materials(1), 800)

    def test_no_room_outcome_is_recorded_for_retry_stability(self) -> None:
        key = "sect-elixir-maintenance:2026-07-11"
        first = self.service.charge_elixir_room_maintenance(key, 3, self.costs)
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE sects SET elixir_room_level=%s WHERE sect_id=%s", (1, 3)
            )
        second = self.service.charge_elixir_room_maintenance(key, 3, self.costs)

        self.assertEqual(first.status, "no_room")
        self.assertEqual(second.status, "no_room")
        self.assertTrue(second.duplicate)
        self.assertEqual(self.materials(3), 1000)

    def test_record_failure_rolls_back_materials_deduction(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_elixir_room_maintenance(conn)
            conn.execute(
                """
                CREATE TRIGGER reject_maintenance
                BEFORE INSERT ON sect_elixir_room_maintenance
                BEGIN
                    SELECT RAISE(ABORT, 'reject maintenance');
                END
                """
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.charge_elixir_room_maintenance(
                "sect-elixir-maintenance:2026-07-11", 1, self.costs
            )

        self.assertEqual(self.materials(1), 1000)

    def test_missing_sect_does_not_create_operation_record(self) -> None:
        result = self.service.charge_elixir_room_maintenance(
            "sect-elixir-maintenance:2026-07-11", 999, self.costs
        )

        self.assertEqual(result.status, "sect_missing")
        with db_backend.connection(self.database) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM sect_elixir_room_maintenance"
            ).fetchone()[0]
        self.assertEqual(count, 0)


if __name__ == "__main__":
    unittest.main()
