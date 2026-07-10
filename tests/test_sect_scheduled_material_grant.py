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


class SectScheduledMaterialGrantTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect-material-grant.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                """
                CREATE TABLE sects (
                    sect_id INTEGER PRIMARY KEY,
                    sect_owner TEXT,
                    sect_scale INTEGER,
                    sect_materials INTEGER,
                    combat_power INTEGER
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE user_xiuxian (
                    user_id TEXT PRIMARY KEY,
                    sect_id INTEGER,
                    power INTEGER
                )
                """
            )
            conn.execute(
                "INSERT INTO sects VALUES (%s, %s, %s, %s, %s)",
                (1, "owner", 120, 50, 1),
            )
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s)",
                (("owner", 1, 300), ("member", 1, 200)),
            )
        self.service = SectMembershipService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def sect_row(self):
        with db_backend.connection(self.database) as conn:
            return tuple(
                conn.execute(
                    "SELECT sect_materials, combat_power FROM sects WHERE sect_id=1"
                ).fetchone()
            )

    def test_first_grant_updates_materials_and_combat_power(self) -> None:
        result = self.service.grant_scheduled_materials(
            "sect-materials:2026-07-11", 1, 2
        )

        self.assertEqual(result.status, "granted")
        self.assertEqual(result.materials, 240)
        self.assertEqual(result.combat_power, 500)
        self.assertEqual(self.sect_row(), (290, 500))

    def test_same_business_day_is_idempotent(self) -> None:
        key = "sect-materials:2026-07-11"
        first = self.service.grant_scheduled_materials(key, 1, 2)
        second = self.service.grant_scheduled_materials(key, 1, 99)

        self.assertEqual(first.status, "granted")
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(second.materials, 240)
        self.assertEqual(self.sect_row(), (290, 500))

    def test_next_business_day_can_grant_again(self) -> None:
        self.service.grant_scheduled_materials("sect-materials:2026-07-11", 1, 2)
        result = self.service.grant_scheduled_materials(
            "sect-materials:2026-07-12", 1, 2
        )

        self.assertEqual(result.status, "granted")
        self.assertEqual(self.sect_row(), (530, 500))

    def test_current_scale_is_read_inside_transaction(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE sects SET sect_scale=%s WHERE sect_id=%s", (150, 1))

        result = self.service.grant_scheduled_materials(
            "sect-materials:2026-07-11", 1, 3
        )

        self.assertEqual(result.materials, 450)
        self.assertEqual(self.sect_row(), (500, 500))

    def test_operation_record_failure_rolls_back_sect_updates(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_scheduled_material_grants(conn)
            conn.execute(
                """
                CREATE TRIGGER reject_scheduled_grant
                BEFORE INSERT ON sect_scheduled_material_grants
                BEGIN
                    SELECT RAISE(ABORT, 'reject grant');
                END
                """
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.grant_scheduled_materials(
                "sect-materials:2026-07-11", 1, 2
            )

        self.assertEqual(self.sect_row(), (50, 1))

    def test_missing_or_inactive_sect_is_not_recorded(self) -> None:
        missing = self.service.grant_scheduled_materials(
            "sect-materials:2026-07-11", 999, 2
        )
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE sects SET sect_owner=NULL WHERE sect_id=1")
        inactive = self.service.grant_scheduled_materials(
            "sect-materials:2026-07-11", 1, 2
        )

        self.assertEqual(missing.status, "sect_missing")
        self.assertEqual(inactive.status, "sect_inactive")
        with db_backend.connection(self.database) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM sect_scheduled_material_grants"
            ).fetchone()[0]
        self.assertEqual(count, 0)


if __name__ == "__main__":
    unittest.main()
