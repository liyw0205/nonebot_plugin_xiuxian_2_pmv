from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.transaction_service import (
    SectMembershipService,
)
from tests.test_db_backend import db_backend


class SectPracticeUpgradeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect-practice.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                """
                CREATE TABLE user_xiuxian (
                    user_id TEXT PRIMARY KEY,
                    sect_id INTEGER,
                    stone INTEGER,
                    atkpractice INTEGER,
                    hppractice INTEGER,
                    mppractice INTEGER
                )
                """
            )
            conn.execute(
                "CREATE TABLE sects (sect_id INTEGER PRIMARY KEY, sect_materials INTEGER)"
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s, %s, %s, %s)",
                ("user", 1, 1000, 2, 3, 4),
            )
            conn.execute("INSERT INTO sects VALUES (%s, %s)", (1, 5000))
        self.service = SectMembershipService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            user = conn.execute(
                "SELECT stone, atkpractice FROM user_xiuxian WHERE user_id=%s",
                ("user",),
            ).fetchone()
            materials = conn.execute(
                "SELECT sect_materials FROM sects WHERE sect_id=%s", (1,)
            ).fetchone()[0]
            return int(user[0]), int(user[1]), int(materials)

    def test_upgrade_changes_personal_and_sect_assets_atomically(self) -> None:
        result = self.service.upgrade_practice(
            "practice-1", "user", 1, "attack", 2, 3, 100, 1000
        )

        self.assertEqual(result.status, "upgraded")
        self.assertEqual(self.state(), (900, 3, 4000))

    def test_health_upgrade_uses_the_same_atomic_boundary(self) -> None:
        result = self.service.upgrade_practice(
            "health-1", "user", 1, "health", 3, 4, 100, 1000
        )

        self.assertEqual(result.status, "upgraded")
        with db_backend.connection(self.database) as conn:
            row = conn.execute(
                "SELECT stone, hppractice FROM user_xiuxian WHERE user_id=%s",
                ("user",),
            ).fetchone()
        self.assertEqual((int(row[0]), int(row[1])), (900, 4))
        self.assertEqual(
            self.scalar_materials(), 4000
        )

    def test_mana_upgrade_uses_the_same_atomic_boundary(self) -> None:
        result = self.service.upgrade_practice(
            "mana-1", "user", 1, "mana", 4, 5, 100, 1000
        )

        self.assertEqual(result.status, "upgraded")
        with db_backend.connection(self.database) as conn:
            row = conn.execute(
                "SELECT stone, mppractice FROM user_xiuxian WHERE user_id=%s",
                ("user",),
            ).fetchone()
        self.assertEqual((int(row[0]), int(row[1])), (900, 5))
        self.assertEqual(self.scalar_materials(), 4000)

    def scalar_materials(self):
        with db_backend.connection(self.database) as conn:
            return int(
                conn.execute(
                    "SELECT sect_materials FROM sects WHERE sect_id=%s", (1,)
                ).fetchone()[0]
            )

    def test_duplicate_operation_does_not_charge_twice(self) -> None:
        self.service.upgrade_practice(
            "practice-repeat", "user", 1, "attack", 2, 3, 100, 1000
        )
        result = self.service.upgrade_practice(
            "practice-repeat", "user", 1, "attack", 2, 3, 100, 1000
        )

        self.assertEqual(result.status, "duplicate")
        self.assertEqual(self.state(), (900, 3, 4000))

    def test_stale_level_and_insufficient_assets_leave_state_unchanged(self) -> None:
        self.assertEqual(
            self.service.upgrade_practice(
                "level", "user", 1, "attack", 1, 2, 100, 1000
            ).status,
            "level_changed",
        )
        self.assertEqual(
            self.service.upgrade_practice(
                "stone", "user", 1, "attack", 2, 3, 1001, 1000
            ).status,
            "stone_insufficient",
        )
        self.assertEqual(
            self.service.upgrade_practice(
                "materials", "user", 1, "attack", 2, 3, 100, 5001
            ).status,
            "materials_insufficient",
        )
        self.assertEqual(self.state(), (1000, 2, 5000))

    def test_failure_rolls_back_personal_and_sect_assets(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_practice_operations(conn)
            conn.execute(
                """
                CREATE TRIGGER fail_practice BEFORE INSERT ON sect_practice_operations
                BEGIN SELECT RAISE(ABORT, 'practice failed'); END
                """
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.upgrade_practice(
                "practice-fail", "user", 1, "attack", 2, 3, 100, 1000
            )
        self.assertEqual(self.state(), (1000, 2, 5000))


if __name__ == "__main__":
    unittest.main()
