from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.transaction_service import SectMembershipService
from tests.test_db_backend import db_backend


class SectDonationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, sect_id INTEGER, stone INTEGER, sect_contribution INTEGER)")
            conn.execute("CREATE TABLE sects (sect_id INTEGER PRIMARY KEY, sect_used_stone INTEGER, sect_scale INTEGER, sect_materials INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s, %s, %s)", ("user", 1, 1000, 20))
            conn.execute("INSERT INTO sects VALUES (%s, %s, %s, %s)", (1, 50, 100, 200))
            conn.execute("INSERT INTO sects VALUES (%s, %s, %s, %s)", (2, 0, 0, 0))
        self.service = SectMembershipService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            user = conn.execute("SELECT sect_id, stone, sect_contribution FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone()
            sect = conn.execute("SELECT sect_used_stone, sect_scale, sect_materials FROM sects WHERE sect_id=%s", (1,)).fetchone()
            table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                ("sect_donation_operations",),
            ).fetchone()
            operations = conn.execute("SELECT COUNT(*) FROM sect_donation_operations").fetchone()[0] if table else 0
        return tuple(map(int, user)), tuple(map(int, sect)), int(operations)

    def test_donation_updates_all_assets_atomically(self) -> None:
        result = self.service.donate("donate-1", "user", 1, 100, 250)
        self.assertEqual(result.status, "donated")
        self.assertEqual(self.state(), ((1, 900, 120), (150, 200, 450), 1))

    def test_duplicate_operation_does_not_apply_twice(self) -> None:
        self.service.donate("donate-repeat", "user", 1, 100, 250)
        result = self.service.donate("donate-repeat", "user", 1, 999, 999)
        self.assertEqual(result.status, "duplicate")
        self.assertEqual((result.stone, result.materials), (100, 250))
        self.assertEqual(self.state(), ((1, 900, 120), (150, 200, 450), 1))

    def test_insufficient_stones_leave_state_unchanged(self) -> None:
        result = self.service.donate("donate-poor", "user", 1, 1001, 250)
        self.assertEqual(result.status, "stone_insufficient")
        self.assertEqual(self.state(), ((1, 1000, 20), (50, 100, 200), 0))

    def test_changed_membership_is_rejected(self) -> None:
        result = self.service.donate("donate-sect", "user", 2, 100, 250)
        self.assertEqual(result.status, "sect_changed")
        self.assertEqual(self.state(), ((1, 1000, 20), (50, 100, 200), 0))

    def test_database_failure_rolls_back_every_asset(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_donation_operations(conn)
            conn.execute("CREATE TRIGGER fail_donation BEFORE INSERT ON sect_donation_operations BEGIN SELECT RAISE(ABORT, 'donation failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.service.donate("donate-fail", "user", 1, 100, 250)
        self.assertEqual(self.state(), ((1, 1000, 20), (50, 100, 200), 0))


if __name__ == "__main__":
    unittest.main()
