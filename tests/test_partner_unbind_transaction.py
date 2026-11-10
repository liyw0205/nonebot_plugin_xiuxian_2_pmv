import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.transaction_service import PartnerUnbindService
from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.core.numeric import as_int_like
from tests.test_db_backend import db_backend


class PartnerUnbindTests(unittest.TestCase):
    def test_partner_facade_defers_unbind_service_construction(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import partner

        self.assertIsNone(partner._partner_unbind_service_instance)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            conn.executemany("INSERT INTO user_xiuxian VALUES(%s)", [("a",), ("b",)])
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE partner(user_id TEXT PRIMARY KEY,partner_id TEXT,bind_time TEXT,affection INTEGER)")
            conn.executemany("INSERT INTO partner VALUES(%s,%s,%s,%s)", [("a", "b", "2026-07-01 00:00:00", 8), ("b", "a", "2026-07-01 00:00:00", 9)])
        self.service = PartnerUnbindService(self.game, self.player)

    def tearDown(self):
        self.temp.cleanup()

    def apply(self, operation="op", *, affection=8, checked_at="2026-07-13 00:00:00"):
        return self.service.apply(operation, "a", "b", expected_user_bind_time="2026-07-01 00:00:00", expected_partner_bind_time="2026-07-01 00:00:00", expected_user_affection=affection, expected_partner_affection=9, checked_at=checked_at, minimum_days=7)

    def state(self):
        with db_backend.connection(self.player) as conn:
            return [tuple(row) for row in conn.execute("SELECT user_id,partner_id,bind_time,affection FROM partner ORDER BY user_id").fetchall()]

    def test_success_duplicate_and_conflict(self):
        self.assertEqual("applied", self.apply().status)
        self.assertEqual([("a", None, None, 0), ("b", None, None, 0)], self.state())
        self.assertEqual("duplicate", self.apply().status)
        self.assertEqual("operation_conflict", self.apply(affection=7).status)

    def test_state_change_minimum_period_and_failure_rollback(self):
        self.assertEqual("state_changed", self.apply(affection=7).status)
        self.assertEqual("too_early", self.apply("early", checked_at="2026-07-05 00:00:00").status)
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE partner_unbind_operations(operation_id TEXT PRIMARY KEY,payload TEXT,partner_id TEXT,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_partner_unbind BEFORE INSERT ON partner_unbind_operations BEGIN SELECT RAISE(ABORT,'forced'); END")
        with self.assertRaises(Exception):
            self.apply("failure")
        self.assertEqual([("a", "b", "2026-07-01 00:00:00", 8), ("b", "a", "2026-07-01 00:00:00", 9)], self.state())

    def legacy_affection(self, first="8", second="9", affinity="TEXT"):
        with db_backend.transaction(self.player) as conn:
            conn.execute("DROP TABLE partner")
            conn.execute(f"CREATE TABLE partner(user_id TEXT PRIMARY KEY,partner_id TEXT,bind_time TEXT,affection {affinity})")
            conn.executemany("INSERT INTO partner VALUES(%s,%s,%s,%s)", [
                ("a", "b", "2026-07-01 00:00:00", first),
                ("b", "a", "2026-07-01 00:00:00", second),
            ])
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS partner_unbind_operations(operation_id TEXT PRIMARY KEY,payload TEXT,partner_id TEXT,created_at TIMESTAMP)")
            conn.execute("DELETE FROM partner_unbind_operations")

    def snapshot(self):
        result = []
        for database in (self.game, self.player):
            with db_backend.connection(database) as conn:
                tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
                result.append({r[0]: [tuple(row) for row in conn.execute(f'SELECT * FROM "{r[0]}" ORDER BY rowid').fetchall()] for r in tables})
        return result

    def apply_numeric(self, first=8, second=9, operation="numeric"):
        return self.service.apply(operation, "a", "b",
            expected_user_bind_time="2026-07-01 00:00:00",
            expected_partner_bind_time="2026-07-01 00:00:00",
            expected_user_affection=first, expected_partner_affection=second,
            checked_at="2026-07-13 00:00:00")

    def test_legacy_numeric_affection_and_replay(self):
        for affinity, first, second in (("TEXT", "8", "9"), ("TEXT", "8e0", "9.0"),
                                        ("TEXT", "100000000000000000001", "1e29"),
                                        ("INTEGER", 1e29, 9)):
            with self.subTest(affinity=affinity, first=first):
                self.legacy_affection(first, second, affinity)
                args = (as_int_like(first), as_int_like(second))
                self.assertEqual("applied", self.apply_numeric(*args).status)
                settled = self.snapshot()
                self.assertEqual("duplicate", self.apply_numeric(*args).status)
                self.assertEqual(settled, self.snapshot())
                self.assertTrue(all(row[1] is None and as_int_like(row[3]) == 0 for row in self.state()))

    def test_legacy_affection_actual_changes_rejected(self):
        for owner in ("a", "b"):
            with self.subTest(owner=owner):
                self.legacy_affection("100000000000000000001", "9e0")
                with db_backend.transaction(self.player) as conn:
                    conn.execute("UPDATE partner SET affection=%s WHERE user_id=%s", ("100000000000000000002" if owner == "a" else "10", owner))
                before = self.snapshot()
                result = self.apply_numeric(100000000000000000001, 9)
                self.assertEqual(("state_changed", "affection_changed", owner), (result.status, result.reason, result.subject))
                self.assertEqual(before, self.snapshot())

    def test_legacy_affection_write_rejection_and_full_rollback(self):
        self.legacy_affection("8e0", "9e0")
        for owner in ("a", "b"):
            with self.subTest(owner=owner):
                with db_backend.transaction(self.player) as conn:
                    conn.execute(f"CREATE TRIGGER reject_unbind BEFORE UPDATE ON partner WHEN OLD.user_id='{owner}' BEGIN SELECT RAISE(IGNORE); END")
                before = self.snapshot()
                with self.assertRaises(DomainError) as caught:
                    self.apply_numeric()
                self.assertEqual("partner_relation_write_rejected", caught.exception.code)
                self.assertEqual("actor" if owner == "a" else "target", caught.exception.details["subject"])
                self.assertEqual(before, self.snapshot())
                with db_backend.transaction(self.player) as conn:
                    conn.execute("DROP TRIGGER reject_unbind")

    def test_affection_changes_after_precheck_still_roll_back(self):
        self.legacy_affection()
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TRIGGER change_target AFTER UPDATE ON partner WHEN OLD.user_id='a' BEGIN UPDATE partner SET affection='10' WHERE user_id='b'; END")
        before = self.snapshot()
        with self.assertRaises(DomainError) as caught:
            self.apply_numeric()
        self.assertEqual("partner_relation_write_rejected", caught.exception.code)
        self.assertEqual("target", caught.exception.details["subject"])
        self.assertEqual(before, self.snapshot())


if __name__ == "__main__":
    unittest.main()
