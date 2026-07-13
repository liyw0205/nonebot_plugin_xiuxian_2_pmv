from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart_pk.project_join_service import ImpartProjectJoinService
from tests.test_db_backend import db_backend


class ImpartProjectJoinTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.player = Path(self.tmp.name) / "player.db"
        self.service = ImpartProjectJoinService(self.player, capacity=2)

    def tearDown(self):
        self.tmp.cleanup()

    def state(self):
        with db_backend.connection(self.player) as conn:
            members = tuple(row[0] for row in conn.execute(
                "SELECT user_id FROM impart_project_members ORDER BY user_id"
            ).fetchall())
            states = tuple(tuple(row) for row in conn.execute(
                "SELECT user_id,pk_num FROM impart_pk_state ORDER BY user_id"
            ).fetchall())
            stats = tuple(tuple(row) for row in conn.execute(
                'SELECT user_id,"虚神界投影次数" FROM statistics ORDER BY user_id'
            ).fetchall())
        return members, states, stats

    def test_success_duplicate_and_already_joined(self):
        result = self.service.join("join-a", "a", legacy_pk_num=4)
        self.assertEqual(("applied", 4, 1), (result.status, result.pk_num, result.member_count))
        self.assertEqual("duplicate", self.service.join("join-a", "a", legacy_pk_num=4).status)
        self.assertEqual("operation_conflict", self.service.join("join-a", "b", legacy_pk_num=4).status)
        self.assertEqual("already_joined", self.service.join("join-a-2", "a", legacy_pk_num=4).status)
        self.assertEqual((("a",), (("a", 4),), (("a", 1),)), self.state())

    def test_pk_exhausted_and_legacy_members_import_once(self):
        result = self.service.join("join-empty", "empty", legacy_pk_num=0, legacy_members=["legacy"])
        self.assertEqual(("pk_exhausted", 0, 1), (result.status, result.pk_num, result.member_count))
        self.assertEqual(["legacy"], self.service.members(["ignored-later"]))
        self.assertTrue(self.service.contains("legacy"))
        self.assertFalse(self.service.contains("ignored-later"))

    def test_capacity_competition_allows_only_one_last_slot(self):
        service_a = ImpartProjectJoinService(self.player, capacity=1)
        service_b = ImpartProjectJoinService(self.player, capacity=1)
        barrier = threading.Barrier(2)
        results = []

        def join(service, operation, user_id):
            barrier.wait()
            results.append(service.join(operation, user_id, legacy_pk_num=7).status)

        threads = [
            threading.Thread(target=join, args=(service_a, "race-a", "a")),
            threading.Thread(target=join, args=(service_b, "race-b", "b")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(["applied", "capacity_full"], sorted(results))
        self.assertEqual(1, len(service_a.members()))

    def test_operation_insert_failure_rolls_back_everything(self):
        self.service.members()
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TRIGGER reject_project_join BEFORE INSERT ON impart_project_join_operations "
                "BEGIN SELECT RAISE(ABORT,'reject'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.service.join("rollback", "a", legacy_pk_num=5)

        with db_backend.connection(self.player) as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM impart_project_members").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM impart_pk_state").fetchone()[0])
            self.assertIsNone(conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='statistics'"
            ).fetchone())

    def test_daily_reset_does_not_resurrect_legacy_members(self):
        self.service.reset_daily(["legacy"])
        self.assertEqual([], self.service.members(["legacy"]))
        self.assertEqual("applied", self.service.join("new-day", "fresh", legacy_pk_num=7).status)
        self.assertEqual(["fresh"], self.service.members())


if __name__ == "__main__":
    unittest.main()
