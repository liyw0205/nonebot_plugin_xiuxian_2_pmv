import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.mentor_application_service import (
    MentorApplicationService,
)
from tests.test_db_backend import db_backend


class MentorApplicationEventReplayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.player = Path(self.temp.name) / "player.db"
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TABLE mentor("
                "user_id TEXT PRIMARY KEY,mentor_protect TEXT,"
                "mentor_apply_time TEXT,mentor_apply_target TEXT)"
            )
            conn.executemany(
                "INSERT INTO mentor VALUES(%s,'off',NULL,NULL)",
                [("m",), ("n",), ("a",), ("b",)],
            )
        self.service = MentorApplicationService(self.player)

    def tearDown(self):
        self.temp.cleanup()

    def status(self, invite_id):
        with db_backend.connection(self.player) as conn:
            row = conn.execute(
                "SELECT status FROM mentor_applications WHERE invite_id=%s",
                (invite_id,),
            ).fetchone()
        return None if row is None else str(row[0])

    def test_created_event_replays_after_application_reaches_terminal_state(self):
        created = self.service.create(
            "event", "m", "a", now=100, ttl_seconds=500
        )
        self.service.resolve("event", "m", "a", "rejected", now=110)

        replayed = self.service.replay_create("event", "m", "a")
        duplicate = self.service.create(
            "event", "m", "a", now=120, ttl_seconds=500
        )
        conflict = self.service.replay_create("event", "n", "a")

        self.assertEqual("applied", created.status)
        self.assertEqual("duplicate", replayed.status)
        self.assertEqual("rejected", replayed.application.status)
        self.assertEqual("duplicate", duplicate.status)
        self.assertEqual("rejected", duplicate.application.status)
        self.assertEqual("invite_conflict", conflict.status)
        with db_backend.connection(self.player) as conn:
            self.assertEqual(1, conn.execute(
                "SELECT COUNT(*) FROM mentor_applications"
            ).fetchone()[0])
            self.assertEqual(1, conn.execute(
                "SELECT COUNT(*) FROM mentor_application_create_operations"
            ).fetchone()[0])

    def test_protected_result_is_replayed_after_protection_is_disabled(self):
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE mentor SET mentor_protect='on' WHERE user_id='m'")
        protected = self.service.create("event", "m", "a", now=100)
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE mentor SET mentor_protect='off' WHERE user_id='m'")

        replayed = self.service.replay_create("event", "m", "a")
        duplicate_attempt = self.service.create("event", "m", "a", now=110)

        self.assertEqual("protected", protected.status)
        self.assertEqual("protected", replayed.status)
        self.assertEqual("protected", duplicate_attempt.status)
        self.assertIsNone(self.status("event"))

    def test_already_pending_result_replays_its_first_outcome(self):
        self.service.create("first", "m", "a", now=100, ttl_seconds=500)
        blocked = self.service.create("event", "m", "a", now=101)
        self.service.resolve("first", "m", "a", "cancelled", now=110)

        replayed = self.service.replay_create("event", "m", "a")

        self.assertEqual("already_pending", blocked.status)
        self.assertEqual("first", blocked.application.invite_id)
        self.assertEqual("already_pending", replayed.status)
        self.assertEqual("cancelled", replayed.application.status)
        self.assertIsNone(self.status("event"))

    def test_create_operation_failure_rolls_back_application_and_cooldown(self):
        self.service.create("seed", "m", "b", now=90, ttl_seconds=500)
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TRIGGER fail_mentor_application_create "
                "BEFORE INSERT ON mentor_application_create_operations "
                "BEGIN SELECT RAISE(ABORT,'forced failure'); END"
            )

        with self.assertRaises(Exception):
            self.service.create("fail", "m", "a", now=100)

        self.assertIsNone(self.status("fail"))
        with db_backend.connection(self.player) as conn:
            row = conn.execute(
                "SELECT mentor_apply_time,mentor_apply_target "
                "FROM mentor WHERE user_id='a'"
            ).fetchone()
            operation = conn.execute(
                "SELECT operation_id FROM mentor_application_create_operations "
                "WHERE operation_id='fail'"
            ).fetchone()
        self.assertEqual((None, None), tuple(row))
        self.assertIsNone(operation)

    def test_resolution_replays_and_conflicts_by_actor_and_target(self):
        self.service.create("invite", "m", "a", now=100, ttl_seconds=500)
        applied = self.service.resolve(
            "invite", "m", "a", "rejected", operation_id="reject", now=110
        )

        replayed = self.service.replay_resolution(
            "reject", "m", "a", "rejected"
        )
        conflict = self.service.replay_resolution(
            "reject", "m", "b", "rejected"
        )
        changed_invite = self.service.resolve(
            "other", "m", "a", "rejected", operation_id="reject", now=120
        )

        self.assertEqual("applied", applied.status)
        self.assertEqual("duplicate", replayed.status)
        self.assertEqual("operation_conflict", conflict.status)
        self.assertEqual("operation_conflict", changed_invite.status)
        self.assertEqual("rejected", self.status("invite"))

    def test_state_changed_resolution_result_is_stable(self):
        self.service.create("invite", "m", "a", now=100, ttl_seconds=500)
        self.service.resolve("invite", "m", "a", "cancelled", now=105)
        changed = self.service.resolve(
            "invite", "m", "a", "expired", operation_id="expire", now=110
        )
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "UPDATE mentor_applications SET status='pending' "
                "WHERE invite_id='invite'"
            )

        replayed = self.service.resolve(
            "invite", "m", "a", "expired", operation_id="expire", now=120
        )

        self.assertEqual("state_changed", changed.status)
        self.assertEqual("state_changed", replayed.status)
        self.assertEqual("pending", self.status("invite"))

    def test_resolution_operation_failure_rolls_back_terminal_state(self):
        self.service.create("seed", "m", "b", now=90, ttl_seconds=500)
        self.service.resolve(
            "seed", "m", "b", "rejected", operation_id="seed-op", now=91
        )
        self.service.create("invite", "m", "a", now=100, ttl_seconds=500)
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TRIGGER fail_mentor_application_resolution "
                "BEFORE INSERT ON mentor_application_resolution_operations "
                "BEGIN SELECT RAISE(ABORT,'forced failure'); END"
            )

        with self.assertRaises(Exception):
            self.service.resolve(
                "invite", "m", "a", "rejected",
                operation_id="fail", now=110,
            )

        self.assertEqual("pending", self.status("invite"))
        with db_backend.connection(self.player) as conn:
            self.assertIsNone(conn.execute(
                "SELECT operation_id FROM mentor_application_resolution_operations "
                "WHERE operation_id='fail'"
            ).fetchone())


if __name__ == "__main__":
    unittest.main()
