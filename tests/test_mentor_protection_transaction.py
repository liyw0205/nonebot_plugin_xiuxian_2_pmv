import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.transaction_service import (
    MentorApplicationService,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.transaction_service import (
    MentorBindService,
)
from tests.test_db_backend import db_backend


class MentorProtectionTransactionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level TEXT)"
            )
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES(%s,%s)",
                [
                    ("m", "洞虚境"),
                    ("n", "洞虚境"),
                    ("a", "筑基境"),
                    ("b", "筑基境"),
                    ("c", "筑基境"),
                    ("d", "筑基境"),
                ],
            )
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TABLE mentor("
                "user_id TEXT PRIMARY KEY,mentor_id TEXT,apprentice_ids TEXT,"
                "mentor_cd_until TEXT,apprentice_cd_until TEXT,"
                "mentor_rebind_cd TEXT,mentor_history TEXT,bind_time TEXT,"
                "breakthrough_reward_count INTEGER,mentor_protect TEXT,"
                "mentor_apply_time TEXT,mentor_apply_target TEXT)"
            )
            conn.executemany(
                "INSERT INTO mentor VALUES(%s,NULL,%s,NULL,NULL,'{}',%s,NULL,0,%s,NULL,NULL)",
                [
                    ("m", '["kept"]', '["history"]', "off"),
                    ("n", "[]", "[]", "off"),
                    ("a", "[]", "[]", "off"),
                    ("b", "[]", "[]", "off"),
                    ("c", "[]", "[]", "off"),
                    ("d", "[]", "[]", "off"),
                ],
            )
        self.applications = MentorApplicationService(self.player)
        self.bind = MentorBindService(self.game, self.player)

    def tearDown(self):
        self.temp.cleanup()

    def bind_application(self, operation="bind", invite_id="invite"):
        return self.bind.apply(
            operation,
            "m",
            "a",
            invite_id,
            bind_time="1970-01-01 00:02:00",
            expected_mentor_level="洞虚境",
            expected_apprentice_level="筑基境",
            max_apprentices=5,
            history_limit=50,
            mentor_desc="收徒",
            apprentice_desc="拜师",
            now=datetime.fromtimestamp(120),
        )

    def application_statuses(self):
        with db_backend.connection(self.player) as conn:
            return dict(conn.execute(
                "SELECT invite_id,status FROM mentor_applications ORDER BY invite_id"
            ).fetchall())

    def test_enable_rejects_only_active_applications_and_replays_result(self):
        self.applications.create("expired", "m", "d", now=100, ttl_seconds=50)
        self.applications.create("active-a", "m", "a", now=101, ttl_seconds=500)
        self.applications.create("active-b", "m", "b", now=102, ttl_seconds=500)
        self.applications.create("other", "n", "c", now=103, ttl_seconds=500)

        applied = self.applications.set_protection(
            "protect", "m", "off", "on", now=200
        )
        duplicate = self.applications.set_protection(
            "protect", "m", "on", "on", now=201
        )

        self.assertEqual("applied", applied.status)
        self.assertEqual(("active-a", "active-b"), applied.rejected_invite_ids)
        self.assertEqual(("a", "b"), applied.rejected_apprentice_ids)
        self.assertEqual("duplicate", duplicate.status)
        self.assertEqual(applied.rejected_invite_ids, duplicate.rejected_invite_ids)
        self.assertEqual(
            {
                "active-a": "rejected",
                "active-b": "rejected",
                "expired": "expired",
                "other": "pending",
            },
            self.application_statuses(),
        )
        with db_backend.connection(self.player) as conn:
            row = conn.execute(
                "SELECT apprentice_ids,mentor_history,mentor_protect "
                "FROM mentor WHERE user_id='m'"
            ).fetchone()
        self.assertEqual(["kept"], json.loads(row[0]))
        self.assertEqual(["history"], json.loads(row[1]))
        self.assertEqual("on", row[2])

    def test_operation_conflict_and_changed_snapshot_do_not_write(self):
        self.assertEqual(
            "applied",
            self.applications.set_protection(
                "protect", "m", "off", "on", now=100
            ).status,
        )
        self.assertEqual(
            "operation_conflict",
            self.applications.set_protection(
                "protect", "m", "on", "off", now=101
            ).status,
        )
        self.assertEqual(
            "operation_conflict",
            self.applications.set_protection(
                "protect", "n", "off", "on", now=101
            ).status,
        )
        changed = self.applications.set_protection(
            "changed", "m", "off", "off", now=102
        )
        self.assertEqual("state_changed", changed.status)
        self.assertEqual("on", self.applications.get_protection("m"))
        with db_backend.connection(self.player) as conn:
            self.assertIsNone(conn.execute(
                "SELECT operation_id FROM mentor_protection_operations "
                "WHERE operation_id='changed'"
            ).fetchone())

    def test_on_to_on_repairs_legacy_pending_application(self):
        self.applications.create(
            "legacy", "m", "a", now=100, ttl_seconds=400
        )
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE mentor SET mentor_protect='on' WHERE user_id='m'")

        result = self.applications.set_protection(
            "repair", "m", "on", "on", now=200
        )

        self.assertEqual("applied", result.status)
        self.assertEqual(("legacy",), result.rejected_invite_ids)
        self.assertEqual("rejected", self.application_statuses()["legacy"])

    def test_operation_failure_rolls_back_status_and_rejections(self):
        self.applications.create("invite", "m", "a", now=100, ttl_seconds=500)
        self.applications.set_protection("seed", "m", "off", "off", now=101)
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TRIGGER fail_mentor_protection_operation "
                "BEFORE INSERT ON mentor_protection_operations "
                "BEGIN SELECT RAISE(ABORT,'forced failure'); END"
            )

        with self.assertRaises(Exception):
            self.applications.set_protection(
                "fail", "m", "off", "on", now=102
            )

        self.assertEqual("off", self.applications.get_protection("m"))
        self.assertEqual("pending", self.application_statuses()["invite"])
        with db_backend.connection(self.player) as conn:
            self.assertIsNone(conn.execute(
                "SELECT operation_id FROM mentor_protection_operations "
                "WHERE operation_id='fail'"
            ).fetchone())

    def test_protection_before_create_rejects_creation(self):
        self.applications.set_protection("protect", "m", "off", "on", now=100)

        result = self.applications.create(
            "invite", "m", "a", now=101, ttl_seconds=500
        )

        self.assertEqual("protected", result.status)
        with db_backend.connection(self.player) as conn:
            self.assertEqual(0, conn.execute(
                "SELECT COUNT(*) FROM mentor_applications"
            ).fetchone()[0])
            apply_state = conn.execute(
                "SELECT mentor_apply_time,mentor_apply_target "
                "FROM mentor WHERE user_id='a'"
            ).fetchone()
        self.assertEqual((None, None), tuple(apply_state))

    def test_accept_before_protection_preserves_accepted_authorization(self):
        self.applications.create("invite", "m", "a", now=100, ttl_seconds=500)
        self.assertEqual("applied", self.bind_application().status)

        protected = self.applications.set_protection(
            "protect", "m", "off", "on", now=130
        )

        self.assertEqual((), protected.rejected_invite_ids)
        self.assertEqual("accepted", self.application_statuses()["invite"])
        with db_backend.connection(self.player) as conn:
            self.assertEqual("m", conn.execute(
                "SELECT mentor_id FROM mentor WHERE user_id='a'"
            ).fetchone()[0])

    def test_protection_before_accept_revokes_pending_authorization(self):
        self.applications.create("invite", "m", "a", now=100, ttl_seconds=500)
        self.applications.set_protection("protect", "m", "off", "on", now=110)

        result = self.bind_application()

        self.assertEqual("invitation_changed", result.status)
        self.assertEqual("rejected", self.application_statuses()["invite"])
        with db_backend.connection(self.player) as conn:
            self.assertIsNone(conn.execute(
                "SELECT mentor_id FROM mentor WHERE user_id='a'"
            ).fetchone()[0])
            self.assertIsNone(conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name='statistics'"
            ).fetchone())


if __name__ == "__main__":
    unittest.main()
