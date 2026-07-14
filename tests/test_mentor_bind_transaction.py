import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.mentor_bind_service import MentorBindService
from tests.test_db_backend import db_backend


class MentorBindTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level TEXT)")
            conn.executemany("INSERT INTO user_xiuxian VALUES (%s,%s)", [("m", "洞虚境"), ("a", "筑基境"), ("x", "筑基境")])
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE mentor(user_id TEXT PRIMARY KEY,mentor_id TEXT,apprentice_ids TEXT,mentor_cd_until TEXT,apprentice_cd_until TEXT,mentor_rebind_cd TEXT,mentor_history TEXT,bind_time TEXT,breakthrough_reward_count INTEGER)")
            conn.executemany("INSERT INTO mentor VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", [
                ("m", None, "[]", None, None, "{}", "[]", None, 0),
                ("a", None, "[]", None, None, "{}", "[]", None, 0),
                ("x", None, "[]", None, None, "{}", "[]", None, 0),
            ])
        self.service = MentorBindService(self.game, self.player)
        self.invite = {("m", "a", "invite-1")}

    def tearDown(self):
        self.temp.cleanup()

    def call(self, operation="op", max_apprentices=5, invite_id="invite-1", bind_time="2026-07-13 12:00:00"):
        return self.service.apply(operation, "m", "a", invite_id, bind_time=bind_time,
            expected_mentor_level="洞虚境", expected_apprentice_level="筑基境", max_apprentices=max_apprentices,
            history_limit=50, mentor_desc="收徒", apprentice_desc="拜师",
            invitation_validator=lambda m, a, i: (m, a, i) in self.invite,
            now=datetime(2026, 7, 13, 12, 0, 0))

    def test_atomic_idempotency_and_conflict(self):
        applied = self.call()
        self.assertEqual("applied", applied.status)
        self.invite.clear()
        duplicate = self.call(bind_time="2026-07-13 12:01:00")
        self.assertEqual("duplicate", duplicate.status)
        self.assertEqual(applied.bind_time, duplicate.bind_time)
        self.assertEqual("operation_conflict", self.call(max_apprentices=6).status)
        self.assertEqual("operation_conflict", self.call(invite_id="invite-2").status)
        with db_backend.connection(self.player) as conn:
            self.assertEqual("m", conn.execute("SELECT mentor_id FROM mentor WHERE user_id='a'").fetchone()[0])
            self.assertEqual(1, conn.execute('SELECT "收徒次数" FROM statistics WHERE user_id=\'m\'').fetchone()[0])

    def test_replay_uses_actor_identity_before_business_checks(self):
        applied = self.call()
        self.invite.clear()

        replayed = self.service.replay("op", "m", "a")
        conflict = self.service.replay("op", "m", "x")

        self.assertEqual("duplicate", replayed.status)
        self.assertEqual(applied.bind_time, replayed.bind_time)
        self.assertEqual("operation_conflict", conflict.status)
        self.assertIsNone(self.service.replay("missing", "m", "a"))

    def test_replays_legacy_payload_with_bind_time(self):
        applied = self.call()
        with db_backend.transaction(self.game) as conn:
            payload = json.loads(conn.execute(
                "SELECT payload FROM mentor_bind_operations WHERE operation_id='op'"
            ).fetchone()[0])
            payload.insert(3, applied.bind_time)
            conn.execute(
                "UPDATE mentor_bind_operations SET payload=%s WHERE operation_id='op'",
                (json.dumps(payload, ensure_ascii=False, separators=(",", ":")),),
            )

        duplicate = self.call(bind_time="2026-07-13 12:10:00")

        self.assertEqual("duplicate", duplicate.status)
        self.assertEqual(applied.bind_time, duplicate.bind_time)

    def test_invitation_capacity_cooldown_and_existing_mentor(self):
        self.assertEqual("invitation_changed", self.call(invite_id="bad").status)
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE mentor SET apprentice_ids=%s WHERE user_id='m'", (json.dumps(["x"]),))
            conn.execute("UPDATE mentor SET mentor_id='m' WHERE user_id='x'")
        self.assertEqual("capacity_reached", self.call("capacity", max_apprentices=1).status)
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE mentor SET apprentice_ids='[]',mentor_cd_until='2099-01-01 00:00:00' WHERE user_id='m'")
        self.assertEqual("cooldown_active", self.call("cooldown").status)
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE mentor SET mentor_cd_until=NULL WHERE user_id='m'")
            conn.execute("UPDATE mentor SET mentor_id='x' WHERE user_id='a'")
        self.assertEqual("already_bound", self.call("bound").status)

    def test_failure_rolls_back_relation_statistics_and_history(self):
        self.call("seed")
        with db_backend.transaction(self.game) as conn:
            conn.execute("DELETE FROM mentor_bind_operations")
            conn.execute("CREATE TRIGGER fail_bind BEFORE INSERT ON mentor_bind_operations BEGIN SELECT RAISE(ABORT,'fail'); END")
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE mentor SET apprentice_ids='[]' WHERE user_id='m'")
            conn.execute("UPDATE mentor SET mentor_id=NULL,bind_time=NULL,mentor_history='[]' WHERE user_id='a'")
            conn.execute("UPDATE mentor SET mentor_history='[]' WHERE user_id='m'")
            conn.execute("DELETE FROM statistics")
        with self.assertRaises(Exception):
            self.call("rollback")
        with db_backend.connection(self.player) as conn:
            self.assertIsNone(conn.execute("SELECT mentor_id FROM mentor WHERE user_id='a'").fetchone()[0])
            self.assertEqual([], json.loads(conn.execute("SELECT apprentice_ids FROM mentor WHERE user_id='m'").fetchone()[0]))
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM statistics").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
