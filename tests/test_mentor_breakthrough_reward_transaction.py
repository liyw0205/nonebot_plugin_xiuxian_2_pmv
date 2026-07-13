import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.mentor_breakthrough_reward_service import MentorBreakthroughRewardService
from tests.test_db_backend import db_backend


class MentorBreakthroughRewardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); root = Path(self.temp.name)
        self.game, self.player = root / "g.db", root / "p.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,power INTEGER)")
            conn.executemany("INSERT INTO user_xiuxian VALUES (%s,%s,%s)", [("m", 1000, 10), ("a", 500, 5)])
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE mentor(user_id TEXT PRIMARY KEY,mentor_id TEXT,apprentice_ids TEXT,mentor_history TEXT,breakthrough_reward_count INTEGER)")
            conn.executemany("INSERT INTO mentor VALUES (%s,%s,%s,%s,%s)", [
                ("m", None, '["a"]', "[]", 0), ("a", "m", "[]", "[]", 1),
            ])
        self.service = MentorBreakthroughRewardService(self.game, self.player)

    def tearDown(self): self.temp.cleanup()

    def call(self, operation="op", event="breakthrough:a:洞虚境:1", reward=100, expected=1000):
        return self.service.apply(operation, "m", "a", "洞虚境", event,
            expected_mentor_exp=expected, expected_apprentice_exp=500, expected_reward_count=1,
            reward_limit=3, reward_exp=reward, max_mentor_exp=2000, mentor_power=99,
            history_limit=50, mentor_desc="徒弟突破返修", apprentice_desc="突破回馈")

    def test_atomic_idempotency_operation_and_business_event(self):
        self.assertEqual("applied", self.call().status)
        self.assertEqual("duplicate", self.call().status)
        self.assertEqual("operation_conflict", self.call(reward=101).status)
        self.assertEqual("event_duplicate", self.call("other").status)
        with db_backend.connection(self.game) as conn:
            self.assertEqual((1100, 99), tuple(conn.execute("SELECT exp,power FROM user_xiuxian WHERE user_id='m'").fetchone()))
        with db_backend.connection(self.player) as conn:
            self.assertEqual(2, conn.execute("SELECT breakthrough_reward_count FROM mentor WHERE user_id='a'").fetchone()[0])
            self.assertEqual(100, conn.execute('SELECT "师父突破返修" FROM statistics WHERE user_id=\'m\'').fetchone()[0])

    def test_state_cap_limit_and_failure_rollback(self):
        self.assertEqual("state_changed", self.call(expected=999).status)
        with self.assertRaises(ValueError):
            self.service.apply("limit", "m", "a", "洞虚境", "event-limit", expected_mentor_exp=1000,
                expected_apprentice_exp=500, expected_reward_count=3, reward_limit=3, reward_exp=1,
                max_mentor_exp=2000, mentor_power=11, history_limit=50, mentor_desc="x", apprentice_desc="x")
        with self.assertRaises(ValueError):
            self.service.apply("cap", "m", "a", "洞虚境", "event-cap", expected_mentor_exp=1000,
                expected_apprentice_exp=500, expected_reward_count=1, reward_limit=3, reward_exp=1001,
                max_mentor_exp=2000, mentor_power=11, history_limit=50, mentor_desc="x", apprentice_desc="x")
        self.call("seed", "seed-event")
        with db_backend.transaction(self.game) as conn:
            conn.execute("DELETE FROM mentor_breakthrough_reward_operations")
            conn.execute("UPDATE user_xiuxian SET exp=1000,power=10 WHERE user_id='m'")
            conn.execute("CREATE TRIGGER fail BEFORE INSERT ON mentor_breakthrough_reward_operations BEGIN SELECT RAISE(ABORT,'x'); END")
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE mentor SET breakthrough_reward_count=1,mentor_history='[]' WHERE user_id='a'")
            conn.execute("UPDATE mentor SET mentor_history='[]' WHERE user_id='m'")
            conn.execute("DELETE FROM statistics")
        with self.assertRaises(Exception): self.call("rollback", "rollback-event")
        with db_backend.connection(self.game) as conn:
            self.assertEqual((1000, 10), tuple(conn.execute("SELECT exp,power FROM user_xiuxian WHERE user_id='m'").fetchone()))
        with db_backend.connection(self.player) as conn:
            self.assertEqual(1, conn.execute("SELECT breakthrough_reward_count FROM mentor WHERE user_id='a'").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM statistics").fetchone()[0])


if __name__ == "__main__": unittest.main()
