import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.mentor_expel_service import MentorExpelService
from tests.test_db_backend import db_backend


class MentorExpelTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); root = Path(self.temp.name)
        self.game, self.player = root / "g.db", root / "p.db"
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE mentor(user_id TEXT PRIMARY KEY,mentor_id TEXT,apprentice_ids TEXT,mentor_cd_until TEXT,apprentice_cd_until TEXT,mentor_rebind_cd TEXT,mentor_history TEXT,bind_time TEXT,breakthrough_reward_count INTEGER)")
            conn.executemany("INSERT INTO mentor VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", [
                ("m", None, '["a"]', None, None, "{}", "[]", None, 0),
                ("a", "m", "[]", None, None, "{}", "[]", "old", 2),
            ])
        self.service = MentorExpelService(self.game, self.player)

    def tearDown(self): self.temp.cleanup()

    def call(self, operation="op", pair="2026-08-12 12:00:00"):
        return self.service.apply(operation, "m", "a", occurred_at="2026-07-13 12:00:00",
            mentor_cd_until="2026-07-20 12:00:00", apprentice_cd_until="2026-07-27 12:00:00",
            pair_rebind_until=pair, history_limit=50, mentor_desc="逐出", apprentice_desc="被逐出")

    def test_atomic_idempotency_conflict_and_relation_change(self):
        self.assertEqual("applied", self.call().status)
        self.assertEqual("duplicate", self.call().status)
        self.assertEqual("operation_conflict", self.call(pair="2026-08-13 12:00:00").status)
        self.assertEqual("state_changed", self.call("new").status)
        with db_backend.connection(self.player) as conn:
            row = conn.execute("SELECT mentor_id,apprentice_cd_until,mentor_rebind_cd FROM mentor WHERE user_id='a'").fetchone()
            self.assertIsNone(row[0]); self.assertEqual("2026-07-27 12:00:00", row[1])
            self.assertEqual("2026-08-12 12:00:00", json.loads(row[2])["m"])

    def test_failure_rolls_back_everything(self):
        self.call("seed")
        with db_backend.transaction(self.game) as conn:
            conn.execute("DELETE FROM mentor_expel_operations")
            conn.execute("CREATE TRIGGER fail BEFORE INSERT ON mentor_expel_operations BEGIN SELECT RAISE(ABORT,'x'); END")
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE mentor SET apprentice_ids='[\"a\"]',mentor_cd_until=NULL,mentor_history='[]' WHERE user_id='m'")
            conn.execute("UPDATE mentor SET mentor_id='m',bind_time='old',breakthrough_reward_count=2,apprentice_cd_until=NULL,mentor_rebind_cd='{}',mentor_history='[]' WHERE user_id='a'")
            conn.execute("DELETE FROM statistics")
        with self.assertRaises(Exception): self.call("rollback")
        with db_backend.connection(self.player) as conn:
            self.assertEqual("m", conn.execute("SELECT mentor_id FROM mentor WHERE user_id='a'").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM statistics").fetchone()[0])


if __name__ == "__main__": unittest.main()
