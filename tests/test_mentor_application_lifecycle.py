import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.mentor_application_service import MentorApplicationService
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.mentor_bind_service import MentorBindService
from tests.test_db_backend import db_backend


class MentorApplicationLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level TEXT)")
            conn.executemany("INSERT INTO user_xiuxian VALUES(%s,%s)", [("m", "洞虚境"), ("a", "筑基境")])
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TABLE mentor(user_id TEXT PRIMARY KEY,mentor_id TEXT,apprentice_ids TEXT,"
                "mentor_cd_until TEXT,apprentice_cd_until TEXT,mentor_rebind_cd TEXT,mentor_history TEXT,"
                "bind_time TEXT,breakthrough_reward_count INTEGER,mentor_protect TEXT)"
            )
            conn.executemany("INSERT INTO mentor VALUES(%s,NULL,'[]',NULL,NULL,'{}','[]',NULL,0,'off')", [("m",), ("a",)])
        self.apps = MentorApplicationService(self.player)
        self.bind = MentorBindService(self.game, self.player)

    def tearDown(self):
        self.temp.cleanup()

    def test_create_reject_expire_and_restart_recovery(self):
        self.assertEqual("applied", self.apps.create("i1", "m", "a", now=100, ttl_seconds=60).status)
        recovered = MentorApplicationService(self.player).find_pending_by_apprentice("a", now=120)
        self.assertEqual("i1", recovered.invite_id)
        self.assertEqual("applied", self.apps.resolve("i1", "m", "a", "rejected", now=130).status)
        self.assertIsNone(self.apps.find_pending_by_apprentice("a", now=131))
        self.apps.create("i2", "m", "a", now=200, ttl_seconds=10)
        self.assertEqual([], self.apps.list_pending("m", now=211))

    def test_bind_consumes_persistent_application_atomically(self):
        self.apps.create("i1", "m", "a", now=100, ttl_seconds=1000)
        result = self.bind.apply(
            "bind:i1", "m", "a", "i1", bind_time="1970-01-01 00:02:00",
            expected_mentor_level="洞虚境", expected_apprentice_level="筑基境",
            max_apprentices=5, history_limit=50, mentor_desc="收徒", apprentice_desc="拜师",
            now=datetime.fromtimestamp(120),
        )
        self.assertEqual("applied", result.status)
        with db_backend.connection(self.player) as conn:
            self.assertEqual("accepted", conn.execute("SELECT status FROM mentor_applications WHERE invite_id='i1'").fetchone()[0])
            self.assertEqual("m", conn.execute("SELECT mentor_id FROM mentor WHERE user_id='a'").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
