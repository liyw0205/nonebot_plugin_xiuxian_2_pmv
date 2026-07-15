from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_map.transaction_service import MapMissionClaimService
from tests.test_db_backend import db_backend


class MapMissionClaimServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game, self.player = root / "game.sqlite3", root / "player.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("u", 10))
            conn.execute("CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE map_mission (user_id TEXT PRIMARY KEY,date TEXT,mission_type TEXT,target INTEGER,claimed INTEGER,settlement TEXT)")
            conn.execute("INSERT INTO map_mission VALUES (%s,%s,%s,%s,%s,%s)", ("u", "2026-07-13", "gather", 5, 0, "snapshot"))
            conn.execute("CREATE TABLE map_daily_limit (user_id TEXT PRIMARY KEY,date TEXT,gather_count INTEGER)")
            conn.execute("INSERT INTO map_daily_limit VALUES (%s,%s,%s)", ("u", "2026-07-13", 5))
        self.service = MapMissionClaimService(self.game, self.player)
        self.mission = {"date": "2026-07-13", "mission_type": "gather", "target": 5, "claimed": 0, "settlement": "snapshot"}
        self.daily = {"date": "2026-07-13", "gather_count": 5}
        self.items = [{"id": 1, "name": "材料", "type": "材料", "amount": 2}]

    def tearDown(self):
        self.temp_dir.cleanup()

    def claim(self, operation_id="op", **overrides):
        values = dict(stone=7, items=self.items, max_goods=99)
        values.update(overrides)
        return self.service.claim(operation_id, "u", self.mission, self.daily, "gather_count", values["stone"], values["items"], values["max_goods"])

    def state(self):
        with db_backend.connection(self.game) as conn:
            stone = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("u",)).fetchone()
            item = conn.execute("SELECT goods_num,bind_num FROM back WHERE user_id=%s AND goods_id=%s", ("u", 1)).fetchone()
        with db_backend.connection(self.player) as conn:
            claimed = conn.execute("SELECT claimed FROM map_mission WHERE user_id=%s", ("u",)).fetchone()
        return int(stone[0]), int(claimed[0]), tuple(map(int, item)) if item else None

    def test_success_marks_claimed_and_grants_rewards_together(self):
        result = self.claim()
        self.assertEqual((result.status, result.stone, result.rewards), ("applied", 7, ((1, 2),)))
        self.assertEqual(self.state(), (17, 1, (2, 2)))

    def test_duplicate_incomplete_and_stale_claims_do_not_change_state(self):
        first, duplicate = self.claim("repeat"), self.claim("repeat")
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual(self.state(), (17, 1, (2, 2)))
        self.setUp()
        self.daily["gather_count"] = 4
    def test_duplicate_incomplete_and_stale_claims_do_not_change_state(self):
        first, duplicate = self.claim("repeat"), self.claim("repeat")
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual(self.state(), (17, 1, (2, 2)))
        self.setUp()
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE map_daily_limit SET gather_count=%s WHERE user_id=%s", (4, "u"))
        self.daily["gather_count"] = 4
        self.assertEqual(self.claim("incomplete").status, "not_completed")
        self.daily["gather_count"] = 5
        self.mission["settlement"] = "other"
        self.assertEqual(self.claim("stale").status, "state_changed")
        self.assertEqual(self.state(), (10, 0, None))
        self.setUp()
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE map_mission_claim_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stone INTEGER NOT NULL,rewards TEXT NOT NULL,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_claim BEFORE INSERT ON map_mission_claim_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.claim("rollback")
        self.assertEqual(self.state(), (10, 0, None))


if __name__ == "__main__":
    unittest.main()
