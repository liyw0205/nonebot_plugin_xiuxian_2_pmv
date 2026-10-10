import json
import tempfile
import unittest
from pathlib import Path

from ..repository import MapInteractiveStartSqlRepository
from tests.test_db_backend import db_backend


class MapInteractiveStartRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_stamina INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',12)")
            conn.execute("CREATE TABLE map_interactive_start_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_status TEXT NOT NULL,stamina INTEGER NOT NULL,action_json TEXT NOT NULL)")
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE map_status(user_id TEXT PRIMARY KEY,realm TEXT,heaven TEXT,node_id TEXT)")
            conn.execute("INSERT INTO map_status VALUES('u','凡界','一重天','n1')")
            conn.execute("CREATE TABLE map_daily_limit(user_id TEXT PRIMARY KEY,date TEXT,gather_count INTEGER,resource_total_count INTEGER)")
            conn.execute("INSERT INTO map_daily_limit VALUES('u','2026-09-15',2,4)")
            conn.execute("CREATE TABLE map_cooldown(user_id TEXT PRIMARY KEY,gather_cd_until TEXT)")
            conn.execute("INSERT INTO map_cooldown VALUES('u','')")
            conn.execute("CREATE TABLE map_interactive_actions(user_id TEXT PRIMARY KEY,action_id TEXT UNIQUE,action_type TEXT,status TEXT,state_json TEXT,settlement_json TEXT,ready_at TEXT,expires_at TEXT,cooldown_seconds INTEGER,updated_at TEXT)")
        self.repo = MapInteractiveStartSqlRepository(self.game, self.player)
        self.position = {"realm": "凡界", "heaven": "一重天", "node_id": "n1"}
        self.daily = {"date": "2026-09-15", "gather_count": 2, "resource_total_count": 4}

    def tearDown(self):
        self.tmp.cleanup()

    def call(self, operation="op", *, start_ts="2026-09-15 12:00:00", expected_cooldown="", **changes):
        values = dict(
            expected_stamina=12,
            stamina_cost=6,
            expected_position=self.position,
            expected_daily=self.daily,
            daily_limit=5,
            expected_cooldown=expected_cooldown,
            action={
                "action_id": operation,
                "action": "采集",
                "start_ts": start_ts,
                "ready_ts": "2026-09-15 12:00:10",
                "expire_ts": "2026-09-15 12:01:00",
                "cooldown_sec": 20,
            },
        )
        values.update(changes)
        return self.repo.start(operation, "u", "采集", **values)

    def test_success_and_replay(self):
        self.assertEqual("applied", self.call()["status"])
        self.assertEqual("duplicate", self.call()["status"])
        with db_backend.connection(self.game) as conn:
            self.assertEqual(6, conn.execute("SELECT user_stamina FROM user_xiuxian").fetchone()[0])

    def test_active_cooldown_is_enforced_and_replayed(self):
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE map_cooldown SET gather_cd_until='2026-09-15 12:00:30' WHERE user_id='u'")
        result = self.call(expected_cooldown="2026-09-15 12:00:30")
        self.assertEqual("cooldown", result["status"])
        self.assertEqual("2026-09-15 12:00:30", result["action"]["cooldown_until"])
        replay = self.call(expected_cooldown="2026-09-15 12:00:30")
        self.assertEqual("cooldown", replay["status"])
        self.assertEqual("2026-09-15 12:00:30", replay["action"]["cooldown_until"])
        with db_backend.connection(self.game) as conn:
            self.assertEqual(12, conn.execute("SELECT user_stamina FROM user_xiuxian").fetchone()[0])

    def test_stale_cooldown_snapshot_rejected(self):
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE map_cooldown SET gather_cd_until='2026-09-15 12:00:30' WHERE user_id='u'")
        self.assertEqual("state_changed", self.call(expected_cooldown="")["status"])
        with db_backend.connection(self.game) as conn:
            self.assertEqual(12, conn.execute("SELECT user_stamina FROM user_xiuxian").fetchone()[0])

    def test_expired_active_action_is_closed_and_cooldown_started(self):
        previous_action = {
            "action_id": "old",
            "action": "采集",
            "start_ts": "2026-09-15 11:58:00",
            "ready_ts": "2026-09-15 11:58:10",
            "expire_ts": "2026-09-15 11:59:00",
            "cooldown_sec": 15,
        }
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "INSERT INTO map_interactive_actions VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("u", "old", "采集", "active", json.dumps(previous_action), "", "2026-09-15 11:58:10", "2026-09-15 11:59:00", 15, "2026-09-15 11:58:00"),
            )
        result = self.call(start_ts="2026-09-15 12:00:00")
        self.assertEqual("cooldown", result["status"])
        self.assertEqual("2026-09-15 12:00:15", result["action"]["cooldown_until"])
        replay = self.call(start_ts="2026-09-15 12:00:00")
        self.assertEqual("cooldown", replay["status"])
        self.assertEqual("2026-09-15 12:00:15", replay["action"]["cooldown_until"])
        with db_backend.connection(self.player) as conn:
            self.assertEqual(("expired", "2026-09-15 12:00:15"), tuple(conn.execute("SELECT status,gather_cd_until FROM map_interactive_actions JOIN map_cooldown USING(user_id)").fetchone()))
        with db_backend.connection(self.game) as conn:
            self.assertEqual(12, conn.execute("SELECT user_stamina FROM user_xiuxian").fetchone()[0])

    def test_unexpired_active_action_still_blocks_start(self):
        previous_action = {"action_id": "old", "action": "采集", "expire_ts": "2026-09-15 12:01:00"}
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "INSERT INTO map_interactive_actions VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("u", "old", "采集", "active", json.dumps(previous_action), "", "2026-09-15 11:59:10", "2026-09-15 12:01:00", 15, "2026-09-15 11:59:00"),
            )
        result = self.call()
        self.assertEqual("already_running", result["status"])
        self.assertEqual("old", result["action"]["action_id"])
        with db_backend.connection(self.game) as conn:
            self.assertEqual(12, conn.execute("SELECT user_stamina FROM user_xiuxian").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
