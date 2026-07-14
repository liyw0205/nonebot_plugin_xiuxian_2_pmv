from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_map.explore_start_service import MapExploreStartService
from tests.test_db_backend import db_backend


class MapExploreStartTransactionTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game, self.player = root / "game.sqlite3", root / "player.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,user_stamina INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("u", 12))
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE map_status (user_id TEXT PRIMARY KEY,realm TEXT,heaven TEXT,node_id TEXT)")
            conn.execute("INSERT INTO map_status VALUES (%s,%s,%s,%s)", ("u", "凡界", "一重天", "n1"))
            conn.execute("CREATE TABLE map_daily_limit (user_id TEXT PRIMARY KEY,date TEXT,explore_count INTEGER,resource_total_count INTEGER)")
            conn.execute("INSERT INTO map_daily_limit VALUES (%s,%s,%s,%s)", ("u", "2026-07-13", 2, 4))
            conn.execute("CREATE TABLE map_cooldown (user_id TEXT PRIMARY KEY,explore_start_cd_until TEXT)")
            conn.execute("INSERT INTO map_cooldown VALUES (%s,%s)", ("u", ""))
            conn.execute("CREATE TABLE map_explore_status (user_id TEXT PRIMARY KEY,running INTEGER,node_type TEXT,node_name TEXT,start_time TEXT,duration_min INTEGER,settlement TEXT,max_duration_min INTEGER,interval_min INTEGER)")
            conn.execute("INSERT INTO map_explore_status VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", ("u", 0, "", "", "", 0, "", 0, 0))
        self.service = MapExploreStartService(self.game, self.player)
        self.position = {"realm": "凡界", "heaven": "一重天", "node_id": "n1"}
        self.status = {"running": 0, "node_type": "", "node_name": "", "start_time": "", "duration_min": 0, "settlement": "", "max_duration_min": 0, "interval_min": 0}
        self.daily = {"date": "2026-07-13", "explore_count": 2, "resource_total_count": 4}
        self.new_status = {"running": 1, "node_type": "遗迹", "node_name": "古迹", "start_time": "2026-07-13 12:00:00", "duration_min": 20, "settlement": "", "max_duration_min": 120, "interval_min": 20}

    def tearDown(self):
        self.temp_dir.cleanup()

    def start(self, operation_id="op", **changes):
        values = {"stamina": 12, "cost": 6, "position": self.position, "status": self.status, "daily": self.daily, "limit": 5, "old_cd": "", "new_cd": "2026-07-13 12:00:30", "new_status": self.new_status}
        values.update(changes)
        return self.service.start(operation_id, "u", values["stamina"], values["cost"], values["position"], values["status"], values["daily"], values["limit"], values["old_cd"], values["new_cd"], values["new_status"])

    def current(self):
        with db_backend.connection(self.game) as conn:
            stamina = int(conn.execute("SELECT user_stamina FROM user_xiuxian WHERE user_id=%s", ("u",)).fetchone()[0])
        with db_backend.connection(self.player) as conn:
            status = conn.execute("SELECT running,node_name,duration_min FROM map_explore_status WHERE user_id=%s", ("u",)).fetchone()
            cooldown = conn.execute("SELECT explore_start_cd_until FROM map_cooldown WHERE user_id=%s", ("u",)).fetchone()[0]
        return stamina, tuple(status), str(cooldown)

    def test_start_spends_stamina_and_creates_complete_state(self):
        result = self.start()
        self.assertEqual((result.status, result.stamina), ("applied", 6))
        self.assertEqual(self.current(), (6, (1, "古迹", 20), "2026-07-13 12:00:30"))

    def test_duplicate_is_idempotent_and_stale_position_is_rejected(self):
        self.assertEqual(self.start("repeat").status, "applied")
        self.assertEqual(self.start("repeat").status, "duplicate")
        self.assertEqual(self.current()[0], 6)
        self.setUp()
        stale = dict(self.position, node_id="n2")
        self.assertEqual(self.start("stale", position=stale).status, "state_changed")
        self.assertEqual(self.current(), (12, (0, "", 0), ""))

    def test_operation_failure_rolls_back_stamina_status_and_cooldown(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE map_explore_start_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stamina INTEGER NOT NULL,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_start BEFORE INSERT ON map_explore_start_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.start("rollback")
        self.assertEqual(self.current(), (12, (0, "", 0), ""))

    def test_legacy_reward_plan_schema_is_migrated_before_start(self):
        with db_backend.transaction(self.player) as conn:
            conn.execute("DROP TABLE map_explore_status")
            conn.execute(
                "CREATE TABLE map_explore_status ("
                "user_id TEXT PRIMARY KEY,running INTEGER,node_type TEXT,node_name TEXT,start_time TEXT,"
                "duration_min INTEGER,max_duration_min INTEGER,interval_min INTEGER,reward_plan TEXT)"
            )
            conn.execute(
                "INSERT INTO map_explore_status VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                ("u", 0, "", "", "", 0, 0, 0, '{"legacy": true}'),
            )
        legacy_status = dict(self.status, settlement='{"legacy": true}')
        result = self.start("legacy", status=legacy_status)
        self.assertEqual((result.status, result.stamina), ("applied", 6))
        with db_backend.connection(self.player) as conn:
            columns = set(conn.column_names("map_explore_status"))
            row = conn.execute(
                "SELECT running,settlement,reward_plan FROM map_explore_status WHERE user_id=%s", ("u",)
            ).fetchone()
        self.assertIn("settlement", columns)
        self.assertEqual(tuple(row), (1, "", ""))

    def test_idle_snapshot_conflict_is_not_reported_as_running(self):
        stale = dict(self.status, node_name="旧地点")
        self.assertEqual(self.start("idle-conflict", status=stale).status, "state_changed")
        self.assertEqual(self.current(), (12, (0, "", 0), ""))
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE map_explore_status SET running=%s WHERE user_id=%s", (1, "u"))
        self.assertEqual(self.start("running-conflict").status, "already_running")

    def test_legacy_null_reward_plan_is_normalized_to_empty_settlement(self):
        with db_backend.transaction(self.player) as conn:
            conn.execute("DROP TABLE map_explore_status")
            conn.execute(
                "CREATE TABLE map_explore_status ("
                "user_id TEXT PRIMARY KEY,running INTEGER,node_type TEXT,node_name TEXT,start_time TEXT,"
                "duration_min INTEGER,max_duration_min INTEGER,interval_min INTEGER,reward_plan TEXT)"
            )
            conn.execute(
                "INSERT INTO map_explore_status VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                ("u", 0, "", "", "", 0, 0, 0, None),
            )
        self.assertEqual(self.start("legacy-null").status, "applied")
        with db_backend.connection(self.player) as conn:
            row = conn.execute(
                "SELECT running,settlement,reward_plan FROM map_explore_status WHERE user_id=%s", ("u",)
            ).fetchone()
        self.assertEqual(tuple(row), (1, "", ""))


if __name__ == "__main__":
    unittest.main()
