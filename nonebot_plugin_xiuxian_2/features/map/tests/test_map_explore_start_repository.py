import tempfile
import unittest
from pathlib import Path

from ..repository import MapExploreStartSqlRepository
from tests.test_db_backend import db_backend


class MapExploreStartRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_stamina INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',12)")
            conn.execute("CREATE TABLE map_explore_start_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stamina INTEGER NOT NULL)")
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE map_status(user_id TEXT PRIMARY KEY,realm TEXT,heaven TEXT,node_id TEXT)")
            conn.execute("INSERT INTO map_status VALUES('u','凡界','一重天','n1')")
            conn.execute("CREATE TABLE map_daily_limit(user_id TEXT PRIMARY KEY,date TEXT,explore_count INTEGER,resource_total_count INTEGER)")
            conn.execute("INSERT INTO map_daily_limit VALUES('u','2026-09-15',2,4)")
            conn.execute("CREATE TABLE map_cooldown(user_id TEXT PRIMARY KEY,explore_start_cd_until TEXT)")
            conn.execute("INSERT INTO map_cooldown VALUES('u','')")
            conn.execute("CREATE TABLE map_explore_status(user_id TEXT PRIMARY KEY,running INTEGER,node_type TEXT,node_name TEXT,start_time TEXT,duration_min INTEGER,settlement TEXT,max_duration_min INTEGER,interval_min INTEGER)")
            conn.execute("INSERT INTO map_explore_status VALUES('u',0,'','','',0,'',0,0)")
        self.repo = MapExploreStartSqlRepository(self.game, self.player)
        self.position = {"realm": "凡界", "heaven": "一重天", "node_id": "n1"}
        self.status = {"running": 0, "node_type": "", "node_name": "", "start_time": "", "duration_min": 0, "settlement": "", "max_duration_min": 0, "interval_min": 0}
        self.daily = {"date": "2026-09-15", "explore_count": 2, "resource_total_count": 4}
        self.target = {"running": 1, "node_type": "遗迹", "node_name": "古迹", "start_time": "2026-09-15 12:00:00", "duration_min": 20, "settlement": "", "max_duration_min": 120, "interval_min": 20}

    def tearDown(self): self.tmp.cleanup()

    def call(self, operation="op", **changes):
        values = dict(expected_stamina=12, stamina_cost=6, expected_position=self.position, expected_status=self.status, expected_daily=self.daily, daily_limit=5, expected_cooldown="", cooldown_until="2026-09-15 12:00:30", new_status=self.target)
        values.update(changes)
        return self.repo.start(operation, "u", **values)

    def test_success_and_replay(self):
        self.assertEqual("applied", self.call()["status"])
        self.assertEqual("duplicate", self.call()["status"])
        with db_backend.connection(self.game) as conn: self.assertEqual(6, conn.execute("SELECT user_stamina FROM user_xiuxian").fetchone()[0])

    def test_stale_position_rejected(self):
        self.assertEqual("state_changed", self.call(expected_position=dict(self.position, node_id="n2"))["status"])

    def test_operation_failure_rolls_back(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TRIGGER fail_start BEFORE INSERT ON map_explore_start_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(db_backend.IntegrityError): self.call("rollback")
        with db_backend.connection(self.game) as conn: self.assertEqual(12, conn.execute("SELECT user_stamina FROM user_xiuxian").fetchone()[0])


if __name__ == "__main__": unittest.main()
