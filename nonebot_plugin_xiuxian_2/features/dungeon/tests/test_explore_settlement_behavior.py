import tempfile
import unittest
from pathlib import Path

from ..repository import DungeonSessionSqlRepository
from tests.test_db_backend import db_backend


class DungeonExploreSettlementBehaviorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,hp INTEGER,mp INTEGER,stone INTEGER,exp INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',80,30,100,1000)")
            conn.execute("CREATE TABLE user_cd(user_id TEXT,type INTEGER)")
            conn.execute("INSERT INTO user_cd VALUES('u',0)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            conn.execute("CREATE TABLE dungeon_explore_operations(operation_id TEXT PRIMARY KEY,request_identity TEXT NOT NULL,phase TEXT NOT NULL,prepared_json TEXT NOT NULL DEFAULT '{}',result_status TEXT NOT NULL DEFAULT '',result_json TEXT NOT NULL DEFAULT '{}',current_layer INTEGER NOT NULL DEFAULT 0,dungeon_status TEXT NOT NULL DEFAULT '',created_at TEXT,updated_at TEXT)")
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE player_dungeon_status(user_id TEXT PRIMARY KEY,dungeon_id TEXT,dungeon_name TEXT,dungeon_status TEXT,current_layer INTEGER,total_layers INTEGER,last_reset_date TEXT,reset_generation INTEGER,reset_operation_id TEXT)")
            conn.execute("INSERT INTO player_dungeon_status VALUES('u','d','D','not_started',0,3,'2026-07-15',1,'reset-1')")
            conn.execute("CREATE TABLE teams(user_id TEXT PRIMARY KEY,leader TEXT,members TEXT,version INTEGER DEFAULT 0)")
        self.repo = DungeonSessionSqlRepository(self.game, self.player)

    def tearDown(self):
        self.temp.cleanup()

    def plan(self, **changes):
        plan = {
            "expected_status": {"dungeon_id":"d","dungeon_name":"D","dungeon_status":"not_started","current_layer":0,"total_layers":3,"last_reset_date":"2026-07-15","reset_generation":1,"reset_operation_id":"reset-1"},
            "team": None,
            "members": [{"user_id":"u","expected":{"hp":80,"mp":30,"stone":100,"exp":1000,"cd_type":0},"final_hp":55,"final_mp":12,"stone_delta":7,"exp_delta":9,"items":[{"id":9,"name":"奖品","type":"道具","amount":1,"expected_num":0,"expected_bind_num":0}]}],
            "advance": True,
            "complete": False,
            "response": {"message":"done"},
        }
        plan.update(changes)
        return plan

    def state(self):
        with db_backend.connection(self.game) as conn:
            wallet = tuple(conn.execute("SELECT hp,mp,stone,exp FROM user_xiuxian WHERE user_id='u'").fetchone())
            item = conn.execute("SELECT goods_num,bind_num FROM back WHERE user_id='u' AND goods_id=9").fetchone()
        with db_backend.connection(self.player) as conn:
            dungeon = tuple(conn.execute("SELECT dungeon_status,current_layer FROM player_dungeon_status WHERE user_id='u'").fetchone())
        return wallet, tuple(item) if item else None, dungeon

    def test_success_replay_and_state_progress(self):
        self.repo.prepare("op", "u", self.plan())
        first = self.repo.settle("op", "u", 99)
        replay = self.repo.settle("op", "u", 99)
        self.assertEqual((first["status"], replay["status"]), ("applied", "duplicate"))
        self.assertEqual(self.state(), ((55, 12, 107, 1009), (1, 1), ("exploring", 1)))

    def test_snapshot_conflict_completes_rejection_without_rewards(self):
        self.repo.prepare("op", "u", self.plan())
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=101 WHERE user_id='u'")
        result = self.repo.settle("op", "u", 99)
        self.assertEqual(result["result_status"], "state_changed")
        self.assertEqual(self.state(), ((80, 30, 101, 1000), None, ("not_started", 0)))

    def test_inventory_full_rejection_leaves_prepared_operation_and_assets(self):
        self.repo.prepare("op", "u", self.plan())
        result = self.repo.settle("op", "u", 0)
        self.assertEqual(result["result_status"], "inventory_full")
        self.assertEqual(self.state(), ((80, 30, 100, 1000), None, ("not_started", 0)))

    def test_completion_trigger_rolls_back_assets_and_keeps_prepared_operation(self):
        self.repo.prepare("op", "u", self.plan())
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TRIGGER fail_complete BEFORE UPDATE OF phase ON dungeon_explore_operations WHEN NEW.phase='completed' BEGIN SELECT RAISE(ABORT,'fail'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.repo.settle("op", "u", 99)
        self.assertEqual(self.state(), ((80, 30, 100, 1000), None, ("not_started", 0)))


if __name__ == "__main__":
    unittest.main()
