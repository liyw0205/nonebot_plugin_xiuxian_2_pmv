import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift.speedup_service import RiftSpeedupService
from tests.test_db_backend import db_backend


class RiftSpeedupSettlementTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.db"
        self.rift = {"name": "test", "rank": 2, "time": 60}
        self.cd = {"type": 3, "create_time": "2026-07-14 10:00:00.000000", "scheduled_time": "60"}
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE rift_entries(user_id TEXT PRIMARY KEY,rift_data TEXT,status TEXT,duration INTEGER)"
            )
            conn.execute(
                "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)"
            )
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER)")
            conn.execute("INSERT INTO rift_entries VALUES('u',%s,'active',60)", (json.dumps(self.rift),))
            conn.execute("INSERT INTO user_cd VALUES('u',3,%s,'60')", (self.cd["create_time"],))
            conn.execute("INSERT INTO back VALUES('u',20012,1)")
            conn.execute("INSERT INTO back VALUES('u',20013,1)")
        self.service = RiftSpeedupService(self.database)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_normal_speedup_is_atomic_and_idempotent(self):
        first = self.service.apply("op", "u", 20012, self.rift, self.cd, 50)
        duplicate = self.service.apply("op", "u", 20012, self.rift, self.cd, 50)
        self.assertEqual((first.status, first.new_time, duplicate.status), ("applied", 30, "duplicate"))
        self.assertEqual(first.rift_data["time"], 30)
        with db_backend.connection(self.database) as conn:
            entry = conn.execute("SELECT rift_data,duration FROM rift_entries").fetchone()
            self.assertEqual((json.loads(entry[0])["time"], entry[1]), (30, 30))
            self.assertEqual(conn.execute("SELECT scheduled_time FROM user_cd").fetchone()[0], "30")
            self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE goods_id=20012").fetchone()[0], 0)

    def test_production_entry_rechecks_database_and_retries_idempotently(self):
        first = self.service.apply("event", "u", 20012, remaining_ratio=50)
        duplicate = self.service.apply("event", "u", 20012, remaining_ratio=50)
        self.assertEqual((first.status, duplicate.status, duplicate.new_time), ("applied", "duplicate", 30))
        self.assertEqual(duplicate.rift_data, {**self.rift, "time": 30})
        self.assertEqual(duplicate.create_time, self.cd["create_time"])
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE goods_id=20012").fetchone()[0], 0)

    def test_production_entry_rejects_inconsistent_rift_state_without_consuming_item(self):
        with db_backend.transaction(self.database) as conn:
            stale = {**self.rift, "time": 59}
            conn.execute("UPDATE rift_entries SET rift_data=%s,duration=60", (json.dumps(stale),))
        result = self.service.apply("inconsistent", "u", 20012, remaining_ratio=50)
        self.assertEqual(result.status, "state_changed")
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE goods_id=20012").fetchone()[0], 1)
            self.assertFalse(conn.table_exists("rift_speedup_operations"))

    def test_big_speedup_uses_same_transaction_kernel(self):
        result = self.service.apply("big", "u", 20013, self.rift, self.cd, 10)
        self.assertEqual((result.status, result.new_time), ("applied", 6))
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE goods_id=20013").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT duration FROM rift_entries").fetchone()[0], 6)

    def test_minimum_one_minute_and_no_speedup_boundary(self):
        with db_backend.transaction(self.database) as conn:
            small = {**self.rift, "time": 11}
            conn.execute("UPDATE rift_entries SET rift_data=%s,duration=11", (json.dumps(small),))
            conn.execute("UPDATE user_cd SET scheduled_time='11'")
        cd = {**self.cd, "scheduled_time": "11"}
        result = self.service.apply("minimum", "u", 20013, small, cd, 10)
        self.assertEqual(result.new_time, 1)

        with db_backend.transaction(self.database) as conn:
            short = {**self.rift, "time": 10}
            conn.execute("UPDATE rift_entries SET rift_data=%s,duration=10", (json.dumps(short),))
            conn.execute("UPDATE user_cd SET scheduled_time='10'")
            conn.execute("UPDATE back SET goods_num=1 WHERE goods_id=20012")
        result = self.service.apply("not-needed", "u", 20012, short, {**self.cd, "scheduled_time": "10"}, 50)
        self.assertEqual(result.status, "not_needed")
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE goods_id=20012").fetchone()[0], 1)

    def test_state_item_and_payload_conflicts_are_rejected(self):
        self.assertEqual(
            self.service.apply("state", "u", 20012, self.rift, {**self.cd, "scheduled_time": "59"}, 50).status,
            "state_changed",
        )
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE back SET goods_num=0 WHERE goods_id=20012")
        self.assertEqual(self.service.apply("missing", "u", 20012, self.rift, self.cd, 50).status, "item_missing")
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE back SET goods_num=1 WHERE goods_id=20012")
        self.assertEqual(self.service.apply("op", "u", 20012, self.rift, self.cd, 50).status, "applied")
        self.assertEqual(self.service.apply("op", "u", 20012, self.rift, self.cd, 10).status, "state_changed")

    def test_operation_failure_rolls_back_all_state(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE rift_speedup_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT,new_time INTEGER,rift_data TEXT,created_at TEXT)"
            )
            conn.execute(
                "CREATE TRIGGER fail_speedup BEFORE INSERT ON rift_speedup_operations "
                "BEGIN SELECT RAISE(ABORT,'fail'); END"
            )
        with self.assertRaises(Exception):
            self.service.apply("fail", "u", 20012, self.rift, self.cd, 50)
        with db_backend.connection(self.database) as conn:
            entry = conn.execute("SELECT rift_data,duration FROM rift_entries").fetchone()
            self.assertEqual((json.loads(entry[0])["time"], entry[1]), (60, 60))
            self.assertEqual(conn.execute("SELECT scheduled_time FROM user_cd").fetchone()[0], "60")
            self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE goods_id=20012").fetchone()[0], 1)

    def test_existing_operation_table_is_migrated(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE rift_speedup_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT,new_time INTEGER)"
            )
        result = self.service.apply("migrated", "u", 20012, self.rift, self.cd, 50)
        self.assertEqual(result.status, "applied")
        with db_backend.connection(self.database) as conn:
            self.assertTrue(conn.column_exists("rift_speedup_operations", "rift_data"))
            self.assertTrue(conn.column_exists("rift_speedup_operations", "create_time"))


if __name__ == "__main__":
    unittest.main()
