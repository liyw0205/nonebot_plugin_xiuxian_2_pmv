import tempfile
import unittest
from pathlib import Path

from ..migrations import apply_work_settlement_operations
from ..settlement_repository import WorkSettlementSqlRepository
from ....infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class WorkSettlementRepositoryTests(unittest.TestCase):
    def test_applied_duplicate_and_state_changed(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',90)")
                conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
                conn.execute("INSERT INTO user_cd VALUES('u',2,'2026-01-01','任务')")
                conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            with DatabaseUnitOfWork(db) as uow:
                apply_work_settlement_operations(uow)
            repo = WorkSettlementSqlRepository(db)
            first = repo.settle("s1", "u", {"create_time":"2026-01-01","scheduled_time":"任务"}, 10, {"goods_id":1,"goods_name":"奖励","goods_type":"物品","quantity":2}, 100)
            duplicate = repo.settle("s1", "u", {"create_time":"2026-01-01","scheduled_time":"任务"}, 99, None, 100)
            stale = repo.settle("s2", "u", {"create_time":"old","scheduled_time":"任务"}, 10, None, 100)
            self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))

    def test_application_reward_shape_and_result_metadata_are_persisted(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',90)")
                conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
                conn.execute("INSERT INTO user_cd VALUES('u',2,'2026-01-01','任务')")
                conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            with DatabaseUnitOfWork(db) as uow:
                apply_work_settlement_operations(uow)

            result = WorkSettlementSqlRepository(db).settle(
                "metadata", "u", {"create_time": "2026-01-01", "scheduled_time": "任务"},
                10, {"id": 1, "name": "奖励", "type": "物品"}, 100, 2,
                success_kind="big", item_msg="一品:奖励",
            )

            self.assertEqual((result.status, result.exp, result.item_awarded), ("applied", 10, True))
            self.assertEqual((result.success_kind, result.item_msg, result.scheduled_time), ("big", "一品:奖励", "任务"))
            with db_backend.connection(db) as conn:
                item = conn.execute("SELECT goods_name,goods_type,goods_num FROM back WHERE user_id='u' AND goods_id=1").fetchone()
                cd = conn.execute("SELECT type,create_time,scheduled_time FROM user_cd WHERE user_id='u'").fetchone()
            self.assertEqual(tuple(item), ("奖励", "物品", 1))
            self.assertEqual(tuple(cd), (0, "0", None))

    def test_inventory_limit_rejects_without_changing_work_state(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',90)")
                conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
                conn.execute("INSERT INTO user_cd VALUES('u',2,'2026-01-01','任务')")
                conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
                conn.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) VALUES('u',1,'已有','物品',2,0)")
            with DatabaseUnitOfWork(db) as uow:
                apply_work_settlement_operations(uow)

            result = WorkSettlementSqlRepository(db).settle(
                "full", "u", {"create_time": "2026-01-01", "scheduled_time": "任务"},
                10, {"id": 1, "name": "奖励", "type": "物品"}, 100, 2,
            )

            self.assertEqual(result.status, "inventory_full")
            with db_backend.connection(db) as conn:
                exp = conn.execute("SELECT exp FROM user_xiuxian WHERE user_id='u'").fetchone()[0]
                cd = conn.execute("SELECT type FROM user_cd WHERE user_id='u'").fetchone()[0]
                operations = conn.execute("SELECT COUNT(*) FROM work_settlement_operations").fetchone()[0]
            self.assertEqual((exp, cd, operations), (90, 2, 0))

    def test_missing_startup_schema_is_not_created_by_request(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',90)")
                conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
                conn.execute("INSERT INTO user_cd VALUES('u',2,'2026-01-01','任务')")

            result = WorkSettlementSqlRepository(db).settle(
                "missing", "u", {"create_time": "2026-01-01", "scheduled_time": "任务"}, 10, None, 100
            )

            self.assertEqual(result.status, "schema_missing")
            with db_backend.connection(db) as conn:
                tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn("work_settlement_operations", tables)

    def test_startup_migration_adds_missing_result_column_without_replacing_receipts(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE work_settlement_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,exp INTEGER NOT NULL,item_awarded INTEGER NOT NULL,created_at TEXT)")
                conn.execute("INSERT INTO work_settlement_operations VALUES('historic','[\"u\"]',100,1,'old')")

            with DatabaseUnitOfWork(db) as uow:
                apply_work_settlement_operations(uow)

            with db_backend.connection(db) as conn:
                columns = {row[1] for row in conn.execute("PRAGMA table_info(work_settlement_operations)")}
                row = conn.execute("SELECT operation_id,exp FROM work_settlement_operations").fetchone()
            self.assertIn("result_json", columns)
            self.assertEqual(tuple(row), ("historic", 100))
