import tempfile
import unittest
from pathlib import Path
from ..task_settlement_repository import SectTaskSettlementSqlRepository
from tests.test_db_backend import db_backend

class SectTaskSettlementRepositoryTests(unittest.TestCase):
    def test_hp_and_stone_settlement_replay_atomically(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'sect.db'
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,stone INTEGER,hp INTEGER,exp INTEGER,sect_task INTEGER,sect_contribution INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('user',1,1000,500,2000,0,20)")
                c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_used_stone INTEGER,sect_scale INTEGER,sect_materials INTEGER)'); c.execute("INSERT INTO sects VALUES(1,50,100,200)")
                c.execute('CREATE TABLE sect_task_state(user_id TEXT,sect_id INTEGER,task_key TEXT,task_data TEXT,period TEXT,status TEXT,progress INTEGER,target INTEGER,updated_at TEXT,completed_at TEXT,PRIMARY KEY(user_id,period))'); c.execute("INSERT INTO sect_task_state VALUES('user',1,'trial','{}','p','accepted',0,1,'now',NULL)")
            repo=SectTaskSettlementSqlRepository(db)
            first=repo.settle('op','user',1,'p','hp',100,300,25); duplicate=repo.settle('op','user',1,'p','hp',999,999,999)
            self.assertEqual((first['status'],duplicate['status']),('settled','duplicate'))
