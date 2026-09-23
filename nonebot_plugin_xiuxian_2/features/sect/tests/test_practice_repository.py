import tempfile
import unittest
from pathlib import Path
from ..practice_repository import SectPracticeSqlRepository
from tests.test_db_backend import db_backend

class SectPracticeRepositoryTests(unittest.TestCase):
    def test_all_practice_types_replay_and_deduct_assets(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'sect.db'
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER,attack_practice INTEGER,health_practice INTEGER,mana_practice INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('user',1,3,2,3,4)")
                c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,sect_materials INTEGER,sect_used_stone INTEGER)'); c.execute("INSERT INTO sects VALUES(1,'owner',5000,1000)")
            repo=SectPracticeSqlRepository(db)
            for name, level, next_level in [('attack',2,3),('health',3,4),('mana',4,5)]:
                self.assertEqual(repo.upgrade(f'op-{name}','user',1,name,level,next_level,100,1000)['status'],'upgraded')
            self.assertEqual(repo.upgrade('op-attack','user',1,'attack',2,3,999,999)['status'],'duplicate')
            with db_backend.connection(db) as c:
                self.assertEqual(tuple(c.execute('SELECT attack_practice,health_practice,mana_practice,sect_materials,sect_used_stone FROM user_xiuxian JOIN sects ON sects.sect_id=user_xiuxian.sect_id WHERE user_id="user"').fetchone()),(3,4,5,2000,700))
