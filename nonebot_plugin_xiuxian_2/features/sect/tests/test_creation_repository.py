import tempfile
import unittest
from pathlib import Path
from ..creation_repository import SectCreationSqlRepository
from tests.test_db_backend import db_backend

class SectCreationRepositoryTests(unittest.TestCase):
    def test_creation_is_idempotent_and_binds_owner_atomically(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'sect.db'
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER,stone INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('user',NULL,NULL,1000)")
                c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY AUTOINCREMENT,sect_name TEXT,sect_owner TEXT,sect_scale INTEGER,sect_used_stone INTEGER,join_open INTEGER,closed INTEGER,combat_power INTEGER)'); c.execute("INSERT INTO sects(sect_name,sect_owner,sect_scale,sect_used_stone,join_open,closed,combat_power) VALUES('已有宗门','other',0,0,1,0,0)")
            repo=SectCreationSqlRepository(db)
            first=repo.create('op','user','青云宗',300,0)
            duplicate=repo.create('op','user','其他',999,9)
            self.assertEqual((first['status'],duplicate['status']),('created','duplicate'))
            with db_backend.connection(db) as c:self.assertEqual(tuple(c.execute('SELECT sect_id,sect_position,stone FROM user_xiuxian WHERE user_id="user"').fetchone()),(first['sect_id'],0,700))
