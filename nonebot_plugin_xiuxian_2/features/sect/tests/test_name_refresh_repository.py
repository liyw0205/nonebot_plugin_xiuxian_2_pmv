import tempfile
import unittest
from pathlib import Path
from ..name_refresh_repository import SectNameRefreshSqlRepository
from tests.test_db_backend import db_backend

class SectNameRefreshRepositoryTests(unittest.TestCase):
    def test_charge_is_idempotent_and_requires_unaffiliated_user(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'sect.db'
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,stone INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('user',NULL,1000)")
            repo=SectNameRefreshSqlRepository(db)
            first=repo.charge('op','user',100); duplicate=repo.charge('op','user',999)
            self.assertEqual((first['status'],duplicate['status']),('charged','duplicate'))
            with db_backend.connection(db) as c:self.assertEqual(c.execute('SELECT stone FROM user_xiuxian WHERE user_id="user"').fetchone()[0],900)
