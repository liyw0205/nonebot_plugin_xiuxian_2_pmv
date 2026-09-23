import tempfile
import unittest
from pathlib import Path
from ..scheduled_material_repository import SectScheduledMaterialSqlRepository
from tests.test_db_backend import db_backend

class SectScheduledMaterialRepositoryTests(unittest.TestCase):
    def test_grant_is_idempotent_and_updates_power(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'sect.db'
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_scale INTEGER,sect_owner TEXT,sect_materials INTEGER,combat_power INTEGER)')
                c.execute("INSERT INTO sects VALUES(1,120,'owner',50,0)")
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,power INTEGER)')
                c.execute("INSERT INTO user_xiuxian VALUES('owner',1,300)")
            repo=SectScheduledMaterialSqlRepository(db)
            first=repo.grant('grant:1',1,2)
            second=repo.grant('grant:1',1,99)
            self.assertEqual((first['status'],second['status']),("granted","duplicate"))
            with db_backend.connection(db) as c:
                self.assertEqual(tuple(c.execute('SELECT sect_materials,combat_power FROM sects WHERE sect_id=1').fetchone()),(290,300))
