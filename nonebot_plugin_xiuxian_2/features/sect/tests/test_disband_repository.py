import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from ..disband_repository import SectDisbandSqlRepository
from tests.test_db_backend import db_backend

class SectDisbandRepositoryTests(unittest.TestCase):
    def test_inactive_sole_owner_disbands_and_replays(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'sect.db'; checked=datetime(2026,7,14)
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT,sect_owner TEXT,closed INTEGER)')
                c.execute("INSERT INTO sects VALUES(1,'青云','owner',0)")
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER,sect_contribution INTEGER)')
                c.execute("INSERT INTO user_xiuxian VALUES('owner',1,0,100)")
                c.execute('CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,last_check_info_time TEXT)')
                c.execute("INSERT INTO user_cd VALUES('owner','2026-06-01T00:00:00')")
            repo=SectDisbandSqlRepository(db)
            first=repo.disband_inactive('op',1,'inactive_sole_owner',expected_sect_name='青云',expected_owner_id='owner',expected_closed=False,expected_member_ids=('owner',),expected_active_candidate_ids=(),checked_at=checked,inactivity_days=30)
            self.assertEqual(first['status'],'disbanded')
            self.assertEqual(repo.disband_inactive('op',1,'inactive_sole_owner',expected_sect_name='青云',expected_owner_id='owner',expected_closed=False,expected_member_ids=('owner',),expected_active_candidate_ids=(),checked_at=checked,inactivity_days=30)['status'],'duplicate')
