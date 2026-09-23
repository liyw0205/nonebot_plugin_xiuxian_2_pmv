import tempfile
import unittest
from pathlib import Path
from ..closing_enter_repository import ImpartClosingEnterSqlRepository
from tests.test_db_backend import db_backend

class ImpartClosingEnterRepositoryTests(unittest.TestCase):
    def test_enter_replay_and_busy_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            game,player=Path(temp)/'game.db',Path(temp)/'player.db'
            with db_backend.transaction(game) as c: c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,root_type TEXT)'); c.execute("INSERT INTO user_xiuxian VALUES('u','天灵根')"); c.execute('CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)'); c.execute("INSERT INTO user_cd VALUES('u',0,'',NULL)")
            with db_backend.transaction(player) as c: c.execute('CREATE TABLE statistics(user_id TEXT PRIMARY KEY,虚神界闭关次数 INTEGER)'); c.execute("INSERT INTO statistics VALUES('u',0)")
            repo=ImpartClosingEnterSqlRepository(game,player); first=repo.enter('c','u','2026-01-01'); dup=repo.enter('c','u','2026-01-01'); self.assertEqual((first.status,dup.status),('applied','duplicate'))
