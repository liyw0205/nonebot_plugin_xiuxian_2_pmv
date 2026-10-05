import tempfile
import unittest
from pathlib import Path
from ..visit_reward_repository import DongfuVisitRewardSqlRepository
from ..migrations import apply_dongfu_event_replay
from ....infrastructure.database import DatabaseUnitOfWork
from . import install_operation_schema
from tests.test_db_backend import db_backend

class DongfuVisitRewardRepositoryTests(unittest.TestCase):
    def test_reward_replay_and_dongfu_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            game,player=Path(temp)/'game.db',Path(temp)/'player.db'
            with db_backend.transaction(game) as c: c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('v',0)")
            with db_backend.transaction(player) as c: c.execute('CREATE TABLE dongfu_status(user_id TEXT PRIMARY KEY,built INTEGER)'); c.execute("INSERT INTO dongfu_status VALUES('v',1)"); c.execute("INSERT INTO dongfu_status VALUES('t',1)")
            install_operation_schema(game)
            repo=DongfuVisitRewardSqlRepository(game,player); first=repo.reward('r','v','t',10); dup=repo.reward('r','v','t',45000); self.assertEqual((first.status,first.gain,dup.status,dup.gain),('rewarded',10,'duplicate',10))

    def test_upgrade_keeps_legacy_payload_and_replays_stored_gain(self):
        with tempfile.TemporaryDirectory() as temp:
            game,player=Path(temp)/'game.db',Path(temp)/'player.db'
            with DatabaseUnitOfWork(game) as uow:
                uow.execute('CREATE TABLE dongfu_visit_reward_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TEXT)')
                uow.execute('INSERT INTO dongfu_visit_reward_operations(operation_id,payload) VALUES(?,?)',('old','v|t|12345'))
                apply_dongfu_event_replay(uow)
                columns={str(row['name']) for row in uow.query_all('PRAGMA table_info("dongfu_visit_reward_operations")')}
                self.assertIn('gain',columns)
                self.assertEqual(uow.query_one('SELECT payload,gain FROM dongfu_visit_reward_operations WHERE operation_id=?',('old',))['payload'],'v|t|12345')
            with DatabaseUnitOfWork(game) as uow:
                uow.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)')
                uow.execute("INSERT INTO user_xiuxian VALUES('v',0)")
            with DatabaseUnitOfWork(player) as uow:
                uow.execute('CREATE TABLE dongfu_status(user_id TEXT PRIMARY KEY,built INTEGER)')
                uow.execute("INSERT INTO dongfu_status VALUES('v',1)")
                uow.execute("INSERT INTO dongfu_status VALUES('t',1)")
            result=DongfuVisitRewardSqlRepository(game,player).reward('old','v','t',50000)
            self.assertEqual((result.status,result.gain),('duplicate',12345))
