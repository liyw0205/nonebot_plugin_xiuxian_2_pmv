import tempfile
import unittest
from pathlib import Path
from ..closing_settlement_repository import ImpartClosingSettlementSqlRepository
from tests.test_db_backend import db_backend
from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import (
    apply_impart_pk_closing_statistics,
    apply_impart_pk_impart_operations,
    apply_impart_pk_operations,
    apply_impart_pk_player_operations,
)

class ImpartClosingSettlementRepositoryTests(unittest.TestCase):
    def test_settlement_replay_and_state_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); game,impart,player=root/'game.db',root/'impart.db',root/'player.db'
            with db_backend.transaction(game) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,hp INTEGER,mp INTEGER,atk INTEGER,power INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('u',100,1,2,3,4)"); c.execute('CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)'); c.execute("INSERT INTO user_cd VALUES('u',4,'start',NULL)")
            with db_backend.transaction(impart) as c: c.execute('CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,exp_day INTEGER)'); c.execute("INSERT INTO xiuxian_impart VALUES('u',30)")
            with DatabaseUnitOfWork(player, immediate=True) as uow:
                apply_impart_pk_closing_statistics(uow)
                uow.execute("INSERT INTO statistics(user_id) VALUES('u')")
            with DatabaseUnitOfWork(game, immediate=True) as uow: apply_impart_pk_operations(uow)
            repo=ImpartClosingSettlementSqlRepository(game,impart,player); first=repo.settle('s','u','start',100,30,20,10,60,40,50,6,999); dup=repo.settle('s','u','start',100,30,999,10,60,40,50,6,999); self.assertEqual((first.status,dup.status),('applied','duplicate'))
