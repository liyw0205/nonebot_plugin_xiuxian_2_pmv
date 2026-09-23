from pathlib import Path
import tempfile
import unittest
from ..elixir_room_repository import SectElixirRoomSqlRepository
from tests.test_db_backend import db_backend

class SectElixirRoomRepositoryTests(unittest.TestCase):
    def test_upgrade_replays_and_deducts_stone_and_scale(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'sect.db'
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('owner',1,0)")
                c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,elixir_room_level INTEGER,sect_used_stone INTEGER,sect_scale INTEGER)'); c.execute("INSERT INTO sects VALUES(1,'owner',1,1000,2000)")
            repo=SectElixirRoomSqlRepository(db)
            self.assertEqual(repo.upgrade('op','owner',1,1,2,100,200)['status'],'upgraded')
            self.assertEqual(repo.upgrade('op','owner',1,1,2,100,200)['status'],'duplicate')
            with db_backend.connection(db) as c:self.assertEqual(tuple(c.execute('SELECT elixir_room_level,sect_used_stone,sect_scale FROM sects').fetchone()),(2,900,1800))
