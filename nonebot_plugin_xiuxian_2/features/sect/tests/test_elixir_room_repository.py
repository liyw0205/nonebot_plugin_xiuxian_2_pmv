from pathlib import Path
import tempfile
import unittest
from ..elixir_room_repository import SectElixirRoomSqlRepository
from tests.test_db_backend import db_backend
from .fixtures import migrate_sect

class SectElixirRoomRepositoryTests(unittest.TestCase):
    def test_upgrade_replays_and_deducts_stone_and_scale(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'sect.db'
            migrate_sect(db)
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('owner',1,0)")
                c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,elixir_room_level INTEGER,sect_used_stone INTEGER,sect_scale INTEGER)'); c.execute("INSERT INTO sects VALUES(1,'owner',1,1000,2000)")
            repo=SectElixirRoomSqlRepository(db)
            self.assertEqual(repo.upgrade('op','owner',1,1,2,100,200)['status'],'upgraded')
            self.assertEqual(repo.upgrade('op','owner',1,1,2,100,200)['status'],'duplicate')
            with db_backend.connection(db) as c:self.assertEqual(tuple(c.execute('SELECT elixir_room_level,sect_used_stone,sect_scale FROM sects').fetchone()),(2,900,1800))

    def test_receipt_ignore_is_specific_and_rolls_back(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "sect.db"
            migrate_sect(db)
            with db_backend.transaction(db) as c:
                c.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)")
                c.execute("INSERT INTO user_xiuxian VALUES('owner',1,0)")
                c.execute("CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,elixir_room_level INTEGER,sect_used_stone INTEGER,sect_scale INTEGER)")
                c.execute("INSERT INTO sects VALUES(1,'owner',1,1000,2000)")
                c.execute("CREATE TRIGGER ignore_elixir_receipt BEFORE INSERT ON sect_elixir_room_operations BEGIN SELECT RAISE(IGNORE); END")
            result = SectElixirRoomSqlRepository(db).upgrade("ignore", "owner", 1, 1, 2, 100, 200)
            self.assertEqual(
                (result["status"], result["reason"], result["subject"]),
                ("state_changed", "sect_elixir_room_receipt_write_rejected", "ignore"),
            )
            with db_backend.connection(db) as c:
                self.assertEqual(tuple(c.execute("SELECT elixir_room_level,sect_used_stone,sect_scale FROM sects").fetchone()), (1, 1000, 2000))
                self.assertEqual(c.execute("SELECT COUNT(*) FROM sect_elixir_room_operations").fetchone()[0], 0)

    def test_receipt_rewrite_and_late_state_rewrite_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "sect.db"
            migrate_sect(db)
            with db_backend.transaction(db) as c:
                c.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)")
                c.execute("INSERT INTO user_xiuxian VALUES('owner',1,0)")
                c.execute("CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,elixir_room_level INTEGER,sect_used_stone INTEGER,sect_scale INTEGER)")
                c.execute("INSERT INTO sects VALUES(1,'owner',1,1000,2000)")
                c.execute("CREATE TRIGGER rewrite_elixir_receipt AFTER INSERT ON sect_elixir_room_operations BEGIN UPDATE sect_elixir_room_operations SET to_level=99 WHERE operation_id=NEW.operation_id; END")
            result = SectElixirRoomSqlRepository(db).upgrade("rewrite", "owner", 1, 1, 2, 100, 200)
            self.assertEqual(result["reason"], "sect_elixir_room_receipt_write_rejected")
            with db_backend.transaction(db) as c:
                c.execute("DROP TRIGGER rewrite_elixir_receipt")
                c.execute("CREATE TRIGGER late_elixir_state AFTER INSERT ON sect_elixir_room_operations BEGIN UPDATE sects SET sect_scale=sect_scale+777 WHERE sect_id=1; END")
            result = SectElixirRoomSqlRepository(db).upgrade("late", "owner", 1, 1, 2, 100, 200)
            self.assertEqual(result["reason"], "sect_elixir_room_state_write_rejected")
            with db_backend.connection(db) as c:
                self.assertEqual(tuple(c.execute("SELECT elixir_room_level,sect_used_stone,sect_scale FROM sects").fetchone()), (1, 1000, 2000))

    def test_same_operation_conflict_keeps_actor_subject(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "sect.db"
            migrate_sect(db)
            with db_backend.transaction(db) as c:
                c.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)")
                c.execute("INSERT INTO user_xiuxian VALUES('owner',1,0)")
                c.execute("CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,elixir_room_level INTEGER,sect_used_stone INTEGER,sect_scale INTEGER)")
                c.execute("INSERT INTO sects VALUES(1,'owner',1,1000,2000)")
            repo = SectElixirRoomSqlRepository(db)
            self.assertEqual(repo.upgrade("conflict", "owner", 1, 1, 2, 100, 200)["status"], "upgraded")
            result = repo.upgrade("conflict", "other", 1, 1, 2, 100, 200)
            self.assertEqual((result["status"], result["reason"], result["subject"]), ("state_changed", "operation_conflict", "actor"))
