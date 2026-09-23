import tempfile
import unittest
from pathlib import Path

from ..reset_repository import DungeonResetSqlRepository
from tests.test_db_backend import db_backend


class DungeonResetRepositoryTests(unittest.TestCase):
    def test_automatic_publication_replays_without_reroll(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "player.db"
            with db_backend.transaction(database) as conn:
                conn.execute("CREATE TABLE dungeon_global_state(user_id TEXT PRIMARY KEY,dungeon_id TEXT,dungeon_name TEXT,date TEXT,total_layers INTEGER,dungeon_type TEXT,description TEXT,reset_generation INTEGER,reset_operation_id TEXT)")
                conn.execute("CREATE TABLE player_dungeon_status(user_id TEXT PRIMARY KEY,dungeon_id TEXT,dungeon_name TEXT,dungeon_status TEXT,current_layer INTEGER,total_layers INTEGER,last_reset_date TEXT,reset_generation INTEGER,reset_operation_id TEXT)")
                conn.execute("INSERT INTO player_dungeon_status VALUES('u','old','Old','exploring',2,3,'2026-07-13',1,'old-op')")
            repository = DungeonResetSqlRepository(database)
            snapshot = {"dungeon_id":"d1","dungeon_name":"D1","total_layers":3,"dungeon_type":"explore","description":""}
            first = repository.reset("op-1", "2026-07-14", "crossday", lambda: snapshot)
            duplicate = repository.reset("op-2", "2026-07-14", "daily", lambda: (_ for _ in ()).throw(AssertionError("must not reroll")))
            self.assertEqual((first["status"], duplicate["status"]), ("applied", "duplicate"))
            self.assertEqual(first["generation"], duplicate["generation"])
            self.assertEqual(repository.ensure_player_status("u")["reset_generation"], 1)
