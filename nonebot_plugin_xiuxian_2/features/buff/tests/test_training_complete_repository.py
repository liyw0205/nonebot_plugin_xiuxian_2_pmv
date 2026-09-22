import tempfile
import unittest
from pathlib import Path

from ..training_complete_repository import NormalTrainingCompleteSqlRepository
from tests.test_db_backend import db_backend


class NormalTrainingCompleteRepositoryTests(unittest.TestCase):
    def test_missing_operation_is_stable(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            with db_backend.transaction(game) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,stone INTEGER,hp INTEGER,mp INTEGER)")
                conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            result = NormalTrainingCompleteSqlRepository(game, player).complete("missing", "2026-W01")
            self.assertEqual(result.status, "operation_missing")
