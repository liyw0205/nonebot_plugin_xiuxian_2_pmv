import tempfile
import unittest
from pathlib import Path

from ..bet_repository import DufangBetSqlRepository
from tests.test_db_backend import db_backend


class DufangBetRepositoryTests(unittest.TestCase):
    def test_place_replay_and_insufficient(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            with db_backend.transaction(game) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',100)")
            repo = DufangBetSqlRepository(game, player)
            first = repo.place("b1", "u", 30, "now")
            duplicate = repo.place("b1", "u", 30, "now")
            insufficient = repo.place("b2", "u", 100, "later")
            self.assertEqual((first.status, duplicate.status, insufficient.status), ("applied", "duplicate", "stone_insufficient"))
