import tempfile
import unittest
from pathlib import Path

from ..stone_training_repository import StoneTrainingSqlRepository
from tests.test_db_backend import db_backend


class StoneTrainingRepositoryTests(unittest.TestCase):
    def test_exp_capped_is_rejected_without_write(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            with db_backend.transaction(game) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,stone INTEGER,power INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',100,100,1)")
            result = StoneTrainingSqlRepository(game, player).settle("s1", "u", requested_stone=100, expected_exp=100, expected_stone=100, exp_cap=100, power_multiplier=1.0)
            self.assertEqual(result.status, "exp_capped")
