import tempfile
import unittest
from pathlib import Path

from ..upgrade_repository import BlessedSpotUpgradeSqlRepository
from tests.test_db_backend import db_backend


class BlessedSpotUpgradeRepositoryTests(unittest.TestCase):
    def test_applied_duplicate_and_state_changed(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            with db_backend.transaction(game) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER,blessed_spot_flag INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',500,1)")
            with db_backend.transaction(player) as conn:
                conn.execute("CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,灵田数量 TEXT)")
                conn.execute("INSERT INTO mix_elixir_info VALUES('u','1')")
            repository = BlessedSpotUpgradeSqlRepository(game, player)
            first = repository.upgrade("u1", "u", 1, 200, 10)
            duplicate = repository.upgrade("u1", "u", 1, 200, 10)
            stale = repository.upgrade("u2", "u", 1, 200, 10)
            self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))
