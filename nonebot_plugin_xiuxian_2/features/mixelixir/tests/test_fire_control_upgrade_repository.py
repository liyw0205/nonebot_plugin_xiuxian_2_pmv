import tempfile
import unittest
from pathlib import Path

from ..fire_control_upgrade_repository import MixelixirFireControlUpgradeSqlRepository
from tests.test_db_backend import db_backend


class MixelixirFireControlUpgradeRepositoryTests(unittest.TestCase):
    def test_applied_duplicate_and_state_changed(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            with db_backend.transaction(game) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',100)")
            with db_backend.transaction(player) as conn:
                conn.execute("CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,控火等级 TEXT,炼丹经验 TEXT)")
                conn.execute("INSERT INTO mix_elixir_info VALUES('u','1','100')")
            repo = MixelixirFireControlUpgradeSqlRepository(game, player)
            first = repo.upgrade("u1", "u", 1, 100, 100, 2, 50)
            duplicate = repo.upgrade("u1", "u", 1, 100, 100, 2, 50)
            stale = repo.upgrade("u2", "u", 1, 100, 100, 2, 50)
            self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))
