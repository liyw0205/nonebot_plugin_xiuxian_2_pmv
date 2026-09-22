import json
import tempfile
import unittest
from pathlib import Path

from ..refine_reward_repository import MixelixirRefineRewardSqlRepository
from tests.test_db_backend import db_backend


class MixelixirRefineRewardRepositoryTests(unittest.TestCase):
    def test_missing_task_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            with db_backend.transaction(game) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u')")
            with db_backend.transaction(player) as conn:
                conn.execute("CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,丹药控火 TEXT,炼丹记录 TEXT,炼丹经验 TEXT)")
                conn.execute("INSERT INTO mix_elixir_info VALUES('u','1','{}','10')")
            result = MixelixirRefineRewardSqlRepository(game, player).claim("r1", "u", "missing", 99)
            self.assertEqual(result.status, "task_missing")
