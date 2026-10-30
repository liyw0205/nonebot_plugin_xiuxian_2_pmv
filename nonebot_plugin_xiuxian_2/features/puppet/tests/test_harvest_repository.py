import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ..harvest_repository import HarvestReward, PuppetHarvestSqlRepository
from tests.test_db_backend import db_backend


class PuppetHarvestRepositoryTests(unittest.TestCase):
    """This fixture must mirror the real player schema: 灵田傀儡 IS the level."""

    def build(self, temp: str, *, puppet_level: int = 1, stone: int = 10_000_000, last: str = "2026-01-01 00:00:00"):
        game = Path(temp) / "game.db"
        player = Path(temp) / "player.db"
        with db_backend.transaction(game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level TEXT,stone INTEGER,"
                "blessed_spot_flag INTEGER,puppet_status INTEGER)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES('u','化神境初期',%s,1,1)", (stone,))
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))"
            )
        with db_backend.transaction(player) as conn:
            conn.execute(
                'CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,"收取时间" TEXT,'
                '"收取等级" INTEGER,"灵田数量" INTEGER,"药材速度" INTEGER,"灵田傀儡" INTEGER)'
            )
            conn.execute(
                'INSERT INTO mix_elixir_info VALUES(\'u\',%s,0,1,0,%s)', (last, puppet_level)
            )
        return game, player

    @staticmethod
    def factory(level, quantity):
        return [(3001, "恒心草", "药材", quantity)]

    def harvest(self, repo, **kwargs):
        params = {
            "operation_id": "h1",
            "now": datetime(2026, 1, 2, 0, 0),
            "time_cost_hours": 23,
            "speed_base": 0.05,
            "harvest_costs": {1: 500},
            "harvest_bonus": 0,
            "reward_factory": self.factory,
        }
        params.update(kwargs)
        return repo.harvest("u", **params)

    def test_applied_duplicate_and_state_changed(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            repo = PuppetHarvestSqlRepository(game, player, max_goods_num=99)
            first = self.harvest(repo)
            duplicate = self.harvest(repo, now=datetime(2026, 1, 3, 0, 0))
            self.assertEqual((first.status, duplicate.status), ("harvested", "duplicate"))
            self.assertEqual(duplicate.stone_cost, 500)
            self.assertEqual([reward.quantity for reward in duplicate.rewards], [1])

    def test_result_rewards_are_frozen_dataclasses(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            repo = PuppetHarvestSqlRepository(game, player, max_goods_num=99)
            result = self.harvest(repo)
            self.assertTrue(result.harvested)
            self.assertIsInstance(result.rewards[0], HarvestReward)
            self.assertEqual(
                (
                    result.rewards[0].goods_id,
                    result.rewards[0].goods_name,
                    result.rewards[0].goods_type,
                    result.rewards[0].quantity,
                ),
                (3001, "恒心草", "药材", 1),
            )


if __name__ == "__main__":
    unittest.main()
