import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ..harvest_repository import PuppetHarvestSqlRepository
from tests.test_db_backend import db_backend


class PuppetHarvestRepositoryTests(unittest.TestCase):
    def test_applied_duplicate_and_state_changed(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            with db_backend.transaction(game) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',10000000)")
                conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))")
            with db_backend.transaction(player) as conn:
                conn.execute("CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,收取时间 TEXT,灵田傀儡 INTEGER,傀儡等级 INTEGER)")
                conn.execute("INSERT INTO mix_elixir_info VALUES('u','2026-01-01 00:00:00',1,1)")
            repo = PuppetHarvestSqlRepository(game, player, max_goods_num=99)
            rewards = lambda level, quantity: [(3001, "恒心草", "药材", quantity)]
            first = repo.harvest("u", operation_id="h1", now=datetime(2026, 1, 2, 0, 0), time_cost_hours=23, speed_base=0.05, harvest_costs={1: 500}, harvest_bonus=0, reward_factory=rewards)
            duplicate = repo.harvest("u", operation_id="h1", now=datetime(2026, 1, 3, 0, 0), time_cost_hours=23, speed_base=0.05, harvest_costs={1: 500}, harvest_bonus=0, reward_factory=rewards)
            self.assertEqual((first.status, duplicate.status), ("harvested", "duplicate"))
