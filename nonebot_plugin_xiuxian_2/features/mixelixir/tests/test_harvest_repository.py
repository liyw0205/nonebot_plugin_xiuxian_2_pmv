import tempfile
import unittest
from pathlib import Path

from ..harvest_repository import MixelixirHarvestSqlRepository
from tests.test_db_backend import db_backend


class MixelixirHarvestRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))")
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,收取时间 TEXT)")
            conn.execute("INSERT INTO mix_elixir_info VALUES('u','2026-01-01 00:00:00')")

    def tearDown(self):
        self.temp.cleanup()

    def test_applied_duplicate_and_state_changed(self):
        repo = MixelixirHarvestSqlRepository(self.game, self.player)
        rewards = ({"item_id": 1, "name": "灵草", "quantity": 2},)
        first = repo.harvest("h1", "u", "2026-01-01 00:00:00", "2026-01-02 00:00:00", rewards, max_goods_num=99)
        duplicate = repo.harvest("h1", "u", "bad", "2099", rewards, max_goods_num=99)
        stale = repo.harvest("h2", "u", "2026-01-01 00:00:00", "2099", rewards, max_goods_num=99)
        self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))

    def test_inventory_full_preserves_state(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("INSERT INTO back VALUES('u',1,'灵草','材料',99,'','',99)")
        repo = MixelixirHarvestSqlRepository(self.game, self.player)
        result = repo.harvest("h2", "u", "2026-01-01 00:00:00", "2026-01-02 00:00:00", ({"item_id": 1, "name": "灵草", "quantity": 2},), max_goods_num=99)
        self.assertEqual(result.status, "inventory_full")
