import json
import tempfile
import unittest
from pathlib import Path

from ..refine_cost_repository import MixelixirRefineCostSqlRepository
from tests.test_db_backend import db_backend


class MixelixirRefineCostRepositoryTests(unittest.TestCase):
    def test_operation_identity_and_missing_material(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,mixelixir_num INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',1)")
                conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))")
                conn.execute("INSERT INTO back VALUES('u',1,'草','药材',3,0)")
                conn.execute("CREATE TABLE mixelixir_recipe_sets(user_id TEXT PRIMARY KEY,recipe_set_id TEXT UNIQUE,daily_count INTEGER,materials_json TEXT,furnaces_json TEXT,recipes_json TEXT)")
                conn.execute("INSERT INTO mixelixir_recipe_sets VALUES('u','set',1,?, ?, ?)", (json.dumps([[1,'草',3]]), json.dumps([]), json.dumps({"recipe":"x"})))
            repo = MixelixirRefineCostSqlRepository(db)
            first = repo.start("r1", "u", "set", 1, {"materials": [[1, "草", 3]], "furnaces": [], "recipes": {"recipe":"x"}}, {}, 100)
            duplicate = repo.start("r1", "u", "set", 1, {"materials": [[1, "草", 3]], "furnaces": [], "recipes": {"recipe":"x"}}, {}, 100)
            self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
