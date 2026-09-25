import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_mixelixir_refine_claim
from ..refine_cost_repository import MixelixirRefineCostSqlRepository
from tests.test_db_backend import db_backend


class MixelixirRefineCostRepositoryTests(unittest.TestCase):
    def _database(self, temp: str) -> Path:
        db = Path(temp) / "game.db"
        with db_backend.transaction(db) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,mixelixir_num INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',1)")
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))"
            )
            conn.execute("INSERT INTO back VALUES('u',1,'草','药材',3,0)")
            conn.execute("INSERT INTO back VALUES('u',2,'炉','炼丹炉',1,0)")
        with DatabaseUnitOfWork(db) as uow:
            apply_mixelixir_refine_claim(uow)
        return db

    def _start(self, repo, operation_id="r1", materials=None):
        return repo.start(
            operation_id,
            "u",
            "custom",
            1,
            {"丹药控火": "1", "炼丹记录": {}, "炼丹经验": "10"},
            {"丹药控火": "1", "炼丹记录": {"20": {"name": "丹", "num": 1}}, "炼丹经验": "15"},
            99,
            recipe_key="custom:草2",
            materials=materials or {1: 2},
            furnace_id=2,
            reward_id=20,
            reward_name="丹",
            reward_quantity=1,
        )

    def test_operation_identity_persists_complete_claim_snapshot(self):
        with tempfile.TemporaryDirectory() as temp:
            db = self._database(temp)
            repo = MixelixirRefineCostSqlRepository(db)
            first = self._start(repo)
            duplicate = self._start(repo)
            self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
            with db_backend.transaction(db) as conn:
                self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE user_id='u' AND goods_id=1").fetchone()[0], 1)
                self.assertEqual(conn.execute("SELECT mixelixir_num FROM user_xiuxian WHERE user_id='u'").fetchone()[0], 2)
                task = conn.execute(
                    "SELECT recipe_key,reward_id,reward_name,reward_quantity,expected_mix_state,updated_mix_state "
                    "FROM mixelixir_refine_tasks WHERE task_id=?",
                    (first.task_id,),
                ).fetchone()
            self.assertEqual(task[:4], ("custom:草2", 20, "丹", 1))
            self.assertIn('"炼丹经验":"15"', task[5])

    def test_insufficient_material_does_not_create_task_or_advance_daily_count(self):
        with tempfile.TemporaryDirectory() as temp:
            db = self._database(temp)
            repo = MixelixirRefineCostSqlRepository(db)
            result = self._start(repo, materials={1: 4})
            self.assertEqual(result.status, "item_insufficient")
            with db_backend.transaction(db) as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM mixelixir_refine_tasks").fetchone()[0], 0)
                self.assertEqual(conn.execute("SELECT mixelixir_num FROM user_xiuxian WHERE user_id='u'").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
