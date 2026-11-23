import sqlite3
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

    def test_each_cost_write_point_rolls_back_and_recovers_after_fault_removal(self):
        write_points = {
            "material": (
                "UPDATE OF goods_num ON back WHEN OLD.goods_id=1",
                "UPDATE back SET goods_num=NEW.goods_num+1 WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id",
                "mixelixir_refine_cost_material",
                "item:1",
            ),
            "daily": (
                "UPDATE OF mixelixir_num ON user_xiuxian WHEN OLD.user_id='u'",
                "UPDATE user_xiuxian SET mixelixir_num=NEW.mixelixir_num+1 WHERE user_id=NEW.user_id",
                "mixelixir_refine_cost_daily",
                "daily_count",
            ),
            "task": (
                "INSERT ON mixelixir_refine_tasks WHEN NEW.task_id='r1:task'",
                "UPDATE mixelixir_refine_tasks SET recipe_key=NEW.recipe_key||':changed' WHERE task_id=NEW.task_id",
                "mixelixir_refine_cost_task",
                "task",
            ),
            "receipt": (
                "INSERT ON mixelixir_refine_cost_operations WHEN NEW.operation_id='r1'",
                "UPDATE mixelixir_refine_cost_operations SET task_id=NEW.task_id||':changed' WHERE operation_id=NEW.operation_id",
                "mixelixir_refine_cost_receipt",
                "receipt",
            ),
        }
        for point, (event, rewrite, reason_prefix, subject) in write_points.items():
            for fault in ("ignore", "rewrite", "abort"):
                with self.subTest(point=point, fault=fault), tempfile.TemporaryDirectory() as temp:
                    db = self._database(temp)
                    trigger = f"inject_{point}_{fault}"
                    timing = "AFTER" if fault == "rewrite" else "BEFORE"
                    body = rewrite if fault == "rewrite" else (
                        "SELECT RAISE(IGNORE)" if fault == "ignore"
                        else "SELECT RAISE(ABORT,'isolated cost write failure')"
                    )
                    with DatabaseUnitOfWork(db) as uow:
                        uow.execute(f"CREATE TRIGGER {trigger} {timing} {event} BEGIN {body}; END")
                    repo = MixelixirRefineCostSqlRepository(db)
                    if fault == "abort":
                        with self.assertRaises(sqlite3.IntegrityError):
                            self._start(repo)
                    else:
                        result = self._start(repo)
                        self.assertEqual(result.status, "write_rejected")
                        self.assertEqual(result.reason, f"{reason_prefix}_{'projection_mismatch' if fault == 'rewrite' else 'write_rejected'}")
                        self.assertEqual(result.subject, subject)
                    with db_backend.transaction(db) as conn:
                        self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE user_id='u' AND goods_id=1").fetchone()[0], 3)
                        self.assertEqual(conn.execute("SELECT mixelixir_num FROM user_xiuxian WHERE user_id='u'").fetchone()[0], 1)
                        self.assertEqual(conn.execute("SELECT COUNT(*) FROM mixelixir_refine_tasks").fetchone()[0], 0)
                        self.assertEqual(conn.execute("SELECT COUNT(*) FROM mixelixir_refine_cost_operations").fetchone()[0], 0)
                        self.assertEqual(conn.execute("PRAGMA integrity_check").fetchone()[0], "ok")
                    with DatabaseUnitOfWork(db) as uow:
                        uow.execute(f"DROP TRIGGER {trigger}")
                    recovered = self._start(repo)
                    self.assertEqual(recovered.status, "applied")


if __name__ == "__main__":
    unittest.main()
