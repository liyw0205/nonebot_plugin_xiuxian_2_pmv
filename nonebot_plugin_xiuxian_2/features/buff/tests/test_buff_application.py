import tempfile
import unittest
from pathlib import Path

from ....core.errors import ValidationError
from ..application import BuffApplication
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger


class Repo:
    def invoke(self, action, *args, **kwargs): return {"status": "applied", "action": action}


class BuffApplicationTest(unittest.TestCase):
    def test_pvp_settlement_requires_opponent(self):
        app = BuffApplication("game.db", "player.db")
        with self.assertRaisesRegex(ValidationError, "opponent_id is required"):
            app.pvp_settle(operation_id="pvp-1", user_id="u")

    def test_operation_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                OperationLedger().ensure_schema(uow)
            app = BuffApplication(database, Path(directory) / "player.db", repository=Repo())
            self.assertTrue(app.open(operation_id="buff-1", user_id="u").ok)
            self.assertTrue(app.open(operation_id="buff-1", user_id="u").replayed)

    def test_closing_settlement_returns_operation_outcome(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                OperationLedger().ensure_schema(uow)
                uow.execute(
                    "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,stone INTEGER,hp INTEGER,mp INTEGER,atk INTEGER,power INTEGER)"
                )
                uow.execute(
                    "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)"
                )
                uow.execute(
                    "INSERT INTO user_xiuxian VALUES(?,?,?,?,?,?,?)",
                    ("u", 100, 50, 1, 2, 3, 4),
                )
                uow.execute(
                    "INSERT INTO user_cd VALUES(?,?,?,?)", ("u", 1, "start", None)
                )

            app = BuffApplication(database, Path(directory) / "player.db")
            result = app.closing_settle(
                operation_id="closing-1",
                user_id="u",
                expected_create_time="start",
                exp_gain=20,
                stone_cost=10,
                new_hp=30,
                new_mp=40,
                new_atk=5,
                new_power=999,
            )

            self.assertTrue(result.ok)
            self.assertEqual("applied", result.data["status"])
            replay = app.closing_settle(
                operation_id="closing-1",
                user_id="u",
                expected_create_time="start",
                exp_gain=20,
                stone_cost=10,
                new_hp=30,
                new_mp=40,
                new_atk=5,
                new_power=999,
            )
            self.assertTrue(replay.ok)
            self.assertTrue(replay.replayed)


if __name__ == "__main__": unittest.main()
