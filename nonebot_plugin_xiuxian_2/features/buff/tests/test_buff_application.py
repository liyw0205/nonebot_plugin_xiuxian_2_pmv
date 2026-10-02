import tempfile
import unittest
from pathlib import Path

from ....core.errors import ValidationError
from ..application import BuffApplication
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger, OutboxStore, ReconcileService
from ..migrations import apply_closing_settlement_game


class Repo:
    def invoke(self, action, *args, **kwargs): return {"status": "applied", "action": action}


class FailOnceEffects:
    def __init__(self):
        self.calls = []

    def on_closing_settled(self, *, payload, event_id):
        self.calls.append((event_id, dict(payload)))
        if len(self.calls) == 1:
            raise RuntimeError("interrupted after a partial projection")


class RecordingEffects:
    def __init__(self):
        self.calls = []

    def on_closing_settled(self, *, payload, event_id):
        self.calls.append((event_id, dict(payload)))


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
                OutboxStore().ensure_schema(uow)
                apply_closing_settlement_game(uow)
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
                exp_time=45,
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
                exp_time=45,
            )
            self.assertTrue(replay.ok)
            self.assertTrue(replay.replayed)

    def test_effect_outbox_retries_without_reapplying_assets(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                OperationLedger().ensure_schema(uow)
                OutboxStore().ensure_schema(uow)
                apply_closing_settlement_game(uow)
                uow.execute(
                    "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,stone INTEGER,hp INTEGER,mp INTEGER,atk INTEGER,power INTEGER)"
                )
                uow.execute(
                    "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)"
                )
                uow.execute("INSERT INTO user_xiuxian VALUES('u',100,50,1,2,3,4)")
                uow.execute("INSERT INTO user_cd VALUES('u',1,'start',NULL)")
            effects = FailOnceEffects()
            app = BuffApplication(database, Path(directory) / "player.db", closing_effects=effects)
            request = dict(
                operation_id="closing-recover",
                user_id="u",
                expected_create_time="start",
                exp_gain=20,
                stone_cost=10,
                new_hp=30,
                new_mp=40,
                new_atk=5,
                new_power=999,
                exp_time=45,
            )

            first = app.closing_settle(**request)
            self.assertTrue(first.ok)
            self.assertIn("补偿", first.message)
            replay = app.closing_settle(**request)

            self.assertTrue(replay.ok)
            self.assertTrue(replay.replayed)
            self.assertEqual(effects.calls[0], effects.calls[1])
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(120, uow.query_one("SELECT exp FROM user_xiuxian WHERE user_id='u'")["exp"])
                self.assertEqual(1, uow.query_one("SELECT COUNT(*) AS n FROM closing_settlement_operations")["n"])
                self.assertEqual("sent", uow.query_one("SELECT status FROM domain_outbox")["status"])

    def test_started_ledger_recovers_core_commit_from_outbox(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                OperationLedger().ensure_schema(uow)
                OutboxStore().ensure_schema(uow)
                apply_closing_settlement_game(uow)
                uow.execute(
                    "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,stone INTEGER,hp INTEGER,mp INTEGER,atk INTEGER,power INTEGER)"
                )
                uow.execute(
                    "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)"
                )
                uow.execute("INSERT INTO user_xiuxian VALUES('u',100,50,1,2,3,4)")
                uow.execute("INSERT INTO user_cd VALUES('u',1,'start',NULL)")
            effects = RecordingEffects()
            app = BuffApplication(database, Path(directory) / "player.db", closing_effects=effects)
            values = {
                "expected_create_time": "start",
                "exp_gain": 20,
                "stone_cost": 10,
                "new_hp": 30,
                "new_mp": 40,
                "new_atk": 5,
                "new_power": 999,
                "exp_time": 45,
            }
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                app.ledger.begin(
                    uow,
                    "closing-ledger-gap",
                    "buff.closing_settle",
                    {"user_id": "u", **values},
                )
            from ..closing_repository import ClosingSettlementSqlRepository

            ClosingSettlementSqlRepository(database).settle(
                "closing-ledger-gap", "u", **{
                    "expected_create_time": values["expected_create_time"],
                    "exp_gain": values["exp_gain"],
                    "stone_cost": values["stone_cost"],
                    "hp": values["new_hp"],
                    "mp": values["new_mp"],
                    "atk": values["new_atk"],
                    "power": values["new_power"],
                    "exp_time": values["exp_time"],
                }
            )

            recovered = app.closing_replay("closing-ledger-gap")

            self.assertTrue(recovered.ok)
            self.assertTrue(recovered.replayed)
            self.assertEqual(1, len(effects.calls))
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(120, uow.query_one("SELECT exp FROM user_xiuxian WHERE user_id='u'")["exp"])
                self.assertEqual("sent", uow.query_one("SELECT status FROM domain_outbox")["status"])

    def test_reconcile_handler_drains_pending_closing_effect_event(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                OperationLedger().ensure_schema(uow)
                OutboxStore().ensure_schema(uow)
                apply_closing_settlement_game(uow)
                uow.execute(
                    "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,stone INTEGER,hp INTEGER,mp INTEGER,atk INTEGER,power INTEGER)"
                )
                uow.execute(
                    "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)"
                )
                uow.execute("INSERT INTO user_xiuxian VALUES('u',100,50,1,2,3,4)")
                uow.execute("INSERT INTO user_cd VALUES('u',1,'start',NULL)")
            request = dict(
                operation_id="closing-cli-reconcile",
                user_id="u",
                expected_create_time="start",
                exp_gain=20,
                stone_cost=10,
                new_hp=30,
                new_mp=40,
                new_atk=5,
                new_power=999,
                exp_time=45,
            )
            self.assertTrue(BuffApplication(database, Path(directory) / "player.db").closing_settle(**request).ok)
            effects = RecordingEffects()
            configured = BuffApplication(
                database,
                Path(directory) / "player.db",
                closing_effects=effects,
            )

            with DatabaseUnitOfWork(database, immediate=True) as uow:
                report = ReconcileService().run(
                    uow,
                    handlers={"buff.closing.effects": configured.reconcile_outbox_event},
                )

            self.assertTrue(report.clean)
            self.assertEqual(1, len(effects.calls))


if __name__ == "__main__": unittest.main()
