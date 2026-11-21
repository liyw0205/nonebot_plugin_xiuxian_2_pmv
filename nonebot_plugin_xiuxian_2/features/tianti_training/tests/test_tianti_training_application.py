from __future__ import annotations

import tempfile
import json
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from ....core.errors import DomainError, OperationConflictError
from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema
from ..application import TiantiTrainingApplication
from ..repository import TiantiMedicineBathSqlRepository


class _Repository:
    def __init__(self):
        self.calls = []

    def train(self, operation_id, user_id, requested_stone):
        self.calls.append(("train", operation_id))
        return {"status": "trained", "user_id": user_id, "stone_cost": requested_stone, "hp_gain": requested_stone // 10, "new_hp": 20}

    def apply_bath(self, operation_id, user_id, consume_plan, effect, slot_name, started_at, duration_minutes, *, sect_fairyland_level=0):
        self.calls.append(("bath", operation_id))
        return {"status": "applied", "user_id": user_id, "consumed": list(consume_plan), "effect": effect, "bath_name": slot_name, "end_time": "2026-09-12 16:00:00", "settlement": {"status": "ok"}}

    def breakthrough(self, operation_id, user_id, *, cultivation_rank, roll_success):
        self.calls.append(("breakthrough", operation_id))
        return {"status": "completed", "user_id": user_id, "old_level": "初境", "new_level": "次境" if roll_success else "初境", "hp_cost": 10, "new_hp": 90, "success": roll_success}

    def open_qiaoxue(self, operation_id, user_id, roll):
        self.calls.append(("qiaoxue", operation_id))
        return {"status": "opened", "user_id": user_id, "qiaoxue": {"name": "窍一", "effect_type": "hp_gain_pct", "effect_value": 0.01}, "hp_cost": 10, "new_hp": 90, "opened_count": 1, "unlock_limit": 3}


class _InitializingRepository(_Repository):
    def __init__(self):
        super().__init__()
        self.order = []

    def ensure_profile(self, user_id):
        self.order.append(("ensure", user_id))

    def breakthrough(self, operation_id, user_id, *, cultivation_rank, roll_success):
        self.order.append(("breakthrough", user_id))
        return {"status": "hp_insufficient", "user_id": user_id}


class TiantiTrainingApplicationTests(unittest.TestCase):
    @staticmethod
    def _application(root, repository):
        game_database = root / "game.db"
        player_database = root / "player.db"
        for database in (game_database, player_database):
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                apply_platform_schema(uow)
        return TiantiTrainingApplication(game_database, player_database, repository=repository)

    def test_all_actions_are_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repository = _Repository()
            app = self._application(root, repository)
            first = app.train(operation_id="train-1", user_id="u", requested_stone=100)
            replay = app.train(operation_id="train-1", user_id="u", requested_stone=100)
            self.assertTrue(first.ok)
            self.assertTrue(replay.replayed)
            self.assertEqual(repository.calls, [("train", "train-1")])

            bath = app.apply_bath(
                operation_id="bath-1", user_id="u", consume_plan=({"item_id": 1, "name": "药材", "amount": 2},),
                effect=1.5, slot_name="午酉血火", started_at=datetime(2026, 9, 12, 10), duration_minutes=360,
            )
            breakthrough = app.breakthrough(operation_id="break-1", user_id="u", cultivation_rank=1, roll_success=True)
            qiaoxue = app.open_qiaoxue(operation_id="q-1", user_id="u", roll=0)
            self.assertTrue(all(item.ok for item in (bath, breakthrough, qiaoxue)))

    def test_rejection_is_recorded_without_calling_again_on_replay(self):
        class Rejecting(_Repository):
            def train(self, operation_id, user_id, requested_stone):
                self.calls.append(("train", operation_id))
                return {"status": "stone_insufficient", "user_id": user_id}

        with tempfile.TemporaryDirectory() as directory:
            repository = Rejecting()
            app = self._application(Path(directory), repository)
            first = app.train(operation_id="reject-1", user_id="u", requested_stone=100)
            second = app.train(operation_id="reject-1", user_id="u", requested_stone=100)
            self.assertFalse(first.ok)
            self.assertEqual(second.code, "stone_insufficient")
            self.assertEqual(repository.calls, [("train", "reject-1")])

    def test_changed_request_conflict_does_not_record_failure_or_replace_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repository = _Repository()
            app = self._application(root, repository)
            first = app.train(
                operation_id="train-conflict", user_id="u", requested_stone=100
            )
            self.assertTrue(first.ok)

            with patch.object(
                app.ledger, "record_failure", wraps=app.ledger.record_failure
            ) as record_failure:
                with self.assertRaises(OperationConflictError):
                    app.train(operation_id="train-conflict", user_id="u", requested_stone=101)

            record_failure.assert_not_called()
            self.assertEqual(repository.calls, [("train", "train-conflict")])
            with DatabaseUnitOfWork(root / "game.db", read_only=True) as uow:
                ledger = uow.query_one(
                    "SELECT status,result_json FROM operation_ledger "
                    "WHERE operation_id='train-conflict' AND action='tianti.train'"
                )
            self.assertEqual(ledger["status"], "applied")

    def test_breakthrough_initializes_profile_before_starting_operation(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _InitializingRepository()
            app = self._application(Path(directory), repository)
            result = app.breakthrough(
                operation_id="break-init",
                user_id="u",
                cultivation_rank=1,
                roll_success=True,
            )
            self.assertFalse(result.ok)
            self.assertEqual(repository.order, [("ensure", "u"), ("breakthrough", "u")])

    def test_bath_domain_rejection_is_retryable_and_replay_is_zero_write(self):
        class Profile:
            @staticmethod
            def default_data():
                return {
                    "tianti_level": "one",
                    "tianti_hp": 10,
                    "last_settle_time": "2026-09-14 09:00:00",
                    "medicine_last_time": None,
                    "medicine_end_time": None,
                    "medicine_effect": 0.0,
                    "medicine_name": "",
                    "opened_qiaoxue": [],
                    "opened_qiaoxue_detail": [],
                    "qiaoxue_stage_opened": {},
                }

            @classmethod
            def clean(cls, row):
                data = cls.default_data()
                for key, value in row.items():
                    if value is not None:
                        data[key] = value
                for key in ("opened_qiaoxue", "opened_qiaoxue_detail", "qiaoxue_stage_opened"):
                    if isinstance(data[key], str):
                        import json

                        data[key] = json.loads(data[key])
                data["tianti_hp"] = int(data["tianti_hp"])
                data["medicine_effect"] = float(data["medicine_effect"] or 0)
                return data

            @staticmethod
            def levels():
                return {"one": {"rank": 1, "hp_gain_per_min": 2, "need_hp": 0}}

            @staticmethod
            def cap(_data):
                return 1000

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            game, player = root / "game.db", root / "player.db"
            app = self._application(root, repository=None)
            with DatabaseUnitOfWork(game) as uow:
                uow.execute(
                    "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,"
                    "bind_num INTEGER,UNIQUE(user_id,goods_id))"
                )
                uow.execute("INSERT INTO back VALUES('u',1,5,5)")
                uow.execute(
                    "CREATE TABLE tianti_medicine_bath_operations("
                    "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,"
                    "request_json TEXT NOT NULL,result_json TEXT NOT NULL)"
                )
                uow.execute(
                    "CREATE TRIGGER ignore_bath_operation BEFORE INSERT "
                    "ON tianti_medicine_bath_operations BEGIN SELECT RAISE(IGNORE); END"
                )
            with DatabaseUnitOfWork(player) as uow:
                fields = tuple(Profile.default_data())
                columns = ("user_id", *fields)
                placeholders = ",".join("?" for _ in columns)
                defaults = Profile.default_data()
                for key in ("opened_qiaoxue", "opened_qiaoxue_detail", "qiaoxue_stage_opened"):
                    import json

                    defaults[key] = json.dumps(defaults[key])
                uow.execute(
                    "CREATE TABLE tianti_info(user_id TEXT PRIMARY KEY,tianti_level TEXT,"
                    "tianti_hp TEXT,last_settle_time TEXT,medicine_last_time TEXT,"
                    "medicine_end_time TEXT,medicine_effect TEXT,medicine_name TEXT,"
                    "opened_qiaoxue TEXT,opened_qiaoxue_detail TEXT,qiaoxue_stage_opened TEXT)"
                )
                uow.execute(
                    f"INSERT INTO tianti_info({','.join(columns)}) VALUES({placeholders})",
                    ("u", *(defaults[field] for field in fields)),
                )
            repo = TiantiMedicineBathSqlRepository(game, player, profile_reader=Profile())
            app = TiantiTrainingApplication(game, player, repository=repo)
            request = dict(
                operation_id="bath-recover",
                user_id="u",
                consume_plan=({"item_id": 1, "name": "药材甲", "amount": 2},),
                effect=1.5,
                slot_name="午酉血火",
                started_at=datetime(2026, 9, 14, 10, 0),
                duration_minutes=60,
            )

            with self.assertRaises(DomainError) as error:
                app.apply_bath(**request)
            self.assertEqual(error.exception.code, "tianti_bath_receipt_write_rejected")
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                self.assertEqual(tuple(uow.query_one("SELECT goods_num,bind_num FROM back WHERE user_id='u'").values()), (5, 5))
                ledger = uow.query_one(
                    "SELECT status,result_json FROM operation_ledger "
                    "WHERE operation_id='bath-recover'"
                )
                self.assertEqual(ledger["status"], "failed")
                failure = json.loads(ledger["result_json"])
                self.assertEqual(failure["code"], "tianti_bath_receipt_write_rejected")
                self.assertIn("tianti_bath_receipt_write_rejected", failure["message"])
                self.assertEqual(uow.query_one("SELECT COUNT(*) AS n FROM tianti_medicine_bath_operations")["n"], 0)
            with DatabaseUnitOfWork(player, read_only=True) as uow:
                profile = uow.query_one("SELECT medicine_effect,medicine_end_time FROM tianti_info WHERE user_id='u'")
                self.assertEqual(tuple(profile.values()), ("0.0", None))

            with DatabaseUnitOfWork(game) as uow:
                uow.execute("DROP TRIGGER ignore_bath_operation")
            recovered = app.apply_bath(**request)
            self.assertTrue(recovered.ok)
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                self.assertEqual(tuple(uow.query_one("SELECT goods_num,bind_num FROM back WHERE user_id='u'").values()), (3, 3))
                self.assertEqual(uow.query_one("SELECT status FROM operation_ledger WHERE operation_id='bath-recover'")["status"], "applied")
            replay = app.apply_bath(**request)
            self.assertTrue(replay.replayed)
            self.assertEqual(
                replay.data,
                json.loads(json.dumps(recovered.data, ensure_ascii=False, default=str)),
            )
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                self.assertEqual(tuple(uow.query_one("SELECT goods_num,bind_num FROM back WHERE user_id='u'").values()), (3, 3))
                self.assertEqual(uow.query_one("SELECT COUNT(*) AS n FROM tianti_medicine_bath_operations")["n"], 1)


if __name__ == "__main__":
    unittest.main()
