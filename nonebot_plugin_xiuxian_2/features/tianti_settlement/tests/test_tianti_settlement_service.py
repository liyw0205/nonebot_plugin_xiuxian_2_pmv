from __future__ import annotations

import tempfile
import unittest
import sqlite3
from datetime import datetime
from pathlib import Path

from ....core.errors import OperationConflictError
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ....plugin import apply_platform_schema
from ..application import TiantiSettlementApplication
from ..repository import TiantiSettlementSqlRepository
from ..migrations import apply_tianti_settlement_operations


class _Repository:
    def __init__(self, status="settled"):
        self.status = status
        self.calls = 0

    def settle(self, operation_id, user_id, settled_at, *, sect_fairyland_level=0):
        self.calls += 1
        return {"status": self.status, "detail": {"real_gain": 10, "new_hp": 20}}


class _FlakyRepository(_Repository):
    def __init__(self):
        super().__init__()
        self.failed = True

    def settle(self, operation_id, user_id, settled_at, *, sect_fairyland_level=0):
        self.calls += 1
        if self.failed:
            self.failed = False
            raise RuntimeError("temporary player database failure")
        return {"status": "settled", "detail": {"real_gain": 5, "new_hp": 15}}


class _Profile:
    def default_data(self):
        return {
            "tianti_level": "one", "tianti_hp": 10, "last_settle_time": None,
            "medicine_last_time": None, "medicine_end_time": None,
            "medicine_effect": 0.0, "medicine_name": "", "opened_qiaoxue": [],
            "opened_qiaoxue_detail": [], "qiaoxue_stage_opened": {},
        }

    def levels(self):
        return {"one": {"rank": 1, "hp_gain_per_min": 2, "need_hp": 0}}

    def clean(self, row):
        data = self.default_data()
        data.update({key: value for key, value in (row or {}).items() if value is not None})
        return data

    def cap(self, data):
        return 1000


class _FailOnceFinishLedger(OperationLedger):
    def __init__(self):
        super().__init__()
        self.fail_once = True

    def finish(self, uow, outcome):
        super().finish(uow, outcome)
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("simulated interruption before commit")


class TiantiSettlementApplicationTests(unittest.TestCase):
    def _setup_player_db(self, database):
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            apply_platform_schema(uow)
            apply_tianti_settlement_operations(uow)
        with sqlite3.connect(database) as conn:
            conn.execute(
                "CREATE TABLE tianti_info ("
                "user_id TEXT PRIMARY KEY, tianti_level TEXT, tianti_hp TEXT, last_settle_time TEXT, "
                "medicine_last_time TEXT, medicine_end_time TEXT, medicine_effect TEXT, medicine_name TEXT, "
                "opened_qiaoxue TEXT, opened_qiaoxue_detail TEXT, qiaoxue_stage_opened TEXT)"
            )
            conn.execute(
                "INSERT INTO tianti_info VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("u", "one", "10", "2026-09-12 09:00:00", None, None, "0", "", "[]", "[]", "{}"),
            )

    def test_replay_is_idempotent_and_audited(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            self._setup_player_db(database)
            repository = _Repository()
            app = TiantiSettlementApplication(database, repository=repository)
            kwargs = {
                "operation_id": "tianti-1", "user_id": "u",
                "settled_at": datetime(2026, 9, 12, 10, 0), "sect_fairyland_level": 1,
            }
            first = app.settle(**kwargs)
            second = app.settle(**kwargs)
            self.assertTrue(first.ok)
            self.assertTrue(second.replayed)
            self.assertEqual(repository.calls, 1)

    def test_rejection_is_stable(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            self._setup_player_db(database)
            repository = _Repository("state_changed")
            app = TiantiSettlementApplication(database, repository=repository)
            kwargs = {"operation_id": "tianti-2", "user_id": "u", "settled_at": datetime(2026, 9, 12, 10, 0)}
            first = app.settle(**kwargs)
            second = app.settle(**kwargs)
            self.assertFalse(first.ok)
            self.assertEqual(first.code, "state_changed")
            self.assertEqual(second.code, "state_changed")

    def test_operation_payload_conflict_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            self._setup_player_db(database)
            app = TiantiSettlementApplication(database, repository=_Repository())
            app.settle(operation_id="tianti-3", user_id="u", settled_at=datetime(2026, 9, 12, 10, 0))
            with self.assertRaises(OperationConflictError):
                app.settle(operation_id="tianti-3", user_id="other", settled_at=datetime(2026, 9, 12, 10, 0))

    def test_repository_failure_is_recorded_and_retryable(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            self._setup_player_db(database)
            repository = _FlakyRepository()
            app = TiantiSettlementApplication(database, repository=repository)
            kwargs = {"operation_id": "tianti-4", "user_id": "u", "settled_at": datetime(2026, 9, 12, 10, 0)}
            with self.assertRaisesRegex(RuntimeError, "temporary player database failure"):
                app.settle(**kwargs)
            retry = app.settle(**kwargs)
            self.assertTrue(retry.ok)
            self.assertEqual(repository.calls, 2)

    def test_profile_and_operation_ledger_rollback_together(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            self._setup_player_db(database)
            repository = TiantiSettlementSqlRepository(
                database,
                profile_reader=_Profile(),
                spirit_vein_multiplier=lambda: 1.5,
            )
            app = TiantiSettlementApplication(
                database,
                repository=repository,
                ledger=_FailOnceFinishLedger(),
            )
            kwargs = {
                "operation_id": "atomic-settle",
                "user_id": "u",
                "settled_at": datetime(2026, 9, 12, 10, 0),
            }
            with self.assertRaisesRegex(RuntimeError, "simulated interruption"):
                app.settle(**kwargs)
            with sqlite3.connect(database) as conn:
                self.assertEqual(conn.execute("SELECT tianti_hp FROM tianti_info WHERE user_id='u'").fetchone()[0], "10")
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM tianti_settlement_operations").fetchone()[0], 0)
                self.assertEqual(conn.execute("SELECT status FROM operation_ledger WHERE operation_id='atomic-settle'").fetchone()[0], "failed")
            result = app.settle(**kwargs)
            self.assertTrue(result.ok)
            self.assertEqual(result.data["detail"]["real_gain"], 180)
            with sqlite3.connect(database) as conn:
                self.assertEqual(conn.execute("SELECT tianti_hp FROM tianti_info WHERE user_id='u'").fetchone()[0], "190")
                self.assertEqual(conn.execute("SELECT status FROM operation_ledger WHERE operation_id='atomic-settle'").fetchone()[0], "applied")

    def test_started_ledger_from_previous_split_transaction_is_recoverable(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            self._setup_player_db(database)
            settled_at = datetime(2026, 9, 12, 10, 0)
            payload = {
                "user_id": "u",
                "settled_at": settled_at.isoformat(),
                "sect_fairyland_level": 0,
            }
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                OperationLedger().begin(uow, "stale-started", "tianti.settle", payload)
            repository = TiantiSettlementSqlRepository(database, profile_reader=_Profile())
            result = TiantiSettlementApplication(database, repository=repository).settle(
                operation_id="stale-started",
                user_id="u",
                settled_at=settled_at,
            )
            self.assertTrue(result.ok)
            self.assertEqual(result.data["detail"]["real_gain"], 120)


if __name__ == "__main__":
    unittest.main()
