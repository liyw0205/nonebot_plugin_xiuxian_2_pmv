from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ..application import SectApplication


def _prepare_ledger(database: Path) -> None:
    with DatabaseUnitOfWork(database) as uow:
        OperationLedger().ensure_schema(uow)


class _Repository:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def join(self, *args, **kwargs):
        self.calls.append("join")
        return {"status": "joined", "user_id": "u", "sect_id": 1, "member_count": 2, "member_limit": 20}

    def purchase(self, *args, **kwargs):
        self.calls.append("purchase")
        return {"status": "applied", "quantity": 1, "cost": 10}

    def learn_main(self, *args, **kwargs):
        self.calls.append("main")
        return {"status": "learned", "materials_cost": 10, "materials_left": 90}

    def learn_secondary(self, *args, **kwargs):
        self.calls.append("secondary")
        return {"status": "learned", "materials_cost": 10, "materials_left": 80}

    def claim_elixir(self, *args, **kwargs):
        self.calls.append("elixir")
        return {"status": "applied", "rewards": []}

    def claim(self, *args, **kwargs):
        self.calls.append("weekly")
        return {"status": "applied", "rewards": [("目标一", "灵石5")]}


class _MainBuffReceiptRejectRepository(_Repository):
    def learn_main(self, *args, **kwargs):
        self.calls.append("main")
        return {
            "status": "state_changed",
            "reason": "sect_mainbuff_receipt_write_rejected",
            "subject": "op-main-reject",
            "materials_cost": 10,
        }


class _SecBuffReceiptRejectRepository(_Repository):
    def learn_secondary(self, *args, **kwargs):
        self.calls.append("secondary")
        return {
            "status": "state_changed",
            "reason": "sect_secbuff_receipt_write_rejected",
            "subject": "op-sec-reject",
            "materials_cost": 10,
        }


class _PositionRepository(_Repository):
    def __init__(self, *, reject_once: bool = False) -> None:
        super().__init__()
        self.reject_once = reject_once

    def change_position(self, operation_id, actor_id, target_id, requested_position, position_limits, *, manager_max_position):
        self.calls.append((operation_id, requested_position))
        if self.reject_once:
            self.reject_once = False
            return {
                "status": "state_changed",
                "reason": "sect_position_receipt_write_rejected",
                "subject": operation_id,
                "actor_id": actor_id,
                "target_id": target_id,
            }
        return {
            "status": "changed",
            "actor_id": actor_id,
            "target_id": target_id,
            "sect_id": 1,
            "actor_name": "长老",
            "target_name": "成员",
            "old_position": 5,
            "new_position": requested_position,
        }


class SectApplicationTests(unittest.TestCase):
    def test_started_ledger_is_retried_by_frozen_mutations(self):
        cases = (
            (
                "sect-join-started",
                "sect.join",
                {"user_id": "u", "sect_id": 1, "member_position": 12},
                lambda app: app.join(
                    operation_id="sect-join-started", user_id="u", sect_id=1
                ),
                "join",
            ),
            (
                "sect-main-started",
                "sect.learn_main",
                {
                    "user_id": "u",
                    "sect_id": 1,
                    "buff_id": 2,
                    "materials_cost": 10,
                    "expected_catalog": "[]",
                },
                lambda app: app.learn_main(
                    operation_id="sect-main-started",
                    user_id="u",
                    sect_id=1,
                    buff_id=2,
                    materials_cost=10,
                    expected_catalog="[]",
                ),
                "main",
            ),
        )
        for operation_id, action, payload, invoke, expected_call in cases:
            with self.subTest(action=action):
                with tempfile.TemporaryDirectory() as directory:
                    repository = _Repository()
                    database = Path(directory) / "game.db"
                    _prepare_ledger(database)
                    ledger = OperationLedger()
                    with DatabaseUnitOfWork(database, immediate=True) as uow:
                        ledger.begin(uow, operation_id, action, payload)
                    app = SectApplication(database, repository=repository)
                    result = invoke(app)
                    self.assertTrue(result.ok)
                    self.assertEqual(repository.calls, [expected_call])

    def test_join_and_purchase_are_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            database = Path(directory) / "game.db"
            _prepare_ledger(database)
            app = SectApplication(database, repository=repository)
            first = app.join(operation_id="sect-join-1", user_id="u", sect_id=1)
            replay = app.join(operation_id="sect-join-1", user_id="u", sect_id=1)
            purchase = app.purchase(operation_id="sect-buy-1", user_id="u", sect_id=1, item_id=2, item_name="丹", item_type="丹药", quantity=1, unit_cost=10, weekly_limit=1, legacy_purchased=0, max_goods_num=99)
            self.assertTrue(first.ok and replay.replayed and purchase.ok)
            self.assertEqual(repository.calls, ["join", "purchase"])

    def test_learning_and_claim_use_operation_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            database = Path(directory) / "game.db"
            _prepare_ledger(database)
            app = SectApplication(database, repository=repository)
            self.assertTrue(app.learn_main(operation_id="sect-main-1", user_id="u", sect_id=1, buff_id=2, materials_cost=10, expected_catalog="[]").ok)
            self.assertTrue(app.learn_secondary(operation_id="sect-sec-1", user_id="u", sect_id=1, buff_id=3, materials_cost=10, expected_catalog="[]").ok)
            self.assertTrue(app.claim_elixir(operation_id="sect-elixir-1", user_id="u", sect_id=1, contribution_required=1, materials_required=1, rewards=[], max_goods_num=99).ok)
            self.assertEqual(repository.calls, ["main", "secondary", "elixir"])

    def test_mainbuff_write_rejection_keeps_specific_reason_and_is_retryable(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            _prepare_ledger(database)
            repository = _MainBuffReceiptRejectRepository()
            app = SectApplication(database, repository=repository)
            result = app.learn_main(
                operation_id="op-main-reject",
                user_id="u",
                sect_id=1,
                buff_id=2,
                materials_cost=10,
                expected_catalog="[]",
            )
            self.assertFalse(result.ok)
            self.assertEqual("failed", result.status)
            self.assertEqual("sect_mainbuff_receipt_write_rejected", result.code)
            self.assertEqual(result.data["subject"], "op-main-reject")
            self.assertEqual(repository.calls, ["main"])

    def test_secbuff_write_rejection_keeps_specific_reason(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            _prepare_ledger(database)
            repository = _SecBuffReceiptRejectRepository()
            result = SectApplication(database, repository=repository).learn_secondary(
                operation_id="op-sec-reject", user_id="u", sect_id=1, buff_id=2,
                materials_cost=10, expected_catalog="[]",
            )
            self.assertFalse(result.ok)
            self.assertEqual("sect_secbuff_receipt_write_rejected", result.code)
            self.assertEqual(result.data["subject"], "op-sec-reject")

    def test_position_write_rejection_is_specific_and_same_request_recovers(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            _prepare_ledger(database)
            repository = _PositionRepository(reject_once=True)
            app = SectApplication(database, repository=repository)
            args = dict(operation_id="op-position-recover", user_id="u", target_id="t", requested_position=4, position_limits={4: 2}, manager_max_position=2)
            failed = app.change_position(**args)
            self.assertFalse(failed.ok)
            self.assertEqual((failed.status, failed.code, failed.data["subject"]), ("failed", "sect_position_receipt_write_rejected", "op-position-recover"))
            recovered = app.change_position(**args)
            self.assertTrue(recovered.ok)
            self.assertEqual([4, 4], [call[1] for call in repository.calls])

    def test_position_replay_and_changed_payload_keep_distinct_outcomes(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            _prepare_ledger(database)
            repository = _PositionRepository()
            app = SectApplication(database, repository=repository)
            args = dict(operation_id="op-position-replay", user_id="u", target_id="t", requested_position=4, position_limits={4: 2}, manager_max_position=2)
            first = app.change_position(**args)
            replay = app.change_position(**args)
            conflict = app.change_position(**{**args, "requested_position": 5})
            self.assertEqual((first.status, replay.status, replay.replayed), ("applied", "replayed", True))
            self.assertEqual((conflict.status, conflict.code, conflict.data["subject"]), ("rejected", "operation_conflict", "actor"))
            self.assertEqual([4], [call[1] for call in repository.calls])

    def test_weekly_claim_uses_atomic_repository_receipt_without_outer_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            database = Path(directory) / "game.db"
            app = SectApplication(database, weekly_repository=repository)
            result = app.claim_weekly(
                operation_id="sect-weekly-1",
                user_id="u",
                sect_id=1,
                week_key="2026-W29",
                goals=[{"key": "g1", "target": 1, "rewards": {}}],
                max_goods_num=99,
            )
            self.assertTrue(result.ok)
            self.assertEqual("applied", result.data["status"])
            self.assertEqual(["weekly"], repository.calls)
            self.assertFalse(database.exists())


if __name__ == "__main__":
    unittest.main()
