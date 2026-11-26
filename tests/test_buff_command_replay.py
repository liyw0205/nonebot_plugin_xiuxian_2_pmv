"""Frozen business receipts survive old application hashes and interrupted finish."""
from __future__ import annotations

import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.core.errors import OperationConflictError
from nonebot_plugin_xiuxian_2.features.buff.application import BuffApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.database.ledger import request_hash
from tests.test_blessed_spot_schema_ownership import Pair


class BuffCommandReplayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.pair = Pair(Path(self.temp.name)).migrate().seed(stone=1000)
        self.app = BuffApplication(self.pair.game, self.pair.player)

    def training(self, operation="training", **changes):
        request = dict(requested_stone=100, expected_exp=100, expected_stone=1000, exp_cap=1000, power_multiplier=2.0)
        request.update(changes)
        return self.app.stone_training(operation_id=operation, user_id="u", **request)

    def test_new_snapshot_after_success_replays_and_changed_request_conflicts(self):
        first = self.training()
        after = self.pair.training_state()
        repeat = self.training(expected_exp=110, expected_stone=900)
        self.assertTrue(first.ok and repeat.ok and repeat.replayed)
        self.assertEqual(repeat.data, first.data)
        self.assertEqual(self.pair.training_state(), after)
        with self.assertRaises(OperationConflictError):
            self.training(requested_stone=200)
        with self.assertRaises(OperationConflictError):
            self.app.stone_training(operation_id="training", user_id="other", requested_stone=100, expected_exp=110, expected_stone=900, exp_cap=1000, power_multiplier=2.0)
        self.assertEqual(self.pair.training_state(), after)

    def test_historical_hash_is_replayed_only_after_business_identity_check(self):
        first = self.training()
        with DatabaseUnitOfWork(self.pair.game) as uow:
            uow.execute("UPDATE operation_ledger SET request_hash=? WHERE operation_id='training'", (request_hash(dict(user_id="u", requested_stone=100, expected_exp=100, expected_stone=1000, exp_cap=1000, power_multiplier=2.0)),))
        self.assertEqual(self.training(expected_exp=110, expected_stone=900).data, first.data)
        self.assertEqual(self.pair.training_state()[:2], (110, 900))

    def test_started_ledger_after_business_commit_recovers_frozen_result(self):
        with patch.object(self.app.ledger, "finish", side_effect=RuntimeError("interrupted finish")):
            with self.assertRaisesRegex(RuntimeError, "interrupted finish"):
                self.training()
        after = self.pair.training_state()
        with DatabaseUnitOfWork(self.pair.game) as uow:
            uow.execute("UPDATE operation_ledger SET status='started',result_json=NULL WHERE operation_id='training'")
        recovered = self.training(expected_exp=110, expected_stone=900)
        self.assertTrue(recovered.ok and recovered.replayed)
        self.assertEqual(self.pair.training_state(), after)
        with DatabaseUnitOfWork(self.pair.game, read_only=True) as uow:
            self.assertEqual(self.app.ledger.get(uow, "training", "buff.stone_training").status, "applied")

    def test_real_snapshot_change_still_rejects_before_any_asset_write(self):
        before = self.pair.training_state()
        result = self.training(expected_exp=99)
        self.assertFalse(result.ok)
        self.assertEqual(result.data["reason"], "exp_changed")
        self.assertEqual(self.pair.training_state(), before)

    def test_training_keeps_exp_above_int64_exact(self):
        from nonebot_plugin_xiuxian_2.core.numeric import bind_sqlite_param, exact_crystal_integer

        starting = 2 ** 63 + 1
        with DatabaseUnitOfWork(self.pair.game) as uow:
            uow.execute("UPDATE user_xiuxian SET exp=?", (bind_sqlite_param(starting, exact=True),))
        result = self.training(expected_exp=starting, exp_cap=starting + 1000)
        self.assertTrue(result.ok)
        with DatabaseUnitOfWork(self.pair.game, read_only=True) as uow:
            row = uow.query_one("SELECT exp,stone,power FROM user_xiuxian")
        self.assertEqual(starting + 10, exact_crystal_integer(row["exp"]))
        self.assertEqual(900, exact_crystal_integer(row["stone"]))
        self.assertEqual((starting + 10) * 2, exact_crystal_integer(row["power"]))
        self.assertEqual((starting + 10) * 2, result.data["power"])

    def test_training_rejects_ignored_receipt_and_rewritten_balance(self):
        from nonebot_plugin_xiuxian_2.core.errors import DomainError

        for trigger in (
            "CREATE TRIGGER deny_training BEFORE INSERT ON stone_training_operations BEGIN SELECT RAISE(IGNORE); END",
            "CREATE TRIGGER deny_training BEFORE UPDATE ON user_xiuxian BEGIN SELECT RAISE(IGNORE); END",
            "CREATE TRIGGER deny_training AFTER UPDATE OF exp ON user_xiuxian BEGIN UPDATE user_xiuxian SET stone=stone+1 WHERE user_id=NEW.user_id; END",
            "CREATE TRIGGER deny_training AFTER INSERT ON stone_training_operations BEGIN UPDATE stone_training_operations SET payload='[]' WHERE operation_id=NEW.operation_id; END",
            "CREATE TRIGGER deny_training AFTER INSERT ON stone_training_operations BEGIN UPDATE stone_training_operations SET result_json='{}' WHERE operation_id=NEW.operation_id; END",
            "CREATE TRIGGER deny_training AFTER INSERT ON stone_training_operations BEGIN DELETE FROM stone_training_operations WHERE operation_id=NEW.operation_id; END",
            "CREATE TRIGGER deny_training AFTER INSERT ON stone_training_operations BEGIN UPDATE user_xiuxian SET stone=stone+1; END",
        ):
            with self.subTest(trigger=trigger):
                with DatabaseUnitOfWork(self.pair.game) as uow:
                    uow.execute("UPDATE user_xiuxian SET exp=100,stone=1000,power=0")
                with DatabaseUnitOfWork(self.pair.game) as uow:
                    uow.execute(trigger)
                operation = "verified-" + str(len(trigger))
                try:
                    before = self.pair.training_state()
                    with self.assertRaises(DomainError) as caught:
                        self.training(operation=operation)
                    self.assertEqual(caught.exception.code, "stone_training_write_rejected")
                    self.assertIn(caught.exception.details["reason"], {
                        "stone_training_player_write_rejected", "stone_training_receipt_write_rejected",
                        "stone_training_player_projection_mismatch", "stone_training_receipt_projection_mismatch",
                    })
                    self.assertEqual(self.pair.training_state(), before)
                    with DatabaseUnitOfWork(self.pair.game, read_only=True) as uow:
                        self.assertIsNone(self.app.ledger.get(uow, operation, "buff.stone_training"))
                finally:
                    with DatabaseUnitOfWork(self.pair.game) as uow:
                        uow.execute("DROP TRIGGER deny_training")
                self.assertTrue(self.training(operation=operation).ok)
                after = self.pair.training_state()
                self.assertTrue(self.training(operation=operation).replayed)
                self.assertEqual(self.pair.training_state(), after)

    def test_ambiguous_historical_exp_is_rejected_without_pending_reservation(self):
        from nonebot_plugin_xiuxian_2.core.errors import DomainError

        with DatabaseUnitOfWork(self.pair.game) as uow:
            uow.execute("UPDATE user_xiuxian SET exp=?", (float(2 ** 63),))
        before = self.pair.training_state()
        with self.assertRaises(DomainError) as caught:
            self.training(expected_exp=2 ** 63, exp_cap=2 ** 63 + 1000)
        self.assertEqual(caught.exception.details["reason"], "exp_precision_lost_real")
        self.assertEqual(self.pair.training_state(), before)
        with DatabaseUnitOfWork(self.pair.game, read_only=True) as uow:
            self.assertIsNone(self.app.ledger.get(uow, "training", "buff.stone_training"))

    def test_receipt_write_failure_rolls_back_training(self):
        with DatabaseUnitOfWork(self.pair.game) as uow:
            uow.execute("CREATE TRIGGER deny_training BEFORE INSERT ON stone_training_operations BEGIN SELECT RAISE(ABORT,'receipt denied'); END")
        before = self.pair.training_state()
        with self.assertRaisesRegex(Exception, "receipt denied"):
            self.training()
        self.assertEqual(self.pair.training_state(), before)

    def test_partial_training_receipt_schema_keeps_the_existing_closed_rejection(self):
        with DatabaseUnitOfWork(self.pair.game) as uow:
            uow.execute("DROP TABLE stone_training_operations")
            uow.execute("CREATE TABLE stone_training_operations(operation_id TEXT PRIMARY KEY)")
        before = self.pair.training_state()
        result = self.training()
        self.assertFalse(result.ok)
        self.assertEqual(result.data["status"], "schema_missing")
        self.assertEqual(self.pair.training_state(), before)
        with DatabaseUnitOfWork(self.pair.game, read_only=True) as uow:
            self.assertEqual([row["name"] for row in uow.query_all("PRAGMA table_info(stone_training_operations)")], ["operation_id"])

    def test_open_ignores_new_clock_but_preserves_cost_and_identity(self):
        import nonebot

        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()
        with DatabaseUnitOfWork(self.pair.game) as uow:
            uow.execute("UPDATE user_xiuxian SET blessed_spot_flag=0")
        first = self.app.open(operation_id="open", user_id="u", stone_cost=300, default_name="洞府", harvest_time="first")
        repeat = self.app.open(operation_id="open", user_id="u", stone_cost=300, default_name="洞府", harvest_time="later")
        self.assertTrue(first.ok and repeat.ok and repeat.replayed)
        self.assertEqual(first.data, repeat.data)
        with self.assertRaises(OperationConflictError):
            self.app.open(operation_id="open", user_id="u", stone_cost=301, default_name="洞府", harvest_time="later")
        with DatabaseUnitOfWork(self.pair.game, read_only=True) as uow:
            self.assertEqual(uow.query_one("SELECT stone FROM user_xiuxian")["stone"], 700)
        with DatabaseUnitOfWork(self.pair.player, read_only=True) as uow:
            self.assertEqual(uow.query_one('SELECT "收取时间" AS harvest FROM mix_elixir_info')["harvest"], "first")


if __name__ == "__main__":
    unittest.main()
