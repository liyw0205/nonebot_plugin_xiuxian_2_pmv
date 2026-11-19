from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

from nonebot_plugin_xiuxian_2.features.title.application import TitleApplication
from nonebot_plugin_xiuxian_2.features.title.migrations import apply_title_schema
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger


class TitleEquipmentRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / "player.db"
        with DatabaseUnitOfWork(self.database) as uow:
            apply_title_schema(uow)
            OperationLedger().ensure_schema(uow)
            uow.execute("INSERT INTO title VALUES('u',?,'1')", (json.dumps(["1", "2"]),))
            uow.execute("CREATE TABLE isolated_title_effect(effect INTEGER)")
        self.application = TitleApplication(self.database)

    def snapshot(self):
        with sqlite3.connect(self.database) as conn:
            return tuple(conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall() for table in (
                "title", "title_transaction_operations", "operation_ledger", "operation_audit", "isolated_title_effect",
            ))

    def execute(self, action, operation_id="equipment"):
        return self.application.execute(
            operation_id=operation_id, user_id="u",
            payload={"action": action, "expected_unlocked": ["1", "2"], "expected_equipped": "1", "title_id": "2"},
        )

    def assert_write_rejected(self, event, fault, reason):
        for action in ("equip", "unequip"):
            with self.subTest(action=action):
                before = self.snapshot()
                with sqlite3.connect(self.database) as conn:
                    conn.execute(f"CREATE TRIGGER reject_title {event} BEGIN "
                                 f"INSERT INTO isolated_title_effect VALUES(1); {fault}; END")
                try:
                    outcome = self.execute(action, f"equipment-{action}")
                finally:
                    with sqlite3.connect(self.database) as conn:
                        conn.execute("DROP TRIGGER reject_title")
                subject = "2" if action == "equip" else "1"
                self.assertEqual((outcome.status, outcome.code), ("failed", reason))
                self.assertEqual((outcome.data["reason"], outcome.data["subject"]), (reason, subject))
                self.assertEqual(self.snapshot(), before)
                recovered = self.execute(action, f"equipment-{action}")
                self.assertTrue(recovered.ok)
                self.assertEqual(self.application.get_state("u")["equipped"], "2" if action == "equip" else "")
                settled = self.snapshot()
                replay = self.execute(action, f"equipment-{action}")
                self.assertTrue(replay.replayed)
                self.assertEqual(self.snapshot(), settled)
                with sqlite3.connect(self.database) as conn:
                    self.assertEqual(conn.execute("PRAGMA integrity_check").fetchone(), ("ok",))
                    conn.execute("UPDATE title SET equipped='1' WHERE user_id='u'")

    def test_state_ignore_rejects_and_recovers(self):
        self.assert_write_rejected(
            "BEFORE UPDATE OF equipped ON title", "SELECT RAISE(IGNORE)", "title_equip_write_rejected",
        )

    def test_state_rewrite_cannot_remove_unlocked_titles(self):
        self.assert_write_rejected(
            "AFTER UPDATE OF equipped ON title", "UPDATE title SET unlocked='[]' WHERE user_id=NEW.user_id",
            "title_equipment_state_projection_mismatch",
        )

    def test_receipt_ignore_rejects_and_recovers(self):
        self.assert_write_rejected(
            "BEFORE INSERT ON title_transaction_operations", "SELECT RAISE(IGNORE)",
            "title_equipment_receipt_write_rejected",
        )

    def test_receipt_rewrite_cannot_change_title(self):
        self.assert_write_rejected(
            "AFTER INSERT ON title_transaction_operations",
            "UPDATE title_transaction_operations SET title_id='wrong' WHERE operation_id=NEW.operation_id",
            "title_equipment_receipt_projection_mismatch",
        )

    def test_receipt_side_effect_cannot_change_equipment(self):
        self.assert_write_rejected(
            "AFTER INSERT ON title_transaction_operations", "UPDATE title SET equipped='wrong' WHERE user_id='u'",
            "title_equipment_state_projection_mismatch",
        )

    def test_ledger_finish_cannot_change_equipment(self):
        self.assert_write_rejected(
            "AFTER INSERT ON operation_audit", "UPDATE title SET equipped='wrong' WHERE user_id='u'",
            "title_equipment_state_projection_mismatch",
        )

    def test_ledger_finish_cannot_change_receipt(self):
        self.assert_write_rejected(
            "AFTER INSERT ON operation_audit", "UPDATE title_transaction_operations SET title_id='wrong'",
            "title_equipment_receipt_projection_mismatch",
        )

    def test_abort_keeps_original_error_and_same_request_can_retry(self):
        for action in ("equip", "unequip"):
            with self.subTest(action=action):
                before = self.snapshot()
                operation_id = f"abort-{action}"
                with sqlite3.connect(self.database) as conn:
                    conn.execute("CREATE TRIGGER abort_title BEFORE INSERT ON title_transaction_operations BEGIN "
                                 "INSERT INTO isolated_title_effect VALUES(1); "
                                 "SELECT RAISE(ABORT,'isolated title equipment abort'); END")
                with self.assertRaisesRegex(sqlite3.IntegrityError, "isolated title equipment abort"):
                    self.execute(action, operation_id)
                after = self.snapshot()
                self.assertEqual((after[0], after[1], after[4]), (before[0], before[1], before[4]))
                failure_audit = [row for row in after[3] if row not in before[3]]
                self.assertEqual(len(failure_audit), 1)
                with sqlite3.connect(self.database) as conn:
                    self.assertEqual(conn.execute("SELECT status FROM operation_ledger WHERE operation_id=?",
                                                  (operation_id,)).fetchone(), ("failed",))
                    conn.execute("DROP TRIGGER abort_title")
                recovered = self.execute(action, operation_id)
                self.assertTrue(recovered.ok, recovered)
                settled = self.snapshot()
                self.assertIn(failure_audit[0], settled[3])
                self.assertTrue(self.execute(action, operation_id).replayed)
                self.assertEqual(self.snapshot(), settled)
                with sqlite3.connect(self.database) as conn:
                    conn.execute("UPDATE title SET equipped='1' WHERE user_id='u'")

    def test_concurrent_equip_and_unequip_allow_only_one_snapshot_to_commit(self):
        barrier = Barrier(2)

        def submit(action):
            barrier.wait(timeout=5)
            return self.execute(action, f"concurrent-{action}")

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(submit, ("equip", "unequip")))
        self.assertEqual(sorted(result.status for result in outcomes), ["applied", "rejected"])
        rejected = next(result for result in outcomes if not result.ok)
        self.assertEqual(rejected.data["reason"], "title_equipped_changed")
        winner = next(result for result in outcomes if result.ok)
        expected = "2" if winner.action == "title.equip" else ""
        with sqlite3.connect(self.database) as conn:
            state = conn.execute("SELECT unlocked,equipped FROM title WHERE user_id='u'").fetchone()
            self.assertEqual((json.loads(state[0]), state[1]), (["1", "2"], expected))
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM title_transaction_operations").fetchone(), (1,))
            self.assertEqual(conn.execute("PRAGMA integrity_check").fetchone(), ("ok",))


if __name__ == "__main__":
    unittest.main()
