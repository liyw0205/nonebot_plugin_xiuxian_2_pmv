import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.transaction_service import (
    PartnerProtectionService,
)
from tests.test_db_backend import db_backend


class PartnerProtectionTransactionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.player = Path(self.temp.name) / "player.db"
        self.service = PartnerProtectionService(self.player)

    def tearDown(self):
        self.temp.cleanup()

    def test_applies_and_replays_the_same_operation(self):
        applied = self.service.set_status("op", "a", "off", "on")
        duplicate = self.service.set_status("op", "a", "on", "on")

        self.assertEqual(("applied", "off", "on"), (
            applied.status, applied.previous_status, applied.current_status,
        ))
        self.assertEqual(("duplicate", "off", "on"), (
            duplicate.status, duplicate.previous_status, duplicate.current_status,
        ))
        self.assertEqual("on", self.service.get_status("a"))
        with db_backend.connection(self.player) as conn:
            self.assertEqual(1, conn.execute(
                "SELECT COUNT(*) FROM partner_protection_operations"
            ).fetchone()[0])

    def test_reusing_operation_with_different_payload_conflicts(self):
        self.assertEqual(
            "applied", self.service.set_status("op", "a", "off", "on").status
        )

        for user_id, expected, target in (
            ("b", "off", "on"),
            ("a", "off", "refusal"),
        ):
            with self.subTest(user_id=user_id, expected=expected, target=target):
                self.assertEqual(
                    "operation_conflict",
                    self.service.set_status("op", user_id, expected, target).status,
                )

    def test_replays_legacy_payload_without_reusing_its_expected_snapshot(self):
        self.service.set_status("op", "a", "off", "on")
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "UPDATE partner_protection_operations SET payload=%s "
                "WHERE operation_id='op'",
                ('["a","off","on"]',),
            )

        result = self.service.set_status("op", "a", "on", "on")

        self.assertEqual("duplicate", result.status)
        self.assertEqual(("off", "on"), (
            result.previous_status, result.current_status,
        ))

    def test_changed_snapshot_does_not_write_operation(self):
        with db_backend.transaction(self.player) as conn:
            PartnerProtectionService.ensure_schema(conn)
            conn.execute(
                "INSERT INTO status(user_id,two_exp_protect) VALUES(%s,%s)",
                ("a", "on"),
            )

        result = self.service.set_status("op", "a", "off", "refusal")

        self.assertEqual("state_changed", result.status)
        self.assertEqual("on", self.service.get_status("a"))
        with db_backend.connection(self.player) as conn:
            self.assertIsNone(conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name='partner_protection_operations'"
            ).fetchone())

    def test_operation_insert_failure_rolls_back_status(self):
        self.service.set_status("seed", "a", "off", "on")
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "UPDATE status SET two_exp_protect='off' WHERE user_id='a'"
            )
            conn.execute(
                "CREATE TRIGGER fail_partner_protection_operation "
                "BEFORE INSERT ON partner_protection_operations "
                "BEGIN SELECT RAISE(ABORT,'forced failure'); END"
            )

        with self.assertRaises(Exception):
            self.service.set_status("fail", "a", "off", "refusal")

        self.assertEqual("off", self.service.get_status("a"))
        with db_backend.connection(self.player) as conn:
            self.assertIsNone(conn.execute(
                "SELECT operation_id FROM partner_protection_operations "
                "WHERE operation_id='fail'"
            ).fetchone())

    def test_rejects_invalid_status_values(self):
        for expected, target in (("unknown", "on"), ("off", "unknown")):
            with self.subTest(expected=expected, target=target):
                with self.assertRaises(ValueError):
                    self.service.set_status("op", "a", expected, target)


if __name__ == "__main__":
    unittest.main()
