import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.transaction_service import (
    PartnerProtectionService,
    db_backend as protection_db_backend,
)
from tests.test_db_backend import db_backend


class PartnerProtectionTransactionTests(unittest.TestCase):
    def test_partner_facade_defers_protection_service_construction(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import partner

        self.assertIsNone(partner._partner_protection_service_instance)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.player = Path(self.temp.name) / "player.db"
        self.service = PartnerProtectionService(self.player)

    def tearDown(self):
        self.temp.cleanup()

    def test_status_read_is_live_readonly_and_skips_writer_initialization(self):
        with db_backend.transaction(self.player) as conn:
            PartnerProtectionService.ensure_schema(conn)
            conn.execute("INSERT INTO status(user_id) VALUES('a')")
        statements = []
        readers = []
        original_connect = sqlite3.connect

        def connect(*args, **kwargs):
            conn = original_connect(*args, **kwargs)
            if kwargs.get("uri") and str(args[0]).endswith("?mode=ro"):
                conn.set_trace_callback(statements.append)
                readers.append(conn)
            return conn

        cases = (
            ("on", "on"), ("off", "off"), ("refusal", "refusal"),
            (" ON ", "on"), (None, "off"), ("invalid", "off"),
        )
        with (
            patch.object(protection_db_backend.sqlite3, "connect", side_effect=connect),
            patch.object(protection_db_backend, "transaction") as writer,
        ):
            for saved, expected in cases:
                with self.subTest(saved=saved):
                    with db_backend.transaction(self.player) as conn:
                        conn.execute(
                            "UPDATE status SET two_exp_protect=%s WHERE user_id='a'",
                            (saved,),
                        )
                    self.assertEqual(expected, self.service.get_status("a"))
            self.assertEqual("off", self.service.get_status("missing"))
            writer.assert_not_called()
        self.assertEqual(len(cases) + 1, len(readers))
        self.assertEqual(len(readers), statements.count("PRAGMA busy_timeout=30000"))
        selects = [sql for sql in statements if sql.startswith("SELECT status.two_exp_protect ")]
        self.assertEqual(len(readers), len(selects))
        self.assertEqual(len(readers) + len(selects), len(statements))
        for conn in readers:
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")

    def test_status_read_initializes_only_missing_legacy_schema_after_closing_reader(self):
        original_reader = protection_db_backend.connect_readonly
        original_writer = protection_db_backend.transaction
        for schema in ("missing_file", "missing_table", "missing_column"):
            with self.subTest(schema=schema):
                database = Path(self.temp.name) / f"{schema}.db"
                service = PartnerProtectionService(database)
                if schema != "missing_file":
                    with db_backend.transaction(database) as conn:
                        if schema == "missing_table":
                            conn.execute("CREATE TABLE unrelated(value TEXT)")
                        else:
                            conn.execute("CREATE TABLE status(user_id TEXT PRIMARY KEY)")
                            conn.execute("INSERT INTO status(user_id) VALUES('a')")
                readers = []

                def readonly(*args, **kwargs):
                    conn = original_reader(*args, **kwargs)
                    readers.append(conn)
                    return conn

                def initializing_writer(*args, **kwargs):
                    for conn in readers:
                        with self.assertRaises(sqlite3.ProgrammingError):
                            conn.execute("SELECT 1")
                    return original_writer(*args, **kwargs)

                with (
                    patch.object(protection_db_backend, "connect_readonly", side_effect=readonly),
                    patch.object(protection_db_backend, "transaction", side_effect=initializing_writer) as writer,
                ):
                    self.assertEqual("off", service.get_status("a"))
                    writer.assert_called_once_with(database)
                with db_backend.connection(database) as conn:
                    self.assertIn("two_exp_protect", conn.column_names("status"))
                    self.assertEqual(
                        int(schema == "missing_column"),
                        conn.execute("SELECT COUNT(*) FROM status").fetchone()[0],
                    )

    def test_status_read_does_not_initialize_on_existing_database_open_failure(self):
        with db_backend.transaction(self.player) as conn:
            PartnerProtectionService.ensure_schema(conn)
        for error in (
            db_backend.OperationalError("unable to open database file"),
            db_backend.OperationalError("database is locked"),
            PermissionError("permission denied"),
        ):
            with (
                self.subTest(error=str(error)),
                patch.object(protection_db_backend, "connect_readonly", side_effect=error),
                patch.object(protection_db_backend, "transaction") as writer,
            ):
                with self.assertRaises(type(error)) as caught:
                    self.service.get_status("a")
                self.assertIs(error, caught.exception)
                writer.assert_not_called()

    def test_status_read_does_not_initialize_or_hide_unrelated_query_failure(self):
        for message in (
            "database is locked", "database disk image is malformed",
            "not authorized", "no such column: user_id", "no such table: other",
        ):
            error = db_backend.OperationalError(message)
            reader = Mock()
            reader.execute.side_effect = error
            with (
                self.subTest(error=message),
                patch.object(protection_db_backend, "connect_readonly", return_value=reader),
                patch.object(protection_db_backend, "transaction") as writer,
            ):
                with self.assertRaises(db_backend.OperationalError) as caught:
                    self.service.get_status("a")
                self.assertIs(error, caught.exception)
                reader.close.assert_called_once_with()
                writer.assert_not_called()

    def test_status_read_rejects_missing_identity_column_without_repairing_it(self):
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE status(two_exp_protect TEXT)")
        with patch.object(protection_db_backend, "transaction") as writer:
            with self.assertRaisesRegex(db_backend.OperationalError, "no such column: user_id"):
                self.service.get_status("a")
            writer.assert_not_called()

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

    def test_state_ignore_is_a_specific_rejection_and_can_retry(self):
        self.service.set_status("seed", "a", "off", "on")
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TRIGGER reject_partner_protection_update "
                "BEFORE UPDATE OF two_exp_protect ON status "
                "BEGIN SELECT RAISE(IGNORE); END"
            )

        rejected = self.service.set_status("retry", "a", "on", "refusal")

        self.assertEqual(
            ("state_changed", "partner_protection_state_write_rejected", "a"),
            (rejected.status, rejected.reason, rejected.subject),
        )
        self.assertEqual("on", self.service.get_status("a"))
        with db_backend.connection(self.player) as conn:
            self.assertEqual(1, conn.execute(
                "SELECT COUNT(*) FROM partner_protection_operations"
            ).fetchone()[0])
            conn.execute("DROP TRIGGER reject_partner_protection_update")
        retried = self.service.set_status("retry", "a", "on", "refusal")
        self.assertEqual("applied", retried.status)
        self.assertEqual("refusal", self.service.get_status("a"))

    def test_state_insert_ignore_is_a_specific_rejection(self):
        with db_backend.transaction(self.player) as conn:
            PartnerProtectionService.ensure_schema(conn)
            conn.execute(
                "CREATE TRIGGER reject_partner_protection_insert "
                "BEFORE INSERT ON status "
                "BEGIN SELECT RAISE(IGNORE); END"
            )

        rejected = self.service.set_status("insert-retry", "a", "off", "on")

        self.assertEqual(
            ("state_changed", "partner_protection_state_write_rejected", "a"),
            (rejected.status, rejected.reason, rejected.subject),
        )
        self.assertEqual("off", self.service.get_status("a"))
        with db_backend.connection(self.player) as conn:
            self.assertIsNone(conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name='partner_protection_operations'"
            ).fetchone())

    def test_state_after_trigger_rewrite_rolls_back(self):
        self.service.set_status("seed", "a", "off", "on")
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TRIGGER rewrite_partner_protection_update "
                "AFTER UPDATE OF two_exp_protect ON status "
                "BEGIN UPDATE status SET two_exp_protect='off' WHERE user_id='a'; END"
            )

        rejected = self.service.set_status("rewrite", "a", "on", "refusal")

        self.assertEqual("partner_protection_state_write_rejected", rejected.reason)
        self.assertEqual("on", self.service.get_status("a"))
        with db_backend.connection(self.player) as conn:
            self.assertEqual(1, conn.execute(
                "SELECT COUNT(*) FROM partner_protection_operations"
            ).fetchone()[0])

    def test_receipt_ignore_rolls_back_state_and_can_retry(self):
        self.service.set_status("seed", "a", "off", "on")
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TRIGGER reject_partner_protection_receipt "
                "BEFORE INSERT ON partner_protection_operations "
                "BEGIN SELECT RAISE(IGNORE); END"
            )

        rejected = self.service.set_status("receipt-retry", "a", "on", "refusal")

        self.assertEqual(
            ("state_changed", "partner_protection_receipt_write_rejected", "receipt-retry"),
            (rejected.status, rejected.reason, rejected.subject),
        )
        self.assertEqual("on", self.service.get_status("a"))
        with db_backend.connection(self.player) as conn:
            self.assertEqual(1, conn.execute(
                "SELECT COUNT(*) FROM partner_protection_operations"
            ).fetchone()[0])
            conn.execute("DROP TRIGGER reject_partner_protection_receipt")
        retried = self.service.set_status("receipt-retry", "a", "on", "refusal")
        self.assertEqual("applied", retried.status)
        self.assertEqual("refusal", self.service.get_status("a"))

    def test_rejects_invalid_status_values(self):
        for expected, target in (("unknown", "on"), ("off", "unknown")):
            with self.subTest(expected=expected, target=target):
                with self.assertRaises(ValueError):
                    self.service.set_status("op", "a", expected, target)


if __name__ == "__main__":
    unittest.main()
