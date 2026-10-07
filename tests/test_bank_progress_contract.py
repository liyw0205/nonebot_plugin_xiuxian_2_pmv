import unittest

from scripts.check_full_refactor_progress import PACKAGE, _bank_command_owner_status, _slice_status


class BankProgressContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        paths = {
            "handlers": "xiuxian/xiuxian_bank/__init__.py",
            "command": "features/bank/command_application.py",
            "receipts": "features/bank/command_receipt_repository.py",
            "replies": "features/bank/command_replies.py",
            "deposit": "features/bank/account_application.py",
            "withdrawal": "features/bank/account_withdrawal_application.py",
            "upgrade": "features/bank/account_upgrade_application.py",
            "interest": "features/bank/account_interest_application.py",
        }
        cls.sources = {name: (PACKAGE / path).read_text(encoding="utf-8") for name, path in paths.items()}

    def test_real_regex_handler_has_source_bound_command_and_existing_writer_evidence(self):
        report = _bank_command_owner_status(self.sources)
        for key, value in report.items():
            with self.subTest(gate=key):
                self.assertTrue(value, key)
                self.assertTrue(_slice_status()["bank"][key], key)

    def test_gate_rejects_bypassed_owner_and_unreachable_writer_imports(self):
        cases = (
            ("handlers", "bank_command_application.execute(", "legacy_bank.execute(",
             "command_facade_orchestration_feature_owned"),
            ("command", "self.deposits.deposit(", "legacy_deposit(", "deposit_application_owned"),
            ("withdrawal", "self.repository.save_withdrawal(", "legacy_save_withdrawal(",
             "withdrawal_application_owned"),
        )
        for source, before, after, gate in cases:
            with self.subTest(gate=gate):
                self.assertIn(before, self.sources[source])
                changed = dict(self.sources)
                changed[source] = changed[source].replace(before, after, 1)
                self.assertFalse(_bank_command_owner_status(changed)[gate])

    def test_gate_rejects_receipt_identity_snapshot_and_rejection_regressions(self):
        cases = (
            ("command", "return previous", "return None", "command_receipts_precede_live_account_reads"),
            ("receipts", "read_only=True", "read_only=False", "legacy_operation_receipts_read_only"),
            ("receipts", "(previous_user, previous_action, previous_amount) != (user_id, action, amount)",
             "(previous_user, previous_action, amount) != (user_id, action, amount)",
             "command_receipts_validate_original_identity"),
            ("command", '"expected_saved_stone": saved', '"expected_saved_stone": 0',
             "command_snapshot_cas_forwarded_to_existing_writers"),
            ("command", "self.deposits.deposit(**snapshot,", "self.deposits.deposit(**{},",
             "command_snapshot_cas_forwarded_to_existing_writers"),
            ("command", "accrued, hours = calculate_interest(", "accrued, hours = legacy_calculate_interest(",
             "command_automatic_interest_and_account_info_feature_owned"),
            ("replies", 'if status not in {"applied", "duplicate"}:',
             'if status not in {"applied", "duplicate", "stone_insufficient"}:',
             "command_reply_rejections_precede_success_fields"),
        )
        for source, before, after, gate in cases:
            with self.subTest(gate=gate, mutation=after):
                self.assertIn(before, self.sources[source])
                changed = dict(self.sources)
                changed[source] = changed[source].replace(before, after, 1)
                self.assertFalse(_bank_command_owner_status(changed)[gate])
