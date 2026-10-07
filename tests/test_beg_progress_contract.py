import unittest

from scripts.check_full_refactor_progress import PACKAGE, _beg_command_owner_status, _slice_status


class BegProgressContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        paths = {
            "facade": "xiuxian/xiuxian_beg/__init__.py",
            "command": "features/beg/command_application.py",
            "reads": "features/beg/command_repository.py",
            "application": "features/beg/application.py",
            "replies": "features/beg/command_replies.py",
        }
        cls.sources = {name: (PACKAGE / path).read_text(encoding="utf-8") for name, path in paths.items()}

    def test_three_frozen_commands_have_bound_owner_evidence_without_losing_daily_reset(self):
        for key, value in _beg_command_owner_status(self.sources).items():
            self.assertTrue(value, key)
            self.assertTrue(_slice_status()["beg"][key], key)
        self.assertTrue(_slice_status()["beg"]["daily_reset_application_owned"])
        self.assertTrue(_slice_status()["beg"]["daily_reset_atomic_no_request_ddl"])

    def test_gate_rejects_focused_owner_replay_reader_and_reply_regressions(self):
        owner = "three_command_handlers_reach_one_feature_owner"
        replay = "command_receipts_precede_live_inputs_and_activity"
        reads = "command_reads_validate_receipts_without_schema_writes"
        writer = "command_reuses_existing_atomic_claim_writers"
        replies = "dynamic_help_and_rejected_replies_do_not_need_claim_effects"
        cases = (
            ("facade", "beg_command_application.execute(", "legacy_beg_application.execute(", owner),
            ("facade", "    XiuConfig,", "    XiuConfig(),", owner),
            ("command", "self.repository.receipt(", "self.repository.legacy_receipt(", replay),
            ("command", "self.activity.update_last_check_info_time(", "legacy_activity_update(", replay),
            ("reads", "read_only=True", "read_only=False", reads),
            ("command", "self.application.execute(", "self.legacy_application.execute(", writer),
            ("replies", 'status not in {"applied", "duplicate"}',
             'status not in {"applied", "duplicate", "expired"}', replies),
            ("command", "config = self.config_provider()",
             "self.repository.profile('unexpected-help-read')\n                config = self.config_provider()", replies),
        )
        for source, before, after, gate in cases:
            with self.subTest(source=source, mutation=after):
                self.assertIn(before, self.sources[source])
                changed = dict(self.sources)
                changed[source] = changed[source].replace(before, after, 1)
                self.assertFalse(_beg_command_owner_status(changed)[gate])
