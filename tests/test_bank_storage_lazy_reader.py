from pathlib import Path
import unittest


class BankStorageLazyReaderTests(unittest.TestCase):
    def test_bank_matcher_has_no_legacy_account_read_path(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_bank/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("PlayerDataManager", source)
        self.assertNotIn("_player_data_manager", source)
        self.assertNotIn("_read_legacy_bankinfo", source)
        self.assertNotIn("_legacy_account_record_status", source)
        self.assertNotIn("get_legacy_info(", source)
        self.assertNotIn("legacy_record_status(", source)
        self.assertIn("legacy_bank_account_storage", source)

    def test_commands_use_game_account_application_and_default_first_use(self):
        package = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2"
        source = (package / "xiuxian/xiuxian_bank/__init__.py").read_text(encoding="utf-8")
        owner = (package / "features/bank/command_application.py").read_text(encoding="utf-8")
        self.assertIn("BankCommandApplication(get_paths().game_db, BANKLEVEL)", source)
        self.assertIn("bank_command_application.execute(", source)
        for name in ("BankDepositApplication", "BankWithdrawalApplication", "BankUpgradeApplication", "BankInterestApplication"):
            self.assertNotIn(name, source)
            self.assertIn(name, owner)
        self.assertIn("expected_saved_stone", owner)
        self.assertIn("expected_saved_at", owner)
        self.assertNotIn("calculate_interest", source)


if __name__ == "__main__":
    unittest.main()
