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
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_bank/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("BankDepositApplication(get_paths().game_db)", source)
        self.assertIn("BankWithdrawalApplication(get_paths().game_db)", source)
        self.assertIn("BankUpgradeApplication(get_paths().game_db)", source)
        self.assertIn("BankInterestApplication(get_paths().game_db)", source)
        self.assertIn('"bank_level": "1"', source)
        self.assertIn('"saved_stone": 0', source)


if __name__ == "__main__":
    unittest.main()
