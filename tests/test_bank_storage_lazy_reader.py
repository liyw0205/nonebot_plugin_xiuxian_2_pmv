from pathlib import Path
import unittest


class BankStorageLazyReaderTests(unittest.TestCase):
    def test_bank_defers_player_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_bank/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("PlayerDataManager", source)
        self.assertNotIn("_player_data_manager", source)
        self.assertNotIn("_player_data_manager().get_fields(", source)
        self.assertIn("legacy_bank_account_storage", source)
        self.assertIn("get_legacy_info(", source)

    def test_bank_reads_legacy_account_only_after_new_account_path(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_bank/__init__.py"
        ).read_text(encoding="utf-8")
        handler = source[source.index("async def bank_"):source.index("def get_give_stone")]
        before_modes = handler.split("if mode == '存灵石'", 1)[0]
        self.assertNotIn("_read_legacy_bankinfo(user_id)", before_modes)
        self.assertEqual(handler.count("_read_legacy_bankinfo(user_id)"), 5)
        self.assertIn("def _read_legacy_bankinfo(user_id):", source)

    def test_default_matcher_guards_each_legacy_write_fallback(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_bank/__init__.py"
        ).read_text(encoding="utf-8")
        handler = source[source.index("async def bank_"):source.index("def get_give_stone")]
        self.assertIn("on_regex(\n    r'^灵庄", source)
        deposit = handler.split("if mode == '存灵石'", 1)[1].split("elif mode == '取灵石'", 1)[0]
        withdrawal = handler.split("elif mode == '取灵石'", 1)[1].split("elif mode == '升级会员'", 1)[0]
        upgrade = handler.split("elif mode == '升级会员'", 1)[1].split("elif mode == '信息'", 1)[0]
        interest = handler.split("elif mode == '结算'", 1)[1]
        for fallback in (deposit, withdrawal, upgrade, interest):
            self.assertIn("_legacy_account_record_status(user_id)", fallback)
        self.assertIn("initial_account={", upgrade)
        self.assertIn("initial_account={", interest)

    def test_default_info_fallback_remains_read_only_and_visible(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_bank/__init__.py"
        ).read_text(encoding="utf-8")
        handler = source[source.index("async def bank_"):source.index("def get_give_stone")]
        info = handler.split("elif mode == '信息'", 1)[1].split("elif mode == '结算'", 1)[0]
        self.assertIn("_read_legacy_bankinfo(user_id)", info)
        self.assertNotIn("BankUpgradeApplication", info)
        self.assertNotIn("BankDepositApplication", info)


if __name__ == "__main__":
    unittest.main()
