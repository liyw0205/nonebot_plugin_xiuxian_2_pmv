from pathlib import Path
import unittest


class BankStorageLazyReaderTests(unittest.TestCase):
    def test_bank_defers_player_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_bank/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_player_data_manager_instance = None", source)
        self.assertIn("def _player_data_manager(", source)
        self.assertNotIn("player_data_manager = PlayerDataManager()", source)
        self.assertIn("_player_data_manager().get_fields(", source)
        self.assertIn("_player_data_manager().update_or_write_data(", source)

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


if __name__ == "__main__":
    unittest.main()
