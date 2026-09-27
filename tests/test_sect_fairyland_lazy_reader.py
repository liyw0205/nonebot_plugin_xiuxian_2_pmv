from pathlib import Path
import unittest


class SectFairylandLazyReaderTests(unittest.TestCase):
    def test_fairyland_claim_status_is_feature_owned_without_player_manager(self):
        package = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2"
        source = (
            package / "xiuxian/xiuxian_sect/sect_fairyland.py"
        ).read_text(encoding="utf-8")
        facade = (package / "xiuxian/xiuxian_sect/__init__.py").read_text(encoding="utf-8")
        self.assertNotIn("PlayerDataManager", source)
        self.assertNotIn("_get_fairyland_last_claim", facade)
        self.assertIn("sect_fairyland_application.get_last_claim_day(", facade)


if __name__ == "__main__":
    unittest.main()
