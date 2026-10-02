from pathlib import Path
import unittest


class WorldEventsLazyReaderTests(unittest.TestCase):
    def test_world_events_defers_sql_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_world_events/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)
        self.assertIn("update_last_check_info_time(", source)
        self.assertNotIn("_sql_message().update_last_check_info_time(", source)
        self.assertIn("_sql_message().update_user_hp(", source)

    def test_status_reader_does_not_write_legacy_state_projection(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_world_events/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("def _save_state(", source)
        self.assertNotIn("update_or_write_data(", source)


if __name__ == "__main__":
    unittest.main()
