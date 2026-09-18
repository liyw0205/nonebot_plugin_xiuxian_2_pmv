from pathlib import Path
import unittest


class TrainingEventsLazyReaderTests(unittest.TestCase):
    def test_training_events_defers_sql_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_training/training_events.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertIn("_sql_message().get_top_users_by_level(", source)
        self.assertIn("_sql_message().get_back_msg(", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)


if __name__ == "__main__":
    unittest.main()
