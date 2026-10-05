from pathlib import Path
import unittest


class TrainingEventsLazyReaderTests(unittest.TestCase):
    def test_training_event_resolver_has_no_legacy_database_reads(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/features/training/event_resolver.py"
        ).read_text(encoding="utf-8")
        self.assertIn("def handle_event(", source)
        self.assertIn("context=context", source)
        self.assertNotIn("XiuxianDateManage", source)
        self.assertNotIn("UserBuffDate", source)
        self.assertNotIn("get_back_msg", source)
        self.assertNotIn("get_top_users_by_level", source)

    def test_training_event_resolver_receives_item_catalog_and_random_source(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/features/training/event_resolver.py"
        ).read_text(encoding="utf-8")
        self.assertIn("def __init__(self, random_source=None)", source)
        self.assertIn("items=items", source)
        self.assertIn("items.get_data_by_item_id(", source)
        self.assertIn("random.Random()", source)


if __name__ == "__main__":
    unittest.main()
