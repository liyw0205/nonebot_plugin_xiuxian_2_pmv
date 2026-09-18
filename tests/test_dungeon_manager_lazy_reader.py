from pathlib import Path
import unittest


class DungeonManagerLazyReaderTests(unittest.TestCase):
    def test_dungeon_module_defers_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_dungeon/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_dungeon_manager_instance = None", source)
        self.assertIn("class _LazyDungeonManager", source)
        self.assertNotIn("dungeon_manager = DungeonManager()", source)
        self.assertIn("dungeon_manager = _LazyDungeonManager()", source)
