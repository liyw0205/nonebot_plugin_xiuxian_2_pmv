import unittest
from pathlib import Path

class SectJoinStateSourceContractTests(unittest.TestCase):
    def test_handlers_use_feature_application(self):
        source = Path('nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/__init__.py').read_text(encoding='utf-8')
        close = source[source.index('async def sect_close_join_'):source.index('@sect_open_join.handle')]
        open_ = source[source.index('async def sect_open_join_'):source.index('@sect_close_mountain.handle')]
        self.assertIn('sect_application.close_join(', close)
        self.assertIn('sect_application.open_join(', open_)
        self.assertNotIn('_sect_close_join_service().close(', close)
        self.assertNotIn('_sect_open_join_service().open(', open_)
