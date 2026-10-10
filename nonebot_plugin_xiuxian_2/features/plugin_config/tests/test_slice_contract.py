import unittest

from ..manifest import FEATURE
from ..web import LEGACY_ROUTES, ROUTES


class PluginConfigSliceContractTests(unittest.TestCase):
    def test_manifest_and_surfaces(self):
        self.assertEqual(FEATURE.key, "plugin_config")
        self.assertEqual((FEATURE.commands, FEATURE.jobs, FEATURE.routes, FEATURE.config), ((), (), (), ()))
        self.assertEqual(ROUTES, ())
        self.assertEqual(len(LEGACY_ROUTES), 2)


if __name__ == "__main__":
    unittest.main()
