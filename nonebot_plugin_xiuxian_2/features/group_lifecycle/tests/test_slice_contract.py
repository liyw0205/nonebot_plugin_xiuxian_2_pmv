import unittest

from ..manifest import FEATURE
from ..web import LEGACY_ROUTES, ROUTES


class GroupLifecycleSliceContractTests(unittest.TestCase):
    def test_manifest_and_surfaces(self):
        self.assertEqual(FEATURE.key, "group_lifecycle")
        self.assertEqual((FEATURE.commands, FEATURE.jobs, FEATURE.routes, FEATURE.config), ((), (), (), ()))
        self.assertEqual(ROUTES, ())
        self.assertEqual(LEGACY_ROUTES, ())


if __name__ == "__main__":
    unittest.main()
