import unittest

from ..manifest import FEATURE
from ..web import LEGACY_ROUTES, ROUTES


class QqBindSliceContractTests(unittest.TestCase):
    def test_manifest_and_routes(self):
        self.assertEqual(FEATURE.key, "qq_bind")
        self.assertEqual((FEATURE.commands, FEATURE.jobs, FEATURE.routes, FEATURE.config), ((), (), (), ()))
        self.assertEqual(ROUTES, ())
        self.assertEqual(len(LEGACY_ROUTES), 5)


if __name__ == "__main__":
    unittest.main()
