from __future__ import annotations

import unittest

from ..application import EmptyFallbackApplication
from ..manifest import FEATURE
from .. import commands, jobs, migrations, web


class FallbackSliceContractTests(unittest.TestCase):
    def test_manifest_and_surfaces_describe_an_ephemeral_adapter(self) -> None:
        self.assertEqual(FEATURE.key, "fallback")
        self.assertEqual(FEATURE.owner, "messaging")
        self.assertEqual(FEATURE.test_tag, "fallback")
        self.assertEqual(commands.COMMANDS, ())
        self.assertEqual(jobs.JOBS, ())
        self.assertEqual(migrations.MIGRATIONS, ())
        self.assertEqual(web.ROUTES, ())
        self.assertTrue(hasattr(EmptyFallbackApplication, "should_respond"))
        self.assertTrue(hasattr(EmptyFallbackApplication, "respond"))


if __name__ == "__main__":
    unittest.main()
