from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import AccessoryPackageApplication


class AccessoryPackageFeatureContractTests(unittest.TestCase):
    def test_application_can_be_constructed_without_initializing_transport(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            application = AccessoryPackageApplication(Path(directory) / "game.db", Path(directory) / "player.db")
            self.assertEqual(application.action, "accessory_package.open")


if __name__ == "__main__":
    unittest.main()
