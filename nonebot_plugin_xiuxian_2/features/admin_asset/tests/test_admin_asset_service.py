from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import AdminAssetApplication


class AdminAssetServiceTests(unittest.TestCase):
    def test_application_rejects_zero_delta(self):
        with tempfile.TemporaryDirectory() as directory:
            app = AdminAssetApplication(Path(directory) / "game.db", repository=object())
            with self.assertRaises(Exception):
                app.adjust_stone(
                    operation_id="admin-1",
                    operator_id="operator",
                    user_id="u",
                    expected_stone=1,
                    requested_delta=0,
                )


if __name__ == "__main__":
    unittest.main()
