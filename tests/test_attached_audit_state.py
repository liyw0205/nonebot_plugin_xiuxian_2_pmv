from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class AttachedAuditStateTests(unittest.TestCase):
    def test_unmigrated_state_has_nullable_metadata_flags(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = subprocess.run(
                [sys.executable, "scripts/audit_attached_accessory.py", "--game", str(root / "game.db"), "--player", str(root / "player.db")],
                check=True,
                capture_output=True,
                text=True,
            )
            payload = json.loads(result.stdout)
            migration = payload["migration"]
            self.assertIsNone(migration["name_valid"])
            self.assertIsNone(migration["checksum_valid"])
            self.assertIsNone(migration["metadata_valid"])


if __name__ == "__main__":
    unittest.main()
