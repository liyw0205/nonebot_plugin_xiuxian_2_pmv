from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from scripts.remote_smoke_write import main


class RemoteSmokeWriteTests(unittest.TestCase):
    def test_marker_is_scoped_to_data_dir_and_records_operation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            previous = os.environ.get("XIUXIAN_DATA_DIR")
            os.environ["XIUXIAN_DATA_DIR"] = directory
            try:
                self.assertEqual(main(["--operation-id", "smoke-op"]), 0)
            finally:
                if previous is None:
                    os.environ.pop("XIUXIAN_DATA_DIR", None)
                else:
                    os.environ["XIUXIAN_DATA_DIR"] = previous
            marker = Path(directory) / "remote-smoke-marker.json"
            self.assertEqual(json.loads(marker.read_text(encoding="utf-8"))["operation_id"], "smoke-op")
            self.assertEqual(tuple(path.name for path in Path(directory).iterdir()), (marker.name,))


if __name__ == "__main__":
    unittest.main()
