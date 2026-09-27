import json
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.json_document import JsonDocumentReader
from ..static_data import MapStaticDataProvider


class MapStaticDataProviderTests(unittest.TestCase):
    def test_each_load_reads_the_current_document_without_caching(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "地图.json"
            provider = MapStaticDataProvider(JsonDocumentReader(), path)
            path.write_text(json.dumps({"revision": 1}), encoding="utf-8")

            first = provider.load()
            path.write_text(json.dumps({"revision": 2}), encoding="utf-8")
            second = provider.load()

        self.assertEqual({"revision": 1}, first)
        self.assertEqual({"revision": 2}, second)
        self.assertIsNot(first, second)

    def test_missing_document_is_reported_by_reader(self):
        with tempfile.TemporaryDirectory() as directory:
            provider = MapStaticDataProvider(
                JsonDocumentReader(),
                Path(directory) / "地图.json",
            )

            with self.assertRaises(FileNotFoundError):
                provider.load()


if __name__ == "__main__":
    unittest.main()
