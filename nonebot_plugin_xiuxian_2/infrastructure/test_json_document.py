import tempfile
import unittest
from pathlib import Path

from .json_document import JsonDocumentReader


class JsonDocumentReaderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "document.json"
        self.reader = JsonDocumentReader()

    def tearDown(self):
        self.temp.cleanup()

    def test_reads_object(self):
        self.path.write_text('{"realm": {"nodes": []}}', encoding="utf-8")
        self.assertEqual({"realm": {"nodes": []}}, self.reader.read_object(self.path))

    def test_missing_document_fails(self):
        with self.assertRaises(FileNotFoundError):
            self.reader.read_object(self.path)

    def test_invalid_or_non_object_document_fails(self):
        self.path.write_text("not-json", encoding="utf-8")
        with self.assertRaises(ValueError):
            self.reader.read_object(self.path)
        self.path.write_text("[]", encoding="utf-8")
        with self.assertRaises(ValueError):
            self.reader.read_object(self.path)


if __name__ == "__main__":
    unittest.main()
