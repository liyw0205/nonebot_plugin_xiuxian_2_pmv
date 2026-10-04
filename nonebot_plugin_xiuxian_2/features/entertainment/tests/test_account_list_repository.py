from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ..repository import (
    MAX_ACCOUNT_LIST_FILE_BYTES,
    MAX_ACCOUNT_LIST_ROWS,
    EntertainmentRepository,
)


class EntertainmentAccountListRepositoryTests(unittest.TestCase):
    def test_returns_redacted_summary_without_mutating_persistent_json(self):
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "accounts.json"
            original = json.dumps(
                [
                    {
                        "api_user_id": "12345",
                        "mode": "cookie",
                        "secret": "session=private-value",
                        "base_url": "https://user:password@api.example.test/v1?key=private#fragment",
                        "label": "primary\naccount",
                        "auto_checkin": True,
                    }
                ]
            )
            state.write_text(original, encoding="utf-8")
            repository = EntertainmentRepository(Path(temp) / "game.db")

            result = repository.list_account_summaries(state)

            self.assertEqual(result.status, "ok")
            self.assertEqual(len(result.accounts), 1)
            summary = result.accounts[0]
            self.assertEqual(summary.api_user_id, "12345")
            self.assertEqual(summary.mode, "cookie")
            self.assertEqual(summary.base_url, "https://api.example.test/v1")
            self.assertEqual(summary.label, "primary account")
            self.assertTrue(summary.auto_checkin)
            self.assertNotIn("secret", repr(result))
            self.assertNotIn("private-value", repr(result))
            self.assertEqual(state.read_text(encoding="utf-8"), original)

    def test_missing_file_returns_empty_without_creating_it_or_its_parent(self):
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "missing" / "accounts.json"
            result = EntertainmentRepository(Path(temp) / "game.db").list_account_summaries(state)

            self.assertEqual(result.status, "missing")
            self.assertFalse(state.exists())
            self.assertFalse(state.parent.exists())

    def test_invalid_json_is_not_renamed_or_rewritten(self):
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "accounts.json"
            state.write_text("not json", encoding="utf-8")
            result = EntertainmentRepository(Path(temp) / "game.db").list_account_summaries(state)

            self.assertEqual(result.status, "invalid")
            self.assertEqual(state.read_text(encoding="utf-8"), "not json")
            self.assertEqual(list(Path(temp).iterdir()), [state])

    def test_file_byte_limit_is_checked_before_json_decode(self):
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "accounts.json"
            original = b"[" + b" " * MAX_ACCOUNT_LIST_FILE_BYTES
            state.write_bytes(original)
            result = EntertainmentRepository(Path(temp) / "game.db").list_account_summaries(state)

            self.assertEqual(result.status, "too_large")
            self.assertEqual(state.stat().st_size, len(original))

    def test_account_count_limit_rejects_without_truncating_or_writing(self):
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "accounts.json"
            original = json.dumps([{"api_user_id": str(index)} for index in range(MAX_ACCOUNT_LIST_ROWS + 1)])
            state.write_text(original, encoding="utf-8")
            result = EntertainmentRepository(Path(temp) / "game.db").list_account_summaries(state)

            self.assertEqual(result.status, "too_many")
            self.assertEqual(state.read_text(encoding="utf-8"), original)


if __name__ == "__main__":
    unittest.main()
