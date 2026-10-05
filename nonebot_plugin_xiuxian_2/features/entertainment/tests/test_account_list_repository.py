from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ..repository import (
    MAX_ACCOUNT_LIST_FILE_BYTES,
    MAX_ACCOUNT_LIST_ROWS,
    MAX_INFO_TARGETS,
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

    def test_checkin_targets_are_bounded_selected_and_redacted(self):
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "accounts.json"
            state.write_text(
                json.dumps(
                    [
                        {
                            "api_user_id": "123",
                            "mode": "token",
                            "secret": "private-one",
                            "base_url": "https://one.test",
                        },
                        {
                            "api_user_id": "456",
                            "mode": "cookie",
                            "secret": "private-two",
                            "base_url": "https://two.test",
                        },
                    ]
                ),
                encoding="utf-8",
            )
            result = EntertainmentRepository(Path(temp) / "game.db").resolve_checkin_targets(state, "2,1-2")

            self.assertEqual(result.status, "ok")
            self.assertEqual([target.index for target in result.targets], [1, 2])
            self.assertEqual(result.targets[1].secret, "private-two")
            self.assertNotIn("private-two", repr(result))

    def test_checkin_rejects_huge_selector_ranges_without_expanding_them(self):
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "accounts.json"
            state.write_text('[{"api_user_id":"123","secret":"secret","base_url":"https://one.test"}]')

            result = EntertainmentRepository(Path(temp) / "game.db").resolve_checkin_targets(state, "1-999999999")

            self.assertEqual(result.status, "invalid_selector")
        self.assertIn("最多选择", result.message)

    def test_info_targets_are_bounded_and_reuse_redacted_credential_dto(self):
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "accounts.json"
            state.write_text(
                json.dumps(
                    [
                        {
                            "api_user_id": str(index + 1),
                            "mode": "token",
                            "secret": f"private-{index}",
                            "base_url": "https://api.test",
                        }
                        for index in range(MAX_INFO_TARGETS + 1)
                    ]
                ),
                encoding="utf-8",
            )
            repository = EntertainmentRepository(Path(temp) / "game.db")

            result = repository.resolve_info_targets(state, "")

            self.assertEqual(result.status, "too_many")
            self.assertIn(str(MAX_INFO_TARGETS), result.message)
            self.assertNotIn("private-", repr(result))

    def test_checkin_rejects_invalid_credential_file_without_mutating_or_backing_it_up(self):
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "accounts.json"
            state.write_text("not json", encoding="utf-8")

            result = EntertainmentRepository(Path(temp) / "game.db").resolve_checkin_targets(state, "")

            self.assertEqual(result.status, "invalid")
            self.assertEqual(state.read_text(encoding="utf-8"), "not json")
            self.assertEqual(list(Path(temp).iterdir()), [state])

    def test_checkin_history_keeps_only_three_rows_and_rejects_oversized_legacy_state(self):
        with tempfile.TemporaryDirectory() as temp:
            history = Path(temp) / "history.json"
            repository = EntertainmentRepository(Path(temp) / "game.db")
            for index in range(5):
                repository.append_checkin_history(
                    history,
                    account_index=index + 1,
                    api_user_id="123",
                    base_url_stored="https://user:password@one.test/path?token=private#fragment",
                    summary=f"result-{index}",
                )
            rows = json.loads(history.read_text(encoding="utf-8"))
            self.assertEqual([row["summary"] for row in rows], ["result-4", "result-3", "result-2"])
            self.assertEqual(rows[0]["base_url"], "https://one.test/path")

            original = b"[" + b" " * 65 * 1024
            history.write_bytes(original)
            with self.assertRaises(ValueError):
                repository.append_checkin_history(
                    history,
                    account_index=1,
                    api_user_id="123",
                    base_url_stored="https://one.test",
                    summary="new result",
                )
            self.assertEqual(history.read_bytes(), original)
            self.assertEqual(list(Path(temp).glob("*.bak")), [])

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
