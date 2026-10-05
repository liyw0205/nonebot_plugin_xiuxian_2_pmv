from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ..newapi_policy import MAX_INFO_TARGETS
from ..repository import EntertainmentRepository
from .newapi_fixtures import migrate_newapi_state


def _state(root: Path, accounts: list[dict], history: list[dict] | None = None):
    accounts_dir = root / "bindings"
    history_dir = root / "history"
    accounts_dir.mkdir()
    history_dir.mkdir()
    (accounts_dir / "123.json").write_text(json.dumps(accounts), encoding="utf-8")
    if history is not None:
        (history_dir / "123.json").write_text(json.dumps(history), encoding="utf-8")
    database = root / "game.db"
    migrate_newapi_state(database, accounts_dir, history_dir)
    return database, accounts_dir, history_dir


class EntertainmentAccountListRepositoryTests(unittest.TestCase):
    def test_sql_summary_is_redacted_and_legacy_source_is_retained(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = [
                {
                    "api_user_id": "12345",
                    "mode": "cookie",
                    "secret": "session=private-value",
                    "base_url": "https://user:password@api.example.test/v1?key=private#fragment",
                    "label": "primary\naccount",
                    "auto_checkin": True,
                    "legacy_extension": "kept",
                }
            ]
            database, accounts_dir, _ = _state(root, raw)
            original = (accounts_dir / "123.json").read_text(encoding="utf-8")
            repository = EntertainmentRepository(database)

            result = repository.list_account_summaries("123")

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
            self.assertEqual((accounts_dir / "123.json").read_text(encoding="utf-8"), original)
            self.assertEqual(repository.resolve_checkin_targets("123", "").targets[0].secret, "session=private-value")

    def test_checkin_targets_are_bounded_selected_and_redacted(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            accounts = [
                {"api_user_id": "123", "mode": "token", "secret": "private-one", "base_url": "https://one.test"},
                {"api_user_id": "456", "mode": "cookie", "secret": "private-two", "base_url": "https://two.test"},
            ]
            database, _, _ = _state(root, accounts)

            result = EntertainmentRepository(database).resolve_checkin_targets("123", "2,1-2")

            self.assertEqual(result.status, "ok")
            self.assertEqual([target.index for target in result.targets], [1, 2])
            self.assertEqual(result.targets[1].secret, "private-two")
            self.assertNotIn("private-two", repr(result))
            self.assertGreater(result.targets[0].account_id, 0)

    def test_checkin_rejects_huge_selector_ranges_without_expanding_them(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database, _, _ = _state(
                root,
                [{"api_user_id": "123", "secret": "secret", "base_url": "https://one.test"}],
            )

            result = EntertainmentRepository(database).resolve_checkin_targets("123", "1-999999999")

            self.assertEqual(result.status, "invalid_selector")
            self.assertIn("最多选择", result.message)

    def test_info_targets_respect_the_smaller_remote_query_limit(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            accounts = [
                {
                    "api_user_id": str(index + 1),
                    "mode": "token",
                    "secret": f"private-{index}",
                    "base_url": "https://api.test",
                }
                for index in range(MAX_INFO_TARGETS + 1)
            ]
            database, _, _ = _state(root, accounts)

            result = EntertainmentRepository(database).resolve_info_targets("123", "")

            self.assertEqual(result.status, "too_many")
            self.assertIn(str(MAX_INFO_TARGETS), result.message)
            self.assertNotIn("private-", repr(result))

    def test_history_keeps_three_rows_and_never_stores_url_credentials(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database, _, _ = _state(root, [])
            repository = EntertainmentRepository(database)
            for index in range(5):
                repository.append_checkin_history(
                    "123",
                    account_index=index + 1,
                    api_user_id="123",
                    base_url_stored="https://user:password@one.test/path?token=private#fragment",
                    summary=f"result-{index}",
                )

            rows = repository.list_checkin_history("123")

            self.assertEqual([row["summary"] for row in rows], ["result-4", "result-3", "result-2"])
            self.assertEqual(rows[0]["base_url"], "https://one.test/path")
            self.assertNotIn("password", repr(rows))
            self.assertNotIn("private", repr(rows))

    def test_unmigrated_database_is_read_only_and_reports_missing(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            result = EntertainmentRepository(database).list_account_summaries("123")
            self.assertEqual(result.status, "missing")
            self.assertFalse(database.exists())


if __name__ == "__main__":
    unittest.main()
