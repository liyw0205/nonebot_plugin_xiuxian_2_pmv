from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ..application import EntertainmentApplication
from .newapi_fixtures import migrate_newapi_state


class EntertainmentApplicationTest(unittest.TestCase):
    def _application(self, directory: Path, accounts: list[dict]):
        accounts_dir = directory / "bindings"
        history_dir = directory / "history"
        accounts_dir.mkdir()
        history_dir.mkdir()
        source = accounts_dir / "u.json"
        source.write_text(json.dumps(accounts), encoding="utf-8")
        database = directory / "game.db"
        migrate_newapi_state(database, accounts_dir, history_dir)
        return EntertainmentApplication(database), source

    def test_list_account_summaries_uses_migrated_feature_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app, source = self._application(
                Path(directory),
                [{"api_user_id": "42", "secret": "hidden", "base_url": "https://api.test"}],
            )
            original = source.read_text(encoding="utf-8")

            result = app.list_account_summaries(user_id="u")

            self.assertEqual(result.status, "ok")
            self.assertEqual(result.accounts[0].api_user_id, "42")
            self.assertEqual(source.read_text(encoding="utf-8"), original)

    def test_resolve_checkin_targets_returns_redacted_application_dto(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app, _ = self._application(
                Path(directory),
                [{"api_user_id": "42", "secret": "hidden", "base_url": "https://api.test"}],
            )

            result = app.resolve_checkin_targets(user_id="u", selector="")

            self.assertEqual(result.status, "ok")
            self.assertEqual(result.targets[0].api_user_id, "42")
            self.assertNotIn("hidden", repr(result))

    def test_info_targets_use_feature_owned_query(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app, _ = self._application(
                Path(directory),
                [{"api_user_id": "42", "secret": "hidden", "base_url": "https://api.test"}],
            )

            result = app.resolve_info_targets(user_id="u", selector="")

            self.assertEqual(result.status, "ok")
            self.assertEqual(result.targets[0].api_user_id, "42")
            self.assertNotIn("hidden", repr(result))

    def test_execute_is_idempotent_for_the_shared_application_executor(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app, _ = self._application(Path(directory), [])
            first = app.execute(operation_id="op-1", user_id="u")
            second = app.execute(operation_id="op-1", user_id="u")
            self.assertEqual(first.operation_id, second.operation_id)
            self.assertTrue(second.replayed)


if __name__ == "__main__":
    unittest.main()
