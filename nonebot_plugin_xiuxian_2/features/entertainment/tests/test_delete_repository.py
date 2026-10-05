from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ..repository import EntertainmentRepository
from .newapi_fixtures import migrate_newapi_state


def _repository(root: Path, accounts: list[dict]) -> EntertainmentRepository:
    accounts_dir = root / "bindings"
    history_dir = root / "history"
    accounts_dir.mkdir()
    history_dir.mkdir()
    (accounts_dir / "u.json").write_text(json.dumps(accounts), encoding="utf-8")
    database = root / "game.db"
    migrate_newapi_state(database, accounts_dir, history_dir)
    return EntertainmentRepository(database)


class EntertainmentDeleteRepositoryTests(unittest.TestCase):
    def test_delete_selected_accounts_keeps_other_rows_and_secrets(self):
        with tempfile.TemporaryDirectory() as temp:
            repository = _repository(
                Path(temp),
                [
                    {"api_user_id": "11", "secret": "a", "base_url": "https://one.test"},
                    {"api_user_id": "22", "secret": "b", "base_url": "https://two.test", "extra": "kept"},
                ],
            )

            result = repository.delete_accounts("delete-1", "u", [1], legacy_state_path="legacy/u.json")
            accounts = repository.resolve_checkin_targets("u", "").targets

            self.assertEqual(result.status, "applied")
            self.assertEqual(len(accounts), 1)
            self.assertEqual(accounts[0].api_user_id, "22")
            self.assertEqual(accounts[0].secret, "b")

    def test_same_operation_replay_does_not_delete_next_account(self):
        with tempfile.TemporaryDirectory() as temp:
            repository = _repository(
                Path(temp),
                [
                    {"api_user_id": "11", "secret": "a", "base_url": "https://one.test"},
                    {"api_user_id": "22", "secret": "b", "base_url": "https://two.test"},
                    {"api_user_id": "33", "secret": "c", "base_url": "https://three.test"},
                ],
            )

            first = repository.delete_accounts("delete-1", "u", [2], legacy_state_path="legacy/u.json")
            replay = repository.delete_accounts("delete-1", "u", [2], legacy_state_path="legacy/u.json")
            accounts = repository.resolve_checkin_targets("u", "").targets

            self.assertEqual(first.status, "applied")
            self.assertTrue(replay.replayed)
            self.assertEqual([account.api_user_id for account in accounts], ["11", "33"])

    def test_delete_all_and_invalid_indices_are_transactional(self):
        with tempfile.TemporaryDirectory() as temp:
            repository = _repository(
                Path(temp),
                [{"api_user_id": "11", "secret": "a", "base_url": "https://one.test"}],
            )
            rejected = repository.delete_accounts("delete-bad", "u", [2], legacy_state_path="legacy/u.json")
            remaining = repository.resolve_checkin_targets("u", "").targets
            deleted = repository.delete_accounts("delete-all", "u", None, legacy_state_path="legacy/u.json")

            self.assertEqual(rejected.status, "rejected")
            self.assertEqual(len(remaining), 1)
            self.assertEqual(deleted.status, "applied")
            self.assertEqual(repository.resolve_checkin_targets("u", "").status, "empty")

    def test_old_started_json_delete_is_not_blindly_replayed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            repository = _repository(
                root,
                [
                    {"api_user_id": "11", "secret": "a", "base_url": "https://one.test"},
                    {"api_user_id": "22", "secret": "b", "base_url": "https://two.test"},
                ],
            )
            payload = {"state_path": "legacy/u.json", "indices": [1], "user_id": "u"}
            with DatabaseUnitOfWork(root / "game.db", immediate=True) as uow:
                OperationLedger().begin(uow, "old-delete", "entertainment.delete_accounts", payload)

            result = repository.delete_accounts("old-delete", "u", [1], legacy_state_path="legacy/u.json")
            accounts = repository.resolve_checkin_targets("u", "").targets

            self.assertEqual(result.status, "rejected")
            self.assertEqual([account.api_user_id for account in accounts], ["11", "22"])
            self.assertIn("结果无法确认", result.message)


if __name__ == "__main__":
    unittest.main()
