from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ..repository import EntertainmentRepository
from .newapi_fixtures import migrate_newapi_state


def _repository(root: Path) -> EntertainmentRepository:
    accounts_dir = root / "bindings"
    history_dir = root / "history"
    accounts_dir.mkdir()
    history_dir.mkdir()
    (accounts_dir / "u.json").write_text(
        json.dumps(
            [
                {"api_user_id": "11", "auto_checkin": False, "secret": "hidden", "base_url": "https://one.test"},
                {"api_user_id": "22", "auto_checkin": True, "secret": "hidden2", "base_url": "https://two.test"},
            ]
        ),
        encoding="utf-8",
    )
    database = root / "game.db"
    migrate_newapi_state(database, accounts_dir, history_dir)
    return EntertainmentRepository(database)


class EntertainmentAutoCheckinRepositoryTests(unittest.TestCase):
    def test_toggle_auto_checkin_is_atomic_and_replay_does_not_flip_back(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            repository = _repository(root)
            target = repository.resolve_checkin_targets("u", "1").targets[0]

            first = repository.toggle_auto_checkin(
                "toggle-1", "u", target.account_id, 1, legacy_state_path="legacy/u.json"
            )
            replay = repository.toggle_auto_checkin(
                "toggle-1", "u", target.account_id, 1, legacy_state_path="legacy/u.json"
            )
            automatic = repository.list_auto_checkin_targets()

            self.assertEqual(first.status, "applied")
            self.assertTrue(replay.replayed)
            self.assertTrue(first.data["enabled"])
            self.assertEqual([entry[2].api_user_id for entry in automatic], ["11", "22"])

    def test_toggle_rejects_missing_account_without_changing_other_accounts(self):
        with tempfile.TemporaryDirectory() as temp:
            repository = _repository(Path(temp))

            result = repository.toggle_auto_checkin(
                "toggle-missing", "u", 999, 9, legacy_state_path="legacy/u.json"
            )
            targets = repository.resolve_checkin_targets("u", "").targets

            self.assertEqual(result.status, "rejected")
            self.assertEqual([target.api_user_id for target in targets], ["11", "22"])

    def test_pre_migration_started_toggle_is_not_reapplied(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            repository = _repository(root)
            target = repository.resolve_checkin_targets("u", "1").targets[0]
            payload = {"state_path": "legacy/u.json", "index": 1, "user_id": "u"}
            with DatabaseUnitOfWork(root / "game.db", immediate=True) as uow:
                OperationLedger().begin(uow, "old-toggle", "entertainment.toggle_auto_checkin", payload)

            result = repository.toggle_auto_checkin(
                "old-toggle", "u", target.account_id, 1, legacy_state_path="legacy/u.json"
            )
            targets = repository.resolve_checkin_targets("u", "").targets

            self.assertEqual(result.status, "rejected")
            self.assertEqual(len(targets), 2)
            self.assertIn("自动重放", result.message)
            self.assertEqual([entry[2].api_user_id for entry in repository.list_auto_checkin_targets()], ["22"])

    def test_scheduler_account_pages_are_stable_and_bounded(self):
        with tempfile.TemporaryDirectory() as temp:
            repository = _repository(Path(temp))
            target = repository.resolve_checkin_targets("u", "1").targets[0]
            repository.toggle_auto_checkin(
                "toggle-page", "u", target.account_id, 1, legacy_state_path="legacy/u.json"
            )
            first = repository.list_auto_checkin_targets(limit=1)
            second = repository.list_auto_checkin_targets(after_account_id=first[-1][0], limit=1)

            self.assertEqual(len(first), 1)
            self.assertEqual(len(second), 1)
            self.assertEqual(first[0][2].api_user_id, "11")
            self.assertEqual(second[0][2].api_user_id, "22")


if __name__ == "__main__":
    unittest.main()
