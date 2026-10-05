from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..repository import EntertainmentRepository
from .newapi_fixtures import migrate_newapi_state


class EntertainmentBindRepositoryTests(unittest.TestCase):
    def test_bind_commits_account_and_ledger_together_and_replays(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            accounts_dir = root / "bindings"
            history_dir = root / "history"
            accounts_dir.mkdir()
            history_dir.mkdir()
            (accounts_dir / "u.json").write_text("[]", encoding="utf-8")
            database = root / "game.db"
            migrate_newapi_state(database, accounts_dir, history_dir)
            repository = EntertainmentRepository(database)

            first = repository.bind_account(
                "bind-1",
                "u",
                mode="token",
                api_user_id="123",
                secret="private-token",
                base_url="https://api.test",
            )
            replay = repository.bind_account(
                "bind-1",
                "u",
                mode="token",
                api_user_id="123",
                secret="private-token",
                base_url="https://api.test",
            )

            targets = repository.resolve_checkin_targets("u", "").targets
            self.assertEqual(first.status, "applied")
            self.assertTrue(replay.replayed)
            self.assertEqual(len(targets), 1)
            self.assertEqual(targets[0].secret, "private-token")
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                ledger = uow.query_one(
                    "SELECT request_hash,result_json FROM operation_ledger WHERE operation_id='bind-1'"
                )
            self.assertNotIn("private-token", ledger["request_hash"])
            self.assertNotIn("private-token", ledger["result_json"])

    def test_binding_rejects_duplicate_and_row_limit(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            accounts_dir = root / "bindings"
            history_dir = root / "history"
            accounts_dir.mkdir()
            history_dir.mkdir()
            accounts_dir.joinpath("u.json").write_text(
                json.dumps(
                    [
                        {
                            "api_user_id": str(index + 1),
                            "secret": "secret",
                            "base_url": f"https://api-{index}.test",
                        }
                        for index in range(48)
                    ]
                ),
                encoding="utf-8",
            )
            database = root / "game.db"
            migrate_newapi_state(database, accounts_dir, history_dir)
            repository = EntertainmentRepository(database)

            duplicate = repository.bind_account(
                "bind-duplicate", "u", mode="token", api_user_id="1", secret="new",
                base_url="https://api-0.test",
            )
            over_limit = repository.bind_account(
                "bind-over-limit", "u", mode="token", api_user_id="99", secret="new",
                base_url="https://other.test",
            )

            self.assertEqual(duplicate.status, "rejected")
            self.assertEqual(over_limit.status, "rejected")
            self.assertEqual(len(repository.resolve_checkin_targets("u", "").targets), 48)


if __name__ == "__main__":
    unittest.main()
