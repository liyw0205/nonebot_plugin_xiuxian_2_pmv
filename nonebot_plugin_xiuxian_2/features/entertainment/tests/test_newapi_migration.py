from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_entertainment, apply_entertainment_newapi
from ..newapi_policy import MAX_ACCOUNT_LIST_FILE_BYTES
from .newapi_fixtures import migrate_newapi_state


class EntertainmentNewApiMigrationTests(unittest.TestCase):
    def test_imports_accounts_and_history_once_without_removing_source(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            accounts_dir = root / "bindings"
            history_dir = root / "history"
            accounts_dir.mkdir()
            history_dir.mkdir()
            account_path = accounts_dir / "123.json"
            history_path = history_dir / "123.json"
            account_path.write_text(
                json.dumps(
                    [
                        {
                            "api_user_id": "42",
                            "secret": "token-value",
                            "base_url": "https://api.test",
                            "custom_field": {"keep": True},
                        }
                    ]
                ),
                encoding="utf-8",
            )
            history_path.write_text(
                json.dumps(
                    [
                        {
                            "at": "2026-10-04 12:00:00",
                            "index": 1,
                            "api_user_id": "42",
                            "base_url": "https://user:password@api.test/path?key=private",
                            "summary": "ok",
                            "source": "auto",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            database = root / "game.db"

            migrate_newapi_state(database, accounts_dir, history_dir)
            account_path.write_text("[]", encoding="utf-8")
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                apply_entertainment_newapi(
                    uow,
                    accounts_directory=accounts_dir,
                    history_directory=history_dir,
                )
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                account = uow.query_one(
                    "SELECT api_user_id,secret,extra_json FROM entertainment_newapi_accounts WHERE user_id='123'"
                )
                history = uow.query_one(
                    "SELECT base_url,source FROM entertainment_newapi_checkin_history WHERE user_id='123'"
                )
                receipt = uow.query_one(
                    "SELECT account_files,history_files,account_rows,history_rows FROM entertainment_newapi_migrations"
                )

            self.assertEqual(account["api_user_id"], "42")
            self.assertEqual(account["secret"], "token-value")
            self.assertEqual(json.loads(account["extra_json"]), {"custom_field": {"keep": True}})
            self.assertEqual(history["base_url"], "https://api.test/path")
            self.assertEqual(history["source"], "auto")
            self.assertEqual(tuple(receipt.values()), (1, 1, 1, 1))
            self.assertTrue(account_path.exists())
            self.assertTrue(history_path.exists())

    def test_invalid_snapshot_aborts_migration_without_rewriting_source(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            accounts_dir = root / "bindings"
            history_dir = root / "history"
            accounts_dir.mkdir()
            history_dir.mkdir()
            source = accounts_dir / "123.json"
            source.write_text("not json", encoding="utf-8")
            database = root / "game.db"

            with self.assertRaisesRegex(ValueError, "invalid JSON"):
                with DatabaseUnitOfWork(database, immediate=True) as uow:
                    apply_entertainment(uow)
                    apply_entertainment_newapi(
                        uow,
                        accounts_directory=accounts_dir,
                        history_directory=history_dir,
                    )

            self.assertEqual(source.read_text(encoding="utf-8"), "not json")
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                tables = {
                    row["name"]
                    for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
                }
            self.assertNotIn("entertainment_newapi_accounts", tables)

    def test_oversized_snapshot_fails_before_import(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            accounts_dir = root / "bindings"
            history_dir = root / "history"
            accounts_dir.mkdir()
            history_dir.mkdir()
            source = accounts_dir / "123.json"
            source.write_bytes(b"[" + b" " * MAX_ACCOUNT_LIST_FILE_BYTES)
            database = root / "game.db"

            with self.assertRaisesRegex(ValueError, "per-file migration limit"):
                with DatabaseUnitOfWork(database, immediate=True) as uow:
                    apply_entertainment(uow)
                    apply_entertainment_newapi(
                        uow,
                        accounts_directory=accounts_dir,
                        history_directory=history_dir,
                    )
            self.assertEqual(source.stat().st_size, MAX_ACCOUNT_LIST_FILE_BYTES + 1)


if __name__ == "__main__":
    unittest.main()
