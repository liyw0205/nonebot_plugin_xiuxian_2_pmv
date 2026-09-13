from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..application import SignInApplication


class _Random:
    @staticmethod
    def randint(lower: int, upper: int) -> int:
        return 25


class SignInApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "game.db"
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("CREATE TABLE user_xiuxian (user_id TEXT, is_sign INTEGER DEFAULT 0, stone INTEGER DEFAULT 0)")
            uow.execute("INSERT INTO user_xiuxian(user_id, is_sign, stone) VALUES (?, ?, ?)", ("u1", 0, 100))
            uow.execute("INSERT INTO user_xiuxian(user_id, is_sign, stone) VALUES (?, ?, ?)", ("u2", 1, 200))
        self.application = SignInApplication(self.database, random_source=_Random())

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_success_and_same_operation_replay_without_double_grant(self) -> None:
        first = self.application.claim(user_id="u1", operation_id="op-1", lower_limit=10, upper_limit=30)
        replay = self.application.claim(user_id="u1", operation_id="op-1", lower_limit=10, upper_limit=30)
        self.assertTrue(first.ok)
        self.assertEqual(first.granted["stone"], 25)
        self.assertEqual(first.before["stone"], 100)
        self.assertEqual(first.after["stone"], 125)
        self.assertEqual(replay.status, "replayed")
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one("SELECT is_sign, stone FROM user_xiuxian WHERE user_id = ?", ("u1",))
            self.assertEqual((row["is_sign"], row["stone"]), (1, 125))

    def test_rejections_do_not_create_sign_operation(self) -> None:
        signed = self.application.claim(user_id="u2", operation_id="op-signed", lower_limit=10, upper_limit=30)
        missing = self.application.claim(user_id="missing", operation_id="op-missing", lower_limit=10, upper_limit=30)
        self.assertEqual(signed.code, "already_signed")
        self.assertEqual(missing.code, "user_missing")
        with DatabaseUnitOfWork(self.database) as uow:
            self.assertEqual(uow.query_one("SELECT COUNT(*) AS count FROM sign_in_operations")["count"], 0)

    def test_operation_failure_rolls_back_asset_mutation(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TABLE sign_in_operations (operation_id TEXT PRIMARY KEY, user_id TEXT, stone INTEGER)"
            )
            uow.execute(
                "CREATE TRIGGER fail_sign_operation BEFORE INSERT ON sign_in_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )
        with self.assertRaises(Exception):
            self.application.claim(user_id="u1", operation_id="op-fail", lower_limit=10, upper_limit=30)
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one("SELECT is_sign, stone FROM user_xiuxian WHERE user_id = ?", ("u1",))
            self.assertEqual((row["is_sign"], row["stone"]), (0, 100))


if __name__ == "__main__":
    unittest.main()
