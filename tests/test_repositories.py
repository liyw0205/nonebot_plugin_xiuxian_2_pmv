from __future__ import annotations

import importlib.util
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path

from tests.test_db_backend import db_backend


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "nonebot_plugin_xiuxian_2"
    / "xiuxian"
    / "xiuxian_utils"
    / "repositories.py"
)
SPEC = importlib.util.spec_from_file_location("xiuxian_repositories", MODULE_PATH)
if SPEC is None or SPEC.loader is None:  # pragma: no cover
    raise RuntimeError(f"Unable to load {MODULE_PATH}")
repositories = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(repositories)


class RepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "repository.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                """
                CREATE TABLE user_xiuxian (
                    user_id TEXT PRIMARY KEY,
                    user_name TEXT,
                    stone INTEGER,
                    exp INTEGER
                )
                """
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s, %s)",
                ("1001", "测试道友", 100, 50),
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s, %s)",
                ("1002", "另一位道友", 20, 10),
            )
        self.logs: list[dict] = []

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    @contextmanager
    def connection(self):
        conn = db_backend.connect(self.database)
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def read_query(self, sql, params=None, *, one=False, dict_row=False, **_):
        with db_backend.connection(self.database) as conn:
            cur = conn.execute(sql, params or ())
            row = cur.fetchone() if one else cur.fetchall()
            if not dict_row or row is None:
                return row
            columns = [column[0] for column in cur.description]
            if one:
                return dict(zip(columns, row))
            return [dict(zip(columns, item)) for item in row]

    def log_change(self, context, **payload):
        self.logs.append({"context": context, **payload})

    def test_user_repository_reads_by_id_and_name(self) -> None:
        repository = repositories.UserRepository(
            self.read_query,
            self.connection,
            lambda row, description: dict(
                zip((column[0] for column in description), row)
            ),
        )

        self.assertEqual(repository.get_by_id("1001")["user_name"], "测试道友")
        self.assertEqual(repository.get_by_name("测试道友")["user_id"], "1001")
        self.assertEqual(repository.get_with_attributes("1001")["stone"], 100)

    def test_economy_repository_updates_stones_and_experience(self) -> None:
        repository = repositories.EconomyRepository(
            self.connection,
            int,
            self.log_change,
        )

        repository.update_stones("1001", 30, 1, {"source": "test"})
        self.assertTrue(repository.try_update_stones("1001", 50, 2))
        self.assertFalse(repository.try_update_stones("1001", 1000, 2))
        repository.add_experience("1001", 25)
        repository.subtract_experience("1001", 1000)

        row = self.read_query(
            "SELECT stone, exp FROM user_xiuxian WHERE user_id=%s",
            ("1001",),
            one=True,
        )
        self.assertEqual(tuple(row), (80, 0))
        self.assertEqual(self.logs[0]["stone_delta"], 30)

    def test_stone_transfer_is_atomic_and_idempotent(self) -> None:
        repository = repositories.EconomyRepository(
            self.connection,
            int,
            self.log_change,
        )

        self.assertTrue(repository.transfer_stones("trade-1", "1001", "1002", 40))
        self.assertFalse(repository.transfer_stones("trade-1", "1001", "1002", 40))
        self.assertFalse(repository.transfer_stones("trade-2", "1001", "1002", 1000))

        sender = self.read_query(
            "SELECT stone FROM user_xiuxian WHERE user_id=%s", ("1001",), one=True
        )
        recipient = self.read_query(
            "SELECT stone FROM user_xiuxian WHERE user_id=%s", ("1002",), one=True
        )
        self.assertEqual(sender[0], 60)
        self.assertEqual(recipient[0], 60)

    def test_stone_transfer_rolls_back_when_recipient_is_missing(self) -> None:
        repository = repositories.EconomyRepository(
            self.connection,
            int,
            self.log_change,
        )
        with self.assertRaises(ValueError):
            repository.transfer_stones("trade-missing", "1001", "missing", 40)
        sender = self.read_query(
            "SELECT stone FROM user_xiuxian WHERE user_id=%s", ("1001",), one=True
        )
        self.assertEqual(sender[0], 100)


if __name__ == "__main__":
    unittest.main()
