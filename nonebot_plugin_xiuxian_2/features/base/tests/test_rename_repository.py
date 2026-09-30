import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_base_player_rename_operations
from ..rename_repository import BaseRenameSqlRepository
from tests.test_db_backend import db_backend


class BaseRenameRepositoryTests(unittest.TestCase):
    @staticmethod
    def _create_player_tables(database: Path) -> None:
        with db_backend.transaction(database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT,root TEXT,stone INTEGER)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES('u','旧名','旧根',100)")
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))"
            )
            conn.execute("INSERT INTO back VALUES('u',20011,1,1)")

    @staticmethod
    def _apply_migration(database: Path) -> None:
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            apply_base_player_rename_operations(uow)

    def test_user_rename_applies_and_replays(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._create_player_tables(database)
            self._apply_migration(database)
            repo = BaseRenameSqlRepository(database)
            first = repo.rename("r1", "u", "user", "新名", stone_cost=10)
            duplicate = repo.rename("r1", "u", "user", "新名", stone_cost=10)
            self.assertEqual((first.status, duplicate.status), ("renamed", "duplicate"))
            replay = repo.get_result("r1")
            self.assertEqual((replay.status, replay.new_name, replay.previous_name), ("duplicate", "新名", "旧名"))

    def test_legacy_receipt_migration_adds_payload_without_losing_replay(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._create_player_tables(database)
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE player_rename_operations("
                    "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,rename_type TEXT NOT NULL,"
                    "new_name TEXT NOT NULL,previous_name TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                conn.execute(
                    "INSERT INTO player_rename_operations(operation_id,user_id,rename_type,new_name,previous_name) "
                    "VALUES('old-op','u','user_name','历史新名','旧名')"
                )

            self._apply_migration(database)
            result = BaseRenameSqlRepository(database).get_result("old-op")

            self.assertEqual((result.status, result.new_name, result.previous_name), ("duplicate", "历史新名", "旧名"))
            with db_backend.connection(database) as conn:
                columns = {row[1] for row in conn.execute("PRAGMA table_info(player_rename_operations)")}
                count = conn.execute("SELECT COUNT(*) FROM player_rename_operations").fetchone()[0]
            self.assertIn("payload", columns)
            self.assertEqual(count, 1)

    def test_missing_startup_migration_fails_closed_without_request_ddl(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._create_player_tables(database)

            result = BaseRenameSqlRepository(database).rename(
                "not-migrated", "u", "user", "新名", stone_cost=10
            )

            self.assertEqual(result.status, "schema_missing")
            with db_backend.connection(database) as conn:
                table = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='player_rename_operations'"
                ).fetchone()
                name = conn.execute("SELECT user_name FROM user_xiuxian WHERE user_id='u'").fetchone()[0]
            self.assertIsNone(table)
            self.assertEqual(name, "旧名")
