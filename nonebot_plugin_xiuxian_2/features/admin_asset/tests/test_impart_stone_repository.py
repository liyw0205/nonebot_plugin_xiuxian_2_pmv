import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import build_migrations, migrations_for_database
from ..application import AdminAssetApplication
from ..impart_stone_repository import AdminImpartStoneSqlRepository
from ..migrations import apply_admin_impart_stone_operations, apply_admin_stone_adjustment
from tests.test_db_backend import db_backend


class AdminImpartStoneRepositoryTests(unittest.TestCase):
    def _databases(self, directory: str) -> tuple[Path, Path]:
        game = Path(directory) / "game.db"
        impart = Path(directory) / "impart.db"
        with db_backend.transaction(game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',900)")
        with DatabaseUnitOfWork(game) as uow:
            apply_admin_stone_adjustment(uow)
            apply_admin_impart_stone_operations(uow)
        with db_backend.transaction(impart) as conn:
            conn.execute("CREATE TABLE xiuxian_impart(user_id TEXT,stone_num INTEGER)")
            conn.execute("INSERT INTO xiuxian_impart VALUES('u',8)")
        return game, impart

    def test_game_receipt_migration_is_game_owned_and_keeps_legacy_receipts(self):
        migrations = build_migrations()
        versions = {
            item.version
            for item in migrations_for_database(migrations, "game_db")
        }
        self.assertIn("admin_asset.008", versions)
        self.assertNotIn(
            "admin_asset.008",
            {item.version for item in migrations_for_database(migrations, "impart_db")},
        )

        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            with db_backend.transaction(game) as conn:
                conn.execute(
                    "CREATE TABLE admin_impart_stone_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                    "previous_stone INTEGER NOT NULL,final_stone INTEGER NOT NULL,"
                    "applied_delta INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                conn.execute(
                    "INSERT INTO admin_impart_stone_operations("
                    "operation_id,payload,previous_stone,final_stone,applied_delta) "
                    "VALUES('old','[\"op\",\"u\",3]',4,7,3)"
                )
            with DatabaseUnitOfWork(game) as uow:
                apply_admin_impart_stone_operations(uow)
            with db_backend.connection(game) as conn:
                self.assertEqual(
                    conn.execute(
                        "SELECT final_stone FROM admin_impart_stone_operations WHERE operation_id='old'"
                    ).fetchone()[0],
                    7,
                )

    def test_adjusts_impart_balance_and_replays_legacy_receipt(self):
        with tempfile.TemporaryDirectory() as temp:
            game, impart = self._databases(temp)
            repo = AdminImpartStoneSqlRepository(game, impart)

            snapshot = repo.snapshot("u")
            first = repo.adjust("a1", "op", "u", snapshot.stone, 5, target_name="道友")
            duplicate = repo.adjust("a1", "op", "u", 999, 5, target_name="道友")
            conflict = repo.adjust("a1", "op", "u", 13, 6, target_name="道友")

            self.assertEqual((snapshot.status, snapshot.stone), ("ok", 8))
            self.assertEqual((first.status, first.previous_stone, first.final_stone), ("adjusted", 8, 13))
            self.assertEqual((duplicate.status, duplicate.applied_delta), ("duplicate", 5))
            self.assertEqual(conflict.status, "operation_conflict")
            with db_backend.connection(game) as conn:
                self.assertEqual(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id='u'").fetchone()[0], 900)
                receipt = conn.execute(
                    "SELECT payload,applied_delta FROM admin_impart_stone_operations WHERE operation_id='a1'"
                ).fetchone()
                self.assertEqual(tuple(receipt), ('["op","u",5]', 5))
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM economy_log WHERE trace_id='a1'").fetchone()[0],
                    1,
                )
            with db_backend.connection(impart) as conn:
                self.assertEqual(conn.execute("SELECT stone_num FROM xiuxian_impart WHERE user_id='u'").fetchone()[0], 13)

    def test_missing_impart_row_uses_none_snapshot_and_subtraction_clamps(self):
        with tempfile.TemporaryDirectory() as temp:
            game, impart = self._databases(temp)
            with db_backend.transaction(impart) as conn:
                conn.execute("DELETE FROM xiuxian_impart WHERE user_id='u'")
            repo = AdminImpartStoneSqlRepository(game, impart)
            snapshot = repo.snapshot("u")
            self.assertEqual((snapshot.status, snapshot.stone), ("ok", None))

            result = repo.adjust("new-row", "op", "u", snapshot.stone, -9)
            self.assertEqual((result.status, result.final_stone, result.applied_delta), ("adjusted", 0, 0))
            with db_backend.connection(impart) as conn:
                self.assertEqual(conn.execute("SELECT stone_num FROM xiuxian_impart WHERE user_id='u'").fetchone()[0], 0)

    def test_application_preserves_absent_row_snapshot_and_returns_success(self):
        with tempfile.TemporaryDirectory() as temp:
            game, impart = self._databases(temp)
            with db_backend.transaction(impart) as conn:
                conn.execute("DELETE FROM xiuxian_impart WHERE user_id='u'")
            application = AdminAssetApplication(game)

            snapshot = application.snapshot_impart_stone("u", impart_database=impart)
            outcome = application.adjust_impart_stone(
                operation_id="application-create",
                operator_id="op",
                user_id="u",
                expected_stone=snapshot.stone,
                requested_delta=4,
                impart_database=impart,
            )

            self.assertEqual((snapshot.status, snapshot.stone), ("ok", None))
            self.assertEqual((outcome.status, outcome.ok), ("adjusted", True))
            self.assertEqual(outcome.data["final_stone"], 4)

    def test_snapshot_cas_and_duplicate_user_rows_fail_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            game, impart = self._databases(temp)
            repo = AdminImpartStoneSqlRepository(game, impart)
            stale = repo.adjust("stale", "op", "u", 7, 2)
            self.assertEqual((stale.status, stale.previous_stone), ("state_changed", 8))

            with db_backend.transaction(impart) as conn:
                conn.execute("INSERT INTO xiuxian_impart VALUES('u',8)")
            self.assertEqual(repo.snapshot("u").status, "invalid_state")
            self.assertEqual(repo.adjust("duplicate-user", "op", "u", 8, 2).status, "invalid_state")

    def test_missing_schema_and_files_are_not_created_or_repaired_on_request(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            impart = Path(temp) / "impart.db"
            with db_backend.transaction(game) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',900)")
            with db_backend.transaction(impart) as conn:
                conn.execute("CREATE TABLE xiuxian_impart(user_id TEXT,stone_num INTEGER)")
                conn.execute("INSERT INTO xiuxian_impart VALUES('u',8)")

            repo = AdminImpartStoneSqlRepository(game, impart)
            self.assertEqual(repo.adjust("not-ready", "op", "u", 8, 2).status, "schema_missing")
            with db_backend.connection(game) as conn:
                tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertEqual(tables, {"user_xiuxian"})
            self.assertEqual(repo.snapshot("u").stone, 8)

            missing_game = Path(temp) / "missing-game.db"
            missing_impart = Path(temp) / "missing-impart.db"
            missing_repo = AdminImpartStoneSqlRepository(missing_game, missing_impart)
            self.assertEqual(missing_repo.snapshot("u").status, "schema_missing")
            self.assertEqual(missing_repo.adjust("missing", "op", "u", None, 1).status, "schema_missing")
            self.assertFalse(missing_game.exists())
            self.assertFalse(missing_impart.exists())

    def test_late_failure_rolls_back_both_databases_and_audit(self):
        with tempfile.TemporaryDirectory() as temp:
            game, impart = self._databases(temp)
            with db_backend.transaction(game) as conn:
                conn.execute(
                    "CREATE TRIGGER reject_impart_receipt BEFORE INSERT "
                    "ON admin_impart_stone_operations "
                    "BEGIN SELECT RAISE(ABORT,'reject impart receipt'); END"
                )
            repo = AdminImpartStoneSqlRepository(game, impart)
            with self.assertRaisesRegex(sqlite3.IntegrityError, "reject impart receipt"):
                repo.adjust("late-failure", "op", "u", 8, 5)
            with db_backend.connection(impart) as conn:
                self.assertEqual(conn.execute("SELECT stone_num FROM xiuxian_impart WHERE user_id='u'").fetchone()[0], 8)
            with db_backend.connection(game) as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM economy_log").fetchone()[0], 0)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM admin_impart_stone_operations").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
