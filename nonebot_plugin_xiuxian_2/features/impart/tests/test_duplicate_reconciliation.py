from __future__ import annotations

import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork, Migration, MigrationRunner
from ..migrations import (
    IMPART_DUPLICATE_ARCHIVE_TABLE,
    apply_impart_duplicate_row_reconciliation,
    restore_impart_duplicate_archive,
)


def reconciliation_runner(*, fail_after: bool = False) -> MigrationRunner:
    migrations = [
        Migration(
            "impart.012",
            "impart_duplicate_row_reconciliation",
            apply_impart_duplicate_row_reconciliation,
        )
    ]
    if fail_after:
        def fail(_uow):
            raise RuntimeError("injected startup migration failure")

        migrations.append(Migration("impart.013", "injected_failure", fail))
    return MigrationRunner(migrations)


class ImpartDuplicateReconciliationTests(unittest.TestCase):
    def _database(self, root: Path) -> Path:
        return root / "impart.db"

    def _seed_legacy_rows(self, database: Path) -> tuple[str, list[tuple]]:
        with DatabaseUnitOfWork(database) as uow:
            uow.execute(
                "CREATE TABLE xiuxian_impart(user_id TEXT,stone_num INTEGER,note TEXT)"
            )
            for rowid, user_id, stone_num, note in (
                (159, "actor", 670, "effective"),
                (619, "actor", 460, "older-copy"),
                (622, "actor", 310, "newer-copy"),
                (710, None, 3, "anonymous-null-a"),
                (711, None, 4, "anonymous-null-b"),
                (712, "", 5, "anonymous-empty-a"),
                (713, "   ", 6, "anonymous-empty-b"),
            ):
                uow.execute(
                    "INSERT INTO xiuxian_impart(rowid,user_id,stone_num,note) "
                    "VALUES(?,?,?,?)",
                    (rowid, user_id, stone_num, note),
                )
            schema = str(
                uow.query_one(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='xiuxian_impart'"
                )["sql"]
            )
            rows = [
                (row["row_id"], row["user_id"], row["stone_num"], row["note"])
                for row in uow.query_all(
                    "SELECT rowid AS row_id,user_id,stone_num,note "
                    "FROM xiuxian_impart ORDER BY rowid"
                )
            ]
        return schema, rows

    def _source_snapshot(self, database: Path) -> tuple[str, list[tuple]]:
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            schema_row = uow.query_one(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='xiuxian_impart'"
            )
            schema = str(schema_row["sql"])
            rows = [
                (row["row_id"], row["user_id"], row["stone_num"], row["note"])
                for row in uow.query_all(
                    "SELECT rowid AS row_id,user_id,stone_num,note "
                    "FROM xiuxian_impart ORDER BY rowid"
                )
            ]
        return schema, rows

    def _apply(self, database: Path, runner: MigrationRunner | None = None) -> None:
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            (runner or reconciliation_runner()).apply(uow)

    def test_startup_keeps_smallest_rowid_archives_conflicts_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            database = self._database(Path(temp))
            _schema, before = self._seed_legacy_rows(database)

            self._apply(database)
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                active = uow.query_all(
                    "SELECT rowid AS row_id,user_id,stone_num,note "
                    "FROM xiuxian_impart ORDER BY rowid"
                )
                archived = uow.query_all(
                    f'SELECT * FROM "{IMPART_DUPLICATE_ARCHIVE_TABLE}" '
                    'ORDER BY "__xiuxian_archive_source_rowid"'
                )
                applied = uow.query_one(
                    "SELECT version FROM schema_migrations WHERE version='impart.012'"
                )
                index = uow.query_one(
                    "SELECT sql FROM sqlite_master WHERE type='index' "
                    "AND name='xiuxian_impart_user_id_unique'"
                )
            self.assertEqual(
                [(r["row_id"], r["user_id"], r["stone_num"]) for r in active if r["user_id"] == "actor"],
                [(159, "actor", 670)],
            )
            self.assertEqual(
                [(r["row_id"], r["user_id"], r["stone_num"]) for r in active if r["user_id"] is None],
                [(710, None, 3), (711, None, 4)],
            )
            self.assertEqual(
                [(r["row_id"], r["user_id"], r["stone_num"]) for r in active if r["user_id"] is not None and not r["user_id"].strip()],
                [(712, "", 5), (713, "   ", 6)],
            )
            self.assertEqual(
                [
                    (r["__xiuxian_archive_source_rowid"], r["stone_num"], r["note"])
                    for r in archived
                ],
                [(619, 460, "older-copy"), (622, 310, "newer-copy")],
            )
            self.assertEqual(
                [r["__xiuxian_archive_effective_rowid"] for r in archived], [159, 159]
            )
            self.assertEqual(
                [r["__xiuxian_archive_classification"] for r in archived],
                ["divergent_history", "divergent_history"],
            )
            self.assertTrue(all(r["__xiuxian_archive_conflicting_fields"] for r in archived))
            self.assertEqual(applied["version"], "impart.012")
            self.assertIsNotNone(index)

            self._apply(database)
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(
                    uow.query_one(f'SELECT COUNT(*) AS n FROM "{IMPART_DUPLICATE_ARCHIVE_TABLE}"')["n"],
                    2,
                )

    def test_generated_columns_are_archived_and_compared(self):
        for storage in ("STORED", "VIRTUAL"):
            with self.subTest(storage=storage), tempfile.TemporaryDirectory() as temp:
                database = self._database(Path(temp))
                with DatabaseUnitOfWork(database) as uow:
                    uow.execute(
                        "CREATE TABLE xiuxian_impart("
                        "user_id TEXT,stone_num INTEGER,"
                        "display_key TEXT GENERATED ALWAYS AS (user_id || ':' || stone_num) "
                        + storage
                        + ")"
                    )
                    uow.execute(
                        "INSERT INTO xiuxian_impart(user_id,stone_num) "
                        "VALUES('actor',670),('actor',460)"
                    )

                self._apply(database)
                with DatabaseUnitOfWork(database, read_only=True) as uow:
                    active = uow.query_one("SELECT rowid,display_key FROM xiuxian_impart")
                    archived = uow.query_one(
                        f'SELECT display_key,"__xiuxian_archive_conflicting_fields" '
                        f'FROM "{IMPART_DUPLICATE_ARCHIVE_TABLE}"'
                    )
                self.assertEqual((active["rowid"], active["display_key"]), (1, "actor:670"))
                self.assertEqual(archived["display_key"], "actor:460")
                self.assertIn("display_key", archived["__xiuxian_archive_conflicting_fields"])

    def test_reserved_archive_columns_are_rejected_even_when_generated(self):
        declarations = (
            '"__xiuxian_archive_source_rowid" TEXT',
            '"__xiuxian_archive_source_rowid" TEXT GENERATED ALWAYS AS (user_id) STORED',
        )
        for declaration in declarations:
            with self.subTest(declaration=declaration), tempfile.TemporaryDirectory() as temp:
                database = self._database(Path(temp))
                with DatabaseUnitOfWork(database) as uow:
                    uow.execute(
                        "CREATE TABLE xiuxian_impart(user_id TEXT,stone_num INTEGER,note TEXT,"
                        + declaration
                        + ")"
                    )
                    uow.execute(
                        "INSERT INTO xiuxian_impart(user_id,stone_num,note) "
                        "VALUES('actor',670,'first'),('actor',460,'second')"
                    )
                before = self._source_snapshot(database)
                with self.assertRaisesRegex(RuntimeError, "reserved archive columns"):
                    self._apply(database)
                self.assertEqual(self._source_snapshot(database), before)
                with DatabaseUnitOfWork(database, read_only=True) as uow:
                    self.assertIsNone(
                        uow.query_one(
                            "SELECT 1 FROM sqlite_master WHERE type='table' "
                            "AND name=?",
                            (IMPART_DUPLICATE_ARCHIVE_TABLE,),
                        )
                    )
                    self.assertIsNone(
                        uow.query_one(
                            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations'"
                        )
                    )

    def test_archive_unique_failure_rolls_back_all_startup_changes(self):
        with tempfile.TemporaryDirectory() as temp:
            database = self._database(Path(temp))
            source_schema, source_rows = self._seed_legacy_rows(database)
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    f'CREATE TABLE "{IMPART_DUPLICATE_ARCHIVE_TABLE}" AS '
                    'SELECT rowid AS "__xiuxian_archive_source_rowid",* '
                    'FROM xiuxian_impart WHERE 0'
                )
                uow.execute(
                    f'CREATE UNIQUE INDEX archive_owner_unique ON "{IMPART_DUPLICATE_ARCHIVE_TABLE}"(user_id)'
                )
                archive_schema = uow.query_one(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                    (IMPART_DUPLICATE_ARCHIVE_TABLE,),
                )["sql"]
                archive_columns = [
                    row["name"]
                    for row in uow.query_all(
                        f'PRAGMA table_info("{IMPART_DUPLICATE_ARCHIVE_TABLE}")'
                    )
                ]

            with self.assertRaises(sqlite3.IntegrityError):
                self._apply(database)

            self.assertEqual(self._source_snapshot(database), (source_schema, source_rows))
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(
                    uow.query_one(
                        f'SELECT COUNT(*) AS n FROM "{IMPART_DUPLICATE_ARCHIVE_TABLE}"'
                    )["n"],
                    0,
                )
                self.assertEqual(
                    uow.query_one(
                        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                        (IMPART_DUPLICATE_ARCHIVE_TABLE,),
                    )["sql"],
                    archive_schema,
                )
                self.assertEqual(
                    [
                        row["name"]
                        for row in uow.query_all(
                            f'PRAGMA table_info("{IMPART_DUPLICATE_ARCHIVE_TABLE}")'
                        )
                    ],
                    archive_columns,
                )
                self.assertIsNone(
                    uow.query_one(
                        "SELECT 1 FROM sqlite_master WHERE type='index' "
                        "AND name='xiuxian_impart_user_id_unique'"
                    )
                )
                self.assertIsNone(
                    uow.query_one(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations'"
                    )
                )

    def test_later_startup_failure_rolls_back_archive_index_and_migration_ledger(self):
        with tempfile.TemporaryDirectory() as temp:
            database = self._database(Path(temp))
            source_schema, source_rows = self._seed_legacy_rows(database)
            with self.assertRaisesRegex(RuntimeError, "injected startup migration failure"):
                self._apply(database, reconciliation_runner(fail_after=True))

            self.assertEqual(self._source_snapshot(database), (source_schema, source_rows))
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertIsNone(
                    uow.query_one(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                        (IMPART_DUPLICATE_ARCHIVE_TABLE,),
                    )
                )
                self.assertIsNone(
                    uow.query_one(
                        "SELECT 1 FROM sqlite_master WHERE type='index' "
                        "AND name='xiuxian_impart_user_id_unique'"
                    )
                )
                self.assertIsNone(
                    uow.query_one(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations'"
                    )
                )

    def test_explicit_recovery_restores_source_schema_rows_and_rowids(self):
        with tempfile.TemporaryDirectory() as temp:
            database = self._database(Path(temp))
            source_schema, source_rows = self._seed_legacy_rows(database)
            self._apply(database)

            with DatabaseUnitOfWork(database, immediate=True) as uow:
                restored_count = restore_impart_duplicate_archive(uow)
            self.assertEqual(restored_count, 2)
            self.assertEqual(self._source_snapshot(database), (source_schema, source_rows))

            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertIsNone(
                    uow.query_one(
                        "SELECT 1 FROM sqlite_master WHERE type='index' "
                        "AND name='xiuxian_impart_user_id_unique'"
                    )
                )
                self.assertIsNone(
                    uow.query_one(
                        "SELECT 1 FROM schema_migrations WHERE version='impart.012'"
                    )
                )
                self.assertEqual(
                    uow.query_one(
                        f'SELECT COUNT(*) AS n FROM "{IMPART_DUPLICATE_ARCHIVE_TABLE}" '
                        'WHERE "__xiuxian_archive_restored_at" IS NOT NULL'
                    )["n"],
                    2,
                )

    def test_formal_startup_route_then_crystal_draw_uses_effective_row(self):
        with tempfile.TemporaryDirectory() as temp:
            database = self._database(Path(temp))
            player = Path(temp) / "player.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE xiuxian_impart(user_id TEXT,stone_num INTEGER,wish INTEGER,exp_day INTEGER)"
                )
                uow.execute(
                    "INSERT INTO xiuxian_impart(rowid,user_id,stone_num,wish,exp_day) "
                    "VALUES(159,'actor',670,0,0),(619,'actor',460,9,5),(622,'actor',310,8,4)"
                )

            from ....plugin import build_migrations, migrations_for_database

            migrations = build_migrations()
            impart_runner = MigrationRunner(
                migrations_for_database(migrations, "impart_db")
            )
            player_runner = MigrationRunner(
                migrations_for_database(migrations, "player_db")
            )
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                impart_runner.apply(uow)
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                self.assertEqual(impart_runner.apply(uow), [])
            with DatabaseUnitOfWork(player, immediate=True) as uow:
                player_runner.apply(uow)

            with DatabaseUnitOfWork(database, read_only=True) as uow:
                active = uow.query_all(
                    "SELECT rowid,user_id,stone_num,stone_num_exact FROM xiuxian_impart ORDER BY rowid"
                )
                archive_count = uow.query_one(
                    f'SELECT COUNT(*) AS n FROM "{IMPART_DUPLICATE_ARCHIVE_TABLE}"'
                )["n"]
            self.assertEqual(
                [(row["rowid"], row["stone_num"]) for row in active], [(159, 670)]
            )
            self.assertEqual(archive_count, 2)

            from ..application import ImpartApplication

            app = ImpartApplication(
                database,
                impart_database=database,
                player_database=player,
            )
            result = app.crystal_draw(
                operation_id="startup-draw",
                user_id="actor",
                expected_stone=670,
                expected_wish=0,
                cost=10,
                new_wish=1,
                exp_minutes=5,
                cards=(),
                card_definitions={},
                statistics_user_id="actor",
                statistics={},
                requested_pulls=1,
            )
            self.assertEqual((result.status, result.stone_num), ("applied", 660))
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(
                    [row["stone_num_exact"] for row in uow.query_all(
                        "SELECT stone_num_exact FROM xiuxian_impart ORDER BY rowid"
                    )],
                    ["660"],
                )
                self.assertEqual(
                    uow.query_one(
                        f'SELECT COUNT(*) AS n FROM "{IMPART_DUPLICATE_ARCHIVE_TABLE}"'
                    )["n"],
                    2,
                )

    def test_concurrent_startup_creates_one_reconciliation_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            database = self._database(Path(temp))
            self._seed_legacy_rows(database)

            def start_once():
                with DatabaseUnitOfWork(database, immediate=True, timeout=10) as uow:
                    return reconciliation_runner().apply(uow)

            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(lambda _item: start_once(), (1, 2)))
            self.assertEqual(sorted(results), [[], ["impart.012"]])
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(
                    uow.query_one(
                        f'SELECT COUNT(*) AS n FROM "{IMPART_DUPLICATE_ARCHIVE_TABLE}"'
                    )["n"],
                    2,
                )
                self.assertEqual(
                    uow.query_one(
                        "SELECT COUNT(*) AS n FROM schema_migrations WHERE version='impart.012'"
                    )["n"],
                    1,
                )


if __name__ == "__main__":
    unittest.main()
