from __future__ import annotations

import hashlib
import inspect
import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.accessory_package import attached_migrations
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, Migration, MigrationRunner
from nonebot_plugin_xiuxian_2.plugin import build_migrations


_CHECKSUM_FIXTURE = Path(__file__).parent / "fixtures" / "published_migration_checksums_v1_3_5.json"
_V1_3_5_SHA = "b94a81710126d1161cc233e1ac2fe8cf4c04428d"


def _load_snapshot() -> dict:
    return json.loads(_CHECKSUM_FIXTURE.read_text(encoding="utf-8"))


def _implementation_sha256(callback) -> str:
    return hashlib.sha256(inspect.getsource(callback).strip().encode()).hexdigest()


class PublishedMigrationChecksumContractTests(unittest.TestCase):
    def test_central_startup_catalog_preserves_the_v1_3_5_release_snapshot(self) -> None:
        fixture = _load_snapshot()
        self.assertEqual(fixture["baseline"], "v1.3.5")
        self.assertEqual(fixture["source_sha"], _V1_3_5_SHA)
        self.assertEqual(len(fixture["migrations"]), 324)
        expected = {
            row["version"]: (row["name"], row["checksum"])
            for row in fixture["migrations"]
        }
        catalog = build_migrations()
        MigrationRunner(catalog)
        actual = {migration.version: (migration.name, migration.checksum) for migration in catalog}
        changed = {
            version: {"expected": expected[version], "actual": actual[version]}
            for version in expected.keys() & actual.keys()
            if expected[version] != actual[version]
        }
        removed = sorted(expected.keys() - actual.keys())

        self.assertEqual(
            (removed, changed),
            ([], {}),
            "published migration history changed; add a new version instead of editing it",
        )

    def test_attached_startup_migrations_match_the_v1_3_5_release_snapshot(self) -> None:
        fixture = _load_snapshot()
        expected = {
            row["version"]: row
            for row in fixture["attached_migrations"]
        }
        attached_definitions = (
            (
                attached_migrations.ATTACHED_SCHEMA_VERSION,
                attached_migrations.ATTACHED_SCHEMA_NAME,
                attached_migrations.apply_attached_player_accessory,
                attached_migrations._checksum,
            ),
            (
                attached_migrations.ATTACHED_OPERATION_VERSION,
                attached_migrations.ATTACHED_OPERATION_NAME,
                attached_migrations.apply_attached_player_accessory_operations,
                None,
            ),
            (
                attached_migrations.ATTACHED_PRESET_VERSION,
                attached_migrations.ATTACHED_PRESET_NAME,
                attached_migrations.apply_attached_player_accessory_presets,
                None,
            ),
        )
        actual = {}
        for version, name, callback, checksum_helper in attached_definitions:
            actual[version] = {
                "version": version,
                "name": name,
                "checksum": hashlib.sha256(f"{version}:{name}:v1".encode()).hexdigest(),
                "implementation_sha256": _implementation_sha256(callback),
                "checksum_helper_sha256": (
                    _implementation_sha256(checksum_helper) if checksum_helper is not None else None
                ),
            }

        changed = {
            version: {"expected": expected[version], "actual": actual[version]}
            for version in expected.keys() & actual.keys()
            if expected[version] != actual[version]
        }
        removed = sorted(expected.keys() - actual.keys())
        self.assertEqual((removed, changed), ([], {}))

    def test_published_history_is_skipped_and_a_new_version_runs_once(self) -> None:
        fixture = _load_snapshot()
        catalog = build_migrations()
        calls = {"new": 0}

        def apply_new(uow: DatabaseUnitOfWork) -> None:
            calls["new"] += 1
            uow.execute("CREATE TABLE migration_contract_added (value TEXT NOT NULL)")
            uow.execute("INSERT INTO migration_contract_added(value) VALUES('once')")

        new = Migration("zzzz_contract.001", "additive_schema", apply_new)
        runner = MigrationRunner((*catalog, new))
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "isolated.sqlite3"
            with DatabaseUnitOfWork(database) as uow:
                runner.ensure_schema(uow)
                uow.execute("CREATE TABLE migration_contract (value TEXT NOT NULL)")
                uow.execute("INSERT INTO migration_contract(value) VALUES('preserved')")
                snapshot = {
                    row["version"]: row
                    for row in fixture["migrations"]
                }
                # The v1.3.5 database can also have been upgraded by the
                # current release. Seed those additive versions as applied;
                # they are not part of the historical snapshot contract.
                applied_rows = []
                for migration in catalog:
                    row = snapshot.get(migration.version)
                    applied_rows.append(
                        (
                            migration.version,
                            migration.name,
                            row["checksum"] if row is not None else migration.checksum,
                        )
                    )
                for version, name, checksum in applied_rows:
                    uow.execute(
                        "INSERT INTO schema_migrations(version,name,checksum,applied_at,duration_ms) "
                        "VALUES(?,?,?,?,?)",
                        (version, name, checksum, "2026-11-07T22:01:03+00:00", 1),
                    )
                pending = runner.pending(uow)
                first_apply = runner.apply(uow)
                second_apply = runner.apply(uow)
                preserved = uow.query_one("SELECT value FROM migration_contract")
                added = uow.query_one("SELECT value FROM migration_contract_added")
                applied = uow.query_all("SELECT version FROM schema_migrations ORDER BY version")

        self.assertEqual([migration.version for migration in pending], [new.version])
        self.assertEqual(first_apply, [new.version])
        self.assertEqual(second_apply, [])
        self.assertEqual(calls, {"new": 1})
        self.assertEqual(preserved["value"], "preserved")
        self.assertEqual(added["value"], "once")
        self.assertEqual(len(applied), len(catalog) + 1)
        self.assertEqual(applied[-1]["version"], new.version)

    def test_changed_v1_3_5_bank_checksum_is_rejected_before_callback(self) -> None:
        published = next(row for row in _load_snapshot()["migrations"] if row["version"] == "bank.003")
        calls = {"changed": 0}

        def apply_changed(uow: DatabaseUnitOfWork) -> None:
            calls["changed"] += 1
            uow.execute("DELETE FROM migration_contract")

        changed = Migration(published["version"], published["name"], apply_changed)
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "isolated.sqlite3"
            with DatabaseUnitOfWork(database) as uow:
                with self.assertRaisesRegex(ValueError, "migration checksum changed: bank.003"):
                    runner = MigrationRunner((changed,))
                    runner.ensure_schema(uow)
                    uow.execute("CREATE TABLE migration_contract (value TEXT NOT NULL)")
                    uow.execute("INSERT INTO migration_contract(value) VALUES('preserved')")
                    uow.execute(
                        "INSERT INTO schema_migrations(version,name,checksum,applied_at,duration_ms) "
                        "VALUES(?,?,?,?,?)",
                        (
                            published["version"],
                            published["name"],
                            published["checksum"],
                            "2026-11-07T22:01:03+00:00",
                            1,
                        ),
                    )
                    runner.apply(uow)
                row = uow.query_one("SELECT value FROM migration_contract")

        self.assertEqual(calls, {"changed": 0})
        self.assertEqual(row["value"], "preserved")

    def test_failed_new_migration_rolls_back_schema_and_history(self) -> None:
        calls = {"failed": 0}

        def apply_failed(uow: DatabaseUnitOfWork) -> None:
            calls["failed"] += 1
            uow.execute("CREATE TABLE migration_contract_partial (value TEXT NOT NULL)")
            uow.execute("INSERT INTO migration_contract_partial(value) VALUES('partial')")
            raise RuntimeError("migration contract injected failure")

        runner = MigrationRunner((Migration("zzzz_contract.002", "rollback", apply_failed),))
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "isolated.sqlite3"
            with self.assertRaisesRegex(RuntimeError, "migration contract injected failure"):
                with DatabaseUnitOfWork(database) as uow:
                    runner.apply(uow)
            with DatabaseUnitOfWork(database) as uow:
                table = uow.query_one(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    ("migration_contract_partial",),
                )
                history = uow.query_one(
                    "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='schema_migrations'"
                )
                applied = (
                    uow.query_all("SELECT version FROM schema_migrations")
                    if history is not None
                    else []
                )

        self.assertEqual(calls, {"failed": 1})
        self.assertIsNone(table)
        self.assertEqual(applied, [])

    def test_concurrent_apply_runs_a_new_migration_once(self) -> None:
        calls = {"concurrent": 0}

        def apply_concurrent(uow: DatabaseUnitOfWork) -> None:
            calls["concurrent"] += 1
            uow.execute("CREATE TABLE migration_contract_concurrent (value TEXT NOT NULL)")
            uow.execute("INSERT INTO migration_contract_concurrent(value) VALUES('once')")

        runner = MigrationRunner((Migration("zzzz_contract.003", "concurrent", apply_concurrent),))
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "isolated.sqlite3"

            def apply_once() -> list[str]:
                with DatabaseUnitOfWork(database, immediate=True, timeout=10) as uow:
                    return runner.apply(uow)

            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(lambda _item: apply_once(), (1, 2)))

            with DatabaseUnitOfWork(database, read_only=True) as uow:
                applied = uow.query_all("SELECT version FROM schema_migrations")
                rows = uow.query_all("SELECT value FROM migration_contract_concurrent")

        self.assertCountEqual(results, [[], ["zzzz_contract.003"]])
        self.assertEqual(calls, {"concurrent": 1})
        self.assertEqual(applied, [{"version": "zzzz_contract.003"}])
        self.assertEqual(rows, [{"value": "once"}])


if __name__ == "__main__":
    unittest.main()
