"""Retained blacklist readers keep live transaction and lifecycle boundaries."""
from __future__ import annotations

from collections import OrderedDict
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.features.admin.blackhouse_repository import AdminBlackhouseSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import uow as uow_module
from tests.test_admin_blackhouse_status import prepare_database
from tests.test_uow_connection_scope import ObservedConnection


REAL_CONNECT = sqlite3.connect
DatabaseUnitOfWork = uow_module.DatabaseUnitOfWork
ReadOnlyConnectionOwner = uow_module.ReadOnlyConnectionOwner


class BlackhouseReadOwnerTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.database = Path(directory.name) / "game.db"
        prepare_database(self.database)
        self.owner = ReadOnlyConnectionOwner()
        self.connections = []
        self.open_arguments = []
        owner_patch = patch.object(AdminBlackhouseSqlRepository, "_read_owner", self.owner)
        schema_patch = patch.object(AdminBlackhouseSqlRepository, "_schema_cache", OrderedDict())
        owner_patch.start()
        schema_patch.start()
        self.addCleanup(owner_patch.stop)
        self.addCleanup(schema_patch.stop)
        self.addCleanup(self.owner.close)

        def connect(*args, **kwargs):
            self.open_arguments.append((args, dict(kwargs)))
            kwargs["factory"] = ObservedConnection
            connection = REAL_CONNECT(*args, **kwargs)
            self.connections.append(connection)
            return connection

        connect_patch = patch.object(uow_module.sqlite3, "connect", side_effect=connect)
        connect_patch.start()
        self.addCleanup(connect_patch.stop)

    def _read(self, database=None, **kwargs):
        return DatabaseUnitOfWork(database or self.database, read_only=True,
                                  read_owner=self.owner, **kwargs)

    def _write(self, sql, params=()):
        connection = REAL_CONNECT(self.database)
        try:
            connection.execute(sql, params)
            connection.commit()
        finally:
            connection.close()

    def test_repository_instances_share_one_idle_reader_and_each_read_new_transaction(self):
        first = AdminBlackhouseSqlRepository(self.database)
        second = AdminBlackhouseSqlRepository(self.database)
        self.assertFalse(first.snapshot("free"))
        self.assertEqual({"free": False, "banned": True}, second.statuses(("free", "banned", "free")))
        self.assertEqual(["banned"], [row["user_id"] for row in first.list_banned()])
        self.assertEqual(1, len(self.connections))
        connection = self.connections[0]
        self.assertEqual(3, connection.statements.count("BEGIN"))
        self.assertEqual(3, connection.commits)
        self.assertEqual(1, connection.statements.count("PRAGMA query_only=ON"))
        self.assertFalse(connection.in_transaction)
        self.assertFalse(connection.closed)
        args, kwargs = self.open_arguments[0]
        self.assertTrue(args[0].endswith("?mode=ro"))
        self.assertTrue(kwargs["uri"])
        self.assertNotEqual(False, kwargs.get("check_same_thread", True))
        AdminBlackhouseSqlRepository.close_read_connection()
        self.assertTrue(connection.closed)
        self.assertEqual({}, AdminBlackhouseSqlRepository._schema_cache)
        self.assertFalse(second.snapshot("free"))
        self.assertEqual(2, len(self.connections))

    def test_external_ban_unban_and_import_marker_remain_live_without_schema_change(self):
        repository = AdminBlackhouseSqlRepository(self.database)
        self.assertFalse(repository.snapshot("free"))
        original = self.connections[0]
        self._write("INSERT INTO admin_blackhouse_users(user_id,name,reason) VALUES('free','Free','test')")
        self.assertTrue(repository.snapshot("free"))
        self._write("DELETE FROM admin_blackhouse_users WHERE user_id='free'")
        self.assertFalse(repository.snapshot("free"))
        self.assertEqual([original], self.connections)
        self._write("DELETE FROM admin_blackhouse_imports")
        with self.assertRaisesRegex(RuntimeError, "schema_missing"):
            repository.snapshot("free")
        self.assertTrue(original.closed)
        self._write("INSERT INTO admin_blackhouse_imports(import_key) VALUES('legacy_json_and_is_ban_v1')")
        self.assertFalse(repository.snapshot("free"))
        self.assertEqual(2, len(self.connections))

    def test_replacement_same_schema_version_revalidates_identity_and_missing_schema(self):
        replacement = self.database.with_name("replacement.db")
        connection = REAL_CONNECT(replacement)
        try:
            connection.execute("CREATE TABLE different(value INTEGER)")
            connection.commit()
        finally:
            connection.close()
        repository = AdminBlackhouseSqlRepository(self.database)
        self.assertFalse(repository.snapshot("free"))
        previous = self.connections[0]
        old_key = next(iter(AdminBlackhouseSqlRepository._schema_cache))
        replacement_connection = REAL_CONNECT(replacement)
        try:
            replacement_connection.execute(f"PRAGMA schema_version={old_key[-1]}")
        finally:
            replacement_connection.close()
        replacement.replace(self.database)
        with self.assertRaisesRegex(RuntimeError, "schema_missing"):
            repository.snapshot("free")
        self.assertTrue(previous.closed)
        self.assertTrue(self.connections[-1].closed)
        self.assertNotEqual(old_key[1:3], (self.database.stat().st_dev, self.database.stat().st_ino))

    def test_metadata_lru_is_bounded_and_touches_recent_identity(self):
        repository = AdminBlackhouseSqlRepository(self.database)
        for index in range(65):
            self._write(f"PRAGMA schema_version={1000 + index}")
            self.assertFalse(repository.snapshot("free"))
        cache = AdminBlackhouseSqlRepository._schema_cache
        self.assertEqual(64, len(cache))
        self.assertEqual(1001, next(iter(cache))[-1])
        oldest = next(iter(cache))
        self.assertTrue(AdminBlackhouseSqlRepository._schema_cached(oldest))
        self.assertEqual(oldest, next(reversed(cache)))
        self.assertTrue(all(key[:3] == (str(self.database.resolve()), self.database.stat().st_dev,
                                        self.database.stat().st_ino) for key in cache))

    def test_replacement_during_read_rejects_old_identity_and_releases_owner(self):
        replacement = self.database.with_name("replacement.db")
        connection = REAL_CONNECT(replacement)
        try:
            connection.execute("CREATE TABLE replacement(value INTEGER)")
            connection.execute("INSERT INTO replacement VALUES(17)")
            connection.commit()
        finally:
            connection.close()

        with self.assertRaisesRegex(sqlite3.OperationalError, "identity changed"):
            with self._read() as reader:
                original = reader.connection
                original_identity = reader.database_identity
                self.assertEqual(2, reader.query_one("SELECT COUNT(*) AS n FROM user_xiuxian")["n"])
                replacement.replace(self.database)
                self.assertEqual(original_identity, reader.database_identity)
                self.assertNotEqual(original_identity[1:],
                                    (self.database.stat().st_dev, self.database.stat().st_ino))
        self.assertTrue(original.closed)
        with self._read() as recovered:
            self.assertIsNot(original, recovered.connection)
            self.assertEqual(17, recovered.query_one("SELECT value FROM replacement")["value"])
            self.assertEqual((str(self.database.resolve()), self.database.stat().st_dev,
                              self.database.stat().st_ino), recovered.database_identity)

    def test_options_and_path_changes_close_incompatible_idle_connection(self):
        other = self.database.with_name("other.db")
        connection = REAL_CONNECT(other)
        try:
            connection.execute("CREATE TABLE other(value INTEGER)")
            connection.commit()
        finally:
            connection.close()
        with self._read() as first:
            original = first.connection
        with self._read(timeout=.25, foreign_keys=False) as changed:
            self.assertIsNot(original, changed.connection)
            self.assertTrue(original.closed)
            self.assertEqual(250, changed.query_one("PRAGMA busy_timeout")["timeout"])
            self.assertEqual(0, changed.query_one("PRAGMA foreign_keys")["foreign_keys"])
            previous = changed.connection
        with self._read(other) as changed_database:
            self.assertTrue(previous.closed)
            self.assertIsNot(previous, changed_database.connection)
        self.assertEqual(1, sum(not row.closed for row in self.connections))

    def test_reentrant_and_worker_reads_never_share_active_or_retained_connection(self):
        with self._read() as outer:
            main_connection = outer.connection
            with self._read() as nested:
                nested_connection = nested.connection
                self.assertIsNot(main_connection, nested_connection)
                self.assertTrue(main_connection.in_transaction)
            self.assertTrue(nested_connection.closed)
        failures, observed = [], []

        def worker():
            try:
                for _ in range(2):
                    with self._read() as reader:
                        observed.append(reader.connection)
                        self.assertEqual(2, reader.query_one("SELECT COUNT(*) AS n FROM user_xiuxian")["n"])
                with self.assertRaisesRegex(RuntimeError, "main thread"):
                    self.owner.close()
            except BaseException as exc:
                failures.append(exc)

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertEqual([], failures)
        self.assertEqual(2, len(observed))
        self.assertTrue(all(row.closed and row is not main_connection for row in observed))
        with self._read() as following:
            self.assertIs(main_connection, following.connection)

    def test_begin_commit_and_rollback_failures_discard_connection_and_release_owner(self):
        for failure in ("begin", "commit", "rollback"):
            with self.subTest(failure=failure):
                with self._read() as initial:
                    damaged = initial.connection
                damaged.fail = failure
                with self.assertRaisesRegex(sqlite3.OperationalError, "injected " + failure):
                    with self._read() as failing:
                        if failure == "rollback":
                            raise ValueError("reject read")
                self.assertTrue(damaged.closed)
                with self._read() as recovered:
                    self.assertIsNot(damaged, recovered.connection)
                    self.assertEqual(2, recovered.query_one("SELECT COUNT(*) AS n FROM user_xiuxian")["n"])

    def test_body_and_caught_sql_failures_discard_connection(self):
        with self.assertRaisesRegex(ValueError, "body failed"):
            with self._read() as failed:
                original = failed.connection
                raise ValueError("body failed")
        self.assertTrue(original.closed)
        with self._read() as caught:
            damaged = caught.connection
            with self.assertRaises(sqlite3.DatabaseError):
                caught.query_one("SELECT value FROM absent_table")
        self.assertTrue(damaged.closed)
        with self._read() as fresh:
            self.assertIsNot(damaged, fresh.connection)

    def test_caught_explicit_commit_failure_discards_connection(self):
        with self._read() as reader:
            damaged = reader.connection
            damaged.fail = "commit"
            with self.assertRaisesRegex(sqlite3.OperationalError, "injected commit"):
                reader.commit()
        self.assertTrue(damaged.closed)
        with self._read() as recovered:
            self.assertIsNot(damaged, recovered.connection)

    def test_caught_fetch_failures_preserve_numeric_reason_and_discard_reader(self):
        for query_method in ("query_one", "query_all"):
            with self.subTest(query_method=query_method):
                with self._read() as initial:
                    original, numeric_state = initial.connection, initial._numeric_functions
                    self.assertEqual("6", initial.query_one(
                        "SELECT xiuxian_int_sub(7,1,'stone','actor') AS value")["value"])
                with self._read() as reader:
                    self.assertIs(original, reader.connection)
                    self.assertIs(numeric_state, reader._numeric_functions)
                    cursors = []
                    original_execute = reader.execute

                    def observe_execute(*args, **kwargs):
                        cursor = original_execute(*args, **kwargs)
                        cursors.append(cursor)
                        return cursor

                    with patch.object(reader, "execute", side_effect=observe_execute):
                        with self.assertRaises(DomainError) as caught:
                            getattr(reader, query_method)(
                                "SELECT 1 AS value UNION ALL "
                                "SELECT xiuxian_int_sub(?,1,'stone','actor')",
                                (float(2 ** 63),),
                            )
                    self.assertEqual("stone_precision_lost_real", caught.exception.details["reason"])
                    self.assertEqual("actor", caught.exception.details["subject"])
                    self.assertEqual(1, len(cursors), "execute must succeed before the fetch fails")
                    self.assertEqual([], reader._read_cursors)
                    with self.assertRaises(sqlite3.ProgrammingError):
                        cursors[0].fetchone()
                self.assertTrue(original.closed)
                with self._read() as recovered:
                    self.assertIsNot(original, recovered.connection)
                    self.assertIsNot(numeric_state, recovered._numeric_functions)
                    self.assertEqual("6", recovered.query_one(
                        "SELECT xiuxian_int_sub(7,1,'stone','actor') AS value")["value"])

    def test_initialization_failure_does_not_leave_owner_locked(self):
        with patch.object(uow_module, "register_numeric_functions", side_effect=ValueError("initialize failed")):
            with self.assertRaisesRegex(ValueError, "initialize failed"):
                with self._read():
                    self.fail("initialization should fail")
        self.assertTrue(self.connections[-1].closed)
        with self._read() as fresh:
            self.assertEqual(2, fresh.query_one("SELECT COUNT(*) AS n FROM user_xiuxian")["n"])

    def test_owned_cursors_close_before_return_and_retained_connection_has_no_transaction(self):
        with self._read() as reader:
            connection = reader.connection
            cursor = reader.execute("SELECT user_id FROM user_xiuxian")
            self.assertIsNotNone(cursor.fetchone())
            reader.query_one("SELECT user_id FROM user_xiuxian")
            self.assertEqual([cursor], reader._read_cursors)
        self.assertFalse(connection.in_transaction)
        with self.assertRaises(sqlite3.ProgrammingError):
            cursor.fetchone()
        with self._read() as following:
            self.assertIs(connection, following.connection)
            with self.assertRaises(sqlite3.OperationalError):
                following.execute("UPDATE user_xiuxian SET is_ban=0")
        self.assertTrue(connection.closed)
        with self.assertRaisesRegex(ValueError, "read_only"):
            DatabaseUnitOfWork(self.database, read_owner=self.owner)

    def test_close_during_active_read_discards_on_exit_and_can_reopen(self):
        with self._read() as reader:
            old = reader.connection
            self.owner.close()
            self.assertEqual(2, reader.query_one("SELECT COUNT(*) AS n FROM user_xiuxian")["n"])
        self.assertTrue(old.closed)
        with self._read() as reopened:
            self.assertIsNot(old, reopened.connection)

    def test_disappearance_never_uses_old_handle_or_creates_database(self):
        with self._read() as reader:
            old = reader.connection
        self.database.unlink()
        with self.assertRaises(sqlite3.OperationalError):
            with self._read():
                self.fail("missing database must fail closed")
        self.assertTrue(old.closed)
        self.assertFalse(self.database.exists())

    def test_repository_disappearance_discards_idle_handle_before_schema_rejection(self):
        repository = AdminBlackhouseSqlRepository(self.database)
        self.assertFalse(repository.snapshot("free"))
        old = self.connections[0]
        self.database.unlink()
        with self.assertRaisesRegex(RuntimeError, "schema_missing"):
            repository.snapshot("free")
        self.assertTrue(old.closed)
        self.assertFalse(self.database.exists())
        self.assertEqual({}, AdminBlackhouseSqlRepository._schema_cache)

    def test_database_shutdown_closes_retained_reader_idempotently_without_startup(self):
        from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
        from nonebot_plugin_xiuxian_2.bootstrap.lifecycle import LifecyclePhase
        from nonebot_plugin_xiuxian_2.paths import configure_paths, get_paths
        from nonebot_plugin_xiuxian_2.plugin import build_lifecycle

        previous_data = get_paths().data
        self.addCleanup(configure_paths, previous_data)
        context = build_runtime_context(
            data_dir=self.database.parent / "lifecycle", settings={}, legacy_startup=False,
        )
        lifecycle, _, _ = build_lifecycle(context)
        opened_before = len(self.connections)
        repository = AdminBlackhouseSqlRepository(self.database)
        self.assertFalse(repository.snapshot("free"))
        self.assertEqual(opened_before + 1, len(self.connections))
        old = self.connections[-1]
        shutdown = lifecycle._shutdown_callbacks[LifecyclePhase.DATABASE]
        failures = []

        def worker_shutdown():
            try:
                shutdown()
            except BaseException as exc:
                failures.append(exc)

        worker = threading.Thread(target=worker_shutdown)
        worker.start()
        worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertEqual([], failures)
        self.assertFalse(old.closed, "worker must not close the main-thread reader")
        shutdown()
        shutdown()
        self.assertTrue(old.closed)
        self.assertEqual({}, AdminBlackhouseSqlRepository._schema_cache)
        self.assertFalse(repository.snapshot("free"))
        self.assertEqual(opened_before + 2, len(self.connections))


if __name__ == "__main__":
    unittest.main()
