"""Request-local reuse retains independent SQLite transaction boundaries."""
from __future__ import annotations

from contextvars import copy_context
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.core.numeric import bind_sqlite_param, exact_crystal_integer
from nonebot_plugin_xiuxian_2.infrastructure.database import uow as uow_module


DatabaseUnitOfWork = uow_module.DatabaseUnitOfWork
REAL_CONNECT = sqlite3.connect


class ObservedConnection(sqlite3.Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.statements = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.fail = None

    def execute(self, sql, *args, **kwargs):
        self.statements.append(sql)
        if self.fail == "begin" and sql.startswith("BEGIN"):
            self.fail = None
            raise sqlite3.OperationalError("injected begin failure")
        if self.fail == "detach" and sql.startswith("DETACH"):
            self.fail = None
            raise sqlite3.OperationalError("injected detach failure")
        return super().execute(sql, *args, **kwargs)

    def commit(self):
        self.commits += 1
        if self.fail == "commit":
            self.fail = None
            raise sqlite3.OperationalError("injected commit failure")
        return super().commit()

    def rollback(self):
        self.rollbacks += 1
        if self.fail == "rollback":
            self.fail = None
            raise sqlite3.OperationalError("injected rollback failure")
        return super().rollback()

    def close(self):
        try:
            return super().close()
        finally:
            self.closed = True


class UowConnectionScopeTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.database = Path(directory.name) / "game.db"
        with REAL_CONNECT(self.database) as connection:
            connection.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY,value INTEGER)")
            connection.execute("INSERT INTO sample VALUES(1,0)")
        connection.close()
        self.connections = []

        def connect(*args, **kwargs):
            kwargs["factory"] = ObservedConnection
            connection = REAL_CONNECT(*args, **kwargs)
            self.connections.append(connection)
            return connection

        patcher = patch.object(uow_module.sqlite3, "connect", side_effect=connect)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_four_transactions_reuse_initialization_and_close_at_scope_exit(self):
        with DatabaseUnitOfWork.connection_scope():
            states = []
            for _ in range(4):
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    states.append(uow._numeric_functions)
                    uow.execute("UPDATE sample SET value=value+1 WHERE id=1")
                self.assertIsNone(uow.connection)
            self.assertEqual(1, len(self.connections))
            connection = self.connections[0]
            self.assertEqual(4, connection.commits)
            self.assertEqual(4, connection.statements.count("BEGIN IMMEDIATE"))
            self.assertEqual(1, connection.statements.count("PRAGMA journal_mode"))
            self.assertEqual(1, connection.statements.count("PRAGMA busy_timeout=30000"))
            self.assertTrue(all(state is states[0] for state in states))
            self.assertFalse(connection.in_transaction)
            self.assertFalse(connection.closed)
        self.assertTrue(connection.closed)
        with self.assertRaises(sqlite3.ProgrammingError):
            connection.execute("SELECT 1")

    def test_default_uows_keep_independent_closed_connections(self):
        for _ in range(2):
            with DatabaseUnitOfWork(self.database) as uow:
                self.assertEqual(0, uow.query_one("SELECT value FROM sample")["value"])
            self.assertTrue(self.connections[-1].closed)
        self.assertEqual(2, len(self.connections))

    def test_active_nested_uow_uses_separate_connection_and_idle_is_bounded(self):
        with DatabaseUnitOfWork.connection_scope():
            with DatabaseUnitOfWork(self.database, read_only=True) as outer:
                outer_connection = outer.connection
                with DatabaseUnitOfWork(self.database, read_only=True) as inner:
                    self.assertIsNot(outer.connection, inner.connection)
                    self.assertTrue(outer.connection.in_transaction)
                    self.assertTrue(inner.connection.in_transaction)
                self.assertIs(outer.connection, outer_connection)
                self.assertEqual(0, outer.query_one("SELECT value FROM sample")["value"])
            self.assertEqual(1, sum(not row.closed for row in self.connections))
        self.assertTrue(all(row.closed for row in self.connections))

    def test_nested_scope_closes_its_connection_then_restores_outer_scope(self):
        with DatabaseUnitOfWork.connection_scope():
            with DatabaseUnitOfWork(self.database) as outer:
                first = outer.connection
            with DatabaseUnitOfWork.connection_scope():
                with DatabaseUnitOfWork(self.database) as inner:
                    second = inner.connection
                    self.assertIsNot(first, second)
            self.assertTrue(second.closed)
            self.assertFalse(first.closed)
            with DatabaseUnitOfWork(self.database) as again:
                self.assertIs(first, again.connection)
        self.assertTrue(first.closed)

    def test_connection_options_do_not_leak_between_uows(self):
        with DatabaseUnitOfWork.connection_scope():
            with DatabaseUnitOfWork(self.database, foreign_keys=False) as first:
                original = first.connection
                self.assertEqual(0, first.query_one("PRAGMA foreign_keys")["foreign_keys"])
            with DatabaseUnitOfWork(self.database, query_only=True, timeout=.25) as second:
                self.assertIsNot(original, second.connection)
                self.assertEqual(1, second.query_one("PRAGMA foreign_keys")["foreign_keys"])
                self.assertEqual(1, second.query_one("PRAGMA query_only")["query_only"])
                self.assertEqual(250, second.query_one("PRAGMA busy_timeout")["timeout"])
                with self.assertRaises(sqlite3.OperationalError):
                    second.execute("UPDATE sample SET value=1")
            with DatabaseUnitOfWork(self.database, read_only=True) as readonly:
                with self.assertRaises(sqlite3.OperationalError):
                    readonly.execute("UPDATE sample SET value=1")
            self.assertEqual(1, sum(not row.closed for row in self.connections))

    def test_new_transaction_sees_external_commit(self):
        with DatabaseUnitOfWork.connection_scope():
            with DatabaseUnitOfWork(self.database) as first:
                connection = first.connection
                self.assertEqual(0, first.query_one("SELECT value FROM sample")["value"])
            writer = REAL_CONNECT(self.database)
            try:
                writer.execute("UPDATE sample SET value=7")
                writer.commit()
            finally:
                writer.close()
            with DatabaseUnitOfWork(self.database) as second:
                self.assertIs(connection, second.connection)
                self.assertEqual(7, second.query_one("SELECT value FROM sample")["value"])

    def test_database_replacement_never_reuses_old_file_connection(self):
        replacement = self.database.with_name("replacement.db")
        writer = REAL_CONNECT(replacement)
        try:
            writer.execute("CREATE TABLE sample(value INTEGER)")
            writer.execute("INSERT INTO sample VALUES(8)")
            writer.commit()
        finally:
            writer.close()
        with DatabaseUnitOfWork.connection_scope():
            with DatabaseUnitOfWork(self.database, read_only=True) as first:
                old = first.connection
                self.assertEqual(0, first.query_one("SELECT value FROM sample")["value"])
            replacement.replace(self.database)
            with DatabaseUnitOfWork(self.database, read_only=True) as following:
                self.assertIsNot(old, following.connection)
                self.assertEqual(8, following.query_one("SELECT value FROM sample")["value"])
            self.assertEqual(1, sum(not row.closed for row in self.connections))

    def test_require_exists_is_checked_before_idle_connection_borrow(self):
        with DatabaseUnitOfWork.connection_scope():
            with DatabaseUnitOfWork(self.database, require_exists=True):
                pass
            self.database.unlink()
            with self.assertRaises(FileNotFoundError):
                with DatabaseUnitOfWork(self.database, require_exists=True):
                    self.fail("deleted database must not use its still-open old handle")
            self.assertFalse(self.database.exists())

    def test_rollback_is_complete_before_next_transaction(self):
        with DatabaseUnitOfWork.connection_scope():
            with self.assertRaisesRegex(ValueError, "business rejection"):
                with DatabaseUnitOfWork(self.database) as rejected:
                    rejected.execute("UPDATE sample SET value=9")
                    raise ValueError("business rejection")
            with DatabaseUnitOfWork(self.database) as following:
                self.assertEqual(0, following.query_one("SELECT value FROM sample")["value"])
                following.execute("UPDATE sample SET value=1")

    def test_numeric_callbacks_retain_exact_values_and_known_failure(self):
        amount = 2 ** 80 + 3
        with DatabaseUnitOfWork.connection_scope():
            with DatabaseUnitOfWork(self.database) as first:
                connection, state = first.connection, first._numeric_functions
                first.execute("UPDATE sample SET value=?", (bind_sqlite_param(amount, exact=True),))
            with DatabaseUnitOfWork(self.database) as second:
                self.assertIs(connection, second.connection)
                self.assertIs(state, second._numeric_functions)
                with self.assertRaises(DomainError) as caught:
                    second.query_one("SELECT xiuxian_int_sub(?,1,'stone','actor')", (float(2 ** 63),))
                self.assertEqual("stone_precision_lost_real", caught.exception.details["reason"])
                second.execute("UPDATE sample SET value=xiuxian_int_store(xiuxian_int_sub(value,1))")
                self.assertEqual(amount - 1, exact_crystal_integer(second.query_one("SELECT value FROM sample")["value"]))

    def test_failed_connection_lifecycle_is_closed_and_not_reused(self):
        for failure in ("begin", "commit", "rollback", "detach"):
            with self.subTest(failure=failure):
                with DatabaseUnitOfWork.connection_scope():
                    with DatabaseUnitOfWork(self.database) as first:
                        damaged = first.connection
                    damaged.fail = failure
                    with self.assertRaisesRegex(sqlite3.OperationalError, "injected " + failure):
                        with DatabaseUnitOfWork(self.database) as failing:
                            if failure == "rollback":
                                failing.execute("UPDATE sample SET value=5")
                                raise ValueError("reject transaction")
                            if failure == "detach":
                                failing.attach_database(":memory:", "extra")
                    self.assertTrue(damaged.closed)
                    with DatabaseUnitOfWork(self.database) as recovered:
                        self.assertIsNot(damaged, recovered.connection)
                        self.assertEqual(0, recovered.query_one("SELECT value FROM sample")["value"])
                        self.assertEqual(["main"], [row["name"] for row in recovered.query_all("PRAGMA database_list")])

    def test_initialization_failure_is_closed_before_recovery(self):
        with DatabaseUnitOfWork.connection_scope():
            with patch.object(uow_module, "register_numeric_functions", side_effect=ValueError("initialization failed")):
                with self.assertRaisesRegex(ValueError, "initialization failed"):
                    with DatabaseUnitOfWork(self.database):
                        self.fail("initialization must fail before entering the body")
            self.assertTrue(self.connections[-1].closed)
            with DatabaseUnitOfWork(self.database) as recovered:
                self.assertEqual(0, recovered.query_one("SELECT value FROM sample")["value"])

    def test_successful_detach_finishes_before_connection_reuse(self):
        with DatabaseUnitOfWork.connection_scope():
            with DatabaseUnitOfWork(self.database) as attached:
                connection = attached.connection
                attached.attach_database(":memory:", "extra")
                attached.execute("CREATE TABLE extra.values_for_request(value INTEGER)")
            with DatabaseUnitOfWork(self.database) as following:
                self.assertIs(connection, following.connection)
                self.assertEqual(["main"], [row["name"] for row in following.query_all("PRAGMA database_list")])

    def test_copied_context_in_another_thread_cannot_borrow_owner_connection(self):
        with DatabaseUnitOfWork.connection_scope():
            with DatabaseUnitOfWork(self.database) as first:
                owner_connection = first.connection
            observed, failures = [], []

            def worker():
                try:
                    with DatabaseUnitOfWork(self.database) as other:
                        observed.append(other.connection)
                        self.assertEqual(0, other.query_one("SELECT value FROM sample")["value"])
                except BaseException as exc:
                    failures.append(exc)

            context = copy_context()
            thread = threading.Thread(target=lambda: context.run(worker))
            thread.start()
            thread.join(3)
            self.assertFalse(thread.is_alive())
            self.assertEqual([], failures)
            self.assertEqual(1, len(observed))
            self.assertIsNot(owner_connection, observed[0])
            self.assertTrue(observed[0].closed)
            self.assertFalse(owner_connection.closed)
            with DatabaseUnitOfWork(self.database) as following:
                self.assertIs(owner_connection, following.connection)

    def test_exception_leaving_scope_closes_idle_connection(self):
        with self.assertRaisesRegex(ValueError, "scope stopped"):
            with DatabaseUnitOfWork.connection_scope():
                with DatabaseUnitOfWork(self.database) as uow:
                    connection = uow.connection
                raise ValueError("scope stopped")
        self.assertTrue(connection.closed)


if __name__ == "__main__":
    unittest.main()
