"""Internal writer scopes preserve live readers without changing caller scopes."""
from contextlib import ExitStack
from contextvars import copy_context
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.infrastructure.database.uow import (
    DatabaseUnitOfWork, ReadOnlyConnectionOwner, WriteConnectionOwner,
)
from tests.test_uow_connection_scope import ObservedConnection, REAL_CONNECT


class UowLiveReaderScopeTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.database = Path(directory.name) / "game.db"
        connection = REAL_CONNECT(self.database)
        try:
            connection.execute("CREATE TABLE sample(value INTEGER)")
            connection.execute("INSERT INTO sample VALUES(1)")
            connection.commit()
        finally:
            connection.close()
        self.reader = ReadOnlyConnectionOwner()
        self.writer = WriteConnectionOwner()
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.addCleanup(self.reader.close)
        self.addCleanup(self.writer.close)
        self.connections = []

        def connect(*args, **kwargs):
            connection = REAL_CONNECT(*args, **kwargs, factory=ObservedConnection)
            self.connections.append(connection)
            return connection

        self.stack.enter_context(patch("sqlite3.connect", side_effect=connect))

    def scope(self, **kwargs):
        return DatabaseUnitOfWork.connection_scope(
            write_owner=self.writer, write_database=self.database,
            reuse_live_readers=True, **kwargs,
        )

    def read(self):
        with DatabaseUnitOfWork(
            self.database, read_only=True, read_owner=self.reader.when_unscoped(),
        ) as uow:
            return uow.connection, uow.query_one("SELECT value FROM sample")["value"]

    def test_reader_and_writer_keep_independent_live_transactions_across_scopes(self):
        for expected in (2, 3):
            with self.scope():
                self.assertIs(self.reader, self.reader.when_unscoped())
                self.assertIsNone(self.writer.when_unscoped())
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    writer = uow.connection
                    uow.execute("UPDATE sample SET value=value+1")
                reader, value = self.read()
                self.assertEqual(expected, value)
                self.assertIsNot(writer, reader)
                self.assertFalse(writer.in_transaction)
                self.assertFalse(reader.in_transaction)
        self.assertEqual(2, len(self.connections))
        self.assertTrue(all(c.commits == 2 and not c.closed for c in self.connections))

    def test_nested_default_scope_restores_opt_in_without_borrowing_retained_reader(self):
        with self.scope():
            retained, _ = self.read()
            with DatabaseUnitOfWork.connection_scope():
                self.assertIsNone(self.reader.when_unscoped())
                self.assertIsNone(self.writer.when_unscoped())
                scoped, value = self.read()
                self.assertEqual(1, value)
                self.assertIsNot(retained, scoped)
                self.assertNotIn("PRAGMA query_only=ON", scoped.statements)
            self.assertTrue(scoped.closed)
            self.assertIs(retained, self.read()[0])
        self.assertFalse(retained.closed)
        self.assertIs(retained, self.read()[0])
        self.assertEqual(3, retained.commits)

    def test_worker_and_reentrant_reads_never_borrow_the_active_or_idle_main_handle(self):
        errors, results = [], []

        def work():
            try:
                results.append(self.read())
            except BaseException as exc:
                errors.append(exc)

        with self.scope():
            with DatabaseUnitOfWork(
                self.database, read_only=True, read_owner=self.reader.when_unscoped(),
            ) as uow:
                retained = uow.connection
                reentrant, value = self.read()
                self.assertEqual(1, value)
                self.assertIsNot(retained, reentrant)
                self.assertTrue(reentrant.closed)
                self.assertTrue(retained.in_transaction)
            worker = threading.Thread(target=copy_context().run, args=(work,))
            worker.start()
            worker.join(3)
            self.assertFalse(worker.is_alive())
        self.assertEqual([], errors)
        self.assertEqual(1, len(results))
        self.assertEqual(1, results[0][1])
        self.assertIsNot(retained, results[0][0])
        self.assertTrue(results[0][0].closed)
        self.assertFalse(retained.closed)
        self.assertFalse(retained.in_transaction)

    def test_opt_in_requires_a_bound_writer_and_default_writer_scopes_stay_scoped(self):
        with self.assertRaisesRegex(ValueError, "opt-in writer"):
            with DatabaseUnitOfWork.connection_scope(reuse_live_readers=True):
                self.fail("an unbound scope must not opt in")
        with DatabaseUnitOfWork.connection_scope(write_owner=self.writer, write_database=self.database):
            self.assertIsNone(self.reader.when_unscoped())
            scoped, _ = self.read()
        self.assertTrue(scoped.closed)
        self.assertIs(self.reader, self.reader.when_unscoped())


if __name__ == "__main__":
    unittest.main()
