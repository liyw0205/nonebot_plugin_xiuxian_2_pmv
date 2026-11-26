"""The opt-in timestamp writer retains handles, never transactions or values."""
from contextlib import ExitStack
from contextvars import copy_context
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.features.info import activity_repository
from nonebot_plugin_xiuxian_2.infrastructure.database import uow as uow_module
from tests.test_uow_connection_scope import ObservedConnection, REAL_CONNECT


class PlayerActivityWriterTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.database = Path(directory.name) / "game.db"
        self.create_database(self.database)
        self.repository = activity_repository.PlayerActivitySqlRepository(self.database)
        self.owner = uow_module.WriteConnectionOwner()
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(activity_repository, "PLAYER_ACTIVITY_WRITE_OWNER", self.owner))
        self.stack.enter_context(patch.object(uow_module, "PLAYER_ACTIVITY_WRITE_OWNER", self.owner))
        self.addCleanup(self.owner.close)
        self.connections = []

        def connect(*args, **kwargs):
            connection = REAL_CONNECT(*args, **kwargs, factory=ObservedConnection)
            self.connections.append(connection)
            return connection

        self.stack.enter_context(patch("sqlite3.connect", side_effect=connect))

    @staticmethod
    def create_database(path):
        with REAL_CONNECT(path) as connection:
            connection.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,last_check_info_time TEXT)")
            connection.execute("INSERT INTO user_cd VALUES('u','old')")
        connection.close()

    def current(self):
        with REAL_CONNECT(self.database) as connection:
            value = connection.execute("SELECT last_check_info_time FROM user_cd WHERE user_id='u'").fetchone()[0]
        connection.close()
        return value

    def test_every_write_commits_immediately_with_one_initialized_idle_handle(self):
        for value in ("first", "second"):
            self.assertEqual(1, self.repository.update_last_check_info_time("u", value))
            self.assertEqual(value, self.current())
            self.assertFalse(self.connections[-1].in_transaction)
        self.assertEqual(0, self.repository.update_last_check_info_time("missing", "unused"))
        self.assertEqual(1, len(self.connections))
        connection = self.connections[0]
        self.assertEqual(3, connection.statements.count("BEGIN IMMEDIATE"))
        self.assertEqual(3, connection.commits)
        self.assertEqual(1, connection.statements.count("PRAGMA journal_mode"))
        self.assertEqual(1, connection.statements.count("PRAGMA foreign_keys=ON"))
        self.assertEqual(1, connection.statements.count("PRAGMA busy_timeout=30000"))
        self.assertNotIn("PRAGMA query_only=ON", connection.statements)

    def test_begin_statement_and_commit_failures_discard_and_rollback_before_recovery(self):
        for failure in ("begin", "statement", "commit"):
            with self.subTest(failure=failure):
                self.assertEqual(1, self.repository.update_last_check_info_time("u", "before"))
                connection = self.connections[-1]
                if failure == "statement":
                    connection.set_authorizer(lambda action, *_: sqlite3.SQLITE_DENY
                                              if action == sqlite3.SQLITE_UPDATE else sqlite3.SQLITE_OK)
                else:
                    connection.fail = failure
                self.assertEqual(0, self.repository.update_last_check_info_time("u", "failed"))
                self.assertTrue(connection.closed)
                self.assertEqual("before", self.current())
                self.assertEqual(1, self.repository.update_last_check_info_time("u", "recovered"))
                self.assertEqual("recovered", self.current())

    def test_rollback_failure_closes_dirty_handle_and_external_state_is_unchanged(self):
        with self.assertRaisesRegex(sqlite3.OperationalError, "rollback failure"):
            with uow_module.DatabaseUnitOfWork(self.database, write_owner=self.owner) as uow:
                uow.execute("UPDATE user_cd SET last_check_info_time='failed'")
                uow.connection.fail = "rollback"
                raise ValueError("abort")
        self.assertTrue(self.connections[-1].closed)
        self.assertEqual("old", self.current())
        self.assertEqual(1, self.repository.update_last_check_info_time("u", "recovered"))

    def test_reentrant_worker_and_explicit_scope_do_not_share_active_owner(self):
        with uow_module.DatabaseUnitOfWork(self.database, write_owner=self.owner) as outer:
            self.assertEqual(1, self.repository.update_last_check_info_time("u", "nested"))
            self.assertIsNot(outer.connection, self.connections[-1])
            self.assertTrue(self.connections[-1].closed)
        retained = self.connections[0]
        results = []
        with uow_module.DatabaseUnitOfWork.connection_scope():
            self.assertIsNone(self.owner.when_unscoped())
            self.assertEqual(1, self.repository.update_last_check_info_time("u", "scoped"))
            scoped = self.connections[-1]
            worker = threading.Thread(target=copy_context().run,
                                      args=(lambda: results.append(
                                          self.repository.update_last_check_info_time("u", "worker")),))
            worker.start()
            worker.join(3)
            self.assertFalse(worker.is_alive())
            self.assertEqual([1], results)
            self.assertTrue(self.connections[-1].closed)
            self.assertFalse(scoped.closed)
        self.assertTrue(scoped.closed)
        self.assertFalse(retained.closed)
        self.assertEqual("worker", self.current())

    def test_path_replacement_deletion_and_shutdown_cannot_reuse_old_database(self):
        self.repository.update_last_check_info_time("u", "first")
        original = self.connections[-1]
        self.owner.close()
        self.assertTrue(original.closed)
        replacement = self.database.with_name("replacement.db")
        self.create_database(replacement)
        self.repository.update_last_check_info_time("u", "old-file")
        old_handle = self.connections[-1]
        replacement.replace(self.database)
        self.assertEqual(1, self.repository.update_last_check_info_time("u", "new-file"))
        self.assertTrue(old_handle.closed)
        self.assertEqual("new-file", self.current())
        self.database.unlink()
        self.assertEqual(0, self.repository.update_last_check_info_time("u", "missing"))
        self.assertFalse(self.database.exists())

    def test_missing_database_open_race_never_creates_an_empty_database(self):
        original_uow = activity_repository.DatabaseUnitOfWork

        def deleted_before_open(*args, **kwargs):
            self.database.unlink()
            return original_uow(*args, **kwargs)

        with patch.object(activity_repository, "DatabaseUnitOfWork", side_effect=deleted_before_open):
            self.assertEqual(0, self.repository.update_last_check_info_time("u", "missing"))
        self.assertFalse(self.database.exists())

    def test_active_writer_rolls_back_if_database_moves_before_commit(self):
        moved = self.database.with_name("moved.db")
        for explicit_commit in (False, True):
            with self.subTest(explicit_commit=explicit_commit):
                with self.assertRaisesRegex(sqlite3.OperationalError, "write database identity changed"):
                    with uow_module.DatabaseUnitOfWork(
                        self.database, immediate=True, require_exists=True, write_owner=self.owner,
                    ) as uow:
                        uow.execute("UPDATE user_cd SET last_check_info_time='uncommitted'")
                        self.database.replace(moved)
                        if explicit_commit:
                            uow.commit()
                self.assertTrue(self.connections[-1].closed)
                moved.replace(self.database)
                self.assertEqual("old", self.current())
                self.assertEqual(1, self.repository.update_last_check_info_time("u", "old"))

    def test_writer_rejects_readonly_options_and_shutdown_releases_idle_handle(self):
        for options in ({"read_only": True}, {"query_only": True},
                        {"read_owner": uow_module.ReadOnlyConnectionOwner()}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                uow_module.DatabaseUnitOfWork(self.database, write_owner=self.owner, **options)
        self.repository.update_last_check_info_time("u", "before")
        retained = self.connections[-1]
        from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
        from nonebot_plugin_xiuxian_2.bootstrap.lifecycle import LifecyclePhase
        from nonebot_plugin_xiuxian_2.paths import configure_paths, get_paths
        from nonebot_plugin_xiuxian_2.plugin import build_lifecycle

        self.addCleanup(configure_paths, get_paths().data)
        context = build_runtime_context(data_dir=self.database.parent / "lifecycle",
                                        settings={}, legacy_startup=False)
        lifecycle, _, _ = build_lifecycle(context)
        lifecycle._shutdown_callbacks[LifecyclePhase.DATABASE]()
        self.assertTrue(retained.closed)
        self.assertEqual(1, self.repository.update_last_check_info_time("u", "after"))


if __name__ == "__main__":
    unittest.main()
