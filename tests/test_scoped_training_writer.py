"""Opt-in training scopes retain handles across requests, never transactions."""
from contextlib import ExitStack
from contextvars import copy_context
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

import nonebot

try:
    nonebot.get_driver()
except ValueError:
    nonebot.init(command_start={"", "/"}, log_level="ERROR")

from nonebot_plugin_xiuxian_2.features.buff import application as buff
from nonebot_plugin_xiuxian_2.features.info import activity_repository
from nonebot_plugin_xiuxian_2.infrastructure.database import AttachedDatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.database import uow as database
from tests.test_blessed_spot_schema_ownership import Pair
from tests.test_uow_connection_scope import ObservedConnection, REAL_CONNECT


class ScopedTrainingWriterTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.path = self.root / "game.db"
        self.create(self.path)
        self.owner = database.WriteConnectionOwner()
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for module in (database, buff):
            self.stack.enter_context(patch.object(module, "STONE_TRAINING_WRITE_OWNER", self.owner))
        self.addCleanup(self.owner.close)
        self.connections = []

        def connect(*args, **kwargs):
            connection = REAL_CONNECT(*args, **kwargs, factory=ObservedConnection)
            self.connections.append(connection)
            return connection

        self.stack.enter_context(patch("sqlite3.connect", side_effect=connect))

    @staticmethod
    def create(path):
        connection = REAL_CONNECT(path)
        try:
            connection.execute("CREATE TABLE sample(value INTEGER)")
            connection.execute("INSERT INTO sample VALUES(0)")
            connection.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,last_check_info_time TEXT)")
            connection.execute("INSERT INTO user_cd VALUES('u','old')")
            connection.commit()
        finally:
            connection.close()

    def scope(self, path=None):
        return database.DatabaseUnitOfWork.connection_scope(
            write_owner=self.owner, write_database=path or self.path,
        )

    def current(self):
        connection = REAL_CONNECT(self.path)
        try:
            return connection.execute("SELECT value FROM sample").fetchone()[0]
        finally:
            connection.close()

    def test_repeated_real_training_keeps_four_commits_per_request_and_exact_replay(self):
        (self.root / "pair").mkdir()
        pair = Pair(self.root / "pair").migrate().seed(stone=1000)
        app = buff.BuffApplication(pair.game, pair.player)
        self.connections.clear()
        arguments = dict(user_id="u", requested_stone=100, expected_exp=100,
                         expected_stone=1000, exp_cap=1000, power_multiplier=2.0)
        first = app.stone_training(operation_id="first", **arguments)
        self.assertTrue(first.ok)
        retained = self.connections[0]
        self.assertEqual(1, len(self.connections))
        self.assertEqual(4, retained.commits)
        self.assertEqual((110, 900), pair.training_state()[:2])
        second = app.stone_training(operation_id="second", **{
            **arguments, "expected_exp": 110, "expected_stone": 900})
        self.assertTrue(second.ok)
        self.assertEqual(8, retained.commits)
        self.assertEqual(8, retained.statements.count("BEGIN IMMEDIATE"))
        self.assertEqual(1, retained.statements.count("PRAGMA journal_mode"))
        self.assertEqual((120, 800), pair.training_state()[:2])
        replay = app.stone_training(operation_id="first", **{
            **arguments, "expected_exp": 120, "expected_stone": 800})
        self.assertTrue(replay.replayed)
        self.assertEqual(first.data, replay.data)
        self.assertEqual((120, 800), pair.training_state()[:2])
        self.assertFalse(retained.in_transaction)
        self.assertFalse(retained.closed)
        self.assertEqual(1, sum(not item.closed for item in self.connections))

    def test_independent_transactions_see_external_commits_without_timestamp_eviction(self):
        other_owner = database.WriteConnectionOwner()
        self.addCleanup(other_owner.close)
        with patch.object(activity_repository, "PLAYER_ACTIVITY_WRITE_OWNER", other_owner):
            for _ in range(2):
                with self.scope():
                    with database.DatabaseUnitOfWork(self.path, immediate=True) as uow:
                        uow.execute("UPDATE sample SET value=value+1")
                self.assertEqual(1, activity_repository.PlayerActivitySqlRepository(
                    self.path).update_last_check_info_time("u", "now"))
        retained = self.connections[0]
        self.assertEqual(2, len(self.connections))
        self.assertEqual(2, retained.commits)
        self.assertEqual(2, self.current())
        self.assertFalse(retained.in_transaction)
        self.assertEqual(1, retained.statements.count("PRAGMA journal_mode"))
        external = REAL_CONNECT(self.path)
        try:
            external.execute("UPDATE sample SET value=7")
            external.commit()
        finally:
            external.close()
        with self.scope():
            with database.DatabaseUnitOfWork(self.path, immediate=True) as uow:
                self.assertIs(retained, uow.connection)
                self.assertEqual(7, uow.query_one("SELECT value FROM sample")["value"])

    def test_default_nested_scope_other_database_readers_and_subclasses_stay_scoped(self):
        other = self.root / "other.db"
        self.create(other)
        with self.scope():
            with database.DatabaseUnitOfWork(self.path) as uow:
                retained = uow.connection
            with database.DatabaseUnitOfWork.connection_scope():
                with database.DatabaseUnitOfWork(self.path) as uow:
                    nested = uow.connection
                    self.assertIsNot(retained, nested)
            self.assertTrue(nested.closed)
            for options in ({"read_only": True}, {"query_only": True}):
                with database.DatabaseUnitOfWork(self.path, **options) as uow:
                    self.assertIsNot(retained, uow.connection)
            with database.DatabaseUnitOfWork(other) as uow:
                self.assertIsNot(retained, uow.connection)
                self.assertEqual(other.resolve(), Path(uow.database_identity[0]))
            with AttachedDatabaseUnitOfWork(self.path, attachments={"other": other}) as uow:
                self.assertIsNot(retained, uow.connection)
                self.assertEqual(0, uow.query_one("SELECT value FROM other.sample")["value"])
            with database.DatabaseUnitOfWork(self.path) as uow:
                self.assertIs(retained, uow.connection)
        self.assertEqual(1, sum(not item.closed for item in self.connections))

    def test_explicit_outer_scope_keeps_training_request_close_boundary(self):
        (self.root / "pair").mkdir()
        pair = Pair(self.root / "pair").migrate().seed(stone=1000)
        app = buff.BuffApplication(pair.game, pair.player)
        self.connections.clear()
        with database.DatabaseUnitOfWork.connection_scope():
            self.assertTrue(app.stone_training(
                operation_id="scoped", user_id="u", requested_stone=100,
                expected_exp=100, expected_stone=1000, exp_cap=1000, power_multiplier=2.0,
            ).ok)
            self.assertTrue(self.connections[0].closed)
            self.assertEqual(1, len(self.connections))
        self.assertIsNone(self.owner._idle)

    def test_owner_is_selected_at_enter_and_does_not_escape_scope_or_uow_reuse(self):
        before = database.DatabaseUnitOfWork(self.path)
        with self.scope():
            escaped = database.DatabaseUnitOfWork(self.path)
            with before as uow:
                retained = uow.connection
        with escaped as uow:
            self.assertIsNot(retained, uow.connection)
        with before as uow:
            self.assertIsNot(retained, uow.connection)
            self.assertFalse(uow.has_bound_database_identity)
        self.assertFalse(retained.closed)
        self.assertTrue(all(item.closed for item in self.connections[1:]))

    def test_workers_and_reentrant_calls_never_borrow_active_main_handle(self):
        errors, results = [], []

        def work():
            try:
                with database.DatabaseUnitOfWork(self.path) as uow:
                    results.append(uow.query_one("SELECT value FROM sample")["value"])
                with self.scope():
                    for _ in range(2):
                        with database.DatabaseUnitOfWork(self.path) as uow:
                            results.append(uow.query_one("SELECT value FROM sample")["value"])
            except BaseException as exc:
                errors.append(exc)

        with self.scope():
            with database.DatabaseUnitOfWork(self.path) as outer:
                retained = outer.connection
                with database.DatabaseUnitOfWork(self.path) as inner:
                    self.assertIsNot(retained, inner.connection)
                    self.assertTrue(outer.connection.in_transaction)
                self.assertTrue(self.connections[-1].closed)
            worker = threading.Thread(target=copy_context().run, args=(work,))
            worker.start()
            worker.join(3)
            self.assertFalse(worker.is_alive())
        self.assertEqual([], errors)
        self.assertEqual([0, 0, 0], results)
        self.assertEqual(4, len(self.connections))
        self.assertTrue(self.connections[-1].closed)
        self.assertFalse(retained.closed)

    def test_begin_statement_commit_and_rollback_failures_discard_before_recovery(self):
        for failure in ("begin", "statement", "commit", "rollback"):
            with self.subTest(failure=failure):
                with self.scope():
                    with database.DatabaseUnitOfWork(self.path, immediate=True):
                        pass
                    retained = self.connections[-1]
                    if failure == "statement":
                        retained.set_authorizer(lambda action, *_: sqlite3.SQLITE_DENY
                                                if action == sqlite3.SQLITE_UPDATE else sqlite3.SQLITE_OK)
                    else:
                        retained.fail = failure
                    with self.assertRaises(sqlite3.DatabaseError):
                        with database.DatabaseUnitOfWork(self.path, immediate=True) as uow:
                            uow.execute("UPDATE sample SET value=10")
                            if failure == "rollback":
                                raise ValueError("abort")
                    self.assertTrue(retained.closed)
                    self.assertEqual(0, self.current())
                    with database.DatabaseUnitOfWork(self.path, immediate=True) as uow:
                        self.assertEqual(0, uow.query_one("SELECT value FROM sample")["value"])

    def test_missing_database_uses_original_create_path_and_replacement_discards_old_handle(self):
        missing = self.root / "missing.db"
        with self.scope(missing):
            with database.DatabaseUnitOfWork(missing) as uow:
                uow.execute("CREATE TABLE created(value)")
        self.assertTrue(missing.is_file())
        self.assertTrue(self.connections[-1].closed)
        with self.scope():
            with database.DatabaseUnitOfWork(self.path) as uow:
                old = uow.connection
        replacement = self.root / "replacement.db"
        self.create(replacement)
        replacement.replace(self.path)
        with self.scope():
            with database.DatabaseUnitOfWork(self.path) as uow:
                self.assertIsNot(old, uow.connection)
                self.assertEqual(0, uow.query_one("SELECT value FROM sample")["value"])
        self.assertTrue(old.closed)
        self.owner.close()
        self.assertTrue(self.connections[-1].closed)

    def test_scope_requires_database_binding_and_explicit_owner_takes_precedence(self):
        for options in ({"write_owner": self.owner}, {"write_database": self.path}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                with database.DatabaseUnitOfWork.connection_scope(**options):
                    pass
        explicit = database.WriteConnectionOwner()
        self.addCleanup(explicit.close)
        with self.scope():
            with database.DatabaseUnitOfWork(self.path, write_owner=explicit) as uow:
                self.assertEqual(0, uow.query_one("SELECT value FROM sample")["value"])
        self.assertIsNone(self.owner._idle)
        self.assertIsNotNone(explicit._idle)

    def test_active_scope_writer_checks_identity_before_implicit_and_explicit_commit(self):
        moved = self.root / "moved.db"
        for explicit in (False, True):
            with self.subTest(explicit=explicit):
                with self.scope(), self.assertRaisesRegex(sqlite3.OperationalError, "write database identity changed"):
                    with database.DatabaseUnitOfWork(self.path, immediate=True) as uow:
                        uow.execute("UPDATE sample SET value=10")
                        self.path.replace(moved)
                        if explicit:
                            uow.commit()
                self.assertTrue(self.connections[-1].closed)
                moved.replace(self.path)
                self.assertEqual(0, self.current())

    def test_shutdown_releases_idle_training_writer(self):
        from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
        from nonebot_plugin_xiuxian_2.bootstrap.lifecycle import LifecyclePhase
        from nonebot_plugin_xiuxian_2.paths import configure_paths, get_paths
        from nonebot_plugin_xiuxian_2.plugin import build_lifecycle

        self.addCleanup(configure_paths, get_paths().data)
        context = build_runtime_context(data_dir=self.root / "lifecycle", settings={}, legacy_startup=False)
        lifecycle, _, _ = build_lifecycle(context)
        with self.scope():
            with database.DatabaseUnitOfWork(self.path):
                pass
        retained = self.connections[-1]
        lifecycle._shutdown_callbacks[LifecyclePhase.DATABASE]()
        self.assertTrue(retained.closed)
        with self.scope():
            with database.DatabaseUnitOfWork(self.path) as uow:
                self.assertEqual(0, uow.query_one("SELECT value FROM sample")["value"])


if __name__ == "__main__":
    unittest.main()
