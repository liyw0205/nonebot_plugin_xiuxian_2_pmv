"""Live projection readers preserve transactions, scopes and numeric failures."""
from contextlib import ExitStack
from contextvars import copy_context
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.features.info import profile_repository
from nonebot_plugin_xiuxian_2.features.sect import sect_info_repository
from nonebot_plugin_xiuxian_2.infrastructure.database import uow as uow_module
from tests.test_uow_connection_scope import ObservedConnection, REAL_CONNECT


class LiveProjectionReaderTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.database = Path(directory.name) / "game.db"
        self._create_database(self.database, "Original")
        self.profile = profile_repository.PlayerProfileSqlRepository(self.database)
        self.sect = sect_info_repository.SectInfoSqlRepository(self.database)
        self.owner = uow_module.ReadOnlyConnectionOwner()
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for module in (uow_module, profile_repository, sect_info_repository):
            self.stack.enter_context(patch.object(module, "LIVE_PROJECTION_READ_OWNER", self.owner))
        self.addCleanup(self.owner.close)
        self.connections = []

        def connect(*args, **kwargs):
            connection = REAL_CONNECT(*args, **kwargs, factory=ObservedConnection)
            self.connections.append(connection)
            return connection

        self.stack.enter_context(patch("sqlite3.connect", side_effect=connect))

    def _create_database(self, database, name):
        connection = REAL_CONNECT(database)
        try:
            connection.execute("CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT,exp,stone)")
            connection.execute("INSERT INTO user_xiuxian VALUES('u',?,100,200)", (name,))
            connection.execute("CREATE TABLE sects(sect_id,sect_name)")
            connection.execute("INSERT INTO sects VALUES(1,?)", (name,))
            connection.commit()
        finally:
            connection.close()

    def write(self, sql, params=()):
        connection = REAL_CONNECT(self.database)
        try:
            connection.execute(sql, params)
            connection.commit()
        finally:
            connection.close()

    def test_profile_and_sect_share_one_idle_reader_and_external_updates_stay_live(self):
        self.assertEqual(200, self.profile.get_user_profile("u")["stone"])
        self.assertEqual("Original", self.sect.get_name_by_id(1))
        exact = 10**80 + 1
        self.write("UPDATE user_xiuxian SET stone=?", (str(exact).encode(),))
        self.write("UPDATE sects SET sect_name='Renamed'")
        another = profile_repository.PlayerProfileSqlRepository(self.database)
        projected = another.get_user_profile("u", raise_errors=True)
        self.assertEqual(exact, projected["stone"])
        projected["stone"] = -1
        self.assertEqual(exact, self.profile.get_user_profile("u")["stone"])
        self.assertEqual("Renamed", self.sect.get_name_by_id(1))
        self.assertEqual(1, len(self.connections))
        reader = self.connections[0]
        self.assertFalse(reader.in_transaction)
        self.assertFalse(reader.closed)
        self.assertEqual(5, reader.statements.count("BEGIN"))
        self.assertEqual(5, reader.commits)
        self.assertEqual(1, reader.statements.count("PRAGMA query_only=ON"))
        self.write("DELETE FROM sects")
        self.assertIsNone(self.sect.get_name_by_id(1))

    def test_explicit_scope_keeps_original_connection_key_and_close_boundary(self):
        self.write("ALTER TABLE user_xiuxian ADD COLUMN level TEXT")
        self.profile.get_user_profile("u")
        retained = self.connections[0]
        with uow_module.DatabaseUnitOfWork.connection_scope():
            self.assertIsNone(self.owner.when_unscoped())
            self.assertEqual(100, self.profile.get_user_profile("u")["exp"])
            self.assertEqual("Original", self.sect.get_name_by_id(1))
            self.assertEqual({"u": {"user_name": "Original", "level": None}},
                             self.profile.get_user_display_profiles_by_ids(["u"]))
            with uow_module.DatabaseUnitOfWork(self.database, read_only=True) as reader:
                self.assertEqual(200, reader.query_one("SELECT stone FROM user_xiuxian")["stone"])
            self.assertEqual(2, len(self.connections))
            scoped = self.connections[1]
            self.assertFalse(scoped.closed)
            self.assertNotIn("PRAGMA query_only=ON", scoped.statements)
        self.assertTrue(scoped.closed)
        self.assertFalse(retained.closed)
        self.assertIs(self.owner, self.owner.when_unscoped())
        self.assertEqual(100, self.profile.get_user_profile("u")["exp"])
        self.assertEqual(2, len(self.connections))

    def test_display_batch_reuses_live_reader_with_earliest_duplicates_and_exact_profiles(self):
        self.write("ALTER TABLE user_xiuxian ADD COLUMN level TEXT")
        self.write("UPDATE user_xiuxian SET level='R1'")
        self.write("INSERT INTO user_xiuxian VALUES('u','Later',300,400,'R2')")
        self.assertEqual("Original", self.profile.get_user_profile("u")["user_name"])
        expected = {"u": {"user_name": "Original", "level": "R1"}}
        self.assertEqual(expected, self.profile.get_user_display_profiles_by_ids(["u", "missing", "u"]))
        self.assertEqual(1, len(self.connections))
        projected = self.profile.get_user_display_profiles_by_ids(["u"])
        projected["u"]["level"] = "changed result"
        self.write("UPDATE user_xiuxian SET level='Updated' WHERE rowid=1")
        self.assertEqual("Updated", self.profile.get_user_display_profiles_by_ids(["u"])["u"]["level"])
        self.assertEqual(1, len(self.connections))
        self.assertFalse(self.connections[0].in_transaction)
        self.assertEqual(4, self.connections[0].commits)
        self.assertEqual({}, self.profile.get_user_display_profiles_by_ids([]))
        self.assertEqual(4, self.connections[0].commits)

    def test_display_batch_failure_discards_reader_and_schema_recovery_is_live(self):
        self.write("ALTER TABLE user_xiuxian ADD COLUMN level TEXT")
        self.profile.get_user_display_profiles_by_ids(["u"])
        reader = self.connections[-1]
        reader.set_authorizer(lambda action, table, *_: sqlite3.SQLITE_DENY
                              if action == sqlite3.SQLITE_READ else sqlite3.SQLITE_OK)
        with self.assertRaises(DomainError) as caught:
            self.profile.get_user_display_profiles_by_ids(["u"])
        self.assertEqual({"reason": "profile_read_failed", "subject": "player_profile"},
                         caught.exception.details)
        self.assertTrue(reader.closed)
        self.assertEqual("Original", self.profile.get_user_display_profiles_by_ids(["u"])["u"]["user_name"])

    def test_worker_with_copied_parent_scope_uses_its_own_short_connection(self):
        self.profile.get_user_profile("u")
        errors, results = [], []

        def work():
            try:
                self.assertIs(self.owner, self.owner.when_unscoped())
                results.append(self.profile.get_user_profile("u", raise_errors=True)["exp"])
            except BaseException as exc:
                errors.append(exc)

        with uow_module.DatabaseUnitOfWork.connection_scope():
            worker = threading.Thread(target=copy_context().run, args=(work,))
            worker.start()
            worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertEqual([], errors)
        self.assertEqual([100], results)
        self.assertEqual(2, len(self.connections))
        self.assertFalse(self.connections[0].closed)
        self.assertTrue(self.connections[1].closed)

    def test_replacement_and_disappearance_cannot_read_old_projection(self):
        self.profile.get_user_profile("u")
        first = self.connections[-1]
        replacement = self.database.with_name("replacement.db")
        self._create_database(replacement, "Replacement")
        replacement.replace(self.database)
        self.assertEqual("Replacement", self.sect.get_name_by_id(1))
        self.assertTrue(first.closed)
        second = self.connections[-1]
        self.database.unlink()
        with self.assertRaises(DomainError) as caught:
            self.profile.get_user_profile("u", raise_errors=True)
        self.assertEqual("profile_read_failed", caught.exception.code)
        self.assertTrue(second.closed)
        self.assertIsNone(self.profile.get_user_profile("u"))
        with self.assertRaises(sqlite3.DatabaseError):
            self.sect.get_name_by_id(1)
        self.assertFalse(self.database.exists())

    def test_read_error_discards_connection_and_recovers_without_hiding_reason(self):
        self.profile.get_user_profile("u")
        reader = self.connections[-1]
        reader.set_authorizer(lambda action, table, *_: sqlite3.SQLITE_DENY
                              if action == sqlite3.SQLITE_READ else sqlite3.SQLITE_OK)
        with self.assertRaises(DomainError) as caught:
            self.profile.get_user_profile("u", raise_errors=True)
        self.assertEqual({"reason": "profile_read_failed", "subject": "player_profile"},
                         caught.exception.details)
        self.assertTrue(reader.closed)
        self.assertEqual(100, self.profile.get_user_profile("u", raise_errors=True)["exp"])

    def test_numeric_precision_error_remains_outside_sql_failure_fallback(self):
        self.profile.get_user_profile("u")
        self.write("UPDATE user_xiuxian SET stone=?", (float(2**53),))
        for strict in (False, True):
            with self.subTest(strict=strict), self.assertRaises(DomainError) as caught:
                self.profile.get_user_profile("u", raise_errors=strict)
            self.assertEqual("stone_precision_lost_real", caught.exception.details["reason"])
        self.write("UPDATE user_xiuxian SET stone=300")
        self.assertEqual(300, self.profile.get_user_profile("u")["stone"])

    def test_shutdown_releases_projection_owner_only_on_main_thread(self):
        from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
        from nonebot_plugin_xiuxian_2.bootstrap.lifecycle import LifecyclePhase
        from nonebot_plugin_xiuxian_2.paths import configure_paths, get_paths
        from nonebot_plugin_xiuxian_2.plugin import build_lifecycle

        self.addCleanup(configure_paths, get_paths().data)
        context = build_runtime_context(data_dir=self.database.parent / "lifecycle",
                                        settings={}, legacy_startup=False)
        lifecycle, _, _ = build_lifecycle(context)
        self.profile.get_user_profile("u")
        reader = self.connections[-1]
        shutdown = lifecycle._shutdown_callbacks[LifecyclePhase.DATABASE]
        errors = []

        def worker_shutdown():
            try:
                shutdown()
            except BaseException as exc:
                errors.append(exc)

        worker = threading.Thread(target=worker_shutdown)
        worker.start()
        worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertEqual([], errors)
        self.assertFalse(reader.closed)
        shutdown()
        shutdown()
        self.assertTrue(reader.closed)
        self.assertEqual("Original", self.sect.get_name_by_id(1))
        self.assertIsNot(reader, self.connections[-1])
