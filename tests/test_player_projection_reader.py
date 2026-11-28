"""Player projections reuse readers while player state and write checks stay live."""
from collections import OrderedDict
from contextlib import ExitStack
from contextvars import copy_context
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.features.info import avatar_repository, profile_repository
from nonebot_plugin_xiuxian_2.features.info.avatar_application import PlayerAvatarApplication
from nonebot_plugin_xiuxian_2.features.info.migrations import (
    apply_avatar_identity_player,
    apply_avatar_initialization_player,
)
from nonebot_plugin_xiuxian_2.features.title import application as title_application
from nonebot_plugin_xiuxian_2.features.title.migrations import apply_title_schema
from nonebot_plugin_xiuxian_2.infrastructure.database import OperationLedger
from nonebot_plugin_xiuxian_2.infrastructure.database import uow as uow_module
from tests.test_uow_connection_scope import ObservedConnection, REAL_CONNECT


DatabaseUnitOfWork = uow_module.DatabaseUnitOfWork
AvatarRepository = avatar_repository.AvatarStateSqlRepository


class PlayerProjectionReaderTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.database = Path(directory.name) / "player.db"
        self._create_database(self.database)
        self.avatar = AvatarRepository(self.database)
        self.title = title_application.TitleApplication(self.database)
        self.owner = uow_module.ReadOnlyConnectionOwner()
        self.game_owner = uow_module.ReadOnlyConnectionOwner()
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for module in (uow_module, avatar_repository, title_application):
            self.stack.enter_context(patch.object(module, "PLAYER_PROJECTION_READ_OWNER", self.owner))
        for module in (uow_module, profile_repository):
            self.stack.enter_context(patch.object(module, "LIVE_PROJECTION_READ_OWNER", self.game_owner))
        self.stack.enter_context(patch.object(AvatarRepository, "_schema_cache", OrderedDict()))
        self.addCleanup(self.owner.close)
        self.addCleanup(self.game_owner.close)
        self.connections = []

        def connect(*args, **kwargs):
            connection = REAL_CONNECT(*args, **kwargs, factory=ObservedConnection)
            self.connections.append(connection)
            return connection

        self.stack.enter_context(patch("sqlite3.connect", side_effect=connect))

    @staticmethod
    def _create_database(database, active_id="main"):
        with DatabaseUnitOfWork(database) as uow:
            apply_avatar_identity_player(uow)
            apply_avatar_initialization_player(uow)
            apply_title_schema(uow)
            OperationLedger().ensure_schema(uow)
            uow.execute(
                "INSERT INTO avatar(user_id,main_id,avatar_id,active_id,create_time) "
                "VALUES('main','main','avatar',?,'2026-10-02 00:00:00')",
                (active_id,),
            )
            uow.execute("INSERT INTO title VALUES('main','[\"1\",\"2\"]','1')")

    def test_avatar_and_title_share_reader_and_real_writes_are_immediately_visible(self):
        self.assertEqual("main", self.avatar.get_active_id("main"))
        self.assertEqual("1", self.title.get_state("main")["equipped"])
        self.assertEqual("main", self.avatar.get_avatar_info("main")["active_id"])
        self.assertEqual(1, len(self.connections))
        reader = self.connections[0]

        toggled = PlayerAvatarApplication(self.database).toggle_active(
            operation_id="toggle-live", user_id="main", expected_active_id="main",
        )
        self.assertTrue(toggled.ok)
        equipped = self.title.execute(
            operation_id="equip-live", user_id="main",
            payload={"action": "equip", "expected_unlocked": ["1", "2"],
                     "expected_equipped": "1", "title_id": "2"},
        )
        self.assertTrue(equipped.ok)
        self.assertEqual("avatar", AvatarRepository(self.database).get_active_id("main"))
        projected = self.title.get_state("main")
        self.assertEqual("2", projected["equipped"])
        projected["equipped"] = "changed result"
        self.assertEqual("2", self.title.get_state("main")["equipped"])
        readers = [connection for connection in self.connections
                   if "PRAGMA query_only=ON" in connection.statements]
        self.assertEqual([reader], readers)
        self.assertEqual(6, reader.statements.count("BEGIN"))
        self.assertEqual(6, reader.commits)
        self.assertFalse(reader.in_transaction)
        self.assertFalse(reader.closed)
        self.assertTrue(all(connection.closed for connection in self.connections if connection is not reader))

    def test_game_and_player_readers_do_not_evict_each_other(self):
        game = self.database.with_name("game.db")
        connection = REAL_CONNECT(game)
        try:
            connection.execute("CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT,exp,stone)")
            connection.execute("INSERT INTO user_xiuxian VALUES('main','Player',100,200)")
            connection.commit()
        finally:
            connection.close()
        profile = profile_repository.PlayerProfileSqlRepository(game)
        self.assertEqual(200, profile.get_user_profile("main")["stone"])
        self.assertEqual("main", self.avatar.get_active_id("main"))
        self.assertEqual("1", self.title.get_state("main")["equipped"])
        self.assertEqual(100, profile.get_user_profile("main")["exp"])
        self.assertEqual("main", self.avatar.get_avatar_info("main")["active_id"])
        self.assertEqual(2, len(self.connections))
        self.assertTrue(all(not connection.closed for connection in self.connections))
        self.assertEqual([2, 3], [connection.statements.count("BEGIN") for connection in self.connections])

    def test_explicit_scope_reuses_bound_schema_metadata_without_caching_player_rows(self):
        self.avatar.get_active_id("main")
        retained = self.connections[0]
        cached = tuple(AvatarRepository._schema_cache.items())
        missing = AvatarRepository(self.database.with_name("missing.db"))
        with DatabaseUnitOfWork.connection_scope():
            self.assertIsNone(self.owner.when_unscoped())
            self.assertEqual("main", self.avatar.get_active_id("main"))
            self.assertEqual("1", self.title.get_state("main")["equipped"])
            self.assertEqual("main", self.avatar.get_avatar_info("main")["active_id"])
            self.assertIsNone(missing.get_active_id("main"))
            self.assertEqual({}, missing.get_avatar_info("main"))
            self.assertEqual(2, len(self.connections))
            scoped = self.connections[-1]
            self.assertFalse(scoped.closed)
            self.assertNotIn("PRAGMA query_only=ON", scoped.statements)
            self.assertEqual(2, scoped.statements.count("PRAGMA schema_version"))
            self.assertNotIn(
                "SELECT name FROM sqlite_master WHERE type='table'", scoped.statements
            )
            self.assertFalse(retained.closed)
        self.assertTrue(scoped.closed)
        self.assertFalse(retained.closed)
        self.assertEqual(cached, tuple(AvatarRepository._schema_cache.items()))
        self.assertEqual("main", self.avatar.get_active_id("main"))
        self.assertEqual(2, len(self.connections))
        toggled = PlayerAvatarApplication(self.database).toggle_active(
            operation_id="toggle-after-scoped-cache", user_id="main",
            expected_active_id="main",
        )
        self.assertTrue(toggled.ok)
        self.assertEqual("avatar", self.avatar.get_active_id("main"))

    def test_scoped_schema_cache_invalidates_on_schema_version_change(self):
        with DatabaseUnitOfWork.connection_scope():
            self.assertEqual("main", self.avatar.get_active_id("main"))
            with DatabaseUnitOfWork(self.database) as uow:
                uow.execute("DROP TABLE avatar")
            self.assertIsNone(self.avatar.get_active_id("main"))

        self.assertEqual(2, len(AvatarRepository._schema_cache))
        statements = [sql for connection in self.connections for sql in connection.statements]
        self.assertEqual(2, statements.count("PRAGMA schema_version"))
        self.assertEqual(
            2,
            statements.count("SELECT name FROM sqlite_master WHERE type='table'"),
        )

    def test_scoped_schema_metadata_does_not_cross_same_version_file_replacements(self):
        self.assertEqual("main", self.avatar.get_active_id("main"))
        original_identity = next(iter(AvatarRepository._schema_cache))[:3]
        connection = REAL_CONNECT(self.database)
        try:
            version = connection.execute("PRAGMA schema_version").fetchone()[0]
        finally:
            connection.close()

        missing_schema = self.database.with_name("missing-schema.db")
        connection = REAL_CONNECT(missing_schema)
        try:
            connection.execute("CREATE TABLE unrelated(value INTEGER)")
            connection.execute(f"PRAGMA schema_version={version}")
            connection.commit()
        finally:
            connection.close()
        ready_schema = self.database.with_name("ready-schema.db")
        self._create_database(ready_schema, active_id="avatar")
        connection = REAL_CONNECT(ready_schema)
        try:
            connection.execute(f"PRAGMA schema_version={version}")
            connection.commit()
        finally:
            connection.close()

        missing_schema.replace(self.database)
        with DatabaseUnitOfWork.connection_scope():
            self.assertIsNone(self.avatar.get_active_id("main"))
        missing_identity = next(reversed(AvatarRepository._schema_cache))[:3]
        ready_schema.replace(self.database)
        with DatabaseUnitOfWork.connection_scope():
            self.assertEqual("avatar", self.avatar.get_active_id("main"))

        self.assertEqual(3, len(AvatarRepository._schema_cache))
        keys = [key[:3] for key in AvatarRepository._schema_cache]
        self.assertEqual(3, len(set(keys)))
        self.assertIn(original_identity, keys)
        self.assertIn(missing_identity, keys)
        self.assertEqual({version}, {key[-1] for key in AvatarRepository._schema_cache})
        self.assertEqual([True, False, True], list(AvatarRepository._schema_cache.values()))

    def test_worker_with_copied_scope_uses_short_readers_and_cannot_close_main_idle(self):
        self.avatar.get_active_id("main")
        retained = self.connections[0]
        missing = AvatarRepository(self.database.with_name("missing.db"))
        errors, results = [], []

        def work():
            try:
                self.assertIs(self.owner, self.owner.when_unscoped())
                results.append(self.avatar.get_active_id("main"))
                results.append(self.title.get_state("main")["equipped"])
                results.append(missing.get_active_id("main"))
                results.append(missing.get_avatar_info("main"))
            except BaseException as exc:
                errors.append(exc)

        with DatabaseUnitOfWork.connection_scope():
            worker = threading.Thread(target=copy_context().run, args=(work,))
            worker.start()
            worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertEqual([], errors)
        self.assertEqual(["main", "1", None, {}], results)
        self.assertEqual(3, len(self.connections))
        self.assertFalse(retained.closed)
        self.assertTrue(all(connection.closed for connection in self.connections[1:]))

    def test_missing_database_closes_main_idle_without_creating_a_file(self):
        self.title.get_state("main")
        retained = self.connections[0]
        self.database.unlink()
        self.assertIsNone(self.avatar.get_active_id("main"))
        self.assertEqual({}, self.avatar.get_avatar_info("main"))
        self.assertTrue(retained.closed)
        with self.assertRaises(sqlite3.DatabaseError):
            self.title.get_state("main")
        self.assertFalse(self.database.exists())
        self.assertEqual(1, len(self.connections))

    def test_avatar_fallback_and_title_error_discard_failed_reader_and_recover(self):
        for method, fallback in ((self.avatar.get_active_id, None), (self.avatar.get_avatar_info, {})):
            with self.subTest(reader=method.__name__):
                self.assertEqual("main", self.avatar.get_active_id("main"))
                reader = self.connections[-1]
                reader.set_authorizer(lambda action, table, *_: sqlite3.SQLITE_DENY
                                      if action == sqlite3.SQLITE_READ and table == "avatar" else sqlite3.SQLITE_OK)
                self.assertEqual(fallback, method("main"))
                self.assertTrue(reader.closed)
                self.assertEqual("main", self.avatar.get_active_id("main"))
        reader = self.connections[-1]
        reader.set_authorizer(lambda action, table, *_: sqlite3.SQLITE_DENY
                              if action == sqlite3.SQLITE_READ and table == "title" else sqlite3.SQLITE_OK)
        with self.assertRaises(sqlite3.DatabaseError):
            self.title.get_state("main")
        self.assertTrue(reader.closed)
        self.assertEqual("1", self.title.get_state("main")["equipped"])

    def test_same_schema_version_file_replacements_do_not_reuse_true_or_false_metadata(self):
        self.assertEqual("main", self.avatar.get_active_id("main"))
        retained = self.connections[-1]
        connection = REAL_CONNECT(self.database)
        try:
            version = connection.execute("PRAGMA schema_version").fetchone()[0]
        finally:
            connection.close()
        missing_schema = self.database.with_name("missing-schema.db")
        connection = REAL_CONNECT(missing_schema)
        try:
            connection.execute("CREATE TABLE unrelated(value INTEGER)")
            connection.execute(f"PRAGMA schema_version={version}")
            connection.commit()
        finally:
            connection.close()
        ready_schema = self.database.with_name("ready-schema.db")
        self._create_database(ready_schema, active_id="avatar")
        connection = REAL_CONNECT(ready_schema)
        try:
            connection.execute(f"PRAGMA schema_version={version}")
            connection.commit()
        finally:
            connection.close()

        missing_schema.replace(self.database)
        self.assertIsNone(self.avatar.get_active_id("main"))
        self.assertTrue(retained.closed)
        missing_reader = self.connections[-1]
        self.assertFalse(next(reversed(AvatarRepository._schema_cache.values())))
        ready_schema.replace(self.database)
        self.assertEqual("avatar", self.avatar.get_active_id("main"))
        self.assertTrue(missing_reader.closed)
        cache = AvatarRepository._schema_cache
        self.assertEqual(3, len(cache))
        self.assertEqual({version}, {key[-1] for key in cache})
        self.assertEqual([True, False, True], list(cache.values()))

    def test_initialization_keeps_schema_checks_live_without_touching_reader_cache(self):
        self.avatar.get_active_id("main")
        retained = self.connections[0]
        cached = tuple(AvatarRepository._schema_cache.items())
        with patch.object(AvatarRepository, "_cached_schema_ready", side_effect=AssertionError("init cache")):
            result = PlayerAvatarApplication(self.database).initialize(
                operation_id="initialize-live", user_id="new-main",
                proposed_avatar_id="new-avatar", proposed_create_time="2026-10-03 00:00:00",
            )
        self.assertTrue(result.ok)
        self.assertEqual(cached, tuple(AvatarRepository._schema_cache.items()))
        self.assertFalse(retained.closed)
        self.assertEqual("new-main", self.avatar.get_active_id("new-main"))
        self.assertTrue(all(connection.closed for connection in self.connections[1:]))

    def test_shutdown_releases_player_owner_only_on_main_thread(self):
        from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
        from nonebot_plugin_xiuxian_2.bootstrap.lifecycle import LifecyclePhase
        from nonebot_plugin_xiuxian_2.paths import configure_paths, get_paths
        from nonebot_plugin_xiuxian_2.plugin import build_lifecycle

        self.addCleanup(configure_paths, get_paths().data)
        context = build_runtime_context(data_dir=self.database.parent / "lifecycle",
                                        settings={}, legacy_startup=False)
        lifecycle, _, _ = build_lifecycle(context)
        self.title.get_state("main")
        retained = self.connections[-1]
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
        self.assertFalse(retained.closed)
        shutdown()
        shutdown()
        self.assertTrue(retained.closed)
        self.assertEqual("main", self.avatar.get_active_id("main"))
        self.assertIsNot(retained, self.connections[-1])


class AvatarMetadataCacheTests(unittest.TestCase):
    def test_lru_keeps_false_results_touches_hits_and_is_bounded_without_database_fixtures(self):
        class MetadataReader:
            def __init__(self, index):
                self.database_identity = ("/metadata/player.db", 7, index)

            def query_one(self, sql):
                if sql != "PRAGMA schema_version":
                    raise AssertionError(sql)
                return {"schema_version": 17}

        database = Path("/metadata/player.db")
        with patch.object(AvatarRepository, "_schema_cache", OrderedDict()) as cache:
            with patch.object(AvatarRepository, "_schema_ready", side_effect=lambda uow: uow.database_identity[-1] != 0) as ready:
                for index in range(64):
                    self.assertEqual(index != 0, AvatarRepository._cached_schema_ready(MetadataReader(index), database))
                first = (*MetadataReader(0).database_identity, 17)
                second = (*MetadataReader(1).database_identity, 17)
                self.assertFalse(AvatarRepository._cached_schema_ready(MetadataReader(0), database))
                self.assertEqual(64, ready.call_count)
                self.assertEqual(first, next(reversed(cache)))
                self.assertTrue(AvatarRepository._cached_schema_ready(MetadataReader(64), database))
                self.assertEqual(65, ready.call_count)
                self.assertEqual(64, len(cache))
                self.assertIn(first, cache)
                self.assertNotIn(second, cache)
                self.assertTrue(all(key[:2] == (str(database), 7) and key[-1] == 17 for key in cache))
