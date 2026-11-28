"""Retained impart readers keep attributes, exact balances and failures live."""
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

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.features.impart import repository as impart_repository
from nonebot_plugin_xiuxian_2.features.impart.application import ImpartApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import uow as database
from tests.test_uow_connection_scope import ObservedConnection, REAL_CONNECT


class ImpartProjectionReaderTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "impart.db"
        self._create_database(self.path)
        self.repository = impart_repository.ImpartRepository(self.path)
        self.application = ImpartApplication(self.path)
        self.owner = database.ReadOnlyConnectionOwner()
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.addCleanup(self.owner.close)
        for module in (database, impart_repository):
            self.stack.enter_context(patch.object(module, "IMPART_PROJECTION_READ_OWNER", self.owner))
        self.connections = []

        def connect(*args, **kwargs):
            connection = REAL_CONNECT(*args, **kwargs, factory=ObservedConnection)
            self.connections.append(connection)
            return connection

        self.stack.enter_context(patch("sqlite3.connect", side_effect=connect))

    @staticmethod
    def _create_database(path, *, bonus=0.25):
        connection = REAL_CONNECT(path)
        try:
            connection.execute(
                "CREATE TABLE xiuxian_impart("
                "user_id TEXT,impart_hp_per REAL DEFAULT 0,impart_atk_per REAL DEFAULT 0,"
                "impart_mp_per REAL DEFAULT 0,impart_exp_up REAL DEFAULT 0,boss_atk REAL DEFAULT 0,"
                "impart_know_per REAL DEFAULT 0,impart_burst_per REAL DEFAULT 0,"
                "impart_mix_per REAL DEFAULT 0,impart_reap_per REAL DEFAULT 0,"
                "impart_two_exp REAL DEFAULT 0,stone_num INTEGER DEFAULT 0,"
                "impart_lv INTEGER DEFAULT 0,impart_num INTEGER DEFAULT 0,"
                "exp_day INTEGER DEFAULT 0,wish INTEGER DEFAULT 0,stone_num_exact TEXT,"
                "stone_num_precision_state TEXT DEFAULT 'exact')"
            )
            connection.execute(
                "INSERT INTO xiuxian_impart(rowid,user_id,impart_atk_per,stone_num,stone_num_exact) "
                "VALUES(159,'u',?,7,'7')", (bonus,),
            )
            connection.commit()
        finally:
            connection.close()

    def write(self, sql, params=()):
        connection = REAL_CONNECT(self.path)
        try:
            connection.execute(sql, params)
            connection.commit()
        finally:
            connection.close()

    def test_instances_share_reader_but_external_attributes_and_exact_balances_stay_live(self):
        original = self.application.state("u")
        self.assertEqual((159, 0.25, 7),
                         (original["state_row_id"], original["impart_atk_per"], original["stone_num"]))
        exact = 10**80 + 1
        self.write("UPDATE xiuxian_impart SET impart_atk_per=?,stone_num_exact=? WHERE user_id='u'",
                   (0.75, str(exact)))
        projected = impart_repository.ImpartRepository(self.path).state("u")
        self.assertEqual((0.75, exact), (projected["impart_atk_per"], projected["stone_num"]))
        projected["impart_atk_per"], projected["stone_num"] = -1, -1
        current = self.application.state("u")
        self.assertEqual((0.75, exact), (current["impart_atk_per"], current["stone_num"]))
        self.assertIsNone(self.repository.state("missing"))
        self.assertEqual(1, len(self.connections))
        reader = self.connections[0]
        self.assertEqual(4, reader.statements.count("BEGIN"))
        self.assertEqual(4, reader.commits)
        self.assertEqual(1, reader.statements.count("PRAGMA query_only=ON"))
        self.assertEqual(0, reader.total_changes)
        self.assertFalse(reader.in_transaction)
        self.assertFalse(reader.closed)

    def test_scope_and_copied_worker_keep_their_original_connection_boundaries(self):
        self.repository.state("u")
        retained = self.connections[0]
        errors, results = [], []

        def work():
            try:
                self.assertIs(self.owner, self.owner.when_unscoped())
                results.append(self.repository.state("u")["stone_num"])
                results.append(self.application.state("missing"))
            except BaseException as exc:
                errors.append(exc)

        with database.DatabaseUnitOfWork.connection_scope():
            self.assertIsNone(self.owner.when_unscoped())
            self.assertEqual(7, self.application.state("u")["stone_num"])
            self.write("UPDATE xiuxian_impart SET stone_num_exact='9'")
            self.assertEqual(9, self.repository.state("u")["stone_num"])
            self.assertEqual(2, len(self.connections))
            scoped = self.connections[1]
            self.assertNotIn("PRAGMA query_only=ON", scoped.statements)
            worker = threading.Thread(target=copy_context().run, args=(work,))
            worker.start()
            worker.join(3)
            self.assertFalse(worker.is_alive())
            self.assertFalse(scoped.closed)
        self.assertEqual([], errors)
        self.assertEqual([9, None], results)
        self.assertEqual(4, len(self.connections))
        self.assertTrue(all(connection.closed for connection in self.connections[1:]))
        self.assertFalse(retained.closed)
        self.assertEqual(9, self.repository.state("u")["stone_num"])
        self.assertEqual(4, len(self.connections))

    def test_ensure_creates_a_live_zero_balance_with_an_independent_writable_transaction(self):
        self.repository.state("u")
        retained = self.connections[0]
        self.assertIsNone(self.repository.state("new"))
        created = self.application.state("new", ensure=True)
        self.assertEqual(("new", 0, "0", "exact"),
                         (created["user_id"], created["stone_num"],
                          created["stone_num_exact"], created["stone_num_precision_state"]))
        self.assertEqual(2, len(self.connections))
        writer = self.connections[1]
        self.assertIn("BEGIN IMMEDIATE", writer.statements)
        self.assertNotIn("PRAGMA query_only=ON", writer.statements)
        self.assertEqual(1, sum(sql.startswith("INSERT OR IGNORE INTO xiuxian_impart")
                                for sql in writer.statements))
        self.assertTrue(writer.closed)
        self.assertFalse(retained.closed)
        self.assertEqual(created, self.repository.state("new"))
        self.assertEqual(2, len(self.connections))

    def test_file_replacement_and_deletion_never_return_the_old_player_or_create_a_file(self):
        self.repository.state("u")
        original = self.connections[-1]
        replacement = self.path.with_name("replacement.db")
        self._create_database(replacement, bonus=0.8)
        replacement.replace(self.path)
        self.assertEqual(0.8, self.repository.state("u")["impart_atk_per"])
        self.assertTrue(original.closed)
        replacement_reader = self.connections[-1]
        self.path.unlink()
        with self.assertRaises(sqlite3.DatabaseError):
            self.repository.state("u")
        self.assertTrue(replacement_reader.closed)
        self.assertFalse(self.path.exists())
        self._create_database(self.path, bonus=0.9)
        self.assertEqual(0.9, self.repository.state("u")["impart_atk_per"])

    def test_read_denial_and_missing_schema_keep_their_errors_and_recover(self):
        self.repository.state("u")
        reader = self.connections[-1]
        reader.set_authorizer(lambda action, table, *_: sqlite3.SQLITE_DENY
                              if action == sqlite3.SQLITE_READ and table == "xiuxian_impart"
                              else sqlite3.SQLITE_OK)
        with self.assertRaises(sqlite3.DatabaseError):
            self.repository.state("u")
        self.assertTrue(reader.closed)
        self.assertEqual(7, self.repository.state("u")["stone_num"])
        recovered = self.connections[-1]
        self.write("ALTER TABLE xiuxian_impart RENAME TO saved_impart")
        with self.assertRaises(DomainError) as caught:
            self.repository.state("u")
        self.assertEqual("schema_missing", caught.exception.code)
        self.assertTrue(recovered.closed)
        self.write("ALTER TABLE saved_impart RENAME TO xiuxian_impart")
        self.assertEqual(7, self.repository.state("u")["stone_num"])

    def test_earliest_legacy_row_and_exact_duplicate_consistency_remain_live(self):
        exact = 10**80 + 1
        self.write("UPDATE xiuxian_impart SET stone_num_exact=?", (str(exact),))
        self.write(
            "INSERT INTO xiuxian_impart(rowid,user_id,impart_atk_per,stone_num,stone_num_exact) "
            "VALUES(619,'u',0.9,0,?)", (str(exact),),
        )
        state = self.repository.state("u", validate_crystal_consistency=True)
        self.assertEqual((159, 0.25, exact),
                         (state["state_row_id"], state["impart_atk_per"], state["stone_num"]))
        reader = self.connections[-1]
        self.write("UPDATE xiuxian_impart SET stone_num_exact=? WHERE rowid=619", (str(exact + 1),))
        self.assertEqual(exact, self.repository.state("u")["stone_num"])
        with self.assertRaises(DomainError) as caught:
            self.repository.state("u", validate_crystal_consistency=True)
        self.assertEqual("impart_rows_inconsistent", caught.exception.code)
        self.assertEqual({"reason": "impart_rows_stone_num", "subject": "u"}, caught.exception.details)
        self.assertTrue(reader.closed)
        self.write("UPDATE xiuxian_impart SET stone_num_exact=? WHERE rowid=619", (str(exact),))
        self.assertEqual(exact, self.repository.state("u", validate_crystal_consistency=True)["stone_num"])

    def test_precision_loss_is_not_hidden_by_reader_reuse_and_recovers_after_repair(self):
        self.repository.state("u")
        reader = self.connections[-1]
        self.write(
            "UPDATE xiuxian_impart SET stone_num=?,stone_num_exact=NULL,"
            "stone_num_precision_state='precision_lost_real'", (float(2**54),),
        )
        with self.assertRaises(DomainError) as caught:
            self.repository.state("u")
        self.assertEqual("impart_crystal_precision_unavailable", caught.exception.code)
        self.assertEqual({"reason": "crystal_precision_lost_real", "subject": "u"}, caught.exception.details)
        self.assertTrue(reader.closed)
        self.write("UPDATE xiuxian_impart SET stone_num_exact='11',stone_num_precision_state='exact'")
        self.assertEqual(11, self.repository.state("u")["stone_num"])

    def test_shutdown_releases_impart_reader_only_on_the_main_thread(self):
        from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
        from nonebot_plugin_xiuxian_2.bootstrap.lifecycle import LifecyclePhase
        from nonebot_plugin_xiuxian_2.paths import configure_paths, get_paths
        from nonebot_plugin_xiuxian_2.plugin import build_lifecycle

        self.addCleanup(configure_paths, get_paths().data)
        context = build_runtime_context(data_dir=self.path.parent / "lifecycle",
                                        settings={}, legacy_startup=False)
        lifecycle, _, _ = build_lifecycle(context)
        self.repository.state("u")
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
        self.assertEqual(7, self.repository.state("u")["stone_num"])
        self.assertIsNot(retained, self.connections[-1])
