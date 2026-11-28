from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.core.errors import ConflictError, OperationConflictError
from nonebot_plugin_xiuxian_2.core.result import OperationOutcome
from nonebot_plugin_xiuxian_2.features.bank.command_receipt_repository import BankCommandReceiptRepository
from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts
from nonebot_plugin_xiuxian_2.features.mixelixir.application import MixelixirApplication
from nonebot_plugin_xiuxian_2.features.mixelixir.migrations import apply_mixelixir_refine_claim
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache


class RecoveryReadStageTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.database = Path(temporary.name) / "game.db"
        process_read_cache.clear()
        self.addCleanup(process_read_cache.clear)
        self.ledger = OperationLedger()
        with DatabaseUnitOfWork(self.database) as uow:
            self.ledger.ensure_schema(uow)
            apply_mixelixir_refine_claim(uow)
            apply_bank_accounts(uow)
        self.app = MixelixirApplication(self.database, self.database, ledger=self.ledger)
        self.bank = BankCommandReceiptRepository(self.database)

    def _task(self, task="t", *, actor="u", recipe="r", status="ready", created="2026-01-01"):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO mixelixir_refine_tasks(task_id,user_id,recipe_set_id,recipe_key,"
                "status,materials_json,reward_quantity,created_at) VALUES(?,?,'custom',?,?, '[]',1,?)",
                (task, actor, recipe, status, created),
            )

    def _record(self, status, *, recipe="r"):
        with DatabaseUnitOfWork(self.database) as uow:
            self.ledger.begin(uow, "op", "mixelixir.refine_cost",
                              {"recipe_set_id": "custom", "recipe_key": recipe})
            if status in {"applied", "rejected"}:
                factory = getattr(OperationOutcome, status)
                self.ledger.finish(uow, factory("op", "mixelixir.refine_cost",
                                               message="terminal", data={"task_id": "t"}))
            else:
                uow.execute("UPDATE operation_ledger SET status=?", (status,))

    @contextmanager
    def _observe(self, module, *, deny=None):
        statements, connections = [], []

        @contextmanager
        def observed(*args, **kwargs):
            self.assertTrue(kwargs.get("read_only"))
            with DatabaseUnitOfWork(*args, **kwargs) as uow:
                connections.append(uow.connection)
                uow.connection.set_trace_callback(statements.append)
                if deny:
                    uow.connection.set_authorizer(
                        lambda action, table, *_: sqlite3.SQLITE_DENY
                        if action == sqlite3.SQLITE_READ and table == deny else sqlite3.SQLITE_OK
                    )
                yield uow

        with patch(f"nonebot_plugin_xiuxian_2.features.{module}.DatabaseUnitOfWork", observed):
            yield statements, connections

    def _state(self, operation="op", recipe="r", actor="u"):
        return self.app.refine_recovery_state(operation_id=operation, user_id=actor, recipe_key=recipe)

    def _receipt(self, operation="op", action="deposit", amount=10):
        return self.bank.find(operation_id=operation, user_id="u", action=action, amount=amount)

    def test_initial_stage_uses_one_read_transaction_and_one_catalog_then_live_rows(self):
        self._task()
        for attempt in range(2):
            with self.subTest(attempt=attempt), self._observe("mixelixir.application") as (sql, connections):
                self.assertEqual(self._state(), (None, "t"))
                self.assertEqual(len(connections), 1)
                self.assertEqual(sum("PRAGMA schema_version" in query for query in sql), 0)
                self.assertEqual(sum("SELECT name FROM sqlite_master" in query for query in sql), 1)
                self.assertFalse(any("SELECT 1 FROM sqlite_master" in query for query in sql))
                self.assertEqual(sum("FROM operation_ledger WHERE" in query for query in sql), 1)
                self.assertEqual(sum("FROM mixelixir_refine_tasks WHERE" in query for query in sql), 1)
                self.assertEqual(sql[-1], "COMMIT")

    def test_terminal_replay_short_circuits_tasks_and_preserves_outcome(self):
        for status in ("applied", "rejected"):
            with self.subTest(status=status):
                with DatabaseUnitOfWork(self.database) as uow:
                    uow.execute("DELETE FROM operation_ledger")
                self._record(status)
                with self._observe("mixelixir.application", deny="mixelixir_refine_tasks") as (sql, _):
                    replay, task = self._state()
                self.assertIsNone(task)
                self.assertEqual(replay.status, status)
                self.assertTrue(replay.replayed)
                self.assertEqual(replay, self.app.refine_cost_replay(operation_id="op", recipe_key="r"))
                self.assertFalse(any("FROM mixelixir_refine_tasks WHERE" in query for query in sql))

    def test_failed_and_reconcile_allow_recovery_but_pending_and_hash_conflicts_do_not(self):
        self._task()
        for status in ("failed", "needs_reconcile", "started"):
            with self.subTest(status=status):
                with DatabaseUnitOfWork(self.database) as uow:
                    uow.execute("DELETE FROM operation_ledger")
                self._record(status)
                if status == "started":
                    with self.assertRaises(ConflictError):
                        self._state()
                else:
                    self.assertEqual(self._state(), (None, "t"))
                with self.assertRaises(OperationConflictError):
                    self._state(recipe="different")

    def test_no_event_skips_ledger_and_tasks_preserve_actor_recipe_status_and_order(self):
        for task, options in (
            ("a", {}), ("b", {}), ("new", {"created": "2026-02-01"}),
            ("other", {"actor": "other", "created": "2026-03-01"}),
            ("recipe", {"recipe": "s", "created": "2026-03-01"}),
            ("claimed", {"status": "claimed", "created": "2026-04-01"}),
        ):
            self._task(task, **options)
        with self._observe("mixelixir.application", deny="operation_ledger"):
            self.assertEqual(self._state(operation=None), (None, "new"))
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("DELETE FROM mixelixir_refine_tasks WHERE task_id='new'")
        self.assertEqual(self._state(), (None, "b"))
        self.assertEqual(self.app.latest_ready_refine_task(user_id="u", recipe_key=" r "), "b")
        self.assertEqual(self.app.latest_ready_refine_task(user_id="u"), "recipe")

    def test_repeated_stage_never_caches_personal_rows_or_hides_read_errors(self):
        self.assertEqual(self._state(), (None, None))
        self._task()
        self.assertEqual(self._state(), (None, "t"))
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE mixelixir_refine_tasks SET status='claimed'")
        self.assertEqual(self._state(), (None, None))
        for table in ("operation_ledger", "mixelixir_refine_tasks"):
            with self.subTest(table=table), self._observe("mixelixir.application", deny=table):
                with self.assertRaises(sqlite3.DatabaseError):
                    self._state()

    def test_missing_tables_are_independent_and_new_schema_is_visible(self):
        self._task()
        self._state()
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("DROP TABLE operation_ledger")
        self.assertEqual(self._state(), (None, "t"))
        self.assertIsNone(self.app.refine_cost_replay(operation_id="op", recipe_key="r"))
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("DROP TABLE mixelixir_refine_tasks")
            self.ledger.ensure_schema(uow)
        self.assertEqual(self._state(), (None, None))
        self.assertIsNone(self.app.latest_ready_refine_task(user_id="u"))
        self._record("applied")
        self.assertEqual(self._state()[0].status, "applied")
        with DatabaseUnitOfWork(self.database) as uow:
            apply_mixelixir_refine_claim(uow)
        self._task("new")
        self.assertEqual(self._state(operation=None), (None, "new"))

    def _legacy_tables(self):
        with DatabaseUnitOfWork(self.database) as uow:
            for table, columns in self.bank._LEGACY.values():
                fields = ",".join(columns)
                uow.execute(f"CREATE TABLE {table}(operation_id TEXT PRIMARY KEY,payload TEXT,{fields})")

    def _deposit(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("INSERT INTO bank_deposit_operations VALUES('op',?,10,0,990,110,'now')",
                        (json.dumps(["u", 10]),))

    def test_bank_five_table_search_uses_one_catalog_preserving_all_receipt_reads(self):
        self._legacy_tables()
        for attempt in range(2):
            with self.subTest(attempt=attempt), self._observe("bank.command_receipt_repository") as (sql, connections):
                self.assertIsNone(self._receipt())
                self.assertEqual(len(connections), 1)
                self.assertEqual(sum("PRAGMA schema_version" in query for query in sql), 0)
                self.assertEqual(sum("SELECT name FROM sqlite_master" in query for query in sql), 1)
                self.assertFalse(any("SELECT 1 FROM sqlite_master" in query for query in sql))
                self.assertEqual(sum(query.startswith("PRAGMA table_info") for query in sql), 5)
                self.assertEqual(sum(query.startswith('SELECT * FROM "bank_') for query in sql), 5)
        self._deposit()
        self.assertEqual(self._receipt()["deposited"], 10)
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("INSERT INTO bank_interest_operations VALUES('op','[\"u\"]',0,990,'now')")
        self.assertEqual(self._receipt()["status"], "operation_conflict")

    def test_bank_repeated_reads_keep_exact_case_schema_validation_and_live_read_errors(self):
        self.assertIsNone(self._receipt())

        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("CREATE TABLE bank_deposit_operations(OPERATION_ID TEXT,payload TEXT,"
                        "deposited,interest,wallet_stone,saved_stone,saved_at)")
        self.assertEqual(self._receipt()["status"], "receipt_invalid")
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("DROP TABLE bank_deposit_operations")
        self._legacy_tables()
        self._deposit()
        self.assertEqual(self._receipt()["status"], "duplicate")
        for table in ("bank_account_operations", "bank_deposit_operations"):
            with self.subTest(table=table), self._observe("bank.command_receipt_repository", deny=table):
                with self.assertRaises(sqlite3.DatabaseError):
                    self._receipt()
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("DELETE FROM bank_deposit_operations")
        self.assertIsNone(self._receipt())

    def test_live_directory_follows_an_old_connection_without_polluting_a_replacement(self):
        from nonebot_plugin_xiuxian_2.features.base.schema_catalog import read_sqlite_table_names

        replacement = self.database.with_name("replacement.db")
        with DatabaseUnitOfWork(replacement) as uow:
            uow.execute("CREATE TABLE replacement_only(value TEXT)")
        with DatabaseUnitOfWork(self.database, read_only=True) as old:
            self.assertIn("operation_ledger", read_sqlite_table_names(old))
            replacement.replace(self.database)
            self.assertNotIn("replacement_only", read_sqlite_table_names(old))
        with DatabaseUnitOfWork(self.database, read_only=True) as new:
            self.assertEqual({"replacement_only"}, read_sqlite_table_names(new))
        self.assertEqual(0, process_read_cache.stats()["entries"])


if __name__ == "__main__":
    unittest.main()
