from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import db_backend
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.player_data_manager import (
    DocumentWriteRejected,
    PlayerDataManager,
)


class TradeStatisticsBatchTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "player.db"
        self.manager = self.make_manager()

    def make_manager(self):
        manager = object.__new__(PlayerDataManager)
        manager.database_path = self.database
        manager.conn = db_backend.connect(self.database, check_same_thread=False)
        manager._conn_lock = threading.RLock()
        manager._ensured_tables = set()
        manager._ensured_fields = set()
        manager._field_list_cache = {}
        manager._field_list_cache_charged_bytes = 0
        self.addCleanup(manager.conn.close)
        return manager

    def test_warm_event_projects_only_changed_fields_and_commits_once(self):
        self.manager.increment_statistics("buyer", {"count": 1, "amount": 20})
        self.manager.update_or_write_data("buyer", "statistics", "unrelated", {"wide": [1, 2]})
        statements = []
        self.manager.conn._raw.set_trace_callback(statements.append)
        self.manager.increment_statistics("buyer", {"count": 2, "amount": 30, "zero": 0})
        self.manager.conn._raw.set_trace_callback(None)
        reads = [sql for sql in statements if sql.startswith("SELECT")]
        self.assertEqual(len(reads), 2)  # Current values and required final projection.
        self.assertTrue(all('"count", "amount"' in sql for sql in reads))
        self.assertFalse(any("SELECT *" in sql or "unrelated" in sql for sql in reads))
        self.assertEqual(statements.count("BEGIN IMMEDIATE"), 1)
        self.assertEqual(statements.count("COMMIT"), 1)
        self.assertEqual(self.manager.get_fields("buyer", "statistics"),
                         {"user_id": "buyer", "count": 3, "amount": 50,
                          "unrelated": {"wide": [1, 2]}})

    def test_legacy_bad_null_and_json_values_preserve_increment_rules(self):
        values = {"bad": "not-a-number", "null": None, "json": '{"old":1}', "number": "12"}
        self.manager._ensure_table_exists("statistics")
        for key in values:
            self.manager._ensure_field_exists("statistics", key)
        self.manager.conn.execute(
            'INSERT INTO statistics(user_id,bad,"null",json,number) VALUES(%s,%s,%s,%s,%s)',
            ("buyer", *values.values()),
        )
        self.manager.conn.commit()
        self.manager.increment_statistics("buyer", {key: 2 for key in values})
        self.assertEqual(self.manager.get_fields("buyer", "statistics"),
                         {"user_id": "buyer", "bad": 2, "null": 2, "json": 2, "number": 14})

    def test_quoted_keys_and_large_integer_remain_exact(self):
        key = 'event "quoted"; amount'
        huge = 10 ** 60 + 17
        self.manager.increment_statistics("buyer", {key: huge})
        self.manager.increment_statistics("buyer", {key: 3})
        row = self.manager.get_fields("buyer", "statistics")
        self.assertIsInstance(row[key], int)
        self.assertEqual(row[key], huge + 3)
        import nonebot
        nonebot.init()
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
        with (patch.object(utils, "_player_data_manager", return_value=self.manager),
              patch.object(utils, "get_impersonating_target", return_value=None)):
            displayed = utils.get_statistics_data("buyer", raise_errors=True)[key]
        self.assertEqual(displayed, huge + 3)
        self.assertEqual(utils.number_to(str(displayed)), utils.number_to(str(huge + 3)))
        self.assertNotEqual(utils.number_to(str(displayed)), "0")
        quoted = db_backend.quote_ident(key)
        self.assertEqual(self.manager.conn.execute(
            f"SELECT typeof({quoted}) FROM statistics WHERE user_id='buyer'"
        ).fetchone()[0], "blob")

    def test_cross_connection_increments_do_not_lose_updates(self):
        self.manager.increment_statistics("buyer", {"count": 1, "amount": 2})
        second = self.make_manager()
        second._ensure_table_exists("statistics")
        for field in ("count", "amount"):
            second._ensure_field_exists("statistics", field)
        ready = threading.Barrier(2)

        def work(manager):
            ready.wait(timeout=5)
            for _ in range(16):
                manager.increment_statistics("buyer", {"count": 1, "amount": 2})

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(work, manager) for manager in (self.manager, second)]
            for future in futures:
                future.result(timeout=10)
        self.assertEqual(self.manager.get_fields("buyer", "statistics"),
                         {"user_id": "buyer", "count": 33, "amount": 66})

    def test_ignored_or_rewritten_event_rolls_back_every_counter(self):
        self.manager.increment_statistics("buyer", {"count": 1, "amount": 20})
        for body in (
            "SELECT RAISE(IGNORE);",
            "UPDATE statistics SET count=999 WHERE user_id=NEW.user_id;",
            "UPDATE statistics SET count=2.75 WHERE user_id=NEW.user_id;",
            "UPDATE statistics SET count='invalid' WHERE user_id=NEW.user_id;",
        ):
            with self.subTest(body=body):
                when = "BEFORE" if "IGNORE" in body else "AFTER"
                self.manager.conn.execute(
                    f"CREATE TRIGGER reject_statistics {when} UPDATE ON statistics BEGIN {body} END"
                )
                self.manager.conn.commit()
                with self.assertRaises(DocumentWriteRejected):
                    self.manager.increment_statistics("buyer", {"count": 1, "amount": 30})
                self.assertFalse(self.manager.conn.in_transaction)
                self.assertEqual(self.manager.get_fields("buyer", "statistics"),
                                 {"user_id": "buyer", "count": 1, "amount": 20})
                self.manager.conn.execute("DROP TRIGGER reject_statistics")
                self.manager.conn.commit()

    def test_insert_ignore_or_fractional_rewrite_does_not_create_partial_event(self):
        self.manager._ensure_table_exists("statistics")
        for field in ("count", "amount"):
            self.manager._ensure_field_exists("statistics", field, "INTEGER")
        for timing, body in (
            ("BEFORE", "SELECT RAISE(IGNORE);"),
            ("AFTER", "UPDATE statistics SET count=1.5 WHERE user_id=NEW.user_id;"),
        ):
            with self.subTest(timing=timing):
                self.manager.conn.execute(
                    f"CREATE TRIGGER reject_insert {timing} INSERT ON statistics BEGIN {body} END"
                )
                self.manager.conn.commit()
                with self.assertRaises(DocumentWriteRejected):
                    self.manager.increment_statistics("new", {"count": 1, "amount": 20})
                self.assertIsNone(self.manager.get_fields("new", "statistics"))
                self.assertFalse(self.manager.conn.in_transaction)
                self.manager.conn.execute("DROP TRIGGER reject_insert")
                self.manager.conn.commit()

    def test_only_related_field_list_cache_entries_are_invalidated(self):
        self.manager.increment_statistics("buyer", {"count": 1, "amount": 20})
        self.manager.get_all_field_data("statistics", "count", cache_ttl=60)
        self.manager.get_all_field_data("statistics", "amount", cache_ttl=60)
        self.manager.increment_statistics("buyer", {"count": 1})
        self.assertEqual({key[2] for key in self.manager._field_list_cache}, {"amount"})
        self.assertEqual(self.manager.get_all_field_data("statistics", "count", cache_ttl=60),
                         [("buyer", 2)])

    def test_empty_event_does_no_io_and_active_transaction_is_preserved(self):
        self.manager.increment_statistics("buyer", {"count": 0})
        self.assertFalse(self.manager.conn.table_exists("statistics"))
        self.manager.conn.execute("BEGIN")
        with self.assertRaisesRegex(RuntimeError, "idle manager connection"):
            self.manager.increment_statistics("buyer", {"count": 1})
        self.assertTrue(self.manager.conn.in_transaction)
        self.manager.conn.rollback()
        with self.assertRaises(ValueError):
            self.manager.increment_statistics("buyer", {"user_id": 1})


class TradeStatisticsAdapterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import nonebot
        nonebot.init()
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade import trade_utils
        cls.utils, cls.trade_utils = utils, trade_utils

    def test_one_event_resolves_impersonation_and_updates_once(self):
        manager = Mock()
        with (patch.object(self.utils, "_player_data_manager", return_value=manager),
              patch.object(self.utils, "get_impersonating_target", return_value="target") as target):
            self.utils.update_statistics_values("actor", {"count": 1, "amount": 20})
        target.assert_called_once_with("actor")
        manager.increment_statistics.assert_called_once_with("target", {"count": 1, "amount": 20})

    def test_event_keeps_system_user_log_and_zero_counter_behavior(self):
        with (patch.object(self.trade_utils, "log_message") as log,
              patch.object(self.trade_utils, "update_statistics_values") as increment):
            self.trade_utils.record_trade_event("0", "title", "detail", {"count": 1})
            log.assert_not_called()
            increment.assert_not_called()
            self.trade_utils.record_trade_event("buyer", "title", "detail", {})
            log.assert_called_once_with("buyer", "[title] detail")
            increment.assert_not_called()
            self.trade_utils.record_trade_event("buyer", "title", "detail", {"count": 1, "zero": 0})
            increment.assert_called_once_with("buyer", {"count": 1, "zero": 0})

    def test_zero_event_skips_impersonation_and_manager_lookup(self):
        with (patch.object(self.utils, "_player_data_manager") as manager,
              patch.object(self.utils, "get_impersonating_target") as target):
            self.utils.update_statistics_values("actor", {"count": 0})
        manager.assert_not_called()
        target.assert_not_called()


if __name__ == "__main__":
    unittest.main()
