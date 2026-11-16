"""Durable creation continuations and actual transaction rollback evidence."""
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.features.trade.guishi_immediate_match import GuishiMatchFailure
from nonebot_plugin_xiuxian_2.features.trade.migrations import (
    apply_trade_guishi_immediate_match, apply_trade_guishi_match,
    apply_trade_guishi_match_statistics, apply_trade_guishi_qiugou,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database


class GuishiImmediateMatchTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.game, self.trade, self.player = [self.root / f"{name}.db" for name in ("game", "trade", "player")]
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER,"
                        "update_time TEXT,state INTEGER,UNIQUE(user_id,goods_id))")
            uow.execute("INSERT INTO back VALUES('seller',1001,10,0,'old',0)")
        with DatabaseUnitOfWork(self.trade) as uow:
            uow.execute("CREATE TABLE guishi_item(id TEXT PRIMARY KEY,user_id TEXT,item_id INTEGER,"
                        "item_name TEXT,item_type TEXT,price INTEGER,quantity INTEGER,filled_quantity INTEGER DEFAULT 0)")
            uow.execute("CREATE TABLE guishi_info(user_id TEXT PRIMARY KEY,stored_stone INTEGER,items TEXT)")
            uow.execute("INSERT INTO guishi_info VALUES('buyer',1000,'{}')")
            apply_trade_guishi_qiugou(uow)
            apply_trade_guishi_match(uow)
            apply_trade_guishi_immediate_match(uow)
        with DatabaseUnitOfWork(self.player) as uow:
            apply_trade_guishi_match_statistics(uow)
        self.app = self.restart()

    def tearDown(self):
        self.directory.cleanup()

    def restart(self):
        clock = SimpleNamespace(now=lambda: datetime(2026, 11, 21, 13, tzinfo=timezone.utc))
        return TradeApplication(self.game, self.trade, player_database=self.player, clock=clock)

    def query(self, database, sql, params=()):
        with sqlite3.connect(database) as connection:
            return connection.execute(sql, params).fetchall()

    def write(self, database, sql, params=()):
        with sqlite3.connect(database) as connection:
            connection.execute(sql, params)

    def snapshot(self):
        return (
            tuple(self.query(self.trade, f"SELECT * FROM {table} ORDER BY 1"))
            for table in ("guishi_info", "guishi_item", "guishi_order_create_operations", "guishi_match_operations",
                          "guishi_immediate_match_requests")
        )

    def state(self):
        return tuple(self.snapshot()) + (tuple(self.query(self.game, "SELECT * FROM back")),
                                         tuple(self.query(self.player, "SELECT * FROM statistics ORDER BY user_id")))

    def create(self, kind="qiugou", *, immediate=True):
        method = self.app.guishi_qiugou if kind == "qiugou" else self.app.guishi_baitan
        return method(operation_id=f"create-{kind}", user_id="buyer" if kind == "qiugou" else "seller",
                      item_id=1001, item_name="灵草", price=20, quantity=2, max_orders=10, immediate_match=immediate)

    def seed_counterpart(self, order_id, kind, price=20, quantity=1):
        self.write(self.trade, "INSERT INTO guishi_item VALUES(?,?,?,?,?,?,?,0)",
                   (order_id, "seller" if kind == "baitan" else "buyer", 1001, "灵草", kind, price, quantity))

    def test_new_migrations_are_routed_idempotent_and_preserve_statistics(self):
        migrations = build_migrations()
        for database, expected in (("trade_db", {"trade.015"}), ("player_db", {"trade.016"}), ("game_db", set())):
            actual = {m.version for m in migrations_for_database(migrations, database)} & {"trade.015", "trade.016"}
            self.assertEqual(actual, expected)
        self.write(self.player, 'INSERT INTO statistics(user_id,"鬼市售出次数") VALUES(\'seller\',7)')
        with DatabaseUnitOfWork(self.player) as uow:
            apply_trade_guishi_match_statistics(uow)
        with DatabaseUnitOfWork(self.trade) as uow:
            apply_trade_guishi_immediate_match(uow)
        self.assertEqual(self.query(self.player, 'SELECT "鬼市售出次数" FROM statistics'), [(7,)])

    def test_creation_without_pending_migration_rolls_back_both_order_types(self):
        self.write(self.trade, "DROP TABLE guishi_immediate_match_requests")
        for kind in ("qiugou", "baitan"):
            with self.subTest(kind=kind):
                with self.assertRaises(DomainError) as caught:
                    self.create(kind)
                self.assertEqual(caught.exception.details['reason'], "schema_missing")
                self.assertEqual(self.query(self.trade, "SELECT stored_stone FROM guishi_info"), [(1000,)])
                self.assertEqual(self.query(self.trade, "SELECT * FROM guishi_item"), [])
                self.assertEqual(self.query(self.trade, "SELECT * FROM guishi_order_create_operations"), [])
                self.assertEqual(self.query(self.game, "SELECT goods_num FROM back"), [(10,)])

    def test_creation_commit_survives_restart_and_completed_request_never_restarts(self):
        created = self.create()
        self.seed_counterpart("sell", "baitan", quantity=1)
        app = self.restart()
        self.assertEqual(len(app.guishi_pending_immediate_matches()), 1)
        result = app.guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual((result.status, len(result.matches)), ("complete", 1))
        self.seed_counterpart("later", "baitan", quantity=1)
        before = self.state()
        self.assertEqual(app.guishi_resume_immediate_match(order_id=created.order_id).matches, ())
        self.assertEqual(before, self.state())

    def test_legacy_creation_replay_does_not_guess_a_new_matching_pass(self):
        created = self.create(immediate=False)
        self.seed_counterpart("sell", "baitan", quantity=2)
        before = self.state()
        replay = self.create()
        self.assertEqual((replay.status, replay.order_id), ("duplicate", created.order_id))
        self.assertEqual(self.app.guishi_resume_immediate_match(order_id=replay.order_id).status, "legacy")
        self.assertEqual(before, self.state())

    def test_pair_statistics_failure_rolls_back_assets_orders_receipt_and_first_statistic(self):
        created = self.create()
        self.seed_counterpart("sell", "baitan", quantity=2)
        self.write(self.player, "CREATE TRIGGER deny_stats BEFORE INSERT ON statistics WHEN NEW.user_id='seller' "
                   "BEGIN SELECT RAISE(ABORT,'stats fault'); END")
        before = self.state()
        with self.assertRaisesRegex(sqlite3.IntegrityError, "stats fault"):
            self.app.guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual(before, self.state())
        self.write(self.player, "DROP TRIGGER deny_stats")
        result = self.restart().guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual(len(result.matches), 1)
        self.assertTrue(result.matches[0].statistics_recorded)
        self.assertEqual(self.query(self.player, 'SELECT "鬼市售出次数","鬼市售出数量","鬼市收入灵石" '
                                    "FROM statistics WHERE user_id='seller'"), [(1, 2, 40)])
        for database in (self.game, self.trade, self.player):
            self.assertEqual(self.query(database, "PRAGMA integrity_check"), [("ok",)])

    def test_complete_mark_failure_leaves_pending_and_does_not_duplicate_settlement(self):
        created = self.create()
        self.seed_counterpart("sell", "baitan", quantity=2)
        self.write(self.trade, "CREATE TRIGGER deny_complete BEFORE UPDATE ON guishi_immediate_match_requests "
                   "WHEN NEW.status='complete' BEGIN SELECT RAISE(ABORT,'mark fault'); END")
        with self.assertRaisesRegex(sqlite3.IntegrityError, "mark fault"):
            self.app.guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual(self.query(self.trade, "SELECT status FROM guishi_immediate_match_requests"), [("pending",)])
        self.assertEqual(self.app.guishi_get_account("seller")[0], 40)
        self.write(self.trade, "DROP TRIGGER deny_complete")
        result = self.restart().guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual((result.status, result.matches), ("complete", ()))
        self.assertEqual(self.app.guishi_get_account("seller")[0], 40)
        self.assertEqual(self.query(self.player, 'SELECT "鬼市售出次数" FROM statistics WHERE user_id=\'seller\''), [(1,)])

    def test_buy_resumption_preserves_partial_failure_and_original_exception(self):
        created = self.create()
        self.seed_counterpart("first", "baitan", price=10)
        self.seed_counterpart("second", "baitan", price=20)
        conflict = f"guishi-match:{created.order_id}:second"
        self.write(self.trade, "INSERT INTO guishi_match_operations(operation_id,payload,result) VALUES(?,?,?)",
                   (conflict, '["bad","pair"]', '{}'))
        result = self.app.guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual((result.status, result.failure.reason, len(result.matches)), ("pending", "operation_conflict", 1))
        self.assertEqual(result.matches[0].baitan_order_id, "first")
        self.assertEqual(self.query(self.trade, "SELECT status,reason,baitan_order_id FROM guishi_immediate_match_requests"),
                         [("pending", "operation_conflict", "second")])
        self.write(self.trade, "DELETE FROM guishi_match_operations WHERE operation_id=?", (conflict,))
        failure = RuntimeError("specific storage fault")
        with patch.object(self.app.guishi_listing_fill_service, "match", side_effect=failure):
            with self.assertRaises(RuntimeError) as caught:
                self.app.guishi_resume_immediate_match(order_id=created.order_id)
        self.assertIs(caught.exception, failure)
        self.assertEqual(self.restart().guishi_resume_immediate_match(order_id=created.order_id).matches[0].quantity, 1)
        self.assertEqual(self.app.guishi_get_account("seller")[0], 30)

    def test_invalid_existing_listing_is_explicit_and_retained(self):
        created = self.create("baitan")
        self.write(self.trade, "UPDATE guishi_item SET item_type='invalid' WHERE id=?", (created.order_id,))
        result = self.app.guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual(result.failure.reason, "baitan_invalid")
        self.assertEqual(len(self.app.guishi_pending_immediate_matches()), 1)

    def test_missing_player_migration_does_not_settle_or_create_request_time_schema(self):
        created = self.create()
        self.seed_counterpart("sell", "baitan", quantity=2)
        self.write(self.player, "DROP TABLE statistics")
        before = tuple(self.snapshot())[:4]
        result = self.app.guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual((result.failure.reason, result.failure.subject), ("schema_missing", "guishi_match_statistics"))
        self.assertIsInstance(result.failure.__cause__, DomainError)
        self.assertEqual(before, tuple(self.snapshot())[:4])
        self.assertEqual(self.query(self.trade, "SELECT status,reason,subject FROM guishi_immediate_match_requests"),
                         [("pending", "schema_missing", "guishi_match_statistics")])
        self.assertEqual(self.query(self.player, "SELECT name FROM sqlite_master WHERE type='table'"), [])

    def test_pair_statistics_keep_exact_large_amount_and_replay_zero_writes(self):
        created = self.create()
        self.seed_counterpart("sell", "baitan", quantity=2)
        total = 2**63 + 101
        self.write(self.player, 'INSERT INTO statistics(user_id,"鬼市收入灵石") VALUES(?,?)',
                   ("seller", str(total).encode("ascii")))
        result = self.app.guishi_resume_immediate_match(order_id=created.order_id)
        actual = self.query(self.player, 'SELECT "鬼市收入灵石" FROM statistics WHERE user_id=\'seller\'')[0][0]
        self.assertEqual(int(actual), total + 40)
        before = self.state()
        replay = self.app.guishi_match(operation_id=f"guishi-match:{created.order_id}:sell",
                                      qiugou_order_id=created.order_id, baitan_order_id="sell")
        self.assertEqual((replay.status, replay.statistics_recorded), ("duplicate", True))
        self.assertEqual(result.matches[0].amount, 40)
        self.assertEqual(before, self.state())

    def test_failed_reason_persistence_does_not_erase_pending_or_original_settlement(self):
        created = self.create()
        self.seed_counterpart("sell", "baitan", quantity=2)
        self.write(self.trade, "INSERT INTO guishi_match_operations(operation_id,payload,result) VALUES(?,?,?)",
                   (f"guishi-match:{created.order_id}:sell", '["bad","pair"]', '{}'))
        self.write(self.trade, "CREATE TRIGGER deny_reason BEFORE UPDATE ON guishi_immediate_match_requests "
                   "BEGIN SELECT RAISE(ABORT,'reason fault'); END")
        before = self.state()
        with self.assertRaises(GuishiMatchFailure) as caught:
            self.app.guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual(caught.exception.reason, "operation_conflict")
        self.assertIsInstance(caught.exception.__cause__, sqlite3.IntegrityError)
        self.assertEqual(str(caught.exception.__cause__), "reason fault")
        self.assertEqual(before, self.state())
        self.write(self.trade, "DROP TRIGGER deny_reason")
        result = self.restart().guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual(result.failure.reason, "operation_conflict")

    def test_ignored_complete_write_is_failure_not_false_complete(self):
        created = self.create()
        self.write(self.trade, "CREATE TRIGGER ignore_complete BEFORE UPDATE ON guishi_immediate_match_requests "
                   "BEGIN SELECT RAISE(IGNORE); END")
        with self.assertRaises(DomainError) as caught:
            self.app.guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual(caught.exception.details, {"reason": "request_write_rejected", "subject": created.order_id})
        self.assertEqual(len(self.app.guishi_pending_immediate_matches()), 1)

    def test_ignored_statistics_update_does_not_commit_match_or_buyer_statistics(self):
        created = self.create()
        self.seed_counterpart("sell", "baitan", quantity=2)
        self.write(self.player, "INSERT INTO statistics(user_id) VALUES('seller')")
        self.write(self.player, "CREATE TRIGGER ignore_stats BEFORE UPDATE ON statistics "
                   "WHEN NEW.user_id='seller' BEGIN SELECT RAISE(IGNORE); END")
        before = self.state()
        result = self.app.guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual((result.failure.reason, result.failure.subject), ("statistics_write_rejected", "seller"))
        after = self.state()
        self.assertEqual(before[:4] + before[5:], after[:4] + after[5:])
        self.assertEqual(len(self.app.guishi_pending_immediate_matches()), 1)


if __name__ == "__main__":
    unittest.main()
