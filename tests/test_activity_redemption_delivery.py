from __future__ import annotations

import asyncio
import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.core.errors import OperationConflictError
from nonebot_plugin_xiuxian_2.features.activity.application import ActivityApplication
from nonebot_plugin_xiuxian_2.features.activity.collect_exchange_repository import ActivityCollectExchangeSqlRepository
from nonebot_plugin_xiuxian_2.features.activity.point_shop_purchase_repository import ActivityPointShopPurchaseSqlRepository
from nonebot_plugin_xiuxian_2.features.activity.migrations import apply_activity_event_receipts, apply_activity_state_schema
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.transaction_service import ActivityCollectExchangeService, ActivityPointShopPurchaseService
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import service
from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_activity as commands
from nonebot.exception import FinishedException


class ActivityRedemptionDeliveryTests(unittest.TestCase):
    rewards = (
        {"type": "stone", "quantity": 50},
        {"id": 101, "name": "活动令", "type": "道具", "quantity": 2},
        {"id": 102, "name": "兑换令", "type": "道具", "quantity": 1},
    )

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def fixture(self, kind, mode):
        root = self.root / f"{kind}-{mode}"
        root.mkdir()
        activity = root / "activity.db"
        game = root / "game.db" if mode == "attached" else activity
        with DatabaseUnitOfWork(activity) as uow:
            apply_activity_state_schema(uow)
            apply_activity_event_receipts(uow)
            OperationLedger().ensure_schema(uow)
            uow.execute("INSERT INTO activity_point_balance(activity_key,user_id,points,total_points) VALUES('a','u',1000,1000)")
            for char in ("端", "午"):
                uow.execute("INSERT INTO activity_collect_inventory(activity_key,user_id,word_char,count) VALUES('a','u',?,2)", (char,))
        with DatabaseUnitOfWork(game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u',10)")
            uow.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
        if kind == "shop":
            repo = ActivityPointShopPurchaseSqlRepository(activity) if mode == "feature" else ActivityPointShopPurchaseService(activity, game)
            invoke = lambda op="op": repo.purchase(op, "u", "a", "i", 2, 100, 4, 8, self.rewards, 100)
        else:
            repo = ActivityCollectExchangeSqlRepository(activity) if mode == "feature" else ActivityCollectExchangeService(activity, game)
            invoke = lambda op="op": repo.exchange(op, "u", "a", "端午", {"端": 1, "午": 1}, 4, self.rewards, 100)
        return activity, game, repo, invoke

    @staticmethod
    def snapshot(*paths):
        result = []
        for path in dict.fromkeys(paths):
            with sqlite3.connect(path) as conn:
                tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
                result.append({name: conn.execute(f'SELECT * FROM "{name}" ORDER BY rowid').fetchall() for name in tables})
        return result

    def test_ignored_write_at_every_step_rolls_back_both_databases(self):
        for kind in ("shop", "collect"):
            steps = [
                ("activity_point_balance" if kind == "shop" else "activity_collect_inventory", "UPDATE", "activity_points_write_rejected" if kind == "shop" else "activity_tokens_write_rejected", ""),
                ("activity_point_purchase" if kind == "shop" else "activity_collect_claim", "INSERT", "activity_purchase_write_rejected" if kind == "shop" else "activity_collect_claim_write_rejected", ""),
                ("user_xiuxian", "UPDATE", "activity_reward_stone_write_rejected", ""),
                ("back", "INSERT", "activity_reward_item_write_rejected", "WHEN NEW.goods_id=102"),
                ("activity_point_purchase_operations" if kind == "shop" else "activity_collect_exchange_operations", "INSERT", "activity_redemption_receipt_write_rejected", ""),
            ]
            for mode in ("feature", "same", "attached"):
                activity, game, _, invoke = self.fixture(kind, mode)
                for table, action, reason, condition in steps:
                    with self.subTest(kind=kind, mode=mode, table=table):
                        database = game if table in {"user_xiuxian", "back"} else activity
                        with sqlite3.connect(database) as conn:
                            conn.execute(f"CREATE TRIGGER reject_step BEFORE {action} ON {table} {condition} BEGIN SELECT RAISE(IGNORE); END")
                        before = self.snapshot(activity, game)
                        try:
                            with self.assertRaises(DomainError) as raised:
                                invoke()
                            self.assertEqual(reason, raised.exception.details.get("reason"))
                            self.assertEqual("actor", raised.exception.details.get("subject"))
                            self.assertEqual(before, self.snapshot(activity, game))
                        finally:
                            with sqlite3.connect(database) as conn:
                                conn.execute("DROP TRIGGER reject_step")

    def test_success_replay_and_duplicate_player_rows_keep_existing_semantics(self):
        for kind in ("shop", "collect"):
            for mode in ("feature", "same", "attached"):
                with self.subTest(kind=kind, mode=mode):
                    activity, game, _, invoke = self.fixture(kind, mode)
                    with sqlite3.connect(game) as conn:
                        conn.execute("INSERT INTO user_xiuxian VALUES('u',10)")
                    self.assertEqual("applied", invoke().status)
                    after = self.snapshot(activity, game)
                    self.assertEqual("duplicate", invoke().status)
                    self.assertEqual(after, self.snapshot(activity, game))
                    with sqlite3.connect(game) as conn:
                        self.assertEqual([(60,), (60,)], conn.execute("SELECT stone FROM user_xiuxian").fetchall())
                        self.assertEqual([(101, 2, 2), (102, 1, 1)], conn.execute("SELECT goods_id,goods_num,bind_num FROM back ORDER BY goods_id").fetchall())

    def application_fixture(self, kind, mode="feature"):
        activity, game, repo, invoke = self.fixture(kind, mode)
        application = ActivityApplication(activity)
        action = "claim_point_shop_item" if kind == "shop" else "claim_collect_phrase"
        query = "兑换令 2" if kind == "shop" else "端午"
        activity_def = {"key": "a", "name": "原活动", "point_name": "原积分"}
        item = {"item_key": "i", "name": "兑换令", "cost": 100, "limit": 4, "stock_limit": 8, "reward": "frozen"}
        phrase = {"phrase": "端午", "name": "原词组", "limit": 4, "reward": "frozen"}
        patches = (
            patch.object(service, "load_config", return_value={}),
            patch.object(service, "activity_runtime_state", return_value={"ok": True, "features": {"shop", "exchange"}}),
            patch.object(service, "_find_point_shop_item", return_value=(activity_def, item)),
            patch.object(service, "_find_collect_phrase", return_value=(activity_def, phrase)),
            patch.object(service, "parse_reward", return_value=list(self.rewards)),
        )
        for item_patch in patches:
            item_patch.start()
            self.addCleanup(item_patch.stop)
        execute = lambda op="op", uid="u", text=query: application.execute(
            operation_id=op, user_id=uid, payload={"action": action, "query": text}
        )
        return activity, game, application, execute

    def test_finish_interrupt_recovers_frozen_business_receipt_and_rejects_identity_reuse(self):
        for kind in ("shop", "collect"):
            for interruption in (RuntimeError, SystemExit):
                with self.subTest(kind=kind, interruption=interruption):
                    database, _, application, execute = self.application_fixture(kind, interruption.__name__)
                    finish = application.ledger.finish
                    def interrupt(uow, outcome):
                        if outcome.ok:
                            raise interruption("after business commit")
                        return finish(uow, outcome)
                    with patch.object(application.ledger, "finish", side_effect=interrupt):
                        with self.assertRaises(interruption):
                            execute()
                    with sqlite3.connect(database) as conn:
                        self.assertEqual("failed" if interruption is RuntimeError else "started", conn.execute("SELECT status FROM operation_ledger").fetchone()[0])
                        receipt_table = "activity_point_purchase_operations" if kind == "shop" else "activity_collect_exchange_operations"
                        self.assertEqual(1, conn.execute(f"SELECT COUNT(*) FROM {receipt_table}").fetchone()[0])
                        frozen = (application.repository.point_shop_purchase if kind == "shop" else application.repository.collect_exchange).lookup_receipt("op", "u").response
                    committed = self.snapshot(database)[0]
                    with patch.object(service, "load_config", side_effect=AssertionError("replay must precede config")):
                        recovered = execute()
                        self.assertTrue(recovered.ok)
                        self.assertEqual(frozen, recovered.data["message"])
                        self.assertTrue(execute().replayed)
                        with self.assertRaises(OperationConflictError):
                            execute(uid="other")
                        with self.assertRaises(OperationConflictError):
                            execute(text="changed query")
                    after = self.snapshot(database)[0]
                    for table in committed.keys() - {"operation_ledger", "operation_audit", "sqlite_sequence"}:
                        self.assertEqual(committed[table], after[table], table)

    def test_started_without_business_receipt_obeys_current_gate_and_keeps_reason(self):
        for kind in ("shop", "collect"):
            with self.subTest(kind=kind):
                database, _, application, execute = self.application_fixture(kind)
                action = "claim_point_shop_item" if kind == "shop" else "claim_collect_phrase"
                query = "兑换令 2" if kind == "shop" else "端午"
                with DatabaseUnitOfWork(database) as uow:
                    application.ledger.begin(uow, "op", "activity." + action, {"query": query, "user_id": "u"})
                before = self.snapshot(database)[0]
                with patch.object(service, "activity_runtime_state", return_value={"ok": False, "reason": "活动已结束"}):
                    result = execute()
                self.assertFalse(result.ok)
                self.assertEqual("activity_unavailable", result.data.get("reason"))
                self.assertEqual("actor", result.data.get("subject"))
                self.assertEqual("活动已结束", result.message)
                replay = execute()
                self.assertFalse(replay.ok)
                self.assertTrue(replay.replayed)
                self.assertEqual(result.data, replay.data)
                after = self.snapshot(database)[0]
                for table in before.keys() - {"operation_ledger", "operation_audit", "sqlite_sequence"}:
                    self.assertEqual(before[table], after[table], table)

    def test_write_rejection_reason_is_durable_and_reaches_real_command_reply(self):
        for kind in ("shop", "collect"):
            with self.subTest(kind=kind):
                database, _, application, _ = self.application_fixture(kind)
                table = "activity_point_purchase_operations" if kind == "shop" else "activity_collect_exchange_operations"
                with sqlite3.connect(database) as conn:
                    conn.execute(f"CREATE TRIGGER reject_receipt BEFORE INSERT ON {table} BEGIN SELECT RAISE(IGNORE); END")
                before = self.snapshot(database)[0]
                handler = (commands.activity_buy_cmd if kind == "shop" else commands.activity_exchange_cmd).handlers[0].call
                query = "兑换令 2" if kind == "shop" else "端午"
                event = SimpleNamespace(message_id="rejected")
                with (
                    patch.object(commands, "activity_application", application),
                    patch.object(commands, "assign_bot", AsyncMock()),
                    patch.object(commands, "_ensure_user", AsyncMock(return_value=(True, {"user_id": "u"}, ""))),
                    patch.object(commands, "handle_send", AsyncMock()) as send,
                ):
                    asyncio.run(handler("bot", event, SimpleNamespace(extract_plain_text=lambda: query)))
                    first_text = send.call_args.args[2]
                    self.assertIn("回执", first_text)
                    self.assertIn("撤销", first_text)
                    self.assertIn("发起方", first_text)
                    asyncio.run(handler("bot", event, SimpleNamespace(extract_plain_text=lambda: query)))
                    self.assertEqual(first_text, send.call_args.args[2])
                after = self.snapshot(database)[0]
                for name in before.keys() - {"operation_ledger", "operation_audit", "sqlite_sequence"}:
                    self.assertEqual(before[name], after[name], name)
                with sqlite3.connect(database) as conn:
                    result = json.loads(conn.execute("SELECT result_json FROM operation_ledger").fetchone()[0])
                    self.assertEqual("activity_redemption_receipt_write_rejected", result["data"]["reason"])
                    self.assertEqual("actor", result["data"]["subject"])

    def test_message_failure_and_nonebot_finish_preserve_committed_result(self):
        for kind in ("shop", "collect"):
            with self.subTest(kind=kind):
                database, _, application, _ = self.application_fixture(kind)
                handler = (commands.activity_buy_cmd if kind == "shop" else commands.activity_exchange_cmd).handlers[0].call
                query = "兑换令 2" if kind == "shop" else "端午"
                event = SimpleNamespace(message_id="delivery")
                with (
                    patch.object(commands, "activity_application", application),
                    patch.object(commands, "assign_bot", AsyncMock()),
                    patch.object(commands, "_ensure_user", AsyncMock(return_value=(True, {"user_id": "u"}, ""))),
                    patch.object(commands, "handle_send", AsyncMock(side_effect=RuntimeError("delivery down"))),
                    patch("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils.handle_send", AsyncMock()) as fallback,
                ):
                    asyncio.run(handler("bot", event, SimpleNamespace(extract_plain_text=lambda: query)))
                    self.assertIn("处理或回复", fallback.call_args.args[2])
                    self.assertNotIn("未扣", fallback.call_args.args[2])
                    committed = self.snapshot(database)
                    with patch.object(commands, "handle_send", AsyncMock(side_effect=FinishedException)):
                        with self.assertRaises(FinishedException):
                            asyncio.run(handler("bot", event, SimpleNamespace(extract_plain_text=lambda: query)))
                    self.assertEqual(committed, self.snapshot(database))
                    self.assertEqual(1, fallback.call_count)

    def test_legacy_receipt_without_display_fields_recovers_before_config(self):
        for kind in ("shop", "collect"):
            with self.subTest(kind=kind):
                database, _, repo, invoke = self.fixture(kind, "feature")
                invoke()
                if kind == "collect":
                    with sqlite3.connect(database) as conn:
                        raw = json.loads(conn.execute("SELECT result_json FROM activity_collect_exchange_operations").fetchone()[0])
                        conn.execute("UPDATE activity_collect_exchange_operations SET result_json=?", (json.dumps(raw[:2]),))
                before = self.snapshot(database)
                claim = service.claim_point_shop_item if kind == "shop" else service.claim_collect_phrase
                with patch.object(service, "load_config", side_effect=AssertionError("legacy replay needs no config")):
                    ok, response = claim("u", "old query", "op", settlement_repository=repo)
                self.assertTrue(ok)
                self.assertIn("兑换成功", response)
                self.assertIn("活动令", response)
                rejected = claim("other", "old query", "op", settlement_repository=repo, detailed_result=True)
                self.assertEqual("operation_conflict", rejected["reason"])
                self.assertEqual(before, self.snapshot(database))

    def test_committed_competing_change_is_rechecked_before_redemption(self):
        for kind in ("shop", "collect"):
            with self.subTest(kind=kind):
                database, _, application, execute = self.application_fixture(kind)
                execute_repository = application.repository.execute
                competitor_snapshot = []
                def competing(*args):
                    with sqlite3.connect(database) as conn:
                        if kind == "shop":
                            conn.execute("UPDATE activity_point_balance SET points=199")
                        else:
                            conn.execute("UPDATE activity_collect_inventory SET count=0 WHERE word_char='午'")
                    competitor_snapshot.extend(self.snapshot(database))
                    return execute_repository(*args)
                with patch.object(application.repository, "execute", side_effect=competing):
                    result = execute()
                self.assertFalse(result.ok)
                self.assertEqual("points_insufficient" if kind == "shop" else "tokens_insufficient", result.data["reason"])
                after = self.snapshot(database)[0]
                for table in after.keys() - {"operation_ledger", "operation_audit", "sqlite_sequence"}:
                    self.assertEqual(competitor_snapshot[0][table], after[table], table)

    def test_update_branches_and_late_abort_roll_back_all_tables(self):
        for kind in ("shop", "collect"):
            for mode in ("feature", "same", "attached"):
                database, game, _, invoke = self.fixture(kind, mode)
                invoke("seed")
                count_table = "activity_point_purchase" if kind == "shop" else "activity_collect_claim"
                receipt_table = "activity_point_purchase_operations" if kind == "shop" else "activity_collect_exchange_operations"
                for table, action, error in (
                    (count_table, "UPDATE", "activity_purchase_write_rejected" if kind == "shop" else "activity_collect_claim_write_rejected"),
                    ("back", "UPDATE", "activity_reward_item_write_rejected"),
                    (receipt_table, "INSERT", "abort"),
                ):
                    with self.subTest(kind=kind, mode=mode, table=table):
                        target = game if table == "back" else database
                        rejection = "ABORT,'late receipt failure'" if error == "abort" else "IGNORE"
                        with sqlite3.connect(target) as conn:
                            conn.execute(f"CREATE TRIGGER reject_update BEFORE {action} ON {table} BEGIN SELECT RAISE({rejection}); END")
                        before = self.snapshot(database, game)
                        try:
                            with self.assertRaises(sqlite3.IntegrityError if error == "abort" else DomainError) as raised:
                                invoke("next")
                            if error != "abort":
                                self.assertEqual(error, raised.exception.details["reason"])
                            self.assertEqual(before, self.snapshot(database, game))
                        finally:
                            with sqlite3.connect(target) as conn:
                                conn.execute("DROP TRIGGER reject_update")

    def test_large_legacy_shop_receipt_does_not_reject_valid_real_projection(self):
        database, _, repo, _ = self.fixture("shop", "feature")
        cost = 10**29
        with DatabaseUnitOfWork(database) as uow:
            uow.execute("UPDATE activity_point_balance SET points=?", (cost * 2,))
        result = repo.purchase("large", "u", "a", "i", 1, cost, 0, 0, self.rewards, 100)
        self.assertTrue(result.succeeded)
        before = self.snapshot(database)
        self.assertEqual("duplicate", repo.lookup_receipt("large", "u").status)
        self.assertEqual(before, self.snapshot(database))

    def test_corrupt_collect_receipt_keeps_specific_reason_and_no_business_write(self):
        database, _, application, execute = self.application_fixture("collect")
        execute()
        with sqlite3.connect(database) as conn:
            conn.execute("UPDATE activity_collect_exchange_operations SET result_json='[]'")
            conn.execute("DELETE FROM operation_ledger")
        before = self.snapshot(database)[0]
        result = execute()
        self.assertFalse(result.ok)
        self.assertEqual("receipt_corrupt", result.data["reason"])
        after = self.snapshot(database)[0]
        for table in before.keys() - {"operation_ledger", "operation_audit", "sqlite_sequence"}:
            self.assertEqual(before[table], after[table], table)

    def test_started_without_receipt_executes_once_and_new_requests_still_use_live_limits(self):
        for kind in ("shop", "collect"):
            with self.subTest(kind=kind):
                database, _, application, execute = self.application_fixture(kind)
                action = "claim_point_shop_item" if kind == "shop" else "claim_collect_phrase"
                query = "兑换令 2" if kind == "shop" else "端午"
                with DatabaseUnitOfWork(database) as uow:
                    application.ledger.begin(uow, "op", "activity." + action, {"query": query, "user_id": "u"})
                first = execute()
                self.assertTrue(first.ok)
                committed = self.snapshot(database)
                self.assertTrue(execute().replayed)
                self.assertEqual(committed, self.snapshot(database))
                with patch.object(service, "activity_runtime_state", return_value={"ok": True, "features": set(), "stage_name": "结算期"}):
                    closed_stage = execute("stage")
                self.assertFalse(closed_stage.ok)
                self.assertEqual("shop_stage_closed" if kind == "shop" else "exchange_stage_closed", closed_stage.data["reason"])
                with sqlite3.connect(database) as conn:
                    if kind == "shop":
                        conn.execute("UPDATE activity_point_purchase SET count=4")
                    else:
                        conn.execute("UPDATE activity_collect_claim SET count=4")
                limited = execute("limited")
                self.assertFalse(limited.ok)
                self.assertEqual("personal_limit" if kind == "shop" else "limit_reached", limited.data["reason"])
                with patch.object(service, "activity_runtime_state", return_value={"ok": False, "reason": "活动已关闭"}):
                    closed = execute("closed")
                self.assertFalse(closed.ok)
                self.assertEqual("activity_unavailable", closed.data["reason"])
                self.assertTrue(execute().replayed)
                self.assertEqual(first.data, execute().data)

    def test_partial_duplicate_player_write_rejection_rolls_back_every_database(self):
        for kind in ("shop", "collect"):
            for mode in ("feature", "same", "attached"):
                with self.subTest(kind=kind, mode=mode):
                    database, game, _, invoke = self.fixture(kind, mode)
                    with sqlite3.connect(game) as conn:
                        conn.execute("INSERT INTO user_xiuxian VALUES('u',20)")
                        conn.execute("CREATE TRIGGER skip_one_player BEFORE UPDATE ON user_xiuxian WHEN OLD.stone=20 BEGIN SELECT RAISE(IGNORE); END")
                    before = self.snapshot(database, game)
                    with self.assertRaises(DomainError) as raised:
                        invoke()
                    self.assertEqual("activity_reward_stone_write_rejected", raised.exception.details["reason"])
                    self.assertEqual(before, self.snapshot(database, game))

    def test_shop_receipt_projection_corruption_remains_strict_for_exact_text(self):
        database, _, repo, invoke = self.fixture("shop", "feature")
        invoke()
        with sqlite3.connect(database) as conn:
            conn.execute("UPDATE activity_point_purchase_operations SET cost=201")
        before = self.snapshot(database)
        with self.assertRaises(DomainError) as raised:
            repo.lookup_receipt("op", "u")
        self.assertEqual("receipt_corrupt", raised.exception.details["reason"])
        self.assertEqual(before, self.snapshot(database))
        previous = {
            "payload": json.dumps(["u", "a", "i", 1, 10**29, 0, 0, 50, [], 100]),
            "quantity": "1e0", "cost": "100000000000000000000000000001",
            "points": 0, "personal_count": 1, "total_count": 1,
        }
        with self.assertRaises(DomainError):
            repo._decode_receipt(previous, "u")
        previous["cost"] = "1e29"
        self.assertEqual(10**29, repo._decode_receipt(previous, "u").cost)

    def test_historical_outer_rejection_without_reason_replays_unchanged(self):
        database, _, application, execute = self.application_fixture("shop")
        with DatabaseUnitOfWork(database) as uow:
            application.ledger.begin(uow, "op", "activity.claim_point_shop_item", {"query": "兑换令 2", "user_id": "u"})
            from nonebot_plugin_xiuxian_2.core.result import OperationOutcome
            historical = OperationOutcome.rejected("op", "activity.claim_point_shop_item", "旧版积分不足", data={"status": "rejected", "message": "旧版积分不足"})
            application.ledger.finish(uow, historical)
        before = self.snapshot(database)
        with patch.object(service, "load_config", side_effect=AssertionError("outer replay is immutable")):
            replay = execute()
        self.assertFalse(replay.ok)
        self.assertTrue(replay.replayed)
        self.assertEqual(historical.data, replay.data)
        self.assertEqual(historical.message, replay.message)
        self.assertEqual(before, self.snapshot(database))

    def test_simultaneous_requests_cannot_oversell_or_exceed_collect_limit(self):
        for kind in ("shop", "collect"):
            for mode in ("feature", "same", "attached"):
                with self.subTest(kind=kind, mode=mode):
                    database, game, repo, _ = self.fixture(kind, mode)
                    barrier = Barrier(2)
                    def redeem(op):
                        barrier.wait(timeout=10)
                        if kind == "shop":
                            return repo.purchase(op, "u", "a", "i", 2, 100, 0, 2, self.rewards, 100)
                        return repo.exchange(op, "u", "a", "端午", {"端": 1, "午": 1}, 1, self.rewards, 100)
                    with ThreadPoolExecutor(max_workers=2) as pool:
                        results = list(pool.map(redeem, ("concurrent-a", "concurrent-b")))
                    self.assertEqual(["applied", "stock_insufficient" if kind == "shop" else "limit_reached"], sorted(result.status for result in results))
                    with sqlite3.connect(game) as conn:
                        self.assertEqual([(60,)], conn.execute("SELECT stone FROM user_xiuxian").fetchall())
                        self.assertEqual([(101, 2, 2), (102, 1, 1)], conn.execute("SELECT goods_id,goods_num,bind_num FROM back ORDER BY goods_id").fetchall())
                    with sqlite3.connect(database) as conn:
                        table = "activity_point_purchase_operations" if kind == "shop" else "activity_collect_exchange_operations"
                        self.assertEqual(1, conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    def test_compatibility_shop_replays_new_feature_receipt_without_weakening_identity(self):
        database, _, repo, _ = self.fixture("shop", "feature")
        arguments = ("op", "u", "a", "i", 2, 100, 4, 8, self.rewards, 100)
        self.assertEqual("applied", repo.purchase(*arguments, response="原活动兑换成功").status)
        before = self.snapshot(database)
        legacy = ActivityPointShopPurchaseService(database, database)
        self.assertEqual("duplicate", legacy.purchase(*arguments).status)
        changed = (*arguments[:5], 101, *arguments[6:])
        self.assertEqual("operation_conflict", legacy.purchase(*changed).status)
        self.assertEqual(before, self.snapshot(database))


if __name__ == "__main__":
    unittest.main()
