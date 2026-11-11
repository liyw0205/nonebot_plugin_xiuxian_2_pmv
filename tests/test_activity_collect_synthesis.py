from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
import sqlite3
import tempfile
from threading import Barrier
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
from urllib.parse import unquote

import nonebot

nonebot.init()

from nonebot.exception import FinishedException
from nonebot_plugin_xiuxian_2.core.errors import OperationConflictError
from nonebot_plugin_xiuxian_2.features.activity.redemption import ActivityRedemptionRejected
from nonebot_plugin_xiuxian_2.core.numeric import as_int_like
from nonebot_plugin_xiuxian_2.features.activity.application import ActivityApplication
from nonebot_plugin_xiuxian_2.features.activity.collect_synthesis import synthesis_cost
from nonebot_plugin_xiuxian_2.features.activity.collect_synthesis_repository import ActivityCollectSynthesisSqlRepository
from nonebot_plugin_xiuxian_2.features.activity.migrations import apply_activity_state_schema, apply_collect_synthesis
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger
from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_activity as commands
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import activity_config, service
from tests import test_activity_view_delivery_contracts as _delivery


def config():
    return {
        "enabled": True, "template_key": "spring", "name": "庆典", "start_time": "0", "end_time": "无限",
        "daily_tasks": [], "weekly_tasks": [],
        "extensions": {"activity_pass": {"enabled": False}, "rank": {"boards": []}},
        "gameplay_activities": [{
            "key": "collect", "name": "庆典集字", "type": "collect_words", "enabled": True,
            "start_time": "0", "end_time": "无限", "drop_events": [],
            "synthesis": {"enabled": True, "cost": 5},
            "letters": [{"char": "福"}, {"char": "运"}, {"char": "仙"}],
            "phrases": [{"name": "福运", "phrase": "福运", "limit": 1, "reward": "灵石x50"}],
        }],
    }


class CollectSynthesisTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.database = Path(temp.name) / "game.db"
        self.cfg = config()
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            apply_activity_state_schema(uow)
            apply_collect_synthesis(uow)
            OperationLedger().ensure_schema(uow)
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT,stone INTEGER,exp INTEGER,power INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u','道友',10,?,?)", (10**30, 10**40))
            uow.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            uow.execute("INSERT INTO activity_collect_inventory(activity_key,user_id,word_char,count) VALUES('collect','u','福',6)")
        self.application = ActivityApplication(self.database)
        self.application.read_model.config_loader = lambda: self.cfg
        self.application.read_model.clock = SimpleNamespace(now=lambda: datetime(2026, 11, 12, 12))
        loader = patch.object(activity_config, "load_config", side_effect=lambda: self.cfg)
        loader.start()
        self.addCleanup(loader.stop)

    def run_action(self, op="op", query="福 运", uid="u", application=None):
        return (application or self.application).execute(
            operation_id=op, user_id=uid, payload={"action": "synthesize_collect_token", "query": query},
        )

    def snapshot(self, *, business=False):
        with sqlite3.connect(self.database) as conn:
            names = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
            return {name: conn.execute(f'SELECT * FROM "{name}" ORDER BY rowid').fetchall()
                    for name in names if not business or name not in {"operation_ledger", "operation_audit", "sqlite_sequence"}}

    def balances(self):
        with sqlite3.connect(self.database) as conn:
            return {char: as_int_like(count) for char, count in conn.execute("SELECT word_char,count FROM activity_collect_inventory WHERE activity_key='collect' AND user_id='u'")}

    def assert_rejected(self, outcome, reason):
        self.assertFalse(outcome.ok)
        self.assertEqual(reason, outcome.data["reason"])
        self.assertEqual("actor", outcome.data["subject"])
        self.assertTrue(outcome.message)

    def test_old_config_opt_in_cost_validation_and_shipped_content(self):
        for rule in (None, {}, {"enabled": False, "cost": 5}, {"enabled": True, "cost": 1}, {"enabled": True, "cost": 101}, {"enabled": True, "cost": "NaN"}, {"enabled": True, "cost": 5.5}):
            with self.subTest(rule=rule):
                self.assertEqual(0, synthesis_cost({"synthesis": rule}))
        self.assertEqual(5, synthesis_cost({"synthesis": {"enabled": "true", "cost": "5"}}))
        default = activity_config._load_default_config()
        self.assertEqual(5, synthesis_cost(default["gameplay_activities"][0]))
        del self.cfg["gameplay_activities"][0]["synthesis"]
        self.assert_rejected(self.run_action(), "collect_synthesis_disabled")

    def test_old_config_backfill_does_not_enable_new_optional_conversion(self):
        for old in ({"template_type": "festival_sign"}, {"daily_sign": {}}, {}):
            with self.subTest(old=old):
                migrated, changed = activity_config._migrate_config(deepcopy(old))
                self.assertTrue(changed)
                self.assertTrue(migrated["gameplay_activities"])
                self.assertTrue(all(synthesis_cost(activity) == 0 for activity in migrated["gameplay_activities"]))
        explicit = activity_config._load_default_config()
        self.assertEqual(5, synthesis_cost(activity_config._migrate_config(explicit)[0]["gameplay_activities"][0]))

    def test_player_center_bag_conversion_then_existing_phrase_reward(self):
        self.assertIn("command=活动背包&", unquote(self.application.read_model.activity_center_text("u")))
        before = self.application.read_model.collect_bag_guidance_text("u")
        self.assertIn("还缺 运x1", before)
        self.assertIn("活动换字 福 运 collect", before)
        old = self.snapshot(business=True)
        converted = self.run_action()
        self.assertTrue(converted.ok)
        self.assertEqual({"福": 1, "运": 1}, self.balances())
        ready = self.application.read_model.collect_bag_guidance_text("u")
        self.assertIn("可兑换｜活动兑换 福运", ready)
        after = self.snapshot(business=True)
        for name in old.keys() - {"activity_collect_inventory", "activity_collect_synthesis_operations"}:
            self.assertEqual(old[name], after[name], name)
        with patch.object(service, "load_config", return_value=self.cfg):
            result = self.application.execute(operation_id="exchange", user_id="u", payload={"action": "claim_collect_phrase", "query": "福运"})
        self.assertTrue(result.ok, result.message)
        self.assertEqual({"福": 0, "运": 0}, self.balances())
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(60, conn.execute("SELECT stone FROM user_xiuxian WHERE user_id='u'").fetchone()[0])
        self.assertIn("已兑换完", self.application.read_model.collect_bag_guidance_text("u"))

    def test_replay_restart_and_identity_conflict(self):
        first = self.run_action()
        after = self.snapshot()
        self.cfg["enabled"] = False
        with patch.object(activity_config, "load_config", side_effect=AssertionError("receipt must precede config")):
            replay = self.run_action(application=ActivityApplication(self.database))
            self.assertEqual(first.data, replay.data)
            self.assertTrue(replay.replayed)
        self.assertEqual(after, self.snapshot())
        for query, uid in (("仙 运", "u"), ("福 运", "other")):
            with self.subTest(query=query, uid=uid), self.assertRaises(OperationConflictError):
                self.run_action(query=query, uid=uid)
        self.assertEqual(after, self.snapshot())

    def test_committed_receipt_recovers_started_and_failed_finish(self):
        for error in (RuntimeError, SystemExit):
            with self.subTest(error=error):
                op = error.__name__
                with DatabaseUnitOfWork(self.database) as uow:
                    uow.execute("UPDATE activity_collect_inventory SET count=6 WHERE word_char='福'")
                with patch.object(self.application.ledger, "finish", side_effect=error("finish interrupted")):
                    with self.assertRaises(error):
                        self.run_action(op)
                before = self.snapshot(business=True)
                with patch.object(activity_config, "load_config", side_effect=AssertionError("frozen receipt only")):
                    recovered = self.run_action(op, application=ActivityApplication(self.database))
                self.assertTrue(recovered.ok)
                self.assertEqual("duplicate", recovered.data["status"])
                self.assertEqual(before, self.snapshot(business=True))

    def test_uncommitted_started_obeys_current_gate(self):
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self.application.ledger.begin(uow, "op", "activity.synthesize_collect_token", {"user_id": "u", "query": "福 运"})
        self.cfg["enabled"] = False
        before = self.snapshot(business=True)
        self.assert_rejected(self.run_action(), "activity_disabled")
        self.assertEqual(before, self.snapshot(business=True))

    def test_each_write_rejection_rolls_back_all_business_tables_and_is_durable(self):
        steps = (
            ("UPDATE", "activity_collect_inventory", "WHEN OLD.word_char='福'", "collect_synthesis_source_write_rejected"),
            ("INSERT", "activity_collect_inventory", "WHEN NEW.word_char='运'", "collect_synthesis_target_write_rejected"),
            ("INSERT", "activity_collect_synthesis_operations", "", "collect_synthesis_receipt_write_rejected"),
        )
        for action, table, condition, reason in steps:
            with self.subTest(reason=reason):
                with sqlite3.connect(self.database) as conn:
                    conn.execute(f"CREATE TRIGGER reject_step BEFORE {action} ON {table} {condition} BEGIN SELECT RAISE(IGNORE); END")
                before = self.snapshot(business=True)
                first = self.run_action(reason)
                self.assert_rejected(first, reason)
                self.assertEqual(before, self.snapshot(business=True))
                with sqlite3.connect(self.database) as conn:
                    conn.execute("DROP TRIGGER reject_step")
                replay = self.run_action(reason)
                self.assertTrue(replay.replayed)
                self.assertEqual(first.data, replay.data)
                self.assertEqual(before, self.snapshot(business=True))

    def test_existing_target_update_rejection_rolls_back_source(self):
        with sqlite3.connect(self.database) as conn:
            conn.execute("INSERT INTO activity_collect_inventory VALUES('collect','u','运',2,'')")
            conn.execute("CREATE TRIGGER reject_target BEFORE UPDATE ON activity_collect_inventory WHEN OLD.word_char='运' BEGIN SELECT RAISE(IGNORE); END")
        before = self.snapshot(business=True)
        self.assert_rejected(self.run_action(), "collect_synthesis_target_write_rejected")
        self.assertEqual(before, self.snapshot(business=True))

    def test_last_source_inventory_competition_commits_once(self):
        barrier = Barrier(2)
        def run(op):
            barrier.wait(timeout=5)
            return self.run_action(op, application=ActivityApplication(self.database))
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(run, ("race-1", "race-2")))
        self.assertEqual(1, sum(r.ok for r in results))
        rejected = next(r for r in results if not r.ok)
        self.assert_rejected(rejected, "collect_synthesis_tokens_insufficient")
        self.assertEqual({"福": 1, "运": 1}, self.balances())

    def test_arguments_unknown_tokens_missing_user_and_low_balance(self):
        for i, (query, uid, reason) in enumerate((
            ("", "u", "collect_synthesis_arguments"),
            ("福", "u", "collect_synthesis_arguments"),
            ("福运 仙", "u", "collect_synthesis_arguments"),
            ("福 福", "u", "collect_synthesis_same_token"),
            ("福 寿", "u", "collect_synthesis_token_unknown"),
            ("仙 运", "u", "collect_synthesis_tokens_insufficient"),
            ("福 运 absent", "u", "collect_synthesis_activity_missing"),
            ("福 运", "missing", "user_missing"),
        )):
            with self.subTest(reason=reason):
                before = self.snapshot(business=True)
                self.assert_rejected(self.run_action(str(i), query, uid), reason)
                self.assertEqual(before, self.snapshot(business=True))

    def test_activity_window_stage_and_multi_activity_disambiguation(self):
        second = deepcopy(self.cfg["gameplay_activities"][0])
        second.update(key="second", name="第二集字")
        self.cfg["gameplay_activities"].append(second)
        self.assert_rejected(self.run_action(), "collect_synthesis_activity_ambiguous")
        self.assertTrue(self.run_action("named", "福 运 庆典集字").ok)
        self.cfg["extensions"]["stages"] = [{"key": "closed", "name": "观礼期", "start_time": "0", "end_time": "无限", "features": ["sign"]}]
        self.assert_rejected(self.run_action("stage"), "activity_feature_closed")
        self.assertNotIn("换字规则", self.application.read_model.collect_bag_guidance_text("u"))
        self.cfg["extensions"].pop("stages")
        self.cfg["gameplay_activities"][0]["end_time"] = "2000-01-01 00:00:00"
        self.assert_rejected(self.run_action("expired", "福 运 collect"), "activity_ended")

    def test_window_checks_use_same_clock_for_main_and_sub_activity(self):
        for start in ("2000-01-01 00:00:00", "2999-01-01 00:00:00"):
            with self.subTest(start=start):
                self.cfg["start_time"] = start
                self.cfg["end_time"] = "2999-01-02 00:00:00"
                fixed = datetime(2999, 1, 1, 12)
                self.assertTrue(activity_config.activity_runtime_state(self.cfg, at_time=fixed)["ok"])
        with patch.object(activity_config, "_now_dt", return_value=datetime(2000, 1, 1)):
            self.assert_rejected(self.run_action("before"), "activity_not_started")

    def test_schema_missing_does_not_create_or_repair_tables(self):
        with sqlite3.connect(self.database) as conn:
            conn.execute("DROP TABLE activity_collect_synthesis_operations")
            conn.execute("CREATE TABLE activity_collect_synthesis_operations(operation_id TEXT)")
        before = self.snapshot(business=True)
        self.assert_rejected(self.run_action(), "schema_missing")
        self.assertEqual(before, self.snapshot(business=True))
        missing = self.database.parent / "absent.db"
        self.assertEqual("schema_missing", ActivityCollectSynthesisSqlRepository(missing).lookup_receipt("op", "u", "福 运")["reason"])
        self.assertFalse(missing.exists())

    def test_corrupt_business_receipt_is_not_reexecuted(self):
        self.run_action()
        with sqlite3.connect(self.database) as conn:
            conn.execute("UPDATE operation_ledger SET status='started',result_json=NULL WHERE operation_id='op'")
            conn.execute("UPDATE activity_collect_synthesis_operations SET result_json='{}'")
        before = self.snapshot(business=True)
        self.assert_rejected(self.run_action(), "receipt_corrupt")
        self.assertEqual(before, self.snapshot(business=True))

    def test_business_receipt_matches_actor_arguments_and_result_tokens(self):
        self.run_action()
        repo = self.application.repository.collect_synthesis
        before = self.snapshot()
        for uid, query in (("other", "福 运"), ("u", "仙 运")):
            with self.subTest(uid=uid, query=query):
                self.assertEqual("operation_conflict", repo.lookup_receipt("op", uid, query)["reason"])
        self.assertEqual(before, self.snapshot())
        with sqlite3.connect(self.database) as conn:
            result = json.loads(conn.execute("SELECT result_json FROM activity_collect_synthesis_operations").fetchone()[0])
            result["target"] = "仙"
            conn.execute("UPDATE activity_collect_synthesis_operations SET result_json=?", (json.dumps(result),))
        corrupt = self.snapshot()
        with self.assertRaises(ActivityRedemptionRejected) as raised:
            repo.lookup_receipt("op", "u", "福 运")
        self.assertEqual("receipt_corrupt", raised.exception.details["reason"])
        self.assertEqual(corrupt, self.snapshot())

    def test_invalid_inventory_is_rejected_without_mutation(self):
        for i, value in enumerate(("NaN", "broken", -1)):
            with self.subTest(value=value):
                with sqlite3.connect(self.database) as conn:
                    conn.execute("UPDATE activity_collect_inventory SET count=?", (value,))
                before = self.snapshot(business=True)
                self.assert_rejected(self.run_action(f"invalid-{i}"), "collect_synthesis_inventory_invalid")
                self.assertEqual(before, self.snapshot(business=True))

    def test_large_inventory_respects_storage_precision_and_leaves_player_assets(self):
        for char in ("福", "运"):
            with self.subTest(char=char):
                with sqlite3.connect(self.database) as conn:
                    conn.execute("UPDATE activity_collect_inventory SET count=6 WHERE word_char='福'")
                    conn.execute("INSERT OR REPLACE INTO activity_collect_inventory VALUES('collect','u',?,?,'')", (char, "1e30"))
                before = self.snapshot(business=True)
                self.assert_rejected(self.run_action(f"large-{char}"), "collect_synthesis_precision_insufficient")
                self.assertEqual(before, self.snapshot(business=True))

    def test_legacy_text_inventory_accepts_scientific_notation_exactly(self):
        with sqlite3.connect(self.database) as conn:
            conn.execute("ALTER TABLE activity_collect_inventory RENAME TO old_inventory")
            conn.execute("CREATE TABLE activity_collect_inventory(activity_key TEXT,user_id TEXT,word_char TEXT,count TEXT,update_time TEXT,PRIMARY KEY(activity_key,user_id,word_char))")
            conn.execute("INSERT INTO activity_collect_inventory VALUES('collect','u','福','1e30','')")
        self.assertTrue(self.run_action().ok)
        self.assertEqual(10**30 - 5, self.balances()["福"])
        self.assertIn(str(10**30 - 5), self.application.read_model.collect_bag_guidance_text("u"))

    def test_migration_is_incremental_idempotent_and_only_for_game_database(self):
        from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database
        before = self.snapshot()
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            apply_collect_synthesis(uow)
        self.assertEqual(before, self.snapshot())
        migrations = build_migrations()
        for key in ("game_db", "player_db", "trade_db", "impart_db", "message_db"):
            selected = migrations_for_database(migrations, key)
            self.assertEqual(key == "game_db", any(m.version == "activity_state.004" for m in selected))

    def test_bag_links_delivery_plain_qq_and_manifest_help(self):
        from nonebot_plugin_xiuxian_2.features.activity.manifest import FEATURE
        text = self.application.read_model.collect_bag_guidance_text("u")
        plain, md = _delivery._send_through_entry(_delivery._bot(onebot=True), text, markdown=True)
        self.assertEqual(1, len(plain))
        self.assertEqual([], md)
        self.assertNotIn("mqqapi", plain[0])
        self.assertEqual(1, plain[0].count("活动换字 福 运 collect"))
        plain, md = _delivery._send_through_entry(_delivery._bot(onebot=False), text, markdown=True)
        self.assertEqual([], plain)
        self.assertEqual(1, len(md))
        self.assertIn("](mqqapi", md[0])
        self.assertIn("活动换字", commands.ACTIVITY_HELP)
        self.assertEqual(1, sum(c.name == "活动换字" for c in FEATURE.commands))

    def test_qq_keyboard_extracts_conversion_without_repeating_body_link(self):
        text = self.application.read_model.collect_bag_guidance_text("u")
        md, rows = _delivery._send_with_keyboard(_delivery._bot(onebot=False), text)
        self.assertEqual(1, len(md))
        self.assertTrue(rows)
        self.assertIn("活动换字 福 运 collect", str(rows))
        self.assertNotIn("活动换字 福 运 collect", md[0])

    def test_real_handler_replay_write_reason_and_normal_finish(self):
        handler = commands.activity_synthesis_cmd.handlers[0].call
        event = SimpleNamespace(message_id="command")
        args = SimpleNamespace(extract_plain_text=lambda: "福 运")
        with (
            patch.object(commands, "activity_application", self.application),
            patch.object(commands, "assign_bot", AsyncMock()),
            patch.object(commands, "_ensure_user", AsyncMock(return_value=(True, {"user_id": "u"}, ""))),
            patch.object(commands, "handle_send", AsyncMock()) as send,
        ):
            asyncio.run(handler("bot", event, args))
            first = send.call_args.args[2]
            self.assertIn("换字成功", first)
            self.assertEqual("活动背包", send.call_args.kwargs["v1"])
            before = self.snapshot()
            asyncio.run(handler("bot", event, args))
            self.assertEqual(first, send.call_args.args[2])
            self.assertEqual(before, self.snapshot())
            with patch.object(commands, "handle_send", AsyncMock(side_effect=FinishedException)):
                with self.assertRaises(FinishedException):
                    asyncio.run(handler("bot", event, args))
            self.assertEqual(before, self.snapshot())
            with sqlite3.connect(self.database) as conn:
                conn.execute("UPDATE activity_collect_inventory SET count=6 WHERE word_char='福'")
                conn.execute("CREATE TRIGGER reject_receipt BEFORE INSERT ON activity_collect_synthesis_operations BEGIN SELECT RAISE(IGNORE); END")
            asyncio.run(handler("bot", SimpleNamespace(message_id="reject"), args))
            self.assertIn("回执", send.call_args.args[2])
            self.assertIn("发起方", send.call_args.args[2])
            self.assertIn("撤销", send.call_args.args[2])

    def test_send_failure_keeps_committed_business_and_reports_context(self):
        handler = commands.activity_synthesis_cmd.handlers[0].call
        with (
            patch.object(commands, "activity_application", self.application),
            patch.object(commands, "assign_bot", AsyncMock()),
            patch.object(commands, "_ensure_user", AsyncMock(return_value=(True, {"user_id": "u"}, ""))),
            patch.object(commands, "handle_send", AsyncMock(side_effect=RuntimeError("send failure"))),
            patch("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils.handle_send", AsyncMock()) as fallback,
            patch("nonebot_plugin_xiuxian_2.xiuxian.command_guard.logger") as logger,
        ):
            asyncio.run(handler("bot", SimpleNamespace(message_id="send", get_user_id=lambda: "u"), SimpleNamespace(extract_plain_text=lambda: "福 运")))
            self.assertEqual({"福": 1, "运": 1}, self.balances())
            self.assertIn("处理或回复", fallback.call_args.args[2])
            self.assertNotIn("未扣", fallback.call_args.args[2])
            self.assertIn("活动换字 actor=u", logger.opt.return_value.error.call_args.args[0])
            self.assertIsInstance(logger.opt.call_args.kwargs["exception"], RuntimeError)


if __name__ == "__main__":
    unittest.main()
