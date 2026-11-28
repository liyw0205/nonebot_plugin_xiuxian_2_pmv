"""Sect task/shop preflight reuses one live stage; settlement rechecks state."""
from __future__ import annotations

import __future__
import ast
import asyncio
from datetime import datetime
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
from nonebot_plugin_xiuxian_2.features.base.keyword_page import parse_page_arg, next_page_command
from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
from nonebot_plugin_xiuxian_2.features.sect.migrations import (
    apply_sect_shop, apply_sect_task_state, apply_sect_task_claim_operations,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger
from tests.test_sect_read_stage import Finished, ROOT, load


class SectTaskShopReadStageTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.database = Path(temp.name) / "sect.db"
        with DatabaseUnitOfWork(self.database) as uow:
            apply_sect_shop(uow)
            apply_sect_task_state(uow)
            apply_sect_task_claim_operations(uow)
            OperationLedger().ensure_schema(uow)
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,"
                        "sect_contribution INTEGER,sect_task INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u',1,100,0)")
            uow.execute("CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_materials INTEGER,"
                        "closed INTEGER)")
            uow.execute("INSERT INTO sects VALUES(1,100,0)")
            uow.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                        "goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,"
                        "bind_num INTEGER,UNIQUE(user_id,goods_id))")
        self.clock = SimpleNamespace(now=lambda: datetime(2026, 7, 11, 8, 9, 10))
        self.app = SectApplication(self.database, clock=self.clock,
                                   random_source=SimpleNamespace(choice=lambda values: values[-1]))
        self.task_config = {"trial": {"type": 1, "cost": 0.2, "give": 0.1,
                                      "sect": 10, "desc": "Trial"}}
        self.helpers = load(ROOT / "sect_member_utils.py", {
            "get_user_sect_task", "refresh_user_sect_task"}, {
            "_TASK_NOT_PROVIDED": object(), "sect_application": self.app,
            "config": {"宗门任务": self.task_config, "每日宗门任务次上限": 3}})
        self.result_failure = load(ROOT.parent / "command_guard.py", {
            "result_failure", "_subject_label"}, {
            "reason_text": reason_text,
            "report_exception": Mock(side_effect=AssertionError("unmapped refusal")),
        })["result_failure"]

    def namespace(self):
        async def finish():
            raise Finished
        sender, pages = AsyncMock(), AsyncMock()
        namespace = {
            "CommandArg": lambda: None,
            "assign_bot": AsyncMock(return_value=(SimpleNamespace(self_id="bot"), None)),
            "check_user": lambda event: (True, self.app.get_user_profile("u"), ""),
            "sect_application": self.app, "handle_send": sender, "send_msg_handler": pages,
            "_sect_operation_id": lambda event, action, *args: f"{action}:{event}",
            "number_to": str, "result_failure": self.result_failure,
            "_task_failure": lambda action, result, uid: result.get("reason") or result["status"],
            "get_user_sect_task": self.helpers["get_user_sect_task"],
            "refresh_user_sect_task": self.helpers["refresh_user_sect_task"],
            "parse_page_arg": parse_page_arg, "next_page_command": next_page_command,
            "get_sect_weekly_purchases_snapshot": lambda uid: {},
            "get_sect_weekly_purchases": lambda uid, item: 0,
            "shop_limit_line": lambda limit, purchased: f"{purchased}/{limit}",
            "config": {"商店商品": {"9": {"cost": 10, "weekly_limit": 5}}},
            "items": SimpleNamespace(get_data_by_item_id=lambda key: {
                "name": "Item", "type": "elixir", "desc": "Description"}),
            "XiuConfig": lambda: SimpleNamespace(max_goods_num=99),
            "safe_log_economy_change": Mock(),
        }
        for matcher in ("sect_shop", "sect_buy", "sect_task_refresh"):
            namespace[matcher] = SimpleNamespace(finish=finish)
        return namespace, sender, pages

    def run_shop(self, matcher, event=1):
        namespace, sender, pages = self.namespace()
        tree = ast.parse((ROOT / "__init__.py").read_text(encoding="utf-8"))
        nodes = [node for node in tree.body if isinstance(node, ast.AsyncFunctionDef)
                 and any(isinstance(decorator, ast.Call)
                         and isinstance(decorator.func, ast.Attribute)
                         and isinstance(decorator.func.value, ast.Name)
                         and decorator.func.value.id == matcher
                         and decorator.func.attr == "handle"
                         for decorator in node.decorator_list)]
        self.assertEqual(1, len(nodes))
        node = nodes[0]
        node.decorator_list = []
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(ROOT / "__init__.py"),
                     "exec", flags=__future__.annotations.compiler_flag), namespace)
        args = SimpleNamespace(extract_plain_text=lambda: "9" if matcher == "sect_buy" else "")
        with self.assertRaises(Finished):
            asyncio.run(namespace[node.name](None, event, args))
        return sender.call_args.args[2] if sender.called else pages.call_args.args[4]

    def run_refresh(self, event=1):
        namespace, sender, _ = self.namespace()
        load(ROOT / "__init__.py", {
            "_daily_sect_task", "_task_claim_replay", "_refresh_sect_task"}, namespace)
        with self.assertRaises(Finished):
            asyncio.run(namespace["_refresh_sect_task"](None, event))
        return sender.call_args.args[2]

    def row(self, sql):
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return uow.query_one(sql)

    def test_both_shop_callers_read_profile_once_and_purchase_replays_after_balance_change(self):
        with patch.object(self.app, "get_user_profile", wraps=self.app.get_user_profile) as reads:
            self.assertIn("名称：Item", self.run_shop("sect_shop")[1])
            self.assertEqual(1, reads.call_count)
            self.assertIn("成功兑换", self.run_shop("sect_buy"))
            self.assertEqual(2, reads.call_count)
        self.assertEqual(90, self.row("SELECT sect_contribution FROM user_xiuxian")["sect_contribution"])
        self.assertEqual(1, self.row("SELECT goods_num FROM back")["goods_num"])
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE user_xiuxian SET sect_contribution=0")
        self.assertIn("成功兑换", self.run_shop("sect_buy"))
        self.assertEqual(0, self.row("SELECT sect_contribution FROM user_xiuxian")["sect_contribution"])
        self.assertEqual(1, self.row("SELECT goods_num FROM back")["goods_num"])

    def test_purchase_rechecks_membership_and_contribution_after_profile_stage(self):
        original = self.app.purchase
        for sql, reason in (("UPDATE user_xiuxian SET sect_id=2", reason_text("sect_changed")),
                            ("UPDATE user_xiuxian SET sect_contribution=0", "贡献度不足")):
            with self.subTest(reason=reason):
                with DatabaseUnitOfWork(self.database) as uow:
                    uow.execute("UPDATE user_xiuxian SET sect_id=1,sect_contribution=100")
                def mutate(**kwargs):
                    with DatabaseUnitOfWork(self.database) as uow:
                        uow.execute(sql)
                    return original(**kwargs)
                with patch.object(self.app, "purchase", side_effect=mutate):
                    self.assertIn(reason, self.run_shop("sect_buy", event=reason))
                self.assertEqual(0, self.row("SELECT COUNT(*) AS n FROM back")["n"])
                self.assertEqual(0, self.row("SELECT COUNT(*) AS n FROM sect_shop_purchase_operations")["n"])
                self.assertEqual(100, self.row("SELECT sect_materials FROM sects")["sect_materials"])
                self.assertEqual(0, self.row("SELECT COUNT(*) AS n FROM sect_shop_weekly_purchases")["n"])

    def test_shop_limit_refusal_still_formats_current_purchase_count(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("INSERT INTO sect_shop_weekly_purchases VALUES('u',?,9,5)",
                        (self.clock.now().strftime("%G-W%V"),))
        reply = self.run_shop("sect_buy")
        self.assertIn("本周已购买5个", reply)
        self.assertIn("无法再购买1个", reply)
        self.assertEqual(100, self.row("SELECT sect_contribution FROM user_xiuxian")["sect_contribution"])
        self.assertEqual(0, self.row("SELECT COUNT(*) AS n FROM back")["n"])
        self.assertEqual(0, self.row("SELECT COUNT(*) AS n FROM sect_shop_purchase_operations")["n"])

    def test_refresh_reuses_stage_once_and_helper_default_reads_fresh(self):
        self.app.accept_task("u", 1, self.task_config)
        with patch.object(self.app, "get_active_task", wraps=self.app.get_active_task) as reads:
            self.assertIn("已刷新", self.run_refresh())
            self.assertEqual(1, reads.call_count)
            self.assertIsNotNone(self.helpers["refresh_user_sect_task"]("u", 1, "direct"))
            self.assertEqual(2, reads.call_count)
            self.assertEqual({"status": "task_missing"}, self.helpers["refresh_user_sect_task"](
                "u", 1, "missing", current_task=None, return_failure=True))
            self.assertEqual(2, reads.call_count)
        self.app.clear_task("u")
        self.assertIsNone(self.helpers["refresh_user_sect_task"]("u", 1, "cleared"))

    def test_refresh_rechecks_task_changes_after_reused_stage(self):
        original = self.app.refresh_task
        for update, reason in (("task_key='changed'", "task_key_changed"),
                               ("status='completed'", "task_resolved")):
            with self.subTest(reason=reason):
                self.app.accept_task("u", 1, self.task_config)
                def mutate(*args, **kwargs):
                    with DatabaseUnitOfWork(self.database) as uow:
                        uow.execute(f"UPDATE sect_task_state SET {update}")
                    return original(*args, **kwargs)
                with patch.object(self.app, "refresh_task", side_effect=mutate), patch.object(
                        self.app, "get_active_task", wraps=self.app.get_active_task) as reads:
                    self.assertEqual(reason, self.run_refresh(event=reason))
                    self.assertEqual(1, reads.call_count)
                self.assertEqual(0, self.row("SELECT COUNT(*) AS n FROM sect_task_claim_operations")["n"])

    def test_explicit_previous_day_stage_is_rejected_without_refresh_or_reread(self):
        current = self.app.accept_task("u", 1, self.task_config)
        with patch.object(self.app, "get_active_task", wraps=self.app.get_active_task) as reads:
            result = self.helpers["refresh_user_sect_task"](
                "u", 1, "stale-day", current_task={**current, "period": "2026-07-10"},
                return_failure=True)
            self.assertEqual("sect_task_period_changed", result["reason"])
            reads.assert_not_called()
        self.assertEqual(current, self.app.get_active_task("u"))
        self.assertEqual(0, self.row("SELECT COUNT(*) AS n FROM sect_task_claim_operations")["n"])
