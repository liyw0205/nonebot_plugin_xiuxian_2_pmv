"""非宠物命令的静默失败收口（always answer）。

日志证据：两份 xiu2 日志共 33 条 `Running Matcher(...) failed`，其中 26 条不在宠物模块
（虚神界闭关 8、双修 7、传承祈愿 4、抑制黑暗动乱 1、日常 1）。这些 handler 内部直接调用
application 层且没有任何兜底，异常只留下一行日志，玩家完全收不到回复。

本套件钉住通用守卫本身的行为，以及五个 handler 确实接线（含 nonebot 依赖注入不变）。
"""

from __future__ import annotations

import ast
import inspect
import sqlite3
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot.params import CommandArg
from nonebot.exception import FinishedException

from nonebot_plugin_xiuxian_2.core.errors import ConflictError, DomainError, ValidationError
from nonebot_plugin_xiuxian_2.xiuxian.command_guard import (
    BUSY_FAILURE_TEXT,
    CONFLICT_FAILURE_TEXT,
    GENERIC_FAILURE_TEXT,
    VALIDATION_FAILURE_TEXT,
    failure_reply,
    failure_text,
    guard_command,
)

PACKAGE = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2"

GUARDED = (
    ("xiuxian/xiuxian_impart_pk/__init__.py", "impart_pk_in_closing_", "虚神界闭关"),
    ("xiuxian/xiuxian_buff/partner.py", "two_exp_invite_", "双修"),
    ("xiuxian/xiuxian_impart/__init__.py", "impart_draw_", "传承祈愿"),
    ("xiuxian/xiuxian_buff/__init__.py", "del_exp_decimal_", "抑制黑暗动乱"),
    ("xiuxian/xiuxian_buff/__init__.py", "daily_info_", "日常"),
)


class _Recorder:
    """替身 handle_send，记录每一次回复。"""

    def __init__(self, *, fail: bool = False) -> None:
        self.calls: list[tuple[object, object, str]] = []
        self.fail = fail

    async def __call__(self, bot, event, msg, *args, **kwargs):
        self.calls.append((bot, event, msg))
        if self.fail:
            raise RuntimeError("delivery down")


class GuardUnitTests(unittest.IsolatedAsyncioTestCase):
    async def test_failure_sends_exactly_one_leak_free_reply(self) -> None:
        recorder = _Recorder()

        @guard_command("虚神界闭关", send=recorder)
        async def handler(bot, event):
            raise sqlite3.OperationalError(
                "no such table: partner_two_exp_usage /srv/data/player.db"
            )

        result = await handler("bot", "event")

        self.assertIsNone(result)
        self.assertEqual(len(recorder.calls), 1)
        bot, event, msg = recorder.calls[0]
        self.assertEqual((bot, event), ("bot", "event"))
        self.assertIn("虚神界闭关", msg)
        self.assertIn(GENERIC_FAILURE_TEXT, msg)
        for leak in ("sqlite3", "no such table", ".db", "Traceback", "partner_two_exp_usage"):
            self.assertNotIn(leak, msg)

    async def test_busy_database_gets_a_retry_hint(self) -> None:
        recorder = _Recorder()

        @guard_command("日常", send=recorder)
        async def handler(bot, event):
            raise sqlite3.OperationalError("database is locked")

        await handler("bot", "event")
        self.assertEqual(len(recorder.calls), 1)
        self.assertIn(BUSY_FAILURE_TEXT, recorder.calls[0][2])

    async def test_conflict_and_validation_texts(self) -> None:
        self.assertEqual(failure_text(ConflictError("busy")), CONFLICT_FAILURE_TEXT)
        self.assertEqual(failure_text(ValidationError("bad input")), VALIDATION_FAILURE_TEXT)
        self.assertEqual(failure_text(DomainError("domain_error", "domain")), GENERIC_FAILURE_TEXT)
        self.assertEqual(failure_text(ValueError("oops")), GENERIC_FAILURE_TEXT)

    async def test_success_path_is_transparent(self) -> None:
        recorder = _Recorder()

        @guard_command("双修", send=recorder)
        async def handler(bot, event, args=None):
            return ("ok", args)

        self.assertEqual(await handler("bot", "event", args="x"), ("ok", "x"))
        self.assertEqual(recorder.calls, [])

    async def test_matcher_finish_is_propagated_without_failure_reply(self) -> None:
        recorder = _Recorder()

        @guard_command("双修", send=recorder)
        async def handler(bot, event):
            raise FinishedException

        with self.assertRaises(FinishedException):
            await handler("bot", "event")

        self.assertEqual(recorder.calls, [])

    async def test_delivery_failure_does_not_mask_the_original(self) -> None:
        recorder = _Recorder(fail=True)

        @guard_command("传承祈愿", send=recorder)
        async def handler(bot, event):
            raise ValueError("boom")

        self.assertIsNone(await handler("bot", "event"))
        self.assertEqual(len(recorder.calls), 1)

    async def test_wrapper_keeps_the_nonebot_signature(self) -> None:
        async def handler(bot, event, args: str = CommandArg()):
            return args

        wrapped = guard_command("抑制黑暗动乱")(handler)
        self.assertEqual(inspect.signature(wrapped), inspect.signature(handler))
        self.assertEqual(list(inspect.signature(wrapped).parameters), ["bot", "event", "args"])

    async def test_failure_reply_is_short_and_action_scoped(self) -> None:
        text = failure_reply("传承祈愿", sqlite3.OperationalError("no such table: x"))
        self.assertTrue(text.startswith("传承祈愿未完成："))
        self.assertLess(len(text), 80)


class RegisteredMatcherTests(unittest.IsolatedAsyncioTestCase):
    """真实注册的 matcher 仍能解析依赖（DI 未被装饰器破坏）。"""

    def test_registered_handlers_keep_their_injected_parameters(self) -> None:
        from nonebot.dependencies.utils import get_typed_signature

        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import daily_info
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import partner

        for matcher in (daily_info, partner.two_exp_invite):
            handler = matcher.handlers[0]
            signature = get_typed_signature(handler.call)
            self.assertEqual(list(signature.parameters)[:2], ["bot", "event"])

        invitation = partner.two_exp_invite.handlers[0]
        args_parameter = get_typed_signature(invitation.call).parameters["args"]
        self.assertIsNotNone(args_parameter.default)


class GuardCoverageContractTests(unittest.TestCase):
    """五个留下过静默失败的 handler 必须带守卫。"""

    def test_every_logged_handler_is_guarded(self) -> None:
        by_file: dict[str, list[tuple[str, str]]] = {}
        for relative, function, action in GUARDED:
            by_file.setdefault(relative, []).append((function, action))

        for relative, expected in by_file.items():
            source = (PACKAGE / relative).read_text(encoding="utf-8")
            tree = ast.parse(source)
            found = {}
            for node in ast.walk(tree):
                if not isinstance(node, ast.AsyncFunctionDef) or node.name not in {
                    name for name, _ in expected
                }:
                    continue
                decorations = []
                for decorator in node.decorator_list:
                    if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Name):
                        arguments = [
                            argument.value
                            for argument in decorator.args
                            if isinstance(argument, ast.Constant)
                        ]
                        decorations.append((decorator.func.id, arguments))
                found[node.name] = decorations
            for function, action in expected:
                with self.subTest(function=function, file=relative):
                    decorations = found.get(function)
                    self.assertIsNotNone(decorations, f"{function} not found in {relative}")
                    self.assertIn(
                        ("guard_command", [action]),
                        decorations,
                        f"{function} is not guarded with action {action}",
                    )

    def test_no_duplicate_guard_implementation_in_pet_module(self) -> None:
        pet = (
            PACKAGE / "xiuxian" / "xiuxian_pet" / "failure_reply.py"
        ).read_text(encoding="utf-8")
        # 宠物模块保留自己的文案与测试，不并入通用守卫
        self.assertIn("guard_pet_handler", pet)
        self.assertNotIn("guard_command", pet)


if __name__ == "__main__":
    unittest.main()
