from __future__ import annotations

import asyncio
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()


class DongfuStatusViewParameterDispatchTests(unittest.TestCase):
    def test_real_dongfu_status_view_parameters(self):
        _assert_real_dongfu_status_view_parameters()

    def test_status_failure_message_survives_onebot_and_qq_formatters(self):
        _assert_status_failure_message_survives_onebot_and_qq_formatters()


def _assert_real_dongfu_status_view_parameters():
    with tempfile.TemporaryDirectory(prefix="xiuxian-dongfu-status-view-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.parameter_probe",
                "--group",
                "dongfu-status-view",
                "--report",
                str(report),
            ],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            timeout=240,
        )
        payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
        failures = [row for row in payload.get("results", []) if row["status"] != "pass"]
        assert result.returncode == 0, (
            result.stdout + result.stderr + json.dumps(failures, ensure_ascii=False, indent=2)
        )
        assert payload["summary"] == {"pass": 2}, result.stdout


def _assert_status_failure_message_survives_onebot_and_qq_formatters():
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from tests.command_probe import make_event

    event = make_event("我的洞府", "900001", 973499)
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    capture = AsyncMock()
    handler = module.my_dongfu.handlers[0].call
    with (
        patch.object(module, "assign_bot", AsyncMock(return_value=(onebot, "900000"))),
        patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
        patch.object(module.dongfu_application, "status", side_effect=sqlite3.DatabaseError("status read denied")),
        patch.object(utils, "handle_send", capture),
    ):
        asyncio.run(handler(bot=onebot, event=event))
    capture.assert_awaited_once()
    message = capture.await_args.args[2]
    assert "我的洞府处理提示" in message and "定位号" in message
    assert "尚未建设洞府" not in message

    for bot, use_markdown, fallback in (
        (onebot, False, False),
        (qq, False, False),
        (qq, True, False),
        (qq, True, True),
    ):
        plain, markdown = AsyncMock(), AsyncMock()
        if fallback:
            markdown.side_effect = RuntimeError("isolated status Markdown failure")
        config = SimpleNamespace(
            markdown_status=use_markdown,
            merge_forward_send=4 if use_markdown else 1,
            markdown_id="",
            markdown_button_status=False,
            reference_reply=False,
            message_optimization=False,
            img=False,
            at_sender=False,
        )
        with (
            patch.object(utils, "XiuConfig", return_value=config),
            patch.object(utils, "handle_send2", plain),
            patch.object(utils, "handle_send_markdown", markdown),
        ):
            asyncio.run(utils.send_msg_handler(bot, event, "我的洞府", "900000", [message]))
        delivered = markdown if use_markdown and not fallback else plain
        delivered.assert_awaited_once()
        assert "我的洞府处理提示" in delivered.await_args.args[2]
        assert "定位号" in delivered.await_args.args[2]
