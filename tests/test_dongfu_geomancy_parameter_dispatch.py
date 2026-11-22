from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()


class DongfuGeomancyParameterDispatchTests(unittest.TestCase):
    def test_real_geomancy_parameters_data_and_read_failure_recovery(self):
        with tempfile.TemporaryDirectory(prefix="xiuxian-dongfu-geomancy-") as directory:
            report = Path(directory) / "report.json"
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tests.parameter_probe",
                    "--group",
                    "dongfu-geomancy",
                    "--report",
                    str(report),
                ],
                cwd=Path(__file__).resolve().parents[1],
                text=True,
                capture_output=True,
                timeout=240,
            )
            payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
            failures = [
                row for row in payload.get("results", []) if row["status"] != "pass"
            ]
            self.assertEqual(
                0,
                result.returncode,
                result.stdout + result.stderr + json.dumps(failures, ensure_ascii=False, indent=2),
            )
            self.assertEqual({"pass": 3}, payload["summary"], result.stdout)

    def test_read_failure_message_survives_onebot_and_qq_formatters(self):
        import asyncio
        import sqlite3

        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
        from tests.command_probe import make_event

        event = make_event("洞府地脉", "900001", 974199)
        onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
        qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
        capture = AsyncMock()
        handler = module.dongfu_geomancy.handlers[0].call
        with (
            patch.object(module, "assign_bot", AsyncMock(return_value=(onebot, "900000"))),
            patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
            patch.object(
                module.dongfu_application,
                "status",
                side_effect=sqlite3.DatabaseError("status read denied"),
            ),
            patch.object(utils, "handle_send", capture),
        ):
            asyncio.run(handler(bot=onebot, event=event))
        capture.assert_awaited_once()
        message = capture.await_args.args[2]
        assert "洞府地脉处理提示" in message and "定位号" in message

        for bot, markdown_enabled, fail_markdown in (
            (onebot, False, False),
            (qq, False, False),
            (qq, True, False),
            (qq, True, True),
        ):
            plain, markdown = AsyncMock(), AsyncMock()
            if fail_markdown:
                markdown.side_effect = RuntimeError("isolated geomancy Markdown failure")
            config = SimpleNamespace(
                markdown_status=markdown_enabled,
                merge_forward_send=4 if markdown_enabled else 1,
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
                asyncio.run(
                    utils.send_msg_handler(bot, event, "洞府地脉", "900000", [message])
                )
            delivered = markdown if markdown_enabled and not fail_markdown else plain
            delivered.assert_awaited_once()
            assert "洞府地脉处理提示" in delivered.await_args.args[2]
            assert "定位号" in delivered.await_args.args[2]
