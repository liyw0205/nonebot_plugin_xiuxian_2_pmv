from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()


def test_real_home_return_parameters_and_writes():
    with tempfile.TemporaryDirectory(prefix="xiuxian-map-home-return-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.parameter_probe",
                "--group",
                "map-home-return",
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
        assert payload["summary"] == {"pass": 9}, result.stdout


def test_home_return_failure_reason_survives_onebot_and_qq_formatters():
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from tests.command_probe import make_event

    event = make_event("回府", "900001", 973399)
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    result = SimpleNamespace(
        ok=False,
        code="map_home_return_receipt_write_rejected",
        data={
            "reason": "map_home_return_receipt_write_rejected",
            "subject": "receipt",
        },
    )
    capture = AsyncMock()
    handler = module.go_home.handlers[0].call
    with (
        patch.object(module, "assign_bot", AsyncMock(return_value=(onebot, "900000"))),
        patch.object(
            module,
            "check_user",
            return_value=(True, {"user_id": "900001"}, ""),
        ),
        patch.object(module.map_application, "return_home", return_value=result),
        patch.object(module, "handle_send", capture),
    ):
        asyncio.run(handler(bot=onebot, event=event))
    message = capture.await_args.args[2]
    assert "回府：回府回执" in message
    assert reason_text("map_home_return_receipt_write_rejected") in message

    for bot, use_markdown, fallback in (
        (onebot, False, False),
        (qq, False, False),
        (qq, True, False),
        (qq, True, True),
    ):
        plain, markdown = AsyncMock(), AsyncMock()
        if fallback:
            markdown.side_effect = RuntimeError("isolated home-return Markdown failure")
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
            asyncio.run(utils.send_msg_handler(bot, event, "回府", "900000", [message]))
        delivered = markdown if use_markdown and not fallback else plain
        delivered.assert_awaited_once()
        assert reason_text("map_home_return_receipt_write_rejected") in delivered.await_args.args[2]
        assert "回府回执" in delivered.await_args.args[2]
