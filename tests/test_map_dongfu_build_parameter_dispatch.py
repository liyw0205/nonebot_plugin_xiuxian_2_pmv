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


def test_real_dongfu_build_parameters_and_writes():
    with tempfile.TemporaryDirectory(prefix="xiuxian-dongfu-build-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.parameter_probe",
                "--group",
                "dongfu-build",
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
        assert payload["summary"] == {"pass": 14}, result.stdout


def test_build_failure_reason_survives_onebot_and_qq_formatters():
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from tests.command_probe import make_event

    event = make_event("建设洞府", "900001", 973099)
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    node = {"id": "n1", "name": "青山", "type": "山脉"}
    map_data = {"meta": {}, "测试界": {"heavens": {"一重天": [node]}}}
    result = SimpleNamespace(data={
        "status": "state_changed",
        "stone": 150_000_000,
        "reason": "map_dongfu_build_receipt_write_rejected",
        "subject": "receipt",
    }, code="map_dongfu_build_receipt_write_rejected")
    config = SimpleNamespace(
        markdown_status=False,
        merge_forward_send=1,
        reference_reply=False,
        message_optimization=False,
        img=False,
        at_sender=False,
    )
    capture = AsyncMock()
    handler = module.build_dongfu.handlers[0].call
    with (
        patch.object(module, "assign_bot", AsyncMock(return_value=(onebot, "900000"))),
        patch.object(module, "check_user", return_value=(True, {"user_id": "900001", "stone": 150_000_000}, "")),
        patch.object(module, "_load_map_data", return_value=map_data),
        patch.object(module, "_get_player_map_status", return_value={
            "realm": "测试界", "heaven": "一重天", "node_id": "n1",
        }),
        patch.object(module.map_application, "dongfu", return_value={"built": 0}),
        patch.object(module.map_application, "build_dongfu", return_value=result),
        patch.object(module, "handle_send", capture),
    ):
        asyncio.run(handler(bot=onebot, event=event))
    message = capture.await_args.args[2]
    assert "建设洞府：建设回执" in message
    assert reason_text("map_dongfu_build_receipt_write_rejected") in message

    for bot, use_markdown, fallback in (
        (onebot, False, False),
        (qq, False, False),
        (qq, True, False),
        (qq, True, True),
    ):
        plain, markdown = AsyncMock(), AsyncMock()
        if fallback:
            markdown.side_effect = RuntimeError("isolated build Markdown failure")
        selected_config = SimpleNamespace(
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
            patch.object(utils, "XiuConfig", return_value=selected_config),
            patch.object(utils, "handle_send2", plain),
            patch.object(utils, "handle_send_markdown", markdown),
        ):
            asyncio.run(utils.send_msg_handler(bot, event, "建设洞府", "900000", [message]))
        delivered = markdown if use_markdown and not fallback else plain
        delivered.assert_awaited_once()
        assert reason_text("map_dongfu_build_receipt_write_rejected") in delivered.await_args.args[2]
        assert "建设回执" in delivered.await_args.args[2]
