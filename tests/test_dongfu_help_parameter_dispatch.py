from __future__ import annotations

import asyncio
import html
import json
import re
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

@pytest.fixture(scope="module")
def dongfu_help_probe(tmp_path_factory):
    report = tmp_path_factory.mktemp("xiuxian-dongfu-help") / "parameters.json"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "tests.parameter_probe",
            "--group",
            "dongfu-help",
            "--report",
            str(report),
        ],
        cwd=Path(__file__).resolve().parents[1],
        text=True,
        capture_output=True,
        timeout=180,
    )
    payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
    details = json.dumps(payload.get("results", []), ensure_ascii=False, indent=2)
    assert result.returncode == 0, result.stdout + result.stderr + details
    assert payload.get("summary") == {"pass": 1}, details
    return payload


def test_real_dongfu_help_manifest_usage_and_read_only_contract(dongfu_help_probe):
    assert dongfu_help_probe["summary"] == {"pass": 1}


def test_dongfu_help_message_survives_onebot_qq_markdown_and_fallback(dongfu_help_probe):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

    event = SimpleNamespace(message_id=974510, get_user_id=lambda: "900001")
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    reply = dongfu_help_probe["results"][0]["evidence"]["replies"][0]
    message = re.sub(r"^\[CQ:at,qq=\d+\]", "", html.unescape(reply)).replace("\n", "\r")
    config = SimpleNamespace(
        markdown_status=True,
        markdown_id="",
        markdown_id2="",
        markdown_button_status=False,
        message_optimization=False,
        img=False,
        at_sender=False,
    )

    for bot in (onebot, qq):
        plain = AsyncMock()
        with (
            patch.object(utils, "XiuConfig", return_value=config),
            patch.object(utils, "is_group_event", return_value=True),
            patch.object(utils, "_allow_native_markdown", return_value=False),
            patch.object(utils, "_send_event_message", plain),
        ):
            asyncio.run(
                utils.handle_send(
                    bot,
                    event,
                    message,
                    native_markdown=True,
                    fallback_msg=message,
                )
            )
        plain.assert_awaited_once()
        assert "洞府种植" in str(plain.await_args.kwargs["message"])

    markdown = AsyncMock()
    with (
        patch.object(utils, "XiuConfig", return_value=config),
        patch.object(utils, "_allow_native_markdown", return_value=True),
        patch.object(utils, "_send_markdown_or_keyboard", markdown),
    ):
        asyncio.run(
            utils.handle_send(
                qq,
                event,
                message,
                native_markdown=True,
                fallback_msg=message,
            )
        )
    markdown.assert_awaited_once()
    assert "洞府扩建" in markdown.await_args.args[2]

    fallback = AsyncMock()
    with (
        patch.object(utils, "XiuConfig", return_value=config),
        patch.object(utils, "_allow_native_markdown", return_value=True),
        patch.object(
            utils,
            "_send_markdown_or_keyboard",
            AsyncMock(side_effect=RuntimeError("isolated dongfu help Markdown failure")),
        ),
        patch.object(utils, "handle_send2", fallback),
    ):
        asyncio.run(
            utils.handle_send(
                qq,
                event,
                message,
                native_markdown=True,
                fallback_msg=message,
            )
        )
    fallback.assert_awaited_once()
    assert fallback.await_args.args[:2] == (qq, event)
    assert fallback.await_args.args[2].replace("\n", "\r") == message
