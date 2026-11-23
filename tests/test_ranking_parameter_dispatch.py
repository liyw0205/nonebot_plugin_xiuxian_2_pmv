from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils


def test_real_ranking_categories_top50_empty_and_isolation():
    with tempfile.TemporaryDirectory() as directory:
        report = Path(directory) / "ranking.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.parameter_probe",
                "--case",
                "ranking-five-categories-top50-empty-readonly",
                "--report",
                str(report),
                "--base-sha",
                "7e7ca019935f69f207ebb0543ee74c25274f3972",
            ],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert '"summary": {"pass": 1}' in result.stdout, result.stdout


def test_ranking_output_onebot_qq_markdown_and_failure_fallback():
    import asyncio

    event = SimpleNamespace(message_id=1001, get_user_id=lambda: "u")
    message = "【位面战力排行榜 前五十】\n第1位：诊断道友，战力300"
    bot_config = SimpleNamespace(
        markdown_status=True,
        markdown_id="",
        markdown_id2="",
        markdown_button_status=False,
        message_optimization=False,
        img=False,
        at_sender=False,
    )
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    plain = AsyncMock()
    with (
        patch.object(utils, "XiuConfig", return_value=bot_config),
        patch.object(utils, "_send_event_message", plain),
    ):
        asyncio.run(utils.handle_send(onebot, event, message))
    plain.assert_awaited_once()
    assert message in str(plain.await_args.kwargs["message"])

    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    markdown = AsyncMock()
    with (
        patch.object(utils, "XiuConfig", return_value=bot_config),
        patch.object(utils, "_allow_native_markdown", return_value=True),
        patch.object(utils, "handle_send_markdown", markdown),
    ):
        asyncio.run(utils.handle_send(qq, event, message))
    markdown.assert_awaited_once_with(qq, event, message, title=None, button_id=None)

    fallback = AsyncMock()
    with (
        patch.object(utils, "XiuConfig", return_value=bot_config),
        patch.object(utils, "_allow_native_markdown", return_value=True),
        patch.object(
            utils,
            "handle_send_markdown",
            AsyncMock(side_effect=RuntimeError("isolated ranking markdown failure")),
        ),
        patch.object(utils, "handle_send2", fallback),
    ):
        asyncio.run(utils.handle_send(qq, event, message))
    fallback.assert_awaited_once_with(qq, event, message)
