from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.xiuxian.command_guard import failure_reply
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils


def test_real_profile_info_aliases_isolation_and_read_failure():
    with tempfile.TemporaryDirectory() as directory:
        report = Path(directory) / "unused.json"
        result = subprocess.run(
            [sys.executable, "-m", "tests.profile_info_probe"],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "aliases, natal display/isolation, activity timestamp and read failures passed" in result.stdout
        assert not report.exists()


def test_profile_read_failure_message_keeps_onebot_qq_and_fallback_contracts():
    bot_config = SimpleNamespace(
        markdown_status=True,
        markdown_id="",
        markdown_id2="",
        markdown_button_status=False,
        message_optimization=False,
        img=False,
        at_sender=False,
    )
    event = SimpleNamespace(message_id=1001, get_user_id=lambda: "u")
    reply = failure_reply(
        "我的修仙信息",
        DomainError(
            "profile_read_failed",
            "player profile could not be read",
            {"reason": "profile_read_failed", "subject": "player_profile"},
        ),
    )

    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    plain = AsyncMock()
    with (
        patch.object(utils, "XiuConfig", return_value=bot_config),
        patch.object(utils, "_allow_native_markdown", return_value=False),
        patch.object(utils, "_send_event_message", plain),
    ):
        import asyncio

        asyncio.run(utils.handle_send(onebot, event, reply))
    plain.assert_awaited_once()
    assert reply in str(plain.await_args.kwargs["message"])

    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    markdown = AsyncMock()
    with (
        patch.object(utils, "XiuConfig", return_value=bot_config),
        patch.object(utils, "_allow_native_markdown", return_value=True),
        patch.object(utils, "handle_send_markdown", markdown),
    ):
        asyncio.run(utils.handle_send(qq, event, reply))
    markdown.assert_awaited_once_with(qq, event, reply, title=None, button_id=None)

    fallback = AsyncMock()
    with (
        patch.object(utils, "XiuConfig", return_value=bot_config),
        patch.object(utils, "_allow_native_markdown", return_value=True),
        patch.object(
            utils,
            "handle_send_markdown",
            AsyncMock(side_effect=RuntimeError("isolated profile markdown failure")),
        ),
        patch.object(utils, "handle_send2", fallback),
    ):
        asyncio.run(utils.handle_send(qq, event, reply))
    fallback.assert_awaited_once_with(qq, event, reply)
