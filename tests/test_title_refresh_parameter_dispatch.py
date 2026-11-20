from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.parametrize("case_key", ["title-refresh-cache-validation"])
def test_real_title_refresh(case_key):
    with tempfile.TemporaryDirectory() as directory:
        report = Path(directory) / "parameters.json"
        result = subprocess.run(
            [sys.executable, "-m", "tests.parameter_probe", "--case", case_key,
             "--report", str(report), "--base-sha", "8c6c9322b2123585463049984cf6d3d2c2450316"],
            cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=180,
        )
        evidence = json.loads(report.read_text(encoding="utf-8")) if report.stat().st_size else {}
        details = json.dumps(evidence.get("results", []), ensure_ascii=False, indent=2)
        assert result.returncode == 0, result.stdout + result.stderr + details
        assert evidence.get("summary") == {"pass": 1}, details

        reply = evidence["results"][0]["evidence"]["invalid_reply"].split("\r", 1)[1]
        import nonebot

        nonebot.init()
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

        event = SimpleNamespace(message_id=999821, get_user_id=lambda: "900001")
        onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
        qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
        config = SimpleNamespace(markdown_status=True, markdown_id="", markdown_id2="",
                                 markdown_button_status=False, message_optimization=False,
                                 img=False, at_sender=False)
        for bot in (onebot, qq):
            plain = AsyncMock()
            with (patch.object(utils, "XiuConfig", return_value=config),
                  patch.object(utils, "is_group_event", return_value=True),
                  patch.object(utils, "_allow_native_markdown", return_value=False),
                  patch.object(utils, "_send_event_message", plain)):
                asyncio.run(utils.handle_send(bot, event, reply))
            plain.assert_awaited_once()
            assert reply in str(plain.await_args.kwargs["message"])

        markdown = AsyncMock()
        with (patch.object(utils, "XiuConfig", return_value=config),
              patch.object(utils, "_allow_native_markdown", return_value=True),
              patch.object(utils, "handle_send_markdown", markdown)):
            asyncio.run(utils.handle_send(qq, event, reply))
        markdown.assert_awaited_once_with(qq, event, reply, title=None, button_id=None)

        fallback = AsyncMock()
        with (patch.object(utils, "XiuConfig", return_value=config),
              patch.object(utils, "_allow_native_markdown", return_value=True),
              patch.object(utils, "handle_send_markdown",
                           AsyncMock(side_effect=RuntimeError("isolated title refresh markdown failure"))),
              patch.object(utils, "handle_send2", fallback)):
            asyncio.run(utils.handle_send(qq, event, reply))
        fallback.assert_awaited_once_with(qq, event, reply)
