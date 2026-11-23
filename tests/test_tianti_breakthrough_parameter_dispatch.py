from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.command_probe import make_event
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils


def test_real_tianti_breakthrough_matcher_write_rejections_and_recovery():
    with tempfile.TemporaryDirectory(prefix="xiuxian-tianti-breakthrough-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.parameter_probe",
                "--group",
                "tianti-breakthrough-parameters",
                "--report",
                str(report),
            ],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            timeout=240,
        )
        payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
        details = json.dumps(payload.get("results", []), ensure_ascii=False, indent=2)
        assert result.returncode == 0, result.stdout + result.stderr + details
        assert payload.get("summary") == {"pass": 7}, details
        assert payload.get("outcomes") == {"recovery": 6, "success": 1}, details


def test_tianti_breakthrough_rejection_reply_contract_for_onebot_qq_and_fallback():
    event = make_event("炼体突破", "900001", 980_020)
    replies = (
        "炼体突破处理提示：玩家档案，炼体突破档案保存失败，本次突破已撤销，故障解除后可重试原指令。",
        "炼体突破处理提示：操作回执，炼体突破回执保存失败，本次突破已撤销，故障解除后可重试原指令。",
        "炼体突破处理提示：玩家档案，炼体档案与突破结果不一致，本次突破已撤销，请联系管理员核对炼体记录。",
        "炼体突破处理提示：操作回执，炼体突破回执与本次结果不一致，本次突破已撤销，请联系管理员核对请求记录。",
    )
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    plain_config = SimpleNamespace(
        markdown_status=False,
        merge_forward_send=1,
        reference_reply=False,
        message_optimization=False,
        img=False,
        at_sender=False,
    )
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    markdown_config = SimpleNamespace(
        markdown_status=True,
        merge_forward_send=4,
        markdown_id="",
        markdown_button_status=False,
        reference_reply=False,
        message_optimization=False,
        img=False,
        at_sender=False,
    )

    for content in replies:
        plain = AsyncMock()
        with (
            patch.object(utils, "XiuConfig", return_value=plain_config),
            patch.object(utils, "handle_send2", plain),
        ):
            asyncio.run(utils.send_msg_handler(onebot, event, "炼体突破", "900000", [content]))
        plain.assert_awaited_once()
        assert content in plain.await_args.args[2]

        markdown = AsyncMock()
        with (
            patch.object(utils, "XiuConfig", return_value=markdown_config),
            patch.object(utils, "handle_send_markdown", markdown),
        ):
            asyncio.run(utils.send_msg_handler(qq, event, "炼体突破", "900000", [content]))
        markdown.assert_awaited_once()
        assert content in markdown.await_args.args[2]

        fallback = AsyncMock()
        with (
            patch.object(utils, "XiuConfig", return_value=markdown_config),
            patch.object(
                utils,
                "handle_send_markdown",
                AsyncMock(side_effect=RuntimeError("markdown unavailable")),
            ),
            patch.object(utils, "handle_send2", fallback),
        ):
            asyncio.run(utils.send_msg_handler(qq, event, "炼体突破", "900000", [content]))
        fallback.assert_awaited_once()
        assert content in fallback.await_args.args[2]
