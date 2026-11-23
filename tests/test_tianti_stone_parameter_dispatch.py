from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.command_probe import make_event
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils


def test_real_stone_training_matcher_parameters_and_recovery():
    with tempfile.TemporaryDirectory(prefix="xiuxian-tianti-stone-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.parameter_probe",
                "--group",
                "tianti-stone-parameters",
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
        assert payload.get("summary") == {"pass": 11}, details
        assert payload.get("outcomes") == {"recovery": 9, "success": 1, "rejection": 1}, details


def test_stone_training_rejection_reply_contract_for_onebot_qq_and_fallback():
    event = make_event("灵石炼体", "900001", 980_010)
    replies = (
        [
            "灵石炼体：灵石钱包，灵石扣除保存失败",
            "本次灵石炼体已撤销，故障解除后可重试原指令。",
        ],
        [
            "灵石炼体：玩家档案，炼体档案气血与本次结果不一致",
            "本次灵石炼体已撤销，请联系管理员核对炼体记录。",
        ],
        [
            "灵石炼体：操作回执，炼体操作回执保存失败",
            "本次灵石炼体已撤销，故障解除后可重试原指令。",
        ],
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
            asyncio.run(utils.send_msg_handler(onebot, event, "灵石炼体", "900000", content))
        plain.assert_awaited_once()
        assert all(value in plain.await_args.args[2] for value in content)

        markdown = AsyncMock()
        with (
            patch.object(utils, "XiuConfig", return_value=markdown_config),
            patch.object(utils, "handle_send_markdown", markdown),
        ):
            asyncio.run(utils.send_msg_handler(qq, event, "灵石炼体", "900000", content))
        markdown.assert_awaited_once()
        assert all(value in markdown.await_args.args[2] for value in content)

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
            asyncio.run(utils.send_msg_handler(qq, event, "灵石炼体", "900000", content))
        fallback.assert_awaited_once()
        assert all(value in fallback.await_args.args[2] for value in content)
