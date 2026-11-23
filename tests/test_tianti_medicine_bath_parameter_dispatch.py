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


def test_real_medicine_bath_matcher_parameters_and_recovery():
    with tempfile.TemporaryDirectory(prefix="xiuxian-tianti-bath-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.parameter_probe",
                "--group",
                "tianti-bath-parameters",
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
        assert payload.get("summary") == {"pass": 3}, details
        assert payload.get("outcomes") == {"success": 3}, details


def test_medicine_bath_reply_contract_for_onebot_qq_markdown_and_fallback():
    event = make_event("炼体药浴", "900001", 979_830)
    messages = (
        [
            "药浴开启成功！",
            "消耗有效药材：恒心草x30，共30份",
            "炼体结算效果：200%",
            "超过上限的有效药材未消耗：214份",
            "药浴前已自动结算炼体气血：100",
        ],
        [
            "炼体药浴处理提示：药浴操作回执保存失败",
            "本次药浴已撤销，故障解除后可重试原指令。",
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

    for content in messages:
        plain = AsyncMock()
        with (
            patch.object(utils, "XiuConfig", return_value=plain_config),
            patch.object(utils, "handle_send2", plain),
        ):
            asyncio.run(
                utils.send_msg_handler(onebot, event, "炼体药浴", "900000", content)
            )
        plain.assert_awaited_once()
        plain_text = plain.await_args.args[2]
        assert all(value in plain_text for value in content), plain_text

        markdown = AsyncMock()
        with (
            patch.object(utils, "XiuConfig", return_value=markdown_config),
            patch.object(utils, "handle_send_markdown", markdown),
        ):
            asyncio.run(
                utils.send_msg_handler(qq, event, "炼体药浴", "900000", content)
            )
        markdown.assert_awaited_once()
        markdown_text = markdown.await_args.args[2]
        assert all(value in markdown_text for value in content), markdown_text

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
            asyncio.run(
                utils.send_msg_handler(qq, event, "炼体药浴", "900000", content)
            )
        fallback.assert_awaited_once()
        fallback_text = fallback.await_args.args[2]
        assert all(value in fallback_text for value in content), fallback_text
