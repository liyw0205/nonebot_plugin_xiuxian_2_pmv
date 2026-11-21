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


def test_my_bath_history_real_matcher_and_read_failure_are_audited():
    with tempfile.TemporaryDirectory(prefix="xiuxian-tianti-bath-history-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.parameter_probe",
                "--group",
                "tianti-bath-history",
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
        assert payload.get("summary") == {"pass": 2}, details
        assert payload.get("outcomes") == {"success": 1, "rejection": 1}, details


def test_bath_history_reply_contract_for_onebot_qq_markdown_and_fallback():
    event = make_event("我的药浴记录", "900001", 979_906)
    content = [
        "近7天药浴记录",
        "开启次数：1 次",
        "累计消耗药材：5 份（1 种）",
        "最高结算效果：150%",
        "- 2026-11-21 11:00:00 有效药浴",
    ]
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    plain_config = SimpleNamespace(
        markdown_status=False,
        merge_forward_send=1,
        reference_reply=False,
        message_optimization=False,
        img=False,
        at_sender=False,
    )
    plain = AsyncMock()
    with (
        patch.object(utils, "XiuConfig", return_value=plain_config),
        patch.object(utils, "handle_send2", plain),
    ):
        asyncio.run(utils.send_msg_handler(onebot, event, "我的药浴记录", "900000", content))
    plain.assert_awaited_once()
    plain_text = plain.await_args.args[2]
    assert all(value in plain_text for value in content)

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
    markdown = AsyncMock()
    with (
        patch.object(utils, "XiuConfig", return_value=markdown_config),
        patch.object(utils, "handle_send_markdown", markdown),
    ):
        asyncio.run(utils.send_msg_handler(qq, event, "我的药浴记录", "900000", content))
    markdown.assert_awaited_once()
    markdown_text = markdown.await_args.args[2]
    assert all(value in markdown_text for value in content)

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
        asyncio.run(utils.send_msg_handler(qq, event, "我的药浴记录", "900000", content))
    fallback.assert_awaited_once()
    fallback_text = fallback.await_args.args[2]
    assert all(value in fallback_text for value in content)
