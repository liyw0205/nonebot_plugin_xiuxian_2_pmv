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


def test_real_qiaoxue_query_and_configuration_error_paths():
    case_keys = (
        "additional-read-我的体窍",
        "additional-read-我的体窍-config-invalid-json",
        "additional-read-我的体窍-config-invalid-shape",
        "additional-read-我的体窍-config-unreadable",
        "additional-read-我的体窍-config-missing",
    )
    with tempfile.TemporaryDirectory(prefix="xiuxian-tiqiao-parameters-") as directory:
        report = Path(directory) / "report.json"
        command = [sys.executable, "-m", "tests.parameter_probe"]
        for key in case_keys:
            command.extend(("--case", key))
        command.extend(("--report", str(report)))
        result = subprocess.run(
            command,
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            timeout=150,
        )
        payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
        failures = [row for row in payload.get("results", []) if row["status"] != "pass"]
        assert result.returncode == 0, result.stdout + result.stderr + json.dumps(
            failures, ensure_ascii=False, indent=2
        )
        assert payload["summary"] == {"pass": len(case_keys)}, result.stdout


def test_qiaoxue_group_content_survives_onebot_qq_markdown_and_fallback():
    event = make_event("我的体窍 天罡", "900001", 979_301)
    content = [
        "【天罡窍穴】",
        "✅ 天罡·魁星窍 | 每分钟气血 +0.46%",
        "⬜ 天罡·天枢窍 | 获得气血 +0.20%",
        "进度：1/36",
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
        asyncio.run(
            utils.send_msg_handler(
                onebot, event, "我的体窍", "900000", content, title="【天罡窍穴】"
            )
        )
    plain.assert_awaited_once()
    plain_text = plain.await_args.args[2]
    assert all(value in plain_text for value in (content[0], content[1], content[3]))

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
        asyncio.run(
            utils.send_msg_handler(
                qq, event, "我的体窍", "900000", content, title="【天罡窍穴】"
            )
        )
    markdown.assert_awaited_once()
    markdown_text = markdown.await_args.args[2]
    assert all(value in markdown_text for value in (content[0], content[1], content[3]))

    fallback = AsyncMock()
    with (
        patch.object(utils, "XiuConfig", return_value=markdown_config),
        patch.object(
            utils,
            "handle_send_markdown",
            AsyncMock(side_effect=RuntimeError("isolated QQ Markdown failure")),
        ),
        patch.object(utils, "handle_send2", fallback),
    ):
        asyncio.run(
            utils.send_msg_handler(
                qq, event, "我的体窍", "900000", content, title="【天罡窍穴】"
            )
        )
    fallback.assert_awaited_once()
    fallback_text = fallback.await_args.args[2]
    assert all(value in fallback_text for value in (content[0], content[1], content[3]))
