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


def test_real_training_shop_queries_all_boundaries_and_catalog_are_read_only():
    with tempfile.TemporaryDirectory(prefix="xiuxian-training-shop-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.parameter_probe",
                "--group",
                "training-shop-parameters",
                "--report",
                str(report),
            ],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            timeout=180,
        )
        payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
        failures = [row for row in payload.get("results", []) if row["status"] != "pass"]
        assert result.returncode == 0, result.stdout + result.stderr + json.dumps(
            failures, ensure_ascii=False
        )
        assert payload["summary"] == {"pass": 4}, result.stdout


def test_training_shop_content_survives_onebot_qq_markdown_and_fallback():
    event = make_event("历练商店 1", "900001", 979_101)
    content = [
        "道友目前拥有的历练成就点：4321点",
        "【历练商店】第1/3页",
        "编号：1999\n名称：渡厄丹\n价格：1000成就点\n每周限购：10/10个",
        "提示：发送 历练商店 2 查看其他页（共3页）",
    ]
    page = ["翻页", "历练商店 2", "状态", "历练状态", "兑换", "历练兑换", "1/3"]

    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    plain = AsyncMock()
    plain_config = SimpleNamespace(
        markdown_status=False,
        merge_forward_send=1,
        reference_reply=False,
        message_optimization=False,
        img=False,
        at_sender=False,
    )
    with (
        patch.object(utils, "XiuConfig", return_value=plain_config),
        patch.object(utils, "handle_send2", plain),
    ):
        asyncio.run(
            utils.send_msg_handler(onebot, event, "历练商店", "900000", content, page=page)
        )
    plain.assert_awaited_once()
    plain_text = plain.await_args.args[2]
    assert all(value in plain_text for value in (content[0], content[2], "历练商店 2"))

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
            utils.send_msg_handler(qq, event, "历练商店", "900000", content, page=page)
        )
    markdown.assert_awaited_once()
    markdown_text = markdown.await_args.args[2]
    assert all(value in markdown_text for value in (content[0], content[2]))
    assert markdown.await_args.kwargs["page"] == page

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
            utils.send_msg_handler(qq, event, "历练商店", "900000", content, page=page)
        )
    fallback.assert_awaited_once()
    fallback_text = fallback.await_args.args[2]
    assert all(value in fallback_text for value in (content[0], content[2]))
