from __future__ import annotations

import asyncio
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()


def test_registered_seed_purchase_parameters_and_sqlite_recovery():
    with tempfile.TemporaryDirectory(prefix="xiuxian-map-seed-purchase-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.parameter_probe",
                "--minimal-map",
                "--group",
                "map-seed-purchase",
                "--report",
                str(report),
            ],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            timeout=300,
        )
        payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
        failures = [row for row in payload.get("results", []) if row["status"] != "pass"]
        assert result.returncode == 0, (
            result.stdout + result.stderr + json.dumps(failures, ensure_ascii=False, indent=2)
        )
        assert payload.get("summary") == {"pass": 5}, result.stdout
        assert payload["outcomes"] == {"success": 2, "rejection": 2, "recovery": 1}
        matrix = next(row for row in payload["results"] if row["key"] == "map-seed-purchase-write-recovery")
        assert len(matrix["evidence"]["write_failures"]) == 9


def test_seed_purchase_known_reason_survives_onebot_and_qq_formatters():
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from tests.command_probe import make_event

    data = module._load_map_data()
    node = next(
        {**candidate, "realm": realm, "heaven": heaven}
        for realm in data["meta"]["realms"]
        for heaven in module._heaven_names(data, realm)
        for candidate in module._nodes(data, realm, heaven)
        if module._is_seed_shop_node(candidate["type"])
    )
    event = make_event("购买种子 青灵草种 2", "900001", 970099)
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    result = SimpleNamespace(
        ok=False,
        code="state_changed",
        data={
            "status": "state_changed",
            "reason": "map_seed_purchase_inventory_projection_mismatch",
            "subject": "item:21001",
        },
    )
    capture = AsyncMock()
    handler = module.buy_seed.handlers[0].call
    with (
        patch.object(module, "assign_bot", AsyncMock(return_value=(onebot, "900000"))),
        patch.object(module, "check_user", return_value=(True, {"user_id": "900001", "stone": 10_000_000}, "")),
        patch.object(module, "_load_map_data", return_value=data),
        patch.object(
            module,
            "_get_player_map_status",
            return_value={"realm": node["realm"], "heaven": node["heaven"], "node_id": node["id"]},
        ),
        patch.object(module, "_find_node_by_id", return_value=node),
        patch.object(module.map_application, "purchase_seed", return_value=result),
        patch.object(module, "handle_send", capture),
        patch.object(utils, "handle_send", capture),
    ):
        asyncio.run(handler(bot=onebot, event=event, args=SimpleNamespace(extract_plain_text=lambda: "青灵草种 2")))
    message = capture.await_args.args[2]
    assert "背包物品（ID 21001）" in message
    assert reason_text("map_seed_purchase_inventory_projection_mismatch") in message

    for bot, use_markdown, fallback in (
        (onebot, False, False),
        (qq, False, False),
        (qq, True, False),
        (qq, True, True),
    ):
        plain, markdown = AsyncMock(), AsyncMock()
        if fallback:
            markdown.side_effect = RuntimeError("isolated seed purchase Markdown failure")
        config = SimpleNamespace(
            markdown_status=use_markdown,
            merge_forward_send=4 if use_markdown else 1,
            markdown_id="",
            markdown_id2="",
            markdown_button_status=False,
            reference_reply=False,
            message_optimization=False,
            img=False,
            at_sender=False,
        )
        with (
            patch.object(utils, "XiuConfig", return_value=config),
            patch.object(utils, "handle_send2", plain),
            patch.object(utils, "handle_send_markdown", markdown),
        ):
            asyncio.run(utils.send_msg_handler(bot, event, "购买种子", "900000", [message]))
        delivered = markdown if use_markdown and not fallback else plain
        delivered.assert_awaited_once()
        rendered = re.sub(r"\r", "", str(delivered.await_args.args[2]))
        assert "背包物品（ID 21001）" in rendered
        assert reason_text("map_seed_purchase_inventory_projection_mismatch") in rendered
