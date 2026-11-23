from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()


def test_registered_map_combat_parameters_and_sqlite_recovery():
    with tempfile.TemporaryDirectory(prefix="xiuxian-map-combat-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.parameter_probe",
                "--minimal-map",
                "--group",
                "map-combat",
                "--report",
                str(report),
            ],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            timeout=360,
        )
        payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
        failures = [row for row in payload.get("results", []) if row["status"] != "pass"]
        assert result.returncode == 0, (
            result.stdout + result.stderr
            + json.dumps(failures, ensure_ascii=False, indent=2)
        )
        assert payload.get("summary") == {"pass": 6}, result.stdout
        assert payload["outcomes"] == {"success": 1, "rejection": 1, "recovery": 4}


def test_registered_map_dao_battle_parameters_and_sqlite_recovery():
    with tempfile.TemporaryDirectory(prefix="xiuxian-map-dao-battle-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.parameter_probe",
                "--minimal-map",
                "--group",
                "map-dao-battle",
                "--report",
                str(report),
            ],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            timeout=240,
        )
        payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
        failures = [row for row in payload.get("results", []) if row["status"] != "pass"]
        assert result.returncode == 0, (
            result.stdout + result.stderr
            + json.dumps(failures, ensure_ascii=False, indent=2)
        )
        assert payload.get("summary") == {"pass": 8}, result.stdout
        assert payload["outcomes"] == {"success": 2, "rejection": 1, "recovery": 5}


def test_combat_failure_reason_survives_onebot_qq_plain_markdown_and_fallback():
    import asyncio

    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian.command_guard import result_failure
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from tests.command_probe import make_event

    reason = "map_combat_settlement_receipt_write_rejected"
    message = result_failure(
        "节点战斗结算",
        {"reason": reason, "subject": "receipt"},
        subjects={"receipt": "战斗结算回执"},
    )
    event = make_event("节点战斗", "900001", 974999)
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))

    for bot, markdown_enabled, fail_markdown in (
        (onebot, False, False),
        (qq, False, False),
        (qq, True, False),
        (qq, True, True),
    ):
        plain, markdown = AsyncMock(), AsyncMock()
        if fail_markdown:
            markdown.side_effect = RuntimeError("isolated combat Markdown failure")
        config = SimpleNamespace(
            markdown_status=markdown_enabled,
            markdown_id="",
            markdown_button_status=False,
            reference_reply=False,
            message_optimization=False,
            img=False,
            at_sender=False,
        )
        with (
            patch.object(utils, "XiuConfig", return_value=config),
            patch.object(utils, "_allow_native_markdown", return_value=True),
            patch.object(utils, "handle_send2", plain),
            patch.object(utils, "handle_send_markdown", markdown),
        ):
            asyncio.run(utils.handle_send(bot, event, message))

        delivered = markdown if markdown_enabled and not fail_markdown else plain
        delivered.assert_awaited_once()
        rendered = str(delivered.await_args.args[2]).replace("\r", "")
        assert "节点战斗结算：战斗结算回执" in rendered, rendered
        assert reason_text(reason) in rendered, rendered
        assert "本次奖励已撤销" in rendered, rendered


def test_dao_battle_failure_reason_survives_onebot_qq_plain_markdown_and_fallback():
    import asyncio

    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian.command_guard import result_failure
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from tests.command_probe import make_event

    reason = "map_dao_battle_receipt_write_rejected"
    message = result_failure(
        "论道切磋", {"reason": reason, "subject": "receipt"},
        subjects={"challenger": "发起方战绩", "target": "对方", "receipt": "论道回执"},
    )
    event = make_event("论道切磋", "900001", 975199)
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))

    for bot, markdown_enabled, fail_markdown in (
        (onebot, False, False), (qq, False, False), (qq, True, False), (qq, True, True),
    ):
        plain, markdown = AsyncMock(), AsyncMock()
        if fail_markdown:
            markdown.side_effect = RuntimeError("isolated dao Markdown failure")
        config = SimpleNamespace(
            markdown_status=markdown_enabled, markdown_id="", markdown_button_status=False,
            reference_reply=False, message_optimization=False, img=False, at_sender=False,
        )
        with (
            patch.object(utils, "XiuConfig", return_value=config),
            patch.object(utils, "_allow_native_markdown", return_value=True),
            patch.object(utils, "handle_send2", plain),
            patch.object(utils, "handle_send_markdown", markdown),
        ):
            asyncio.run(utils.handle_send(bot, event, message))
        delivered = markdown if markdown_enabled and not fail_markdown else plain
        rendered = str(delivered.await_args.args[2]).replace("\r", "")
        assert "论道切磋：论道回执" in rendered, rendered
        assert reason_text(reason) in rendered and "双方战绩均已回滚" in rendered, rendered
