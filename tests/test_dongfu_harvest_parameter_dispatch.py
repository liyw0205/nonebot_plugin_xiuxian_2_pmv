from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import nonebot

nonebot.init()


def test_real_dongfu_harvest_mature_slots_and_same_message_replay():
    with tempfile.TemporaryDirectory(prefix="xiuxian-dongfu-harvest-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.parameter_probe",
                "--group",
                "dongfu-harvest",
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
            failures, ensure_ascii=False, indent=2
        )
        assert payload["summary"] == {"pass": 10}, result.stdout


def test_harvest_guard_preserves_specific_reasons_through_message_adapters():
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, patch

    from nonebot_plugin_xiuxian_2.core.errors import DomainError
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from tests.command_probe import make_event

    event = make_event("洞府收获 1", "900001", 970499)
    plain_config = SimpleNamespace(markdown_status=False, merge_forward_send=1,
                                   reference_reply=False, message_optimization=False, img=False, at_sender=False)
    markdown_config = SimpleNamespace(markdown_status=True, merge_forward_send=4, markdown_id="",
                                      markdown_button_status=False, reference_reply=False,
                                      message_optimization=False, img=False, at_sender=False)
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    handler = module.dongfu_harvest.handlers[0].call
    cases = [("harvest_snapshot_write_rejected", "harvest_snapshot"),
             ("harvest_snapshot_projection_mismatch", "harvest_snapshot")]
    for part, subject in (("state", "plant_slots"), ("inventory", "item:1"),
                          ("receipt", "receipt"), ("outbox", "outbox")):
        cases.extend((f"dongfu_harvest_{part}_{suffix}", subject)
                     for suffix in ("write_rejected", "projection_mismatch"))
    for reason, subject in cases:
        capture = AsyncMock()
        with (patch.object(module, "assign_bot", AsyncMock(return_value=(onebot, "900000"))),
              patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
              patch.object(module.dongfu_application, "operation_receipt",
                           side_effect=DomainError(reason, reason, {"reason": reason, "subject": subject})),
              patch.object(utils, "handle_send", capture)):
            asyncio.run(handler(bot=onebot, event=event))
        capture.assert_awaited_once()
        message = capture.await_args.args[2]
        assert reason_text(reason) in message
        assert next(iter(module._harvest_failure_subjects(DomainError(reason, reason, {"subject": subject})).values())) in message
        for bot, config, markdown, fail_markdown in (
            (onebot, plain_config, False, False), (qq, plain_config, False, False),
            (qq, markdown_config, True, False), (qq, markdown_config, True, True),
        ):
            plain, md = AsyncMock(), AsyncMock()
            if fail_markdown:
                md.side_effect = RuntimeError("isolated Markdown delivery failure")
            with (patch.object(utils, "XiuConfig", return_value=config),
                  patch.object(utils, "handle_send2", plain),
                  patch.object(utils, "handle_send_markdown", md)):
                asyncio.run(utils.send_msg_handler(bot, event, "洞府收获", "900000", [message]))
            delivered = md if markdown and not fail_markdown else plain
            delivered.assert_awaited_once()
            assert reason_text(reason) in delivered.await_args.args[2]


def test_receipt_replay_supports_legacy_rewards_and_rejects_malformed_numbers():
    from unittest.mock import patch
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module

    with patch.object(module.items, "get_data_by_item_id", return_value={"name": "旧药材", "type": "药材"}):
        snapshot = module._harvest_snapshot_from_receipt("u", "old", {
            "payload": json.dumps(["u", [1]]), "rewards": json.dumps([[1, 2]])})
    assert snapshot["items"] == [{"id": 1, "name": "旧药材", "type": "药材", "amount": 2}]
    assert snapshot["expected_slots"] == [] and snapshot["slot_numbers"] == [1]
    for payload, rewards in (
        (["u", ["bad"]], [[1, 2]]), (["u", [1]], [["bad", 2]]),
        (["u", [1]], [[1, "bad"]]), (["u", [], [1], [["bad", "药材", "药材", 2]]], [[1, 2]]),
    ):
        assert module._harvest_snapshot_from_receipt("u", "old", {
            "payload": json.dumps(payload), "rewards": json.dumps(rewards)}) is None
