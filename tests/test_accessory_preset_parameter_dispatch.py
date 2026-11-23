from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


class AccessoryPresetParameterDispatchTests(unittest.TestCase):
    def test_real_preset_commands_cover_writes_and_parameters(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tests.parameter_probe",
                    "--case",
                    "accessory-preset-save-write-recovery",
                    "--case",
                    "accessory-preset-parameters-readonly",
                    "--report",
                    str(report),
                    "--base-sha",
                    "1711fe9972afb9f391a17157b21acc6286b6bebd",
                ],
                cwd=Path(__file__).resolve().parents[1],
                text=True,
                capture_output=True,
                timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr + details)
            self.assertEqual({"pass": 2}, evidence["summary"], details)

    def test_failure_reason_and_preset_subject_survive_onebot_qq_and_fallback(self):
        for reason, phrase in (
            ("accessory_preset_state_write_rejected", "预设状态保存失败"),
            ("accessory_preset_state_projection_mismatch", "预设状态与保存请求不一致"),
            ("accessory_preset_receipt_write_rejected", "预设操作回执保存失败"),
            ("accessory_preset_receipt_projection_mismatch", "预设操作回执内容不一致"),
        ):
            with self.subTest(reason=reason):
                self.assert_failure_delivery(reason, phrase)

    def assert_failure_delivery(self, reason, phrase):
        import nonebot

        nonebot.init()
        from nonebot_plugin_xiuxian_2.features.back.accessory_preset_domain import AccessoryPresetChange
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back import accessory
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

        user_id = "900001"
        event = SimpleNamespace(message_id=970690, get_user_id=lambda: user_id)
        result = AccessoryPresetChange(
            "failed",
            "save_preset",
            user_id,
            reason=reason,
            subject="preset:2",
        )
        application = SimpleNamespace(replay=lambda *_: None, save=lambda *_: result)
        args = SimpleNamespace(extract_plain_text=lambda: "2")
        captured = AsyncMock()
        bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
        with (
            patch.object(accessory, "check_user", return_value=(True, {"user_id": user_id}, "")),
            patch.object(accessory, "_preset_application", return_value=application),
            patch.object(accessory, "handle_send", captured),
        ):
            asyncio.run(accessory.accessory_preset.handlers[0].call(bot, event, args))
        reply = captured.await_args.args[2]
        self.assertIn(phrase, reply)
        self.assertIn("饰品预设2", reply)

        config = SimpleNamespace(
            markdown_status=True, markdown_id="", message_optimization=False,
            img=False, at_sender=False,
        )
        onebot_text = AsyncMock()
        with (
            patch.object(utils, "XiuConfig", return_value=config),
            patch.object(utils, "is_group_event", return_value=True),
            patch.object(utils, "_send_event_message", onebot_text),
        ):
            asyncio.run(utils.handle_send(
                SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11")),
                event,
                reply,
            ))
        self.assertIn(phrase, str(onebot_text.call_args.kwargs["message"]))
        self.assertIn("饰品预设2", str(onebot_text.call_args.kwargs["message"]))

        markdown = AsyncMock()
        qq_bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
        with (
            patch.object(utils, "XiuConfig", return_value=config),
            patch.object(utils, "_allow_native_markdown", return_value=True),
            patch.object(utils, "handle_send_markdown", markdown),
        ):
            asyncio.run(utils.handle_send(qq_bot, event, reply))
        markdown.assert_awaited_once()
        self.assertIn(phrase, markdown.call_args.args[2])
        self.assertIn("饰品预设2", markdown.call_args.args[2])

        failed_markdown = AsyncMock(side_effect=RuntimeError("isolated preset markdown failure"))
        fallback = AsyncMock()
        with (
            patch.object(utils, "XiuConfig", return_value=config),
            patch.object(utils, "_allow_native_markdown", return_value=True),
            patch.object(utils, "handle_send_markdown", failed_markdown),
            patch.object(utils, "handle_send2", fallback),
        ):
            asyncio.run(utils.handle_send(qq_bot, event, reply))
        fallback.assert_awaited_once_with(qq_bot, event, reply)


if __name__ == "__main__":
    unittest.main()
