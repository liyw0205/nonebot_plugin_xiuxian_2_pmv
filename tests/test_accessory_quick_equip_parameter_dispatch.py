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


class AccessoryQuickEquipParameterDispatchTests(unittest.TestCase):
    def test_real_quick_equip_commands_and_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [
                    sys.executable, "-m", "tests.parameter_probe",
                    "--case", "accessory-quick-equip-write-recovery",
                    "--case", "accessory-quick-equip-parameters",
                    "--report", str(report),
                    "--base-sha", "e8a6307140b8192d85872d4dab1cb09ad83c39af",
                ],
                cwd=Path(__file__).resolve().parents[1],
                text=True, capture_output=True, timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr + details)
            self.assertEqual({"pass": 2}, evidence["summary"], details)

    def test_known_reasons_survive_onebot_qq_and_fallback(self):
        import nonebot

        nonebot.init()
        from nonebot_plugin_xiuxian_2.features.back.accessory_quick_equip_domain import AccessoryQuickEquipChange
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back import accessory
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

        event = SimpleNamespace(message_id=970760, get_user_id=lambda: "900001")
        args = SimpleNamespace(extract_plain_text=lambda: "2")
        config = SimpleNamespace(
            markdown_status=True, markdown_id="", message_optimization=False,
            img=False, at_sender=False,
        )
        qq_bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
        onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
        for reason, phrase in (
            ("accessory_quick_equip_state_write_rejected", "预设换装状态保存失败"),
            ("accessory_quick_equip_state_projection_mismatch", "预设换装状态与请求不一致"),
            ("accessory_quick_equip_receipt_write_rejected", "预设换装回执保存失败"),
            ("accessory_quick_equip_receipt_projection_mismatch", "预设换装回执内容不一致"),
        ):
            with self.subTest(reason=reason):
                result = AccessoryQuickEquipChange(
                    "failed", "quick_equip_preset", "900001", reason=reason, subject="preset:2",
                )
                application = SimpleNamespace(replay=lambda *_: None, equip=lambda *_: result)
                captured = AsyncMock()
                with (
                    patch.object(accessory, "check_user", return_value=(True, {"user_id": "900001"}, "")),
                    patch.object(accessory, "_get_data", return_value={"equipped": {}, "bag": []}),
                    patch.object(accessory, "_get_accessory_preset", return_value={"手镯": "target"}),
                    patch.object(accessory, "_quick_equip_application", return_value=application),
                    patch.object(accessory, "handle_send", captured),
                ):
                    asyncio.run(accessory.quick_equip_accessory.handlers[0].call(qq_bot, event, args))
                reply = captured.await_args.args[2]
                self.assertIn(phrase, reply)
                self.assertIn("饰品预设2", reply)
                self.assertNotIn("【成功装备】", reply)

                plain = AsyncMock()
                markdown = AsyncMock()
                with (
                    patch.object(utils, "XiuConfig", return_value=config),
                    patch.object(utils, "is_group_event", return_value=True),
                    patch.object(utils, "_allow_native_markdown", return_value=True),
                    patch.object(utils, "_send_event_message", plain),
                    patch.object(utils, "handle_send_markdown", markdown),
                ):
                    asyncio.run(utils.handle_send(onebot, event, reply))
                    asyncio.run(utils.handle_send(qq_bot, event, reply))
                plain.assert_awaited_once()
                self.assertIn(phrase, str(plain.call_args.kwargs["message"]))
                self.assertIn("饰品预设2", str(plain.call_args.kwargs["message"]))
                markdown.assert_awaited_once_with(qq_bot, event, reply, title=None, button_id=None)

                failed_markdown = AsyncMock(side_effect=RuntimeError("isolated quick equip markdown failure"))
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
