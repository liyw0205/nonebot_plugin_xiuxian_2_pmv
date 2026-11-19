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


class AccessoryUpgradeParameterDispatchTests(unittest.TestCase):
    def test_real_upgrade_command_rejects_write_failures_and_recovers(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tests.parameter_probe",
                    "--case",
                    "accessory-upgrade-write-recovery",
                    "--case",
                    "accessory-upgrade-parameters",
                    "--case",
                    "accessory-upgrade-ambiguous-material",
                    "--report",
                    str(report),
                    "--base-sha",
                    "ab80fd7f19276735e7b530ade81df7897e5b063e",
                ],
                cwd=Path(__file__).resolve().parents[1],
                text=True,
                capture_output=True,
                timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr + details)
            self.assertEqual({"pass": 3}, evidence["summary"], details)

    def test_known_reasons_preserve_main_uid_on_onebot_qq_and_fallback(self):
        import nonebot

        nonebot.init()
        from nonebot_plugin_xiuxian_2.features.back.accessory_upgrade_domain import AccessoryUpgradeChange
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back import accessory
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

        uid, material_uid = "isolated-main-ring", "isolated-material-ring"
        main = {
            "uid": uid, "item_id": 19002, "part": "戒指", "set_type": "烈阳",
            "quality": 1, "name": "烈阳戒指", "wash_count": 3, "affixes": [],
        }
        material = dict(main, uid=material_uid)
        event = SimpleNamespace(message_id=970850, get_user_id=lambda: "900001")
        args = SimpleNamespace(extract_plain_text=lambda: f"戒指 {material_uid}")
        config = SimpleNamespace(
            markdown_status=True, markdown_id="", message_optimization=False,
            img=False, at_sender=False,
        )
        qq_bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
        onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
        for reason, phrase in (
            ("accessory_upgrade_state_write_rejected", "升阶状态保存失败"),
            ("accessory_upgrade_state_projection_mismatch", "升阶状态与请求不一致"),
            ("accessory_upgrade_receipt_write_rejected", "升阶操作回执保存失败"),
            ("accessory_upgrade_receipt_projection_mismatch", "升阶回执内容不一致"),
            ("accessory_material_uid_duplicate", "所选升品材料编号对应多件饰品"),
        ):
            with self.subTest(reason=reason):
                result = AccessoryUpgradeChange(
                    "material_missing" if reason == "accessory_material_uid_duplicate" else "failed",
                    "upgrade", "900001", reason=reason, subject=uid,
                )
                application = SimpleNamespace(replay=lambda *_: None, upgrade=lambda *_: result)
                captured = AsyncMock()
                with (
                    patch.object(accessory, "check_user", return_value=(True, {"user_id": "900001"}, "")),
                    patch.object(accessory, "_get_data", return_value={"equipped": {"戒指": main}, "bag": [material]}),
                    patch.object(accessory, "_upgrade_application", return_value=application),
                    patch.object(accessory, "handle_send", captured),
                ):
                    asyncio.run(accessory.upgrade_accessory.handlers[0].call(qq_bot, event, args))
                reply = captured.await_args.args[2]
                self.assertIn(phrase, reply)
                self.assertIn(uid, reply)
                self.assertNotIn("升阶成功", reply)
                plain, markdown = AsyncMock(), AsyncMock()
                with (
                    patch.object(utils, "XiuConfig", return_value=config),
                    patch.object(utils, "is_group_event", return_value=True),
                    patch.object(utils, "_allow_native_markdown", return_value=True),
                    patch.object(utils, "_send_event_message", plain),
                    patch.object(utils, "handle_send_markdown", markdown),
                ):
                    asyncio.run(utils.handle_send(onebot, event, reply))
                    asyncio.run(utils.handle_send(qq_bot, event, reply))
                self.assertIn(phrase, str(plain.call_args.kwargs["message"]))
                self.assertIn(uid, str(plain.call_args.kwargs["message"]))
                markdown.assert_awaited_once_with(qq_bot, event, reply, title=None, button_id=None)

                fallback = AsyncMock()
                with (
                    patch.object(utils, "XiuConfig", return_value=config),
                    patch.object(utils, "_allow_native_markdown", return_value=True),
                    patch.object(utils, "handle_send_markdown", new=AsyncMock(
                        side_effect=RuntimeError("isolated upgrade markdown failure"),
                    )),
                    patch.object(utils, "handle_send2", fallback),
                ):
                    asyncio.run(utils.handle_send(qq_bot, event, reply))
                fallback.assert_awaited_once_with(qq_bot, event, reply)


if __name__ == "__main__":
    unittest.main()
