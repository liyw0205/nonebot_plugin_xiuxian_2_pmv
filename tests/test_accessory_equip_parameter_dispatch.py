import asyncio
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch


class AccessoryEquipParameterDispatchTests(unittest.TestCase):
    def test_real_equip_commands_report_ignored_writes_without_changing_state(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            cases = (
                "accessory-equip-write-rejection",
                "accessory-unequip-write-rejection",
            )
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tests.parameter_probe",
                    *[argument for key in cases for argument in ("--case", key)],
                    "--report",
                    str(report),
                    "--base-sha",
                    "4f2647a49efd662bc1339e12e3f38eab968c6c91",
                ],
                cwd=Path(__file__).resolve().parents[1],
                text=True,
                capture_output=True,
                timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            diagnostics = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr + diagnostics)
            self.assertEqual({"pass": 2}, evidence["summary"], diagnostics)

    def test_qq_delivers_equipment_write_failure_with_subject(self):
        import nonebot

        nonebot.init()
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back import accessory
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.player_data_manager import DocumentWriteRejected

        event = SimpleNamespace(get_user_id=lambda: "900001")
        bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
        config = SimpleNamespace(markdown_status=True, markdown_id="")
        cases = (
            (accessory.equip_accessory, "fixture-accessory-02", "装备失败"),
            (accessory.unequip_accessory, "手镯", "卸下失败"),
        )
        for matcher, subject, phrase in cases:
            for fallback in (False, True):
                with self.subTest(subject=subject, fallback=fallback):
                    fault = DocumentWriteRejected("player_accessory", "900001", projection_mismatch=fallback)
                    manager = SimpleNamespace(patch_doc=Mock(side_effect=fault))
                    args = SimpleNamespace(extract_plain_text=lambda: subject)
                    markdown = AsyncMock(side_effect=RuntimeError("isolated delivery failure") if fallback else None)
                    plain = AsyncMock()
                    with (
                        patch.object(accessory, "check_user", return_value=(True, {"user_id": "900001"}, "")),
                        patch.object(accessory, "_player_data_manager", return_value=manager),
                        patch.object(utils, "XiuConfig", return_value=config),
                        patch.object(utils, "_allow_native_markdown", return_value=True),
                        patch.object(utils, "handle_send_markdown", markdown),
                        patch.object(utils, "handle_send2", plain),
                    ):
                        asyncio.run(matcher.handlers[0].call(bot, event, args))
                    markdown.assert_awaited_once()
                    message = markdown.await_args.args[2]
                    self.assertIn(phrase, message)
                    self.assertIn(subject, message)
                    self.assertIn(fault.code, message)
                    if fallback:
                        plain.assert_awaited_once_with(bot, event, message)
                    else:
                        plain.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
