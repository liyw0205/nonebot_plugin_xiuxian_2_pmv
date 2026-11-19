import asyncio
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


class AccessoryAffixParameterDispatchTests(unittest.TestCase):
    def test_real_lock_and_unlock_commands_preserve_failures_and_recover(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            cases = (
                "accessory-affix-lock-write-recovery",
                "accessory-affix-unlock-write-recovery",
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
                    "76d716c4983278c6fc0194bfc92700cffa7b09b2",
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

    def test_qq_affix_reply_preserves_failure_and_uid_on_fallback(self):
        import nonebot

        nonebot.init()
        from nonebot_plugin_xiuxian_2.features.back.accessory_affix_domain import AccessoryAffixChange
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back import accessory
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

        user_id, uid = "900001", "fixture-accessory-01"
        bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
        event = SimpleNamespace(message_id=970410, get_user_id=lambda: user_id)
        target = {
            "uid": uid, "quality": 1, "locked_affixes": [0],
            "affixes": [{"type": "攻击", "value": 0.1}, {"type": "速度", "value": 20}],
        }
        cfg = SimpleNamespace(markdown_status=True, markdown_button_status=False, markdown_id2="")
        cases = (
            (accessory.lock_accessory_affix, "lock", "1",
             "accessory_affix_state_write_rejected", "饰品词条状态保存失败"),
            (accessory.unlock_accessory_affix, "unlock", "全部",
             "accessory_affix_receipt_projection_mismatch", "饰品词条操作回执不一致"),
        )
        for matcher, action, tokens, reason, phrase in cases:
            for fallback in (False, True):
                with self.subTest(action=action, fallback=fallback):
                    result = AccessoryAffixChange("failed", action, user_id, reason=reason, subject=uid)
                    app = SimpleNamespace(replay=lambda *_: None, set_locks=lambda *_: result)
                    args = SimpleNamespace(extract_plain_text=lambda: f"{uid} {tokens}")
                    markdown = AsyncMock(side_effect=RuntimeError("isolated QQ delivery failure") if fallback else None)
                    plain = AsyncMock()
                    with (
                        patch.object(accessory, "check_user", return_value=(True, {"user_id": user_id}, "")),
                        patch.object(accessory, "_get_data", return_value={"bag": [target], "equipped": {}}),
                        patch.object(accessory, "_affix_application", return_value=app),
                        patch.object(utils, "XiuConfig", return_value=cfg),
                        patch.object(utils, "_allow_native_markdown", return_value=True),
                        patch.object(utils, "handle_send_markdown_type", markdown),
                        patch.object(utils, "handle_send2", plain),
                    ):
                        asyncio.run(matcher.handlers[0].call(bot, event, args))
                    markdown.assert_awaited_once()
                    reply = markdown.await_args.args[2]
                    self.assertIn(phrase, reply)
                    self.assertIn(uid, reply)
                    if fallback:
                        plain.assert_awaited_once_with(bot, event, reply)
                    else:
                        plain.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
