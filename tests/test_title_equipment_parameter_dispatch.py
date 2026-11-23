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


class TitleEquipmentParameterDispatchTests(unittest.TestCase):
    def test_real_title_equipment_parameters_and_write_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [
                    sys.executable, "-m", "tests.parameter_probe",
                    "--case", "title-equipment-receipt-recovery",
                    "--case", "title-equipment-parameters",
                    "--report", str(report),
                    "--base-sha", "5851d5d5cce240c70cfee663b30a48ae290d4232",
                ],
                cwd=Path(__file__).resolve().parents[1],
                text=True, capture_output=True, timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            details = json.dumps([
                {key: row.get(key) for key in ("key", "status", "error", "evidence")}
                for row in evidence["results"]
            ], ensure_ascii=False, indent=2)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr + details)
            self.assertEqual({"pass": 2}, evidence["summary"], details)

    def test_equipment_reasons_and_title_id_survive_onebot_qq_and_fallback(self):
        import nonebot

        nonebot.init()
        from nonebot.exception import FinishedException
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as title
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

        event = SimpleNamespace(message_id=970960, get_user_id=lambda: "900001")
        qq_bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
        onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
        args = SimpleNamespace(extract_plain_text=lambda: "2")
        config = SimpleNamespace(
            markdown_status=True, markdown_id2="", markdown_button_status=False,
            at_sender=False, message_optimization=False, img=False,
        )
        for matcher in (title.title_equip_cmd, title.title_unequip_cmd):
            for reason, phrase in (
                ("title_equip_write_rejected", "称号佩戴结果保存失败"),
                ("title_equipment_state_projection_mismatch", "称号装备状态与请求不一致"),
                ("title_equipment_receipt_write_rejected", "称号装备操作回执保存失败"),
                ("title_equipment_receipt_projection_mismatch", "称号装备操作回执内容不一致"),
            ):
                with self.subTest(command=matcher, reason=reason):
                    result = SimpleNamespace(status="failed", title_id="2", reason=reason, subject="2", succeeded=False)
                    sent = AsyncMock()
                    with (
                        patch.object(title, "assign_bot", new=AsyncMock(return_value=(qq_bot, None))),
                        patch.object(title, "check_user", return_value=(True, {"user_id": "900001"}, "")),
                        patch.object(title, "find_title_id_by_name_or_id", return_value="2"),
                        patch.object(title, "get_title_by_id", return_value={"name": "隔离称号"}),
                        patch.object(title, "get_user_unlocked_titles", return_value=["1", "2"]),
                        patch.object(title, "get_user_equipped_title", return_value="1"),
                        patch.object(title.title_application, "get_result", return_value=None),
                        patch.object(title, "_run_title_action", return_value=result),
                        patch.object(title, "handle_send", sent),
                    ):
                        with self.assertRaises(FinishedException):
                            arguments = (qq_bot, event, args) if matcher == title.title_equip_cmd else (qq_bot, event)
                            asyncio.run(matcher.handlers[0].call(*arguments))
                    reply, options = sent.await_args.args[2], sent.await_args.kwargs
                    self.assertIn(phrase, reply)
                    self.assertIn("称号 2", reply)
                    self.assertNotIn("成功装备", reply)
                    self.assertNotIn("成功卸下", reply)
                    plain, markdown = AsyncMock(), AsyncMock()
                    with (
                        patch.object(utils, "XiuConfig", return_value=config),
                        patch.object(utils, "is_group_event", return_value=True),
                        patch.object(utils, "_allow_native_markdown", return_value=True),
                        patch.object(utils, "_send_event_message", plain),
                        patch.object(utils, "handle_send_markdown_type", markdown),
                    ):
                        asyncio.run(utils.handle_send(onebot, event, reply, **options))
                        asyncio.run(utils.handle_send(qq_bot, event, reply, **options))
                    self.assertIn(phrase, str(plain.await_args.kwargs["message"]))
                    self.assertIn("称号 2", str(plain.await_args.kwargs["message"]))
                    self.assertEqual(reply, markdown.await_args.args[2])
                    fallback = AsyncMock()
                    with (
                        patch.object(utils, "XiuConfig", return_value=config),
                        patch.object(utils, "_allow_native_markdown", return_value=True),
                        patch.object(utils, "handle_send_markdown_type", new=AsyncMock(
                            side_effect=RuntimeError("isolated title markdown failure"),
                        )),
                        patch.object(utils, "handle_send2", fallback),
                    ):
                        asyncio.run(utils.handle_send(qq_bot, event, reply, **options))
                    fallback.assert_awaited_once_with(qq_bot, event, reply)


if __name__ == "__main__":
    unittest.main()
