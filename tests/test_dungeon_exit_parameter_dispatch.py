from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch


class DungeonExitParameterDispatchTests(unittest.TestCase):
    def test_real_exit_progress_fault_recovery_and_reply_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.parameter_probe", "--group", "dungeon-exit-parameters",
                 "--report", str(report), "--base-sha", "b4f78c435424a4543fc2a93c740c33e449e0a7d9"],
                cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr + details)
            self.assertEqual({"pass": 4}, evidence["summary"], details)

    def test_handler_reasons_survive_onebot_qq_plain_markdown_and_fallback(self):
        import nonebot

        nonebot.init()
        from nonebot.exception import FinishedException
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

        event = SimpleNamespace(message_id=979399, get_user_id=lambda: "900001")
        onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
        qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
        config = SimpleNamespace(markdown_status=True, markdown_id="", message_optimization=False, img=False, at_sender=False)
        for reason, phrase in (
            ("state_write_rejected", "退出副本状态保存失败"),
            ("state_projection_mismatch", "退出副本后的状态与原进度不一致"),
            ("receipt_write_rejected", "退出副本回执保存失败"),
            ("receipt_projection_mismatch", "退出副本回执与请求不一致"),
        ):
            with self.subTest(reason=reason):
                application = SimpleNamespace(
                    session_operation=Mock(return_value=None),
                    session_transition=Mock(return_value={"status": "failed", "dungeon_status": "",
                                                         "reason": "dungeon_exit_" + reason, "subject": "900001"}),
                )
                manager = SimpleNamespace(get_player_status=Mock(return_value={"dungeon_id": "trial"}),
                                          get_dungeon_progress=Mock(return_value={"date": "2026-11-20"}))
                sent = AsyncMock()
                with (patch.object(module, "dungeon_application", application),
                      patch.object(module, "dungeon_manager", manager),
                      patch.object(module, "assign_bot", AsyncMock(return_value=(qq, "900100"))),
                      patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
                      patch.object(module, "handle_send", sent)):
                    with self.assertRaises(FinishedException):
                        asyncio.run(module.handle_dungeon_exit(qq, event))
                sent.assert_awaited_once()
                reply = sent.await_args.args[2]
                self.assertIn(phrase, reply)
                self.assertIn("玩家", reply)
                self.assertNotIn("已退出当前副本", reply)
                for bot in (onebot, qq):
                    plain = AsyncMock()
                    with (patch.object(utils, "XiuConfig", return_value=config),
                          patch.object(utils, "is_group_event", return_value=True),
                          patch.object(utils, "_allow_native_markdown", return_value=False),
                          patch.object(utils, "_send_event_message", plain)):
                        asyncio.run(utils.handle_send(bot, event, reply))
                    plain.assert_awaited_once()
                    self.assertIn(phrase, str(plain.await_args.kwargs["message"]))
                    self.assertIn("玩家", str(plain.await_args.kwargs["message"]))
                markdown = AsyncMock()
                with (patch.object(utils, "XiuConfig", return_value=config),
                      patch.object(utils, "_allow_native_markdown", return_value=True),
                      patch.object(utils, "handle_send_markdown", markdown)):
                    asyncio.run(utils.handle_send(qq, event, reply))
                markdown.assert_awaited_once()
                self.assertEqual(reply, markdown.await_args.args[2])
                failed = AsyncMock(side_effect=RuntimeError("isolated dungeon departure markdown failure"))
                fallback = AsyncMock()
                with (patch.object(utils, "XiuConfig", return_value=config),
                      patch.object(utils, "_allow_native_markdown", return_value=True),
                      patch.object(utils, "handle_send_markdown", failed),
                      patch.object(utils, "handle_send2", fallback)):
                    asyncio.run(utils.handle_send(qq, event, reply))
                fallback.assert_awaited_once_with(qq, event, reply)


if __name__ == "__main__":
    unittest.main()
