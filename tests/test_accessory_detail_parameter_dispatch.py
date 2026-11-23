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


class AccessoryDetailParameterDispatchTests(unittest.TestCase):
    def test_real_owned_detail_queries_preserve_both_databases(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [
                    sys.executable, "-m", "tests.parameter_probe",
                    "--case", "accessory-detail-owned-readonly",
                    "--report", str(report),
                    "--base-sha", "be9a0edb4cb0a7970cc886e8d90ddf3bd7ca9d6e",
                ],
                cwd=Path(__file__).resolve().parents[1],
                text=True, capture_output=True, timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr + details)
            self.assertEqual({"pass": 1}, evidence["summary"], details)

    def test_detail_onebot_qq_and_fallback_preserve_uid_and_location(self):
        from tests.test_accessory_readonly_messages import _capture_route, _delivery_config
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back import accessory
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

        for uid, location in (("adapter-accessory-1", "已装备（手镯）"), ("adapter-accessory-2", "背包")):
            with self.subTest(uid=uid):
                event, qq_bot, sent = _capture_route(accessory.check_accessory, uid)
                reply, options = sent.await_args.args[2], sent.await_args.kwargs
                for phrase in (uid, f"状态：{location}", "当前洗练次数：0/150", "锁定词条：无", "- 无词条"):
                    self.assertIn(phrase, reply)
                plain, markdown = AsyncMock(), AsyncMock()
                onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
                with (
                    patch.object(utils, "XiuConfig", return_value=_delivery_config()),
                    patch.object(utils, "is_group_event", return_value=True),
                    patch.object(utils, "_allow_native_markdown", return_value=True),
                    patch.object(utils, "_send_event_message", plain),
                    patch.object(utils, "handle_send_markdown_type", markdown),
                ):
                    asyncio.run(utils.handle_send(onebot, event, reply, **options))
                    asyncio.run(utils.handle_send(qq_bot, event, reply, **options))
                self.assertIn(uid, str(plain.await_args.kwargs["message"]))
                self.assertIn(f"状态：{location}", str(plain.await_args.kwargs["message"]))
                self.assertEqual(reply, markdown.await_args.args[2])
                self.assertEqual("背包", markdown.await_args.args[3])
                fallback = AsyncMock()
                with (
                    patch.object(utils, "XiuConfig", return_value=_delivery_config()),
                    patch.object(utils, "_allow_native_markdown", return_value=True),
                    patch.object(utils, "handle_send_markdown_type", new=AsyncMock(
                        side_effect=RuntimeError("isolated accessory detail markdown failure"),
                    )),
                    patch.object(utils, "handle_send2", fallback),
                ):
                    asyncio.run(utils.handle_send(qq_bot, event, reply, **options))
                fallback.assert_awaited_once_with(qq_bot, event, reply)


if __name__ == "__main__":
    unittest.main()
