from __future__ import annotations

import importlib
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch
import types
from pathlib import Path

import nonebot

nonebot.init()


class AdapterCompatMessageRecordHookTests(unittest.TestCase):
    def test_missing_records_module_exposes_disabled_hooks_instead_of_empty_success(self) -> None:
        module = importlib.import_module(
            "nonebot_plugin_xiuxian_2.xiuxian.adapter_compat"
        )
        real_import = __import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name.endswith("adapter_message_records"):
                raise ImportError("records unavailable")
            return real_import(name, globals, locals, fromlist, level)

        globals_dict = {
            "__name__": "adapter_compat_test_copy",
            "__package__": module.__package__,
        }
        with patch("builtins.__import__", side_effect=fake_import):
            source = Path(module.__file__).read_text(encoding="utf-8")
            temp_module = types.ModuleType("adapter_compat_test_copy")
            temp_module.__dict__.update(globals_dict)
            sys.modules[temp_module.__name__] = temp_module
            try:
                exec(compile(source, module.__file__, "exec"), temp_module.__dict__)
            finally:
                sys.modules.pop(temp_module.__name__, None)

        test_module = temp_module
        self.assertFalse(test_module.HAS_MESSAGE_RECORDS)
        self.assertFalse(test_module.MESSAGE_RECORD_HOOKS.enabled)
        self.assertIsNone(test_module.MESSAGE_RECORD_HOOKS.record_send_message)
        self.assertIsNone(test_module.MESSAGE_RECORD_HOOKS.record_recv_message)
        self.assertEqual(test_module._extract_result_message_id({"msg_id": "m-1"}), "m-1")
        self.assertEqual(test_module._extract_text_from_message_obj(SimpleNamespace(extract_plain_text=lambda: "abc")), "abc")

    def test_record_wrappers_only_invoke_hook_when_enabled(self) -> None:
        module = importlib.import_module(
            "nonebot_plugin_xiuxian_2.xiuxian.adapter_compat"
        )
        send_calls = []
        recv_calls = []

        enabled_hooks = module.MessageRecordHooks(
            extract_result_message_id=module._fallback_extract_result_message_id,
            extract_text_from_message_obj=module._fallback_extract_text_from_message_obj,
            record_recv_message=lambda bot, event: recv_calls.append((bot, event)),
            record_send_message=lambda bot, **kwargs: send_calls.append((bot, kwargs)),
            enabled=True,
        )

        with patch.object(module, "MESSAGE_RECORD_HOOKS", enabled_hooks):
            module._record_send_if_enabled("bot-1", scene="group")
            module._record_recv_if_enabled("bot-2", "event-1")

        self.assertEqual(send_calls, [("bot-1", {"scene": "group"})])
        self.assertEqual(recv_calls, [("bot-2", "event-1")])

        disabled_hooks = module.MessageRecordHooks(
            extract_result_message_id=module._fallback_extract_result_message_id,
            extract_text_from_message_obj=module._fallback_extract_text_from_message_obj,
            record_recv_message=None,
            record_send_message=None,
            enabled=False,
        )

        with patch.object(module, "MESSAGE_RECORD_HOOKS", disabled_hooks):
            module._record_send_if_enabled("bot-3", scene="private")
            module._record_recv_if_enabled("bot-4", "event-2")

        self.assertEqual(len(send_calls), 1)
        self.assertEqual(len(recv_calls), 1)


if __name__ == "__main__":
    unittest.main()
