"""QQ inbound payload normalization (reference messages + RESUMED dispatch).

Production evidence (xiu2.log):

* 52 × ``pydantic_core.ValidationError: 2 validation errors for
  GroupMessageCreateEventmsg_elements.0.message_type / msg_idx`` — QQ reference
  (引用) messages arrive with ``msg_elements`` entries that only carry
  ``content``.  The pip adapter model requires both fields, so validation fails
  and the whole group message is dropped before any handler runs.
* ``TypeError: 'str' object is not a mapping`` at the adapter's
  ``{"event_id": payload.id, **payload.data}`` for ``RESUMED`` reconnect
  dispatches, where ``data`` is a string.

The plugin already patches ``Adapter.payload_to_event``
(``early_inject.force_payload_mapping_guard``); these cases pin the pure
normalization helpers that patch applies, without importing nonebot.
"""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "nonebot_plugin_xiuxian_2"
    / "xiuxian"
    / "xiuxian_adapter"
    / "early_inject.py"
)
SPEC = importlib.util.spec_from_file_location("xiuxian_early_inject", MODULE_PATH)
if SPEC is None or SPEC.loader is None:  # pragma: no cover
    raise RuntimeError(f"Unable to load {MODULE_PATH}")
early_inject = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(early_inject)


class _Payload:
    """Minimal stand-in for ``nonebot.adapters.qq.event.Dispatch``."""

    def __init__(self, data, payload_id="p1"):
        self.data = data
        self.id = payload_id
        self.copies = 0

    def model_copy(self, update=None):
        self.copies += 1
        clone = _Payload(dict(self.data) if isinstance(self.data, dict) else self.data, self.id)
        for key, value in (update or {}).items():
            setattr(clone, key, value)
        return clone


class NormalizePayloadDataTests(unittest.TestCase):
    def test_string_and_non_mapping_data_become_empty_mapping(self) -> None:
        for data in ("", "RESUMED", None, 42, ["x"]):
            self.assertEqual({}, early_inject.normalize_payload_data(data), data)

    def test_payload_without_msg_elements_is_returned_unchanged(self) -> None:
        data = {"content": "hi", "author": {"id": "u"}}

        self.assertEqual(data, early_inject.normalize_payload_data(data))

    def test_reference_elements_get_the_required_defaults(self) -> None:
        data = {
            "content": "看看",
            "msg_elements": [{"content": "=== 消息 1 ===\n[消息内容] 文生图"}],
        }

        normalized = early_inject.normalize_payload_data(data)

        element = normalized["msg_elements"][0]
        self.assertEqual(0, element["message_type"])
        self.assertEqual("", element["msg_idx"])
        self.assertEqual("=== 消息 1 ===\n[消息内容] 文生图", element["content"])
        # The original mapping is not mutated in place.
        self.assertNotIn("message_type", data["msg_elements"][0])

    def test_complete_elements_keep_their_values(self) -> None:
        data = {
            "msg_elements": [
                {"content": "x", "message_type": 103, "msg_idx": "REFIDX_1", "author": {"bot": True}}
            ]
        }

        normalized = early_inject.normalize_payload_data(data)

        self.assertEqual(data["msg_elements"], normalized["msg_elements"])

    def test_only_missing_fields_are_filled(self) -> None:
        data = {"msg_elements": [{"content": "x", "message_type": 7}]}

        element = early_inject.normalize_payload_data(data)["msg_elements"][0]

        self.assertEqual(7, element["message_type"])
        self.assertEqual("", element["msg_idx"])

    def test_non_dict_entries_and_non_list_msg_elements_are_left_alone(self) -> None:
        self.assertEqual(
            {"msg_elements": ["raw", 3]},
            early_inject.normalize_payload_data({"msg_elements": ["raw", 3]}),
        )
        self.assertEqual(
            {"msg_elements": "not-a-list"},
            early_inject.normalize_payload_data({"msg_elements": "not-a-list"}),
        )


class PayloadGuardTests(unittest.TestCase):
    def test_string_dispatch_is_rewritten_before_the_adapter_sees_it(self) -> None:
        seen = {}

        def original(payload):
            seen["data"] = payload.data
            return "event"

        payload = _Payload("")

        self.assertEqual("event", early_inject._payload_to_event_with_guard(original, payload))
        self.assertEqual({}, seen["data"])
        self.assertEqual(1, payload.copies)

    def test_reference_payload_reaches_the_adapter_normalized(self) -> None:
        seen = {}

        def original(payload):
            seen["elements"] = payload.data["msg_elements"]
            return "event"

        payload = _Payload({"msg_elements": [{"content": "quoted"}]})

        early_inject._payload_to_event_with_guard(original, payload)

        self.assertEqual(
            [{"content": "quoted", "message_type": 0, "msg_idx": ""}],
            seen["elements"],
        )

    def test_clean_payload_is_passed_through_without_copying(self) -> None:
        def original(payload):
            return payload

        payload = _Payload({"content": "hi", "msg_elements": [
            {"content": "x", "message_type": 1, "msg_idx": "i"}
        ]})

        self.assertIs(payload, early_inject._payload_to_event_with_guard(original, payload))
        self.assertEqual(0, payload.copies)


if __name__ == "__main__":
    unittest.main()
