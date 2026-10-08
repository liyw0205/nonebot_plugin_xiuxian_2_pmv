from __future__ import annotations

from pathlib import Path


SOURCE = Path(
    "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_buff/__init__.py"
)


def _closing_handler_source() -> str:
    source = SOURCE.read_text(encoding="utf-8")
    start = source.index("async def in_closing_")
    end = source.index("@out_closing.handle", start)
    return source[start:end]


def test_closing_enter_handler_uses_application_boundary() -> None:
    handler = _closing_handler_source()

    assert "buff_application.closing_enter(" in handler
    assert "operation_id=_closing_enter_operation_id(event, user_id)" in handler
    assert "started_at=runtime_clock.now().strftime" in handler
    assert "_sql_message().in_closing(" not in handler
    assert "check_user_type(" not in handler
    assert "update_statistics_value(" not in handler


def test_closing_enter_adapter_keeps_replay_and_eligibility_replies() -> None:
    handler = _closing_handler_source()

    assert 'if result_status == "ineligible"' in handler
    assert 'if result_status == "duplicate" or result.replayed' in handler
    assert "进入闭关状态，如需出关，发送【出关】！" in handler
    assert "该闭关请求已经处理，无需重复提交。" in handler
    assert "凡人无法闭关！" in handler
    assert "if result.ok:" in handler


def test_closing_enter_operation_id_is_event_stable_and_namespaced() -> None:
    source = SOURCE.read_text(encoding="utf-8")
    start = source.index("def _closing_enter_operation_id(")
    end = source.index("def _normal_training_operation_id(", start)
    helper = source[start:end]

    assert "getattr(event, \"message_id\", \"\")" in helper
    assert 'return f"buff-closing-enter:{event_id}:{user_id}"' in helper
    assert 'return f"buff-closing-enter:{user_id}:{runtime_ids.new_id()}"' in helper
