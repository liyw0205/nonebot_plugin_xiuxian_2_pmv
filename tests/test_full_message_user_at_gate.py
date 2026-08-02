"""全量消息：leading @用户 且未 @ 本 BOT 时忽略。"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]


class Seg:
    def __init__(self, type_: str, **data):
        self.type = type_
        self.data = data


class Msg(list):
    def extract_plain_text(self):
        parts = []
        for s in self:
            if getattr(s, "type", "") == "text":
                parts.append(str((s.data or {}).get("text") or ""))
        return "".join(parts)


def _load_mod():
    import nonebot

    try:
        nonebot.get_driver()
    except ValueError:
        nonebot.init()
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    name = "nonebot_plugin_xiuxian_2.xiuxian"
    if name in sys.modules:
        return importlib.reload(sys.modules[name])
    return importlib.import_module(name)


def _bot():
    return SimpleNamespace(
        self_id="BOT_SELF",
        bot_info=SimpleNamespace(id="BOT_SELF"),
        self_info=SimpleNamespace(id="BOT_SELF", user_id="BOT_SELF"),
    )


def _event(message, *, to_me=False, event_name="GROUP_MESSAGE_CREATE"):
    ev = SimpleNamespace(
        to_me=to_me,
        type=event_name,
        __type__=event_name,
        mentions=[],
        message=message,
    )

    def get_event_name():
        return event_name

    def get_message():
        return message

    ev.get_event_name = get_event_name
    ev.get_message = get_message
    return ev


def test_ignore_leading_user_at_chat_without_self():
    mod = _load_mod()
    bot = _bot()
    # @猎孤_惘 早不买晚不买…
    msg = Msg(
        [
            Seg("group_mention_user", user_id="USER_A", bot=False, is_you=False),
            Seg("text", text=" 早不买晚不买这个月开始就没了"),
        ]
    )
    assert mod._is_leading_other_user_at_without_self(bot, _event(msg)) is True


def test_ignore_leading_user_at_command_without_self():
    mod = _load_mod()
    bot = _bot()
    # @用户 双修
    msg = Msg(
        [
            Seg("mention_user", user_id="USER_A"),
            Seg("text", text=" 双修"),
        ]
    )
    assert mod._is_leading_other_user_at_without_self(bot, _event(msg)) is True


def test_allow_user_at_then_bot():
    mod = _load_mod()
    bot = _bot()
    # @用户 双修@BOT
    msg = Msg(
        [
            Seg("group_mention_user", user_id="USER_A", bot=False, is_you=False),
            Seg("text", text=" 双修"),
            Seg("group_mention_user", user_id="BOT_SELF", bot=True, is_you=True),
        ]
    )
    assert mod._is_leading_other_user_at_without_self(bot, _event(msg)) is False


def test_allow_bot_first_then_user():
    mod = _load_mod()
    bot = _bot()
    # @BOT 双修@用户 / @BOT 刷新@用户
    msg = Msg(
        [
            Seg("group_mention_user", user_id="BOT_SELF", bot=True, is_you=True),
            Seg("text", text=" 双修"),
            Seg("group_mention_user", user_id="USER_A", bot=False, is_you=False),
        ]
    )
    assert mod._is_leading_other_user_at_without_self(bot, _event(msg)) is False


def test_allow_command_then_user_at():
    mod = _load_mod()
    bot = _bot()
    # 双修 @用户
    msg = Msg(
        [
            Seg("text", text="双修 "),
            Seg("mention_user", user_id="USER_A"),
        ]
    )
    assert mod._is_leading_other_user_at_without_self(bot, _event(msg)) is False


def test_allow_when_to_me_true():
    mod = _load_mod()
    bot = _bot()
    msg = Msg(
        [
            Seg("group_mention_user", user_id="USER_A", bot=False, is_you=False),
            Seg("text", text=" 双修"),
        ]
    )
    assert (
        mod._is_leading_other_user_at_without_self(bot, _event(msg, to_me=True))
        is False
    )


def test_non_full_message_event_not_gated():
    mod = _load_mod()
    bot = _bot()
    msg = Msg(
        [
            Seg("group_mention_user", user_id="USER_A", bot=False, is_you=False),
            Seg("text", text=" 双修"),
        ]
    )
    # 非全量艾特通道不走此闸
    assert (
        mod._is_leading_other_user_at_without_self(
            bot, _event(msg, event_name="GROUP_AT_MESSAGE_CREATE")
        )
        is False
    )
