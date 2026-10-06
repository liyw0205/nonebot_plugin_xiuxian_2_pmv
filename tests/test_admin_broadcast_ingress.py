from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from .test_admin_broadcast import ROOT, _bot, _load, runtime


class PrivateEvent(SimpleNamespace):
    pass


class GroupEvent(SimpleNamespace):
    pass


class IgnoredMessage(Exception):
    pass


def _event(scene):
    event_type = PrivateEvent if scene == "private" else GroupEvent
    return event_type(scene=scene, group_id="ingress-group", user_id="ingress-user",
                      message_id="ingress-reply")


def _ingress(runtime, **settings):
    patch_port = AsyncMock(wraps=runtime.namespace["auto_patch_broadcast_for_event"])
    namespace = dict(
        runtime.namespace,
        PrivateMessageEvent=PrivateEvent,
        GroupMessageEvent=GroupEvent,
        IgnoredException=IgnoredMessage,
        is_lifecycle_event=lambda event: False,
        _is_other_bot_at_message=lambda bot, event: False,
        _is_leading_other_user_at_without_self=lambda bot, event: False,
        _normalize_qq_group_at_message=lambda bot, event: False,
        _check_command_ingress_rate_limit=AsyncMock(),
        patch_context=lambda bot, event: (bot, event),
        auto_patch_broadcast_for_event=patch_port,
        put_bot=["bot-a"],
        shield_private=False,
        shield_group=[],
        response_group=False,
    )
    namespace.update(settings)
    _load(ROOT / "__init__.py", ("do_something",), namespace)
    return namespace, patch_port


@pytest.mark.parametrize("scene", ["group", "private"])
def test_normal_message_ingress_reaches_real_broadcast_owner(runtime, scene):
    asyncio.run(runtime.namespace["start_broadcast"](runtime.bot, "global", "notice"))
    runtime.delivery.send.assert_not_awaited()
    namespace, patch_port = _ingress(runtime)
    event = _event(scene)

    asyncio.run(namespace["do_something"](runtime.bot, event))

    patch_port.assert_awaited_once_with(runtime.bot, event)
    request = runtime.delivery.send.call_args.args[1]
    assert request.scene == scene and request.source_message_id == "ingress-reply"
    assert request.target_id == ("ingress-group" if scene == "group" else "ingress-user")
    task = runtime.app.status()[0]
    assert task["sent_groups"] == ({"group:ingress-group"} if scene == "group" else set())
    assert task["sent_users"] == ({"private:ingress-user"} if scene == "private" else set())


@pytest.mark.parametrize("scene,settings", [
    ("private", {"shield_private": True}),
    ("group", {"shield_group": ["ingress-group"], "response_group": False}),
    ("group", {"shield_group": ["another-group"], "response_group": True}),
])
def test_shields_reject_messages_before_broadcast_port(runtime, scene, settings):
    asyncio.run(runtime.namespace["start_broadcast"](runtime.bot, "global", "notice"))
    before = runtime.app.status()
    namespace, patch_port = _ingress(runtime, **settings)

    with pytest.raises(IgnoredMessage):
        asyncio.run(namespace["do_something"](runtime.bot, _event(scene)))

    patch_port.assert_not_awaited()
    runtime.delivery.send.assert_not_awaited()
    assert runtime.app.status() == before


def test_response_group_allowlist_accepts_configured_group(runtime):
    asyncio.run(runtime.namespace["start_broadcast"](runtime.bot, "group", "notice"))
    namespace, patch_port = _ingress(
        runtime, shield_group=["ingress-group"], response_group=True,
    )
    event = _event("group")

    asyncio.run(namespace["do_something"](runtime.bot, event))

    patch_port.assert_awaited_once_with(runtime.bot, event)
    assert runtime.app.status()[0]["sent_groups"] == {"group:ingress-group"}


@pytest.mark.parametrize("configured_bots", [["bot-a"], []])
def test_unconfigured_bot_never_enters_broadcast_port(runtime, configured_bots):
    asyncio.run(runtime.namespace["start_broadcast"](runtime.bot, "group", "notice"))
    namespace, patch_port = _ingress(runtime, put_bot=configured_bots)

    asyncio.run(namespace["do_something"](_bot(bot_id="bot-b"), _event("group")))

    patch_port.assert_not_awaited()
    runtime.delivery.send.assert_not_awaited()
    assert runtime.app.status()[0]["known_groups"] == set()


def test_message_addressed_to_other_bot_is_rejected_before_broadcast_port(runtime):
    asyncio.run(runtime.namespace["start_broadcast"](runtime.bot, "group", "notice"))
    namespace, patch_port = _ingress(
        runtime, _is_other_bot_at_message=lambda bot, event: True,
    )

    with pytest.raises(IgnoredMessage):
        asyncio.run(namespace["do_something"](runtime.bot, _event("group")))

    patch_port.assert_not_awaited()
    runtime.delivery.send.assert_not_awaited()
    namespace["_check_command_ingress_rate_limit"].assert_not_awaited()
