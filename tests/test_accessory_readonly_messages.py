from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back import accessory
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back import accessory_helpers
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils


def _fixture_data():
    instances = []
    for index, item_id in enumerate((19001, 19002, 19003, 19004), 1):
        item = accessory.items.get_data_by_item_id(item_id)
        instances.append(
            {
                "uid": f"adapter-accessory-{index}",
                "item_id": item_id,
                "name": item["name"],
                "part": item["part"],
                "set_type": item["set_type"],
                "quality": 1,
                "affixes": [],
                "locked_affixes": [],
                "wash_count": 0,
            }
        )
    return {
        "equipped": {"手镯": instances[0], "戒指": None, "手环": None, "项链": None},
        "bag": instances[1:],
    }


def _capture_route(matcher, text: str):
    event = SimpleNamespace(get_user_id=lambda: "900001")
    bot = SimpleNamespace(self_id="bot", adapter=SimpleNamespace(get_name=lambda: "QQ"))
    config = SimpleNamespace(markdown_status=True)
    sent = AsyncMock()
    args = SimpleNamespace(extract_plain_text=lambda: text)
    with (
        patch.object(accessory, "check_user", return_value=(True, {"user_id": "900001", "user_name": "道友"}, "")),
        patch.object(accessory, "_get_data", return_value=_fixture_data()),
        patch.object(accessory_helpers, "_get_data", return_value=_fixture_data()),
        patch.object(accessory, "XiuConfig", return_value=config),
        patch.object(accessory, "handle_send", sent),
    ):
        asyncio.run(matcher.handlers[0].call(bot, event, args))
    return event, bot, sent


def _delivery_config():
    return SimpleNamespace(
        markdown_status=True,
        markdown_id="",
        markdown_id2="",
        markdown_button_status=False,
        at_sender=False,
        message_optimization=False,
        img=False,
    )


def test_accessory_queries_keep_onebot_qq_and_fallback_content():
    bag_event, _, bag_sent = _capture_route(accessory.accessory_bag, "")
    bag_reply = bag_sent.call_args.args[2]
    bag_kwargs = bag_sent.call_args.kwargs
    assert bag_kwargs["native_markdown"] is True
    assert "adapter-accessory-1" in bag_kwargs["fallback_msg"]

    event = bag_event
    plain = AsyncMock()
    with (
        patch.object(utils, "XiuConfig", return_value=_delivery_config()),
        patch.object(utils, "get_chat_scene", return_value="group"),
        patch.object(utils, "is_group_event", return_value=True),
        patch.object(utils, "_send_event_message", plain),
    ):
        asyncio.run(
            utils.handle_send(
                SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11")),
                event,
                bag_reply,
                **bag_kwargs,
            )
        )
    assert "烈阳手镯" in str(plain.call_args.kwargs["message"])

    markdown = AsyncMock()
    with (
        patch.object(utils, "XiuConfig", return_value=_delivery_config()),
        patch.object(utils, "get_chat_scene", return_value="group"),
        patch.object(utils, "is_group_event", return_value=True),
        patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="open-id")),
        patch.object(utils, "_send_markdown_or_keyboard", markdown),
    ):
        asyncio.run(
            utils.handle_send(
                SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ")),
                event,
                bag_reply,
                **bag_kwargs,
            )
        )
    assert "烈阳手镯" in markdown.call_args.args[2]
    assert "adapter-accessory-1" in markdown.call_args.args[2]

    failed_markdown = AsyncMock(side_effect=RuntimeError("isolated accessory markdown failure"))
    fallback = AsyncMock()
    with (
        patch.object(utils, "XiuConfig", return_value=_delivery_config()),
        patch.object(utils, "get_chat_scene", return_value="group"),
        patch.object(utils, "is_group_event", return_value=True),
        patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="open-id")),
        patch.object(utils, "_send_markdown_or_keyboard", failed_markdown),
        patch.object(utils, "_send_event_message", fallback),
    ):
        asyncio.run(
            utils.handle_send(
                SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ")),
                event,
                bag_reply,
                **bag_kwargs,
            )
        )
    assert "烈阳手镯" in str(fallback.call_args.kwargs["message"])

    collection_event, _, collection_sent = _capture_route(
        accessory.accessory_collection, "烈阳"
    )
    collection_reply = collection_sent.call_args.args[2]
    assert "【烈阳套装图鉴】" in collection_reply
    assert "已装备1/4件" in collection_reply
    assert collection_sent.call_args.kwargs["md_type"] == "背包"

    collection_markdown = AsyncMock()
    with (
        patch.object(utils, "XiuConfig", return_value=_delivery_config()),
        patch.object(utils, "get_chat_scene", return_value="group"),
        patch.object(utils, "is_group_event", return_value=True),
        patch.object(utils, "handle_send_markdown_type", collection_markdown),
    ):
        asyncio.run(
            utils.handle_send(
                SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ")),
                collection_event,
                collection_reply,
                **collection_sent.call_args.kwargs,
            )
        )
    assert "已装备1/4件" in collection_markdown.call_args.args[2]
