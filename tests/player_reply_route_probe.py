"""Route representative clickable player commands through the isolated live registry."""

from __future__ import annotations

import asyncio
import json
from contextlib import AsyncExitStack

import tests
from tests.command_probe import install_boundary, inventory, isolate_source, make_event, prepare


ROUTES = (
    ("修仙帮助", "修仙帮助"),
    ("副本帮助", "副本帮助"),
    ("我的修仙信息", "我的修仙信息"),
    ("修仙改名", "修仙改名"),
    ("确认使用", "确认使用"),
    ("我的背包 灵石 2", "我的背包"),
    ("查看效果 破境丹", "查看效果"),
    ("探索副本", "探索副本"),
    ("副本信息", "副本信息"),
    ("活动商店", "活动商店"),
    ("活动购买 灵石补给", "活动购买"),
    ("活动兑换 福运绵长", "活动兑换"),
)


async def main() -> None:
    isolate_source()
    install_boundary(tests._data_dir)
    await prepare()

    import nonebot
    from nonebot.adapters.onebot.v11 import Adapter, Bot
    from nonebot.matcher import matchers
    from nonebot.message import _check_matcher
    from nonebot.rule import TrieRule
    from nonebot_plugin_xiuxian_2.xiuxian.on_compat import _CURRENT_EVENT, _route_event

    entries = inventory()
    bot = Bot(Adapter(nonebot.get_driver()), "900000")
    verified = []
    for index, (text, expected_command) in enumerate(ROUTES, 1):
        entry = next(
            item
            for item in entries
            if item["command"] == expected_command or expected_command in item["aliases"]
        )
        event = make_event(text, "900001", 910000 + index)
        token = _CURRENT_EVENT.set(event)
        try:
            state = {}
            TrieRule.get_value(bot, event, state)
            matcher = entry["matcher"]
            if matcher not in matchers[matcher.priority]:
                raise AssertionError(
                    f"click target routed elsewhere: {text!r}; route={_route_event(event)!r}"
                )
            async with AsyncExitStack() as stack:
                if not await _check_matcher(matcher, bot, event, state, stack, {}):
                    raise AssertionError(f"matcher rejected click target: {text!r}")
            verified.append({"input": text, "command": expected_command})
        finally:
            _CURRENT_EVENT.reset(token)

    print(json.dumps({"status": "pass", "routes": verified}, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
