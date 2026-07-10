from __future__ import annotations

import argparse
import asyncio
import json
import sys
import types
from pathlib import Path
from typing import Any
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_PATH = ROOT / "tests" / "fixtures" / "qq_adapter_contract_events.json"


def _install_package_stubs() -> None:
    package_paths = {
        "nonebot_plugin_xiuxian_2": ROOT / "nonebot_plugin_xiuxian_2",
        "nonebot_plugin_xiuxian_2.xiuxian": (
            ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian"
        ),
    }
    for name, path in package_paths.items():
        package = types.ModuleType(name)
        package.__package__ = name
        package.__path__ = [str(path)]
        sys.modules[name] = package


class _SendRouter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def send_to_group(self, **kwargs: Any) -> dict[str, str]:
        self.calls.append(("group", kwargs))
        return {"id": "sent-group"}

    async def send_to_c2c(self, **kwargs: Any) -> dict[str, str]:
        self.calls.append(("c2c", kwargs))
        return {"id": "sent-c2c"}

    async def send_to_channel(self, **kwargs: Any) -> dict[str, str]:
        self.calls.append(("channel", kwargs))
        return {"id": "sent-channel"}

    async def send_to_dms(self, **kwargs: Any) -> dict[str, str]:
        self.calls.append(("dms", kwargs))
        return {"id": "sent-dms"}


def _context_snapshot(context: Any) -> dict[str, Any]:
    return {
        "scene": context.scene,
        "user_id": context.user_id,
        "raw_user_id": context.raw_user_id,
        "group_id": context.group_id,
        "guild_id": context.guild_id,
        "channel_id": context.channel_id,
        "message_id": context.message_id,
        "event_id": context.event_id,
        "reference_id": context.reference_id,
        "attachment_names": [item.filename for item in context.attachments],
    }


async def _run(source: str) -> dict[str, Any]:
    _install_package_stubs()

    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_adapter.diagnostics import (
        get_adapter_diagnostics,
    )

    diagnostics = get_adapter_diagnostics(source)

    from nonebot.adapters.qq import Bot
    from nonebot.adapters.qq.event import (
        C2CMessageCreateEvent,
        GroupAddRobotEvent,
        GroupMessageCreateEvent,
        InteractionCreateEvent,
        MessageCreateEvent,
    )
    from nonebot_plugin_xiuxian_2.xiuxian.qq_compat import (
        from_nonebot_event,
        get_interaction_context,
        get_lifecycle_context,
    )

    fixtures = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    events = {
        "group": GroupMessageCreateEvent(**fixtures["group"]),
        "c2c": C2CMessageCreateEvent(**fixtures["c2c"]),
        "channel": MessageCreateEvent(**fixtures["channel"]),
        "interaction": InteractionCreateEvent(**fixtures["interaction"]),
        "lifecycle": GroupAddRobotEvent(**fixtures["lifecycle"]),
    }

    contexts = {
        name: _context_snapshot(from_nonebot_event(event))
        for name, event in events.items()
    }
    interaction = get_interaction_context(events["interaction"])
    lifecycle = get_lifecycle_context(events["lifecycle"])

    router = _SendRouter()
    results = {
        name: await Bot.send(router, event, "contract reply")
        for name, event in events.items()
    }
    send_calls = [
        {
            "route": route,
            "target": (
                kwargs.get("group_openid")
                or kwargs.get("openid")
                or kwargs.get("channel_id")
                or kwargs.get("guild_id")
            ),
            "msg_id": kwargs.get("msg_id"),
            "event_id": kwargs.get("event_id"),
            "msg_seq": kwargs.get("msg_seq"),
            "msg_ref_id": unquote(str(kwargs.get("msg_ref_id") or "")) or None,
        }
        for route, kwargs in router.calls
    ]

    return {
        "diagnostics": diagnostics,
        "contexts": contexts,
        "interaction": {
            "interaction_id": interaction.interaction_id,
            "user_id": interaction.user_id,
            "message_id": interaction.message_id,
            "button_id": interaction.button_id,
            "button_data": interaction.button_data,
        },
        "lifecycle": {
            "action": lifecycle.action,
            "user_id": lifecycle.user_id,
            "group_id": lifecycle.group_id,
        },
        "send_results": results,
        "send_calls": send_calls,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", choices=("vendor", "installed", "auto"))
    args = parser.parse_args()
    print(json.dumps(asyncio.run(_run(args.source)), sort_keys=True))


if __name__ == "__main__":
    main()
