"""Concurrent live matcher dispatch with one shared transport patch lifetime."""
import asyncio
from contextlib import AsyncExitStack
from contextvars import ContextVar
import traceback
from unittest.mock import patch


async def concurrent_commands(entry, requests):
    import nonebot
    from nonebot.adapters.onebot.v11 import Adapter, Bot
    from nonebot.exception import StopPropagation
    from nonebot.log import logger
    from nonebot.matcher import matchers
    from nonebot.message import _check_matcher, check_and_run_matcher
    from nonebot.rule import TrieRule
    from nonebot_plugin_xiuxian_2.infrastructure.messaging import MessageGatewayAdapter
    from nonebot_plugin_xiuxian_2.xiuxian.on_compat import _CURRENT_EVENT
    from tests.command_probe import _boundary_denials, make_event, ProbeBlocked
    from tests.parameter_exception_watch import watch_handler_catches

    active = ContextVar("concurrent_command_capture")
    errors, blocked, results = [], [], []
    bot = Bot(Adapter(nonebot.get_driver()), "900000")

    async def transport(self, api, **data):
        row = active.get()
        if api.startswith("send_"):
            row["replies"].append(str(data.get("message", data)))
            return {"message_id": row["number"]}
        if api in {"get_group_info", "get_group_member_info", "get_stranger_info"}:
            return {"group_id": 900100, "user_id": int(row["actor"]), "nickname": "诊断道友",
                    "card": "", "role": "owner", "group_name": "诊断群"}
        raise ProbeBlocked(f"Bot API fixture missing: {api}")

    gateway = MessageGatewayAdapter.send

    async def checked_gateway(self, context, reply):
        outcome = await gateway(self, context, reply)
        if not outcome.ok:
            errors.append(f"message gateway rejected delivery: {outcome.error_code}")
        return outcome

    def capture(message):
        record = message.record
        if record["exception"] or record["level"].no >= 40:
            errors.append(str(record["message"]) + str(record["exception"] or ""))

    async def dispatch(text, actor, number):
        row = {"input": text, "actor": actor, "number": number, "replies": []}
        event = make_event(text, actor, number)
        token, boundary = _CURRENT_EVENT.set(event), _boundary_denials.set(blocked)
        current = active.set(row)
        try:
            state = {}
            TrieRule.get_value(bot, event, state)
            assert entry["matcher"] in matchers[entry["matcher"].priority]
            async with AsyncExitStack() as stack:
                assert await _check_matcher(entry["matcher"], bot, event, state, stack, {})
                try:
                    await check_and_run_matcher(entry["matcher"], bot, event, state, stack, {})
                except StopPropagation:
                    pass
        except Exception:
            errors.append(traceback.format_exc())
        finally:
            active.reset(current)
            _CURRENT_EVENT.reset(token)
            _boundary_denials.reset(boundary)
        results.append(row)

    sink = logger.add(capture, level="WARNING")
    before = asyncio.all_tasks()
    try:
        with patch.object(Bot, "call_api", transport), patch.object(MessageGatewayAdapter, "send", checked_gateway), \
                watch_handler_catches(entry) as caught:
            await asyncio.wait_for(asyncio.gather(*(dispatch(*request) for request in requests)), timeout=8)
            pending = asyncio.all_tasks() - before
            if pending:
                _, unfinished = await asyncio.wait(pending, timeout=0.5)
                for task in unfinished:
                    task.cancel()
                if unfinished:
                    await asyncio.gather(*unfinished, return_exceptions=True)
                    blocked.append("background dependency did not finish")
            assert not caught, caught
    finally:
        logger.remove(sink)
    assert not errors and not blocked, (errors, blocked)
    assert len(results) == len(requests) and all(row["replies"] for row in results), results
    return results
