"""NoneBot command wiring for migrated vertical slices.

The handlers in this module deliberately contain only event parsing, use-case
dispatch and reply delivery.  They are installed once per driver by the
composition root; application services remain transport agnostic.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

from nonebot.adapters import Bot, Event, Message

from ...core.errors import DomainError
from ...core.result import ReplyPlan
from .context import CommandContext, context_from_event


def _plain_text(value: Any) -> str:
    if value is None:
        return ""
    extractor = getattr(value, "extract_plain_text", None)
    if callable(extractor):
        try:
            return str(extractor() or "").strip()
        except Exception:
            pass
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (list, tuple)):
        parts: list[str] = []
        for item in value:
            data = getattr(item, "data", None)
            if isinstance(data, dict) and data.get("text") is not None:
                parts.append(str(data["text"]))
            elif str(getattr(item, "type", "")) in {"at", "mention"}:
                continue
            else:
                parts.append(str(item))
        return "".join(parts).strip()
    return str(value).strip()


def _message_id(event: Any, user_id: str, prefix: str) -> str:
    event_id = str(
        getattr(event, "message_id", "")
        or getattr(event, "id", "")
        or getattr(event, "event_id", "")
        or ""
    ).strip()
    return f"{prefix}:{event_id}:{user_id}" if event_id else f"{prefix}:{user_id}"


def _at_user_id(value: Any) -> str | None:
    values = value if isinstance(value, (list, tuple)) else ()
    for item in values:
        typ = str(getattr(item, "type", "") or "").casefold()
        data = getattr(item, "data", None)
        if typ in {"at", "mention", "mention_user"} and isinstance(data, dict):
            for key in ("qq", "user_id", "id", "user_openid", "openid"):
                if data.get(key):
                    return str(data[key])
    return None


def _runtime(holder: dict[str, Any]) -> Any:
    context = holder.get("context")
    if context is None or not getattr(context, "services", None):
        raise RuntimeError("xiuxian runtime is not ready")
    return context


async def _send(holder: dict[str, Any], event: Any, bot: Any, plan: ReplyPlan) -> Any:
    context = context_from_event(event, bot=bot)
    runtime = _runtime(holder)
    gateway = getattr(runtime, "message_gateway", None)
    if gateway is None:
        raise RuntimeError("message gateway is not configured")
    return await gateway.send(context, plan)


async def _dispatch(
    holder: dict[str, Any],
    event: Any,
    bot: Any,
    feature: str,
    build: Callable[[CommandContext, Any], ReplyPlan],
) -> Any:
    context = context_from_event(event, bot=bot)
    try:
        runtime = _runtime(holder)
        plan = build(context, runtime.services[feature])
    except DomainError as exc:
        plan = ReplyPlan(exc.message, reference=True)
    except (KeyError, RuntimeError) as exc:
        plan = ReplyPlan(str(exc), reference=True)
    except Exception:
        plan = ReplyPlan("当前服务暂不可用，请稍后重试。", reference=True)
    return await _send(holder, event, bot, plan)


def _build_daily(context: CommandContext, application: Any) -> ReplyPlan:
    from ...features.daily_fortune.commands import handle_daily_fortune

    return handle_daily_fortune(context, application)


def _build_sign(context: CommandContext, application: Any) -> ReplyPlan:
    from ...features.sign_in.commands import handle_sign_in

    return handle_sign_in(
        application,
        user_id=context.user_id,
        operation_id=_message_id(context.raw_event, context.user_id, "sign"),
        lower_limit=int(getattr(application, "lower_limit", 100000)),
        upper_limit=int(getattr(application, "upper_limit", 500000)),
    )


def _stone_arguments(args: Any) -> tuple[str, int] | tuple[None, None]:
    text = _plain_text(args)
    tokens = text.split()
    if len(tokens) < 2:
        return None, None
    amount_token = tokens[-1]
    if not re.fullmatch(r"\d+", amount_token):
        return None, None
    return " ".join(tokens[:-1]).strip(), int(amount_token)


def _build_stone(context: CommandContext, application: Any, args: Any) -> ReplyPlan:
    target, amount = _stone_arguments(args)
    if target is None or amount is None:
        return ReplyPlan("请输入正确的指令，例如：送灵石 少姜 600000", reference=True)
    recipient_id = _at_user_id(args) or target.lstrip("@")
    recipient = application.resolve_user(recipient_id)
    if recipient is None:
        return ReplyPlan("对方未踏入修仙界，不可赠送！", reference=True)
    recipient_id = str(recipient["user_id"])
    plan = application.reply(
        operation_id=_message_id(context.raw_event, context.user_id, "stone-gift"),
        sender_id=context.user_id,
        recipient_id=recipient_id,
        gross_amount=amount,
    )
    return plan


def register_migrated_matchers(driver: Any, holder: dict[str, Any]) -> tuple[type[Any], ...]:
    """Register migrated matchers once and return their matcher classes."""

    marker = "_xiuxian_migrated_matchers"
    existing = getattr(driver, marker, None)
    if existing is not None:
        return existing

    from nonebot import on_command
    from nonebot.params import CommandArg

    daily = on_command(
        "今日运势",
        aliases={"占卜", "卜卦", "求签", "运势", "算命"},
        priority=1,
        block=True,
    )
    sign = on_command("修仙签到", aliases={"签到"}, priority=1, block=True)
    stone = on_command("送灵石", priority=1, block=True)

    @daily.handle()
    async def _daily_handler(bot: Bot, event: Event):
        return await _dispatch(holder, event, bot, "daily_fortune", _build_daily)

    @sign.handle()
    async def _sign_handler(bot: Bot, event: Event):
        return await _dispatch(holder, event, bot, "sign_in", _build_sign)

    @stone.handle()
    async def _stone_handler(bot: Bot, event: Event, args: Message = CommandArg()):
        context = context_from_event(event, bot=bot)
        try:
            runtime = _runtime(holder)
            plan = _build_stone(context, runtime.services["stone_gift"], args)
        except DomainError as exc:
            plan = ReplyPlan(exc.message, reference=True)
        except (KeyError, RuntimeError) as exc:
            plan = ReplyPlan(str(exc), reference=True)
        except Exception:
            plan = ReplyPlan("当前服务暂不可用，请稍后重试。", reference=True)
        return await _send(holder, event, bot, plan)

    result = (daily, sign, stone)
    setattr(driver, marker, result)
    return result


__all__ = ["register_migrated_matchers"]
