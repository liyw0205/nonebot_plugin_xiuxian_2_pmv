"""NoneBot command wiring for migrated vertical slices.

The handlers in this module deliberately contain only event parsing, use-case
dispatch and reply delivery.  They are installed once per driver by the
composition root; application services remain transport agnostic.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import date, timedelta
from typing import Any

from nonebot.adapters import Bot, Event, Message
from nonebot.exception import MatcherException, ProcessException

from ...core.errors import DomainError
from ...core.result import ReplyPlan
from ...xiuxian.command_guard import report_exception, result_failure, failure_reply
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
    except (MatcherException, ProcessException):
        raise
    except Exception as exc:
        action = {"sign_in": "签到", "daily_fortune": "每日运势", "stone_gift": "赠送灵石", "impart": "传承", "pet": "宠物", "bank": "银行"}.get(feature, "修仙命令")
        plan = ReplyPlan(report_exception(action, exc, phase="业务处理"), reference=True)
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


def _makeup_business_date(args: Any, today: date) -> str:
    """空参数→昨天；`D`→本月第 D 天；其余原样交给应用层校验。"""

    text = _plain_text(args)
    if not text or text in {"昨天", "昨日"}:
        return (today - timedelta(days=1)).isoformat()
    if re.fullmatch(r"\d{1,2}", text):
        try:
            return date(today.year, today.month, int(text)).isoformat()
        except ValueError:
            return text
    return text


def _build_sign_makeup(
    context: CommandContext, application: Any, args: Any
) -> ReplyPlan:
    from ...features.sign_in.clock import sign_in_clock
    from ...features.sign_in.commands import handle_sign_in_makeup

    today = sign_in_clock().now().date()
    return handle_sign_in_makeup(
        application,
        user_id=context.user_id,
        operation_id=_message_id(context.raw_event, context.user_id, "sign-makeup"),
        business_date=_makeup_business_date(args, today),
        cost=int(getattr(application, "makeup_cost", 100000)),
    )


def _stone_arguments(args: Any) -> tuple[str, int] | tuple[None, None]:
    text = _plain_text(args)
    tokens = text.split()
    mentioned = _at_user_id(args)
    if mentioned and len(tokens) == 1 and re.fullmatch(r"[0-9]+", tokens[0]):
        return mentioned, int(tokens[0])
    if len(tokens) < 2:
        return None, None
    amount_token = tokens[-1]
    if not re.fullmatch(r"\d+", amount_token):
        return None, None
    return " ".join(tokens[:-1]).strip(), int(amount_token)


def _build_stone(context: CommandContext, application: Any, args: Any) -> ReplyPlan:
    from ...features.stone_gift.commands import handle_stone_gift

    target, amount = _stone_arguments(args)
    if target is None or amount is None:
        return ReplyPlan("请输入正确的指令，例如：送灵石 少姜 600000", reference=True)
    if amount <= 0:
        return ReplyPlan("赠送数量必须大于0！", reference=True)
    recipient_id = _at_user_id(args) or target.lstrip("@")
    sender = application.resolve_user(context.user_id)
    recipient = application.resolve_user(recipient_id)
    if sender is None:
        return ReplyPlan("请先开始修仙。", reference=True)
    if recipient is None:
        return ReplyPlan("对方未踏入修仙界，不可赠送！", reference=True)
    recipient_id = str(recipient["user_id"])
    if recipient_id == context.user_id:
        return ReplyPlan("不能给自己赠送灵石！", reference=True)
    limits = application.read_limits(sender, recipient)
    plan = handle_stone_gift(
        application,
        operation_id=_message_id(context.raw_event, context.user_id, "stone-gift"),
        sender_id=context.user_id,
        recipient_id=recipient_id,
        gross_amount=amount,
        **limits,
    )
    return plan


def _build_stone_history(context: CommandContext, application: Any) -> ReplyPlan:
    from ...features.stone_gift.history import (
        StoneGiftHistoryApplication,
        format_stone_gift_history,
    )

    summary = StoneGiftHistoryApplication(application.database).summary(context.user_id)
    return ReplyPlan(format_stone_gift_history(summary), reference=True)


def _build_bank_first_use(context: CommandContext, application: Any, args: Any, *, clock: Any, limit: int) -> ReplyPlan:
    from ...features.bank.commands import parse_first_use_deposit

    command = parse_first_use_deposit(
        user_id=context.user_id,
        text=_plain_text(args),
        operation_id=_message_id(context.raw_event, context.user_id, "bank-deposit-v2"),
        clock=clock,
        limit=limit,
    )
    result = application.deposit(**command.__dict__)
    messages = {
        "applied": f"灵庄新存款成功：存入 {result['deposited']} 枚，当前存款 {result['saved_stone']} 枚。",
        "duplicate": "该新存款请求已经处理，无需重复提交。",
        "stone_insufficient": "灵石不足，新存款未结算。",
        "limit_exceeded": "超过灵庄存储上限，新存款未结算。",
        "operation_conflict": "请求冲突，新存款未结算。",
        "user_missing": "未找到修仙数据。",
    }
    return ReplyPlan(messages.get(str(result.get("status")), "新存款未结算。"), reference=True)


def register_bank_first_use_matcher(driver: Any, holder: dict[str, Any], *, limit: int) -> Any:
    """Opt-in matcher for the new bank deposit path; legacy matcher is untouched."""
    marker = "_xiuxian_bank_first_use_matcher"
    existing = getattr(driver, marker, None)
    if existing is not None:
        return existing
    from nonebot import on_command
    from nonebot.params import CommandArg

    matcher = on_command("灵庄新存灵石", priority=1, block=True)

    @matcher.handle()
    async def _bank_first_use_handler(bot: Bot, event: Event, args: Message = CommandArg()):
        context = context_from_event(event, bot=bot)
        try:
            runtime = _runtime(holder)
            plan = _build_bank_first_use(context, runtime.services["bank_first_use"], args, clock=runtime.clock, limit=limit)
        except (MatcherException, ProcessException):
            raise
        except Exception as exc:
            plan = ReplyPlan(report_exception("灵庄新存灵石", exc), reference=True)
        return await _send(holder, event, bot, plan)

    setattr(driver, marker, matcher)
    return matcher


def register_bank_first_use_extended_matchers(driver: Any, holder: dict[str, Any]) -> tuple[Any, ...]:
    """Register only the explicitly enabled bank operation matchers."""
    marker = "_xiuxian_bank_first_use_extended_matchers"
    existing = getattr(driver, marker, None)
    if existing is not None:
        return existing
    from nonebot import on_command
    from nonebot.params import CommandArg

    context = holder.get("context")
    services = getattr(context, "services", {}) if context is not None else {}
    matchers: list[Any] = []

    async def dispatch(bot: Bot, event: Event, args: Any, service: str, parser: Callable[..., Any], action: str) -> Any:
        command_context = context_from_event(event, bot=bot)
        try:
            runtime = _runtime(holder)
            values = parser(
                user_id=command_context.user_id,
                text=_plain_text(args),
                operation_id=_message_id(command_context.raw_event, command_context.user_id, action),
                clock=runtime.clock,
            )
            result = getattr(runtime.services[service], action)(**values)
            labels = {
                "applied": "操作成功。",
                "duplicate": "该请求已经处理，无需重复提交。",
                "user_missing": "未找到修仙数据。",
                "saved_stone_insufficient": "灵庄存款不足。",
                "stone_insufficient": "灵石不足。",
                "operation_conflict": "请求冲突，请勿重复提交不同内容。",
            }
            operation = {"withdraw":"灵庄取款", "upgrade":"灵庄升级", "settle_interest":"灵庄结息"}.get(action, "灵庄操作")
            plan = ReplyPlan(labels.get(str(result.get("status"))) or result_failure(operation, result), reference=True)
        except (MatcherException, ProcessException):
            raise
        except Exception as exc:
            plan = ReplyPlan(report_exception("灵庄操作", exc), reference=True)
        return await _send(holder, event, bot, plan)

    if "bank_first_use_withdrawal" in services:
        withdraw = on_command("灵庄新取灵石", priority=1, block=True)

        @withdraw.handle()
        async def _withdraw_handler(bot: Bot, event: Event, args: Message = CommandArg()):
            from ...features.bank.commands import parse_first_use_deposit

            def parse(**kwargs: Any) -> dict[str, Any]:
                command = parse_first_use_deposit(limit=10**18, interest=0, bank_level="1", **kwargs)
                return {"operation_id": command.operation_id, "user_id": command.user_id, "amount": command.amount, "interest": 0, "bank_level": "1", "settled_at": command.settled_at}

            return await dispatch(bot, event, args, "bank_first_use_withdrawal", parse, "withdraw")

        matchers.append(withdraw)

    if "bank_first_use_upgrade" in services:
        upgrade = on_command("灵庄新升级", priority=1, block=True)

        @upgrade.handle()
        async def _upgrade_handler(bot: Bot, event: Event, args: Message = CommandArg()):
            from ...features.bank.commands import parse_first_use_upgrade

            return await dispatch(bot, event, args, "bank_first_use_upgrade", parse_first_use_upgrade, "upgrade")

        matchers.append(upgrade)

    if "bank_first_use_interest" in services:
        interest = on_command("灵庄新结息", priority=1, block=True)

        @interest.handle()
        async def _interest_handler(bot: Bot, event: Event, args: Message = CommandArg()):
            from ...features.bank.commands import parse_first_use_interest

            return await dispatch(bot, event, args, "bank_first_use_interest", parse_first_use_interest, "settle_interest")

        matchers.append(interest)

    result = tuple(matchers)
    setattr(driver, marker, result)
    return result


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
    sign_makeup = on_command(
        "修仙补签", aliases={"补签", "签到补签"}, priority=1, block=True
    )
    stone = on_command("送灵石", priority=1, block=True)
    stone_history = on_command(
        "我的赠送流水",
        aliases={"赠送流水", "灵石赠送流水"},
        priority=7,
        block=True,
    )

    @daily.handle()
    async def _daily_handler(bot: Bot, event: Event):
        return await _dispatch(holder, event, bot, "daily_fortune", _build_daily)

    @sign.handle()
    async def _sign_handler(bot: Bot, event: Event):
        return await _dispatch(holder, event, bot, "sign_in", _build_sign)

    @sign_makeup.handle()
    async def _sign_makeup_handler(
        bot: Bot, event: Event, args: Message = CommandArg()
    ):
        context = context_from_event(event, bot=bot)
        try:
            runtime = _runtime(holder)
            plan = _build_sign_makeup(
                context, runtime.services["sign_in"], args
            )
        except (MatcherException, ProcessException):
            raise
        except Exception as exc:
            plan = ReplyPlan(report_exception("补签", exc), reference=True)
        return await _send(holder, event, bot, plan)

    @stone.handle()
    async def _stone_handler(bot: Bot, event: Event, args: Message = CommandArg()):
        context = context_from_event(event, bot=bot)
        try:
            runtime = _runtime(holder)
            plan = _build_stone(context, runtime.services["stone_gift"], args)
        except (MatcherException, ProcessException):
            raise
        except DomainError as exc:
            if exc.code != "currency_precision_unavailable":
                raise
            plan = ReplyPlan(failure_reply("赠送灵石", exc), reference=True)
        except Exception as exc:
            plan = ReplyPlan(report_exception("赠送灵石", exc), reference=True)
        return await _send(holder, event, bot, plan)

    @stone_history.handle()
    async def _stone_history_handler(bot: Bot, event: Event):
        return await _dispatch(holder, event, bot, "stone_gift", _build_stone_history)

    result = (daily, sign, stone, stone_history)
    setattr(driver, marker, result)
    return result


__all__ = ["register_bank_first_use_extended_matchers", "register_bank_first_use_matcher", "register_migrated_matchers"]
