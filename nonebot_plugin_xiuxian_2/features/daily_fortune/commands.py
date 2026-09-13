from __future__ import annotations

from ...adapters.nonebot import CommandContext
from ...core.result import ReplyPlan
from .application import DailyFortuneApplication


def handle_daily_fortune(context: CommandContext, application: DailyFortuneApplication) -> ReplyPlan:
    operation_id = context.message_id or f"daily-fortune:{context.user_id}"
    return application.reply(user_id=context.user_id, operation_id=operation_id)


__all__ = ["handle_daily_fortune"]
