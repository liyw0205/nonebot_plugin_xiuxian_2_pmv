from __future__ import annotations

from typing import Any

from ...core.result import ReplyPlan
from .application import SignInApplication
from .lottery_snapshot import LotterySnapshot


def handle_sign_in(application: SignInApplication, *, user_id: str, operation_id: str, lower_limit: int, upper_limit: int) -> ReplyPlan:
    return application.reply(
        user_id=user_id,
        operation_id=operation_id,
        lower_limit=lower_limit,
        upper_limit=upper_limit,
    )


def format_lottery_snapshot(snapshot: LotterySnapshot, number_to: Any) -> str:
    message = "**鸿运当头**\n---\n"
    message += f"当前奖池累计\n> {number_to(snapshot.pool)}灵石\n"
    message += f"本期参与人数\n> {snapshot.participants}位道友\n\n"
    if snapshot.last_winner:
        winner = snapshot.last_winner
        message += "**上期中奖记录**\n"
        message += f"中奖道友\n> {winner.user_name}\n"
        message += f"中奖时间\n> {winner.won_at}\n"
        message += f"中奖金额\n> {number_to(winner.prize)}灵石\n"
    else:
        message += "暂无历史中奖记录，道友快来签到吧！\n"
    return message + "\n> 每次签到自动存入100万灵石到奖池，中奖号码将独享全部奖池！"


__all__ = ["format_lottery_snapshot", "handle_sign_in"]
