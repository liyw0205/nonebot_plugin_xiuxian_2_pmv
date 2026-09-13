from __future__ import annotations

from typing import Any

from ...core.result import ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork
from .application import SignInApplication
from .clock import sign_in_clock
from .lottery_snapshot import LotterySnapshot
from .lottery_application import LotteryApplication
from .lottery_repository import LotteryRepository


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


def format_lottery_result(result: Any, number_to: Any) -> str:
    if result.status == "operation_conflict":
        return "鸿运结算记录冲突，请联系管理员处理。"
    if result.status == "user_missing":
        return "未找到修仙存档，本次鸿运未结算。"
    if result.status == "already_participated" and not result.lottery_number:
        return "本期鸿运已经参与，奖池继续累积~"
    if result.prize_tier == "grand":
        return f"✨鸿运当头！恭喜道友获得特等奖！\n中奖号码：{result.lottery_number}\n获得奖池中{number_to(result.prize)}灵石！🎉🎉🎉"
    prize_names = {"first": "一等奖", "second": "二等奖", "third": "三等奖"}
    if result.prize_tier in prize_names:
        return f"🎉恭喜道友获得{prize_names[result.prize_tier]}！\n中奖号码：{result.lottery_number}\n获得奖池的{number_to(result.prize)}灵石！🎉"
    return "本次签到未中奖，奖池继续累积~"


def settle_lottery(
    database: str,
    *,
    user_id: str,
    user_name: str,
    operation_id: str,
    legacy_settle: Any,
) -> Any:
    occurred_at = sign_in_clock().now()
    business_date = occurred_at.date().isoformat()
    with DatabaseUnitOfWork(database) as uow:
        migrated = LotteryRepository.schema_exists(uow)
    if migrated:
        return LotteryApplication(database).settle(
            operation_id=operation_id,
            user_id=user_id,
            user_name=user_name,
            business_date=business_date,
            occurred_at=occurred_at,
        )
    return legacy_settle(
        operation_id,
        user_id,
        user_name,
        business_date,
        occurred_at=occurred_at,
    )


__all__ = ["format_lottery_result", "format_lottery_snapshot", "handle_sign_in", "settle_lottery"]
